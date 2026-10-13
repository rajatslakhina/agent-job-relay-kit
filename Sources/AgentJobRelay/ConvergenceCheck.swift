/// One way a device's projection disagrees with the server's log.
public enum Divergence: Hashable, Sendable, CustomStringConvertible {
    case appliedThrough(device: UInt64, server: UInt64)
    case phase(device: Phase?, server: Phase?)
    case tokens(device: Int, server: Int)
    case terminal(device: Terminal?, server: Terminal?)
    case artifactMissing(id: String)
    case artifactUnexpected(id: String)
    case artifactBody(id: String)
    case artifactConfidence(id: String, device: Artifact.Confidence, server: Artifact.Confidence)
    case artifactOrder

    public var description: String {
        switch self {
        case let .appliedThrough(device, server): return "applied through \(device), server log has \(server)"
        case let .phase(device, server): return "phase \(device?.name ?? "none") vs server \(server?.name ?? "none")"
        case let .tokens(device, server): return "tokens \(device) vs server \(server)"
        case let .terminal(device, server): return "terminal \(String(describing: device)) vs \(String(describing: server))"
        case let .artifactMissing(id): return "artifact \(id) missing on device"
        case let .artifactUnexpected(id): return "artifact \(id) not in server log"
        case let .artifactBody(id): return "artifact \(id) body differs"
        case let .artifactConfidence(id, device, server): return "artifact \(id) is \(device.rawValue), server says \(server.rawValue)"
        case .artifactOrder: return "artifact order differs"
        }
    }
}

/// Checks a device's `JobState` against the server's authoritative log.
///
/// The expected state is computed by an **independent** fold over the log — sort, take the
/// dense prefix, last-writer-wins per artifact — rather than by running `JobProjection`
/// again. Re-using the reducer under test as its own oracle would make this check pass
/// for any bug the reducer has; the test suite feeds deliberately broken reducers through
/// this function and asserts it reports them.
public enum ConvergenceCheck {
    public static func expectedState(jobID: JobID, serverLog: [JobEvent]) -> JobState {
        var state = JobState(jobID: jobID)
        var bySeq: [UInt64: JobEvent] = [:]
        for event in serverLog where event.jobID == jobID && event.seq > 0 {
            bySeq[event.seq] = bySeq[event.seq] ?? event
        }
        var seq: UInt64 = 1
        var order: [String] = []
        var latest: [String: ArtifactFragment] = [:]
        var finals: Set<String> = []
        while let event = bySeq[seq] {
            state.appliedThrough = seq
            switch event.payload {
            case .phase(let phase):
                state.phase = max(state.phase ?? phase, phase)
            case .usage(let tokens):
                state.tokensUsed = Saturating.add(state.tokensUsed, max(0, tokens))
            case .fragment(let fragment):
                if latest[fragment.artifactID] == nil { order.append(fragment.artifactID) }
                if !finals.contains(fragment.artifactID),
                   fragment.revision > (latest[fragment.artifactID]?.revision ?? Int.min) {
                    latest[fragment.artifactID] = fragment
                    if fragment.isFinal { finals.insert(fragment.artifactID) }
                }
            case .terminal(let terminal):
                state.terminal = terminal
            }
            if state.terminal != nil || seq == UInt64.max { break }
            seq += 1
        }
        state.artifacts = order.compactMap { id in
            guard let fragment = latest[id] else { return nil }
            let confidence: Artifact.Confidence
            if fragment.isFinal {
                confidence = .final
            } else if let terminal = state.terminal, terminal != .completed {
                confidence = .unconfirmed
            } else {
                confidence = .provisional
            }
            return Artifact(id: id, revision: fragment.revision, body: fragment.body, confidence: confidence)
        }
        return state
    }

    /// Every way `device` differs from what the server log says. Empty means converged.
    public static func divergences(device: JobState, serverLog: [JobEvent]) -> [Divergence] {
        let expected = expectedState(jobID: device.jobID, serverLog: serverLog)
        var result: [Divergence] = []
        if device.appliedThrough != expected.appliedThrough {
            result.append(.appliedThrough(device: device.appliedThrough, server: expected.appliedThrough))
        }
        if device.phase != expected.phase { result.append(.phase(device: device.phase, server: expected.phase)) }
        if device.tokensUsed != expected.tokensUsed {
            result.append(.tokens(device: device.tokensUsed, server: expected.tokensUsed))
        }
        if device.terminal != expected.terminal {
            result.append(.terminal(device: device.terminal, server: expected.terminal))
        }
        let deviceByID = Dictionary(device.artifacts.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
        let expectedByID = Dictionary(expected.artifacts.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
        for artifact in expected.artifacts {
            guard let mine = deviceByID[artifact.id] else {
                result.append(.artifactMissing(id: artifact.id))
                continue
            }
            if mine.body != artifact.body || mine.revision != artifact.revision {
                result.append(.artifactBody(id: artifact.id))
            }
            if mine.confidence != artifact.confidence {
                result.append(.artifactConfidence(id: artifact.id, device: mine.confidence, server: artifact.confidence))
            }
        }
        for artifact in device.artifacts where expectedByID[artifact.id] == nil {
            result.append(.artifactUnexpected(id: artifact.id))
        }
        if result.isEmpty, device.artifacts.map(\.id) != expected.artifacts.map(\.id) {
            result.append(.artifactOrder)
        }
        return result
    }
}

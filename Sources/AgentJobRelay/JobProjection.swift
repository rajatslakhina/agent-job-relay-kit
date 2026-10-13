/// A result as the device currently believes it to be.
public struct Artifact: Hashable, Sendable, Codable {
    public enum Confidence: String, Hashable, Sendable, Codable {
        /// The agent may still revise this body.
        case provisional
        /// The agent declared this body final.
        case final
        /// The job ended (failed or cancelled) while this body was still provisional.
        /// It is shown, but never presented as the agent's answer.
        case unconfirmed
    }

    public let id: String
    public internal(set) var revision: Int
    public internal(set) var body: String
    public internal(set) var confidence: Confidence
}

/// The comparable, persistable part of a projection: what the user is shown.
public struct JobState: Hashable, Sendable, Codable {
    public internal(set) var jobID: JobID
    /// Every event with `seq <= appliedThrough` has been applied, in order, exactly once.
    public internal(set) var appliedThrough: UInt64 = 0
    public internal(set) var phase: Phase?
    /// Artifacts in the order the job first produced them.
    public internal(set) var artifacts: [Artifact] = []
    public internal(set) var tokensUsed: Int = 0
    public internal(set) var terminal: Terminal?

    public init(jobID: JobID) { self.jobID = jobID }
}

/// Where every received event ended up. The counters obey a conservation law
/// (`isConserved`) that the tests and the demo both check.
public struct DeliveryLedger: Hashable, Sendable, Codable {
    public internal(set) var received = 0
    public internal(set) var applied = 0
    public internal(set) var duplicates = 0
    public internal(set) var discardedBeyondWindow = 0
    public internal(set) var afterTerminal = 0
    public internal(set) var wrongJob = 0
    /// Events that arrived ahead of a gap and were applied later from the reorder buffer.
    public internal(set) var reordered = 0
    /// Defensive counters for a misbehaving backend. Both stay 0 against a correct server.
    public internal(set) var phaseRegressions = 0
    public internal(set) var staleRevisions = 0
    public internal(set) var receivedByChannel: [Channel: Int] = [:]
    public internal(set) var appliedByChannel: [Channel: Int] = [:]

    /// `received == applied + duplicates + buffered + discardedBeyondWindow + afterTerminal + wrongJob`.
    public func isConserved(buffered: Int) -> Bool {
        var total = applied
        for part in [duplicates, buffered, discardedBeyondWindow, afterTerminal, wrongJob] {
            total = Saturating.add(total, part)
        }
        return total == received
    }
}

/// What a single `ingest` did, so the relay can decide whether to backfill.
public enum IngestOutcome: Hashable, Sendable {
    /// The event (and `drained` buffered successors) were applied.
    case applied(drained: Int)
    case duplicate
    /// Held in the reorder buffer; `missing` is the hole in front of it.
    case buffered(missing: ClosedRange<UInt64>)
    /// Too far ahead to buffer without unbounded memory. Dropped; fetch `missing` instead.
    case beyondWindow(missing: ClosedRange<UInt64>)
    case afterTerminal
    case wrongJob
}

/// The on-device projection of one agent job: a pure, synchronous reducer that turns
/// at-least-once, out-of-order delivery into exactly-once, in-order application.
///
/// Guarantees (each has a test, and a test that a broken reducer violates it):
/// 1. **Ordered**: events apply strictly in `seq` order, so a "completed" badge is never
///    shown over a hole in the log.
/// 2. **Exactly once**: a redelivered `seq` changes nothing (token usage is not double-counted).
/// 3. **Bounded**: at most `reorderWindow` events are buffered ahead of a gap.
/// 4. **Absorbing terminal**: nothing after the terminal event changes the state.
public struct JobProjection: Hashable, Sendable {
    public private(set) var state: JobState
    public private(set) var ledger = DeliveryLedger()
    /// Set locally the moment the user asks to cancel; the server still decides the outcome.
    public private(set) var cancelRequested = false
    public let reorderWindow: Int
    /// Highest `seq` this device has evidence exists (from any event or push hint).
    public private(set) var knownHighWater: UInt64 = 0

    /// Events held ahead of a gap, with the channel they actually arrived on.
    private var pending: [UInt64: BufferedEvent] = [:]

    private struct BufferedEvent: Hashable, Sendable {
        let event: JobEvent
        let channel: Channel
    }

    public init(jobID: JobID, reorderWindow: Int = 64) {
        self.state = JobState(jobID: jobID)
        // A window of 0 would make every out-of-order event a refetch; 1 is the floor.
        self.reorderWindow = max(1, reorderWindow)
    }

    /// Restores from a checkpoint. The reorder buffer is deliberately not persisted:
    /// anything in it is re-fetchable from `appliedThrough`, and persisting it would
    /// let a crash replay a half-validated buffer.
    public init(restoring state: JobState, cancelRequested: Bool, reorderWindow: Int = 64) {
        self.state = state
        self.cancelRequested = cancelRequested
        self.reorderWindow = max(1, reorderWindow)
        self.knownHighWater = state.appliedThrough
    }

    public var bufferedCount: Int { pending.count }
    public var isTerminal: Bool { state.terminal != nil }

    /// `true` when the device knows events exist that it has not applied yet.
    public var isBehind: Bool { !isTerminal && knownHighWater > state.appliedThrough }

    public var status: JobStatus {
        if let terminal = state.terminal {
            switch terminal {
            case .completed: return cancelRequested ? .completedBeforeCancel : .completed
            case .failed(let reason): return .failed(reason: reason)
            case .cancelled: return .cancelled
            }
        }
        if cancelRequested { return .cancelling }
        return .running(state.phase)
    }

    public mutating func requestCancel() {
        cancelRequested = true
    }

    /// Records out-of-band evidence (e.g. a silent push) that the log reaches `seq`.
    public mutating func noteHighWater(_ seq: UInt64) {
        knownHighWater = max(knownHighWater, seq)
    }

    @discardableResult
    public mutating func ingest(_ event: JobEvent, via channel: Channel) -> IngestOutcome {
        Saturating.increment(&ledger.received)
        ledger.receivedByChannel[channel, default: 0] = Saturating.add(ledger.receivedByChannel[channel, default: 0], 1)

        guard event.jobID == state.jobID else {
            Saturating.increment(&ledger.wrongJob)
            return .wrongJob
        }
        // Server contract: seq is 1-based. A 0 can never be applied; treat it as a duplicate
        // of the empty prefix rather than trapping on `seq - 1` below.
        if event.seq <= state.appliedThrough || event.seq == 0 {
            Saturating.increment(&ledger.duplicates)
            return .duplicate
        }
        if isTerminal {
            Saturating.increment(&ledger.afterTerminal)
            return .afterTerminal
        }
        noteHighWater(event.seq)

        let next = state.appliedThrough + 1 // cannot overflow: appliedThrough < event.seq <= UInt64.max
        if event.seq == next {
            apply(event, via: channel)
            let drained = drainBuffer()
            return .applied(drained: drained)
        }

        let missing = next...(event.seq - 1) // event.seq > next here, so the range is non-empty
        if pending[event.seq] != nil {
            Saturating.increment(&ledger.duplicates)
            return .buffered(missing: missing)
        }
        // Bufferable seqs are next+1 ... next+reorderWindow, so the buffer never holds more
        // than `reorderWindow` events. `event.seq > next` here, so the subtraction is safe.
        let slotsAhead = event.seq - next
        guard slotsAhead <= UInt64(reorderWindow) else {
            Saturating.increment(&ledger.discardedBeyondWindow)
            return .beyondWindow(missing: missing)
        }
        pending[event.seq] = BufferedEvent(event: event, channel: channel)
        return .buffered(missing: missing)
    }

    /// The first contiguous hole between what is applied and what is known to exist.
    public var firstGap: ClosedRange<UInt64>? {
        // isBehind implies knownHighWater > appliedThrough, so `+ 1` cannot overflow.
        guard isBehind else { return nil }
        let start = state.appliedThrough + 1
        var end = knownHighWater
        if let lowestBuffered = pending.keys.min(), lowestBuffered > start {
            end = lowestBuffered - 1
        }
        return start <= end ? start...end : nil
    }

    // MARK: - Private

    private mutating func drainBuffer() -> Int {
        var drained = 0
        // `appliedThrough < UInt64.max` keeps `+ 1` from trapping after a pathological
        // event whose seq is UInt64.max has been applied.
        while !isTerminal, state.appliedThrough < UInt64.max,
              let buffered = pending.removeValue(forKey: state.appliedThrough + 1) {
            Saturating.increment(&ledger.reordered)
            apply(buffered.event, via: buffered.channel)
            drained += 1
        }
        if isTerminal, !pending.isEmpty {
            // Anything buffered past the terminal event is, by contract, impossible;
            // account for it instead of silently forgetting it.
            ledger.afterTerminal = Saturating.add(ledger.afterTerminal, pending.count)
            pending.removeAll()
        }
        return drained
    }

    private mutating func apply(_ event: JobEvent, via channel: Channel) {
        state.appliedThrough = event.seq
        Saturating.increment(&ledger.applied)
        ledger.appliedByChannel[channel, default: 0] = Saturating.add(ledger.appliedByChannel[channel, default: 0], 1)

        switch event.payload {
        case .phase(let phase):
            if let current = state.phase, phase < current {
                Saturating.increment(&ledger.phaseRegressions)
            } else {
                state.phase = phase
            }
        case .fragment(let fragment):
            applyFragment(fragment)
        case .usage(let tokens):
            // Negative usage is a backend bug; it must never make spend go down.
            state.tokensUsed = Saturating.add(state.tokensUsed, max(0, tokens))
        case .terminal(let terminal):
            state.terminal = terminal
            if terminal != .completed {
                for index in state.artifacts.indices where state.artifacts[index].confidence == .provisional {
                    state.artifacts[index].confidence = .unconfirmed
                }
            }
        }
    }

    private mutating func applyFragment(_ fragment: ArtifactFragment) {
        let confidence: Artifact.Confidence = fragment.isFinal ? .final : .provisional
        guard let index = state.artifacts.firstIndex(where: { $0.id == fragment.artifactID }) else {
            state.artifacts.append(Artifact(id: fragment.artifactID, revision: fragment.revision,
                                            body: fragment.body, confidence: confidence))
            return
        }
        let existing = state.artifacts[index]
        guard existing.confidence != .final, fragment.revision > existing.revision else {
            Saturating.increment(&ledger.staleRevisions)
            return
        }
        state.artifacts[index].revision = fragment.revision
        state.artifacts[index].body = fragment.body
        state.artifacts[index].confidence = confidence
    }
}

/// What the UI renders as the job's headline status.
public enum JobStatus: Hashable, Sendable {
    case running(Phase?)
    case cancelling
    case completed
    /// The user asked to cancel, but the server finished first. Shown honestly, not as "cancelled".
    case completedBeforeCancel
    case failed(reason: String)
    case cancelled
}

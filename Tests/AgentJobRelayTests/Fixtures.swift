@testable import AgentJobRelay

enum Fixture {
    static let job = JobID("job-1")

    /// The full scripted log a successful research job produces (18 events, 8,870 tokens).
    static func completedLog(_ id: JobID = job) -> [JobEvent] {
        log(id, failing: false)
    }

    /// A job that fails in its research phase (11 events, 5,270 tokens).
    static func failedLog(_ id: JobID = job) -> [JobEvent] {
        log(id, failing: true)
    }

    static func log(_ id: JobID, failing: Bool) -> [JobEvent] {
        let script = ScriptedAgentServer.script(for: JobRequest(prompt: "fixture", tokenBudget: 0), failing: failing)
        return script.enumerated().map { JobEvent(jobID: id, seq: UInt64($0.offset + 1), payload: $0.element) }
    }

    static func usage(_ seq: UInt64, _ tokens: Int, _ id: JobID = job) -> JobEvent {
        JobEvent(jobID: id, seq: seq, payload: .usage(tokens: tokens))
    }

    static func fragment(_ seq: UInt64, _ artifact: String, rev: Int, body: String, final: Bool = false) -> JobEvent {
        JobEvent(jobID: job, seq: seq, payload: .fragment(ArtifactFragment(artifactID: artifact, revision: rev, body: body, isFinal: final)))
    }

    static func terminal(_ seq: UInt64, _ terminal: Terminal) -> JobEvent {
        JobEvent(jobID: job, seq: seq, payload: .terminal(terminal))
    }

    /// A seeded at-least-once, out-of-order delivery of `log`: every event appears at least
    /// once, some twice, in shuffled order.
    static func chaoticDelivery(of log: [JobEvent], seed: UInt64) -> [JobEvent] {
        var rng = SplitMix64(seed: seed)
        var delivery = log
        for event in log where rng.unit() < 0.4 { delivery.append(event) }
        delivery.shuffle(using: &rng)
        return delivery
    }
}

/// A deliberately broken reducer: applies events in arrival order, deduplicating by seq.
/// It is what most "just append what the stream sends" clients do.
struct ArrivalOrderReducer {
    var state: JobState
    private var seen: Set<UInt64> = []
    init(jobID: JobID) { state = JobState(jobID: jobID) }

    mutating func ingest(_ event: JobEvent) {
        guard seen.insert(event.seq).inserted else { return }
        state.appliedThrough = max(state.appliedThrough, event.seq)
        switch event.payload {
        case .phase(let phase): state.phase = phase
        case .usage(let tokens): state.tokensUsed += tokens
        case .terminal(let terminal): state.terminal = terminal
        case .fragment(let fragment):
            let artifact = Artifact(id: fragment.artifactID, revision: fragment.revision, body: fragment.body,
                                    confidence: fragment.isFinal ? .final : .provisional)
            if let index = state.artifacts.firstIndex(where: { $0.id == fragment.artifactID }) {
                state.artifacts[index] = artifact
            } else {
                state.artifacts.append(artifact)
            }
        }
    }
}

/// A second broken reducer: in order, but with no deduplication.
struct NoDedupReducer {
    var state: JobState
    init(jobID: JobID) { state = JobState(jobID: jobID) }

    mutating func ingest(_ event: JobEvent) {
        if case .usage(let tokens) = event.payload { state.tokensUsed += tokens }
        state.appliedThrough = max(state.appliedThrough, event.seq)
    }
}

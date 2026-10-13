/// Deterministic PRNG (SplitMix64) so every simulated failure is reproducible from a seed.
public struct SplitMix64: RandomNumberGenerator, Sendable, Hashable {
    private var state: UInt64
    public init(seed: UInt64) { state = seed }
    public mutating func next() -> UInt64 {
        state &+= 0x9E37_79B9_7F4A_7C15
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }
    /// Uniform in [0, 1) from the top 53 bits.
    public mutating func unit() -> Double { Double(next() >> 11) / Double(UInt64(1) << 53) }
}

public struct SimulatedTransportError: Error, Hashable, Sendable {
    public let message: String
}

/// A stand-in for the agent backend: it owns each job's authoritative event log and
/// releases it a few events at a time, the way a real multi-phase agent run would.
///
/// Fault injection is explicit and counted, so tests can assert on *what the server saw*
/// (e.g. four submit calls, three jobs created) rather than on what the client claims.
public actor ScriptedAgentServer: JobTransport {
    public struct Stats: Hashable, Sendable {
        public internal(set) var submitCalls = 0
        public internal(set) var jobsCreated = 0
        public internal(set) var lostAcks = 0
        public internal(set) var fetchCalls = 0
        public internal(set) var cancelCalls = 0
    }

    private struct Job {
        let script: [JobEvent.Payload]
        var log: [JobEvent] = []
        var cancelRequested = false
        var isTerminal: Bool { log.last.map { if case .terminal = $0.payload { true } else { false } } ?? false }
    }

    public private(set) var stats = Stats()
    private var jobs: [JobID: Job] = [:]
    private var byKey: [IdempotencyKey: JobID] = [:]
    private var lostAcksRemaining: Int
    private let failingJobNumbers: Set<Int>

    /// - Parameters:
    ///   - lostAcks: the first N successful submits create the job and then throw, as if the
    ///     response was lost on a dying connection. This is the case idempotency keys exist for.
    ///   - failingJobNumbers: 1-based job numbers whose script ends in `.failed`.
    public init(lostAcks: Int = 0, failingJobNumbers: Set<Int> = []) {
        self.lostAcksRemaining = max(0, lostAcks)
        self.failingJobNumbers = failingJobNumbers
    }

    public func submit(_ request: JobRequest, key: IdempotencyKey) async throws -> JobID {
        stats.submitCalls += 1
        let id: JobID
        if let existing = byKey[key] {
            id = existing
        } else {
            stats.jobsCreated += 1
            id = JobID("job-\(stats.jobsCreated)")
            byKey[key] = id
            jobs[id] = Job(script: Self.script(for: request, failing: failingJobNumbers.contains(stats.jobsCreated)))
        }
        if lostAcksRemaining > 0 {
            lostAcksRemaining -= 1
            stats.lostAcks += 1
            throw SimulatedTransportError(message: "connection reset after the server accepted \(id)")
        }
        return id
    }

    public func fetchEvents(_ job: JobID, after seq: UInt64, limit: Int) async throws -> [JobEvent] {
        stats.fetchCalls += 1
        guard let log = jobs[job]?.log else { return [] }
        return Array(log.lazy.filter { $0.seq > seq }.prefix(max(0, limit)))
    }

    public func cancel(_ job: JobID) async throws {
        stats.cancelCalls += 1
        jobs[job]?.cancelRequested = true
    }

    /// Releases up to `count` more events of `job`'s script and returns them.
    /// A pending cancel replaces the rest of the script with one `.cancelled` terminal.
    @discardableResult
    public func advance(_ id: JobID, by count: Int) -> [JobEvent] {
        guard var job = jobs[id], !job.isTerminal else { return [] }
        var released: [JobEvent] = []
        for _ in 0..<max(0, count) {
            let seq = UInt64(job.log.count + 1)
            let payload: JobEvent.Payload
            if job.cancelRequested {
                payload = .terminal(.cancelled)
            } else if job.log.count < job.script.count {
                payload = job.script[job.log.count]
            } else {
                break
            }
            let event = JobEvent(jobID: id, seq: seq, payload: payload)
            job.log.append(event)
            released.append(event)
            if job.isTerminal { break }
        }
        jobs[id] = job
        return released
    }

    public func log(of id: JobID) -> [JobEvent] { jobs[id]?.log ?? [] }
    public func highWater(of id: JobID) -> UInt64 { UInt64(jobs[id]?.log.count ?? 0) }
    public func isTerminal(_ id: JobID) -> Bool { jobs[id]?.isTerminal ?? false }
    public func jobIDs() -> [JobID] { jobs.keys.sorted { $0.rawValue < $1.rawValue } }

    /// A four-phase research job: plan → findings → answer, with usage reported as it goes.
    static func script(for request: JobRequest, failing: Bool) -> [JobEvent.Payload] {
        let topic = request.prompt.isEmpty ? "the request" : request.prompt
        let phases = ["Planning", "Researching", "Drafting", "Reviewing"].enumerated().map {
            Phase(ordinal: $0.offset, name: $0.element)
        }
        func fragment(_ id: String, _ revision: Int, _ body: String, final: Bool = false) -> JobEvent.Payload {
            .fragment(ArtifactFragment(artifactID: id, revision: revision, body: body, isFinal: final))
        }
        var script: [JobEvent.Payload] = [
            .phase(phases[0]),
            .usage(tokens: 420),
            fragment("plan", 1, "Plan: scope \(topic)."),
            fragment("plan", 2, "Plan: scope \(topic); list three sources; draft; self-review.", final: true),
            .phase(phases[1]),
            .usage(tokens: 1_850),
            fragment("findings", 1, "Findings: 1 source read."),
            .usage(tokens: 2_100),
            fragment("findings", 2, "Findings: 2 sources read; one contradicts the other."),
        ]
        if failing {
            script += [.usage(tokens: 900), .terminal(.failed(reason: "Tool call timed out: source 3 unreachable"))]
            return script
        }
        script += [
            fragment("findings", 3, "Findings: 3 sources read; contradiction resolved by the newest primary source.", final: true),
            .phase(phases[2]),
            .usage(tokens: 3_300),
            fragment("answer", 1, "Answer (draft 1) on \(topic)."),
            fragment("answer", 2, "Answer (draft 2) on \(topic), citing all three sources."),
            .phase(phases[3]),
            .usage(tokens: 1_200),
            fragment("answer", 3, "Answer on \(topic), citing all three sources; caveat on the disputed figure.", final: true),
            .terminal(.completed),
        ]
        return script
    }
}

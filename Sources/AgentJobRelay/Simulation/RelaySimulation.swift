import Foundation

/// One job as the demo (and the end-to-end tests) see it: the device's projection next to
/// the server's truth.
public struct SimulatedJob: Hashable, Sendable, Identifiable {
    public var id: JobID { projection.state.jobID }
    public let prompt: String
    public let tokenBudget: Int
    public let projection: JobProjection
    public let serverHighWater: UInt64
    public let serverTerminal: Bool
    /// Empty when the device's state equals an independent fold of the server log.
    public let divergences: [Divergence]

    /// How many server events the device has not applied yet.
    public var lag: UInt64 { Saturating.distance(from: projection.state.appliedThrough, to: serverHighWater) }
    public var ledgerConserved: Bool { projection.ledger.isConserved(buffered: projection.bufferedCount) }
    public var converged: Bool { divergences.isEmpty }
}

public struct SimulationSnapshot: Hashable, Sendable {
    public let tick: Int
    public let profile: ChaosProfile
    public let jobs: [SimulatedJob]
    public let health: RelayHealth
    public let server: ScriptedAgentServer.Stats
    public let pendingSubmissions: Int
    public let relaunches: Int
    public let isFinished: Bool

    public var allConverged: Bool { !jobs.isEmpty && jobs.allSatisfy(\.converged) }
    public var allConserved: Bool { jobs.allSatisfy(\.ledgerConserved) }
}

/// A reproducible end-to-end run: a scripted agent backend, a chaotic stream, periodic
/// silent-push hints, and a `JobRelay` that may be killed and relaunched mid-run.
/// The demo app drives this tick by tick; the test suite runs it to completion across
/// every chaos profile and many seeds.
public actor RelaySimulation {
    public struct Plan: Hashable, Sendable {
        public let prompt: String
        /// 0 disables the client-side ceiling.
        public let tokenBudget: Int
        public init(prompt: String, tokenBudget: Int = 0) {
            self.prompt = prompt
            self.tokenBudget = tokenBudget
        }
    }

    public static let defaultPlans: [Plan] = [
        Plan(prompt: "on-device vs cloud inference cost"),
        Plan(prompt: "SwiftData migration risks"),
        Plan(prompt: "App Store review timelines", tokenBudget: 5_000),
    ]

    private let server: ScriptedAgentServer
    private let store = InMemoryRelayStore()
    private let policy: RelayPolicy
    private let plans: [Plan]
    private let eventsPerTick: Int
    private let hintEvery: Int
    private var link: ChaosLink
    private var relay: JobRelay
    private var tick = 0
    private var relaunches = 0
    private var started = false
    private let keyCounter = KeyCounter()

    // FIFO async lock. Every public operation runs to completion before the next starts,
    // so a kill-and-relaunch can never land between the awaits of a half-finished step
    // (actor isolation alone does not prevent that interleaving).
    private var busy = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    private func acquire() async {
        if busy {
            await withCheckedContinuation { waiters.append($0) }
        } else {
            busy = true
        }
    }

    private func release() {
        if waiters.isEmpty {
            busy = false
        } else {
            waiters.removeFirst().resume() // hand the lock straight to the next waiter
        }
    }

    /// Deterministic idempotency keys so a seeded run is byte-for-byte reproducible.
    private final class KeyCounter: @unchecked Sendable {
        // `makeKey` runs on whichever relay instance is current (a different actor after a
        // relaunch), so the counter is shared across isolation domains and guarded by a lock.
        // That is the whole reason for `@unchecked Sendable`.
        private let lock = NSLock()
        private var value = 0
        func next() -> IdempotencyKey {
            lock.lock()
            defer { lock.unlock() }
            value += 1
            return IdempotencyKey("sim-key-\(value)")
        }
    }

    public init(profile: ChaosProfile, seed: UInt64, plans: [Plan] = RelaySimulation.defaultPlans,
                lostAcks: Int = 1, failingJobNumbers: Set<Int> = [2],
                eventsPerTick: Int = 2, hintEvery: Int = 4, policy: RelayPolicy = RelayPolicy(reorderWindow: 8, pageSize: 6)) {
        self.server = ScriptedAgentServer(lostAcks: lostAcks, failingJobNumbers: failingJobNumbers)
        self.policy = policy
        self.plans = plans
        self.eventsPerTick = max(1, eventsPerTick)
        self.hintEvery = max(1, hintEvery)
        self.link = ChaosLink(profile: profile, seed: seed)
        let counter = keyCounter
        self.relay = Self.makeRelay(server: server, store: store, policy: policy, counter: counter)
    }

    private static func makeRelay(server: ScriptedAgentServer, store: InMemoryRelayStore,
                                  policy: RelayPolicy, counter: KeyCounter) -> JobRelay {
        JobRelay(transport: server, store: store, policy: policy,
                 makeKey: { counter.next() },
                 sleep: { _ in }, // backoff is exercised in unit tests; the demo should not stall
                 randomUnit: { 0.5 })
    }

    /// Submits every planned job once. Safe to call repeatedly.
    public func start() async {
        await acquire()
        defer { release() }
        await startLocked()
    }

    private func startLocked() async {
        guard !started else { return }
        started = true
        for plan in plans {
            _ = try? await relay.submit(JobRequest(prompt: plan.prompt, tokenBudget: plan.tokenBudget))
        }
    }

    /// One tick: the server releases events, the stream mangles them, the relay ingests;
    /// every `hintEvery` ticks a silent push carries each job's high-water mark.
    public func step() async {
        await acquire()
        defer { release() }
        await stepLocked()
    }

    private func stepLocked() async {
        await startLocked()
        tick += 1
        var released: [JobEvent] = []
        for id in await server.jobIDs() {
            released += await server.advance(id, by: eventsPerTick)
        }
        let delivered = link.transmit(released)
        await relay.receive(delivered, via: .stream)
        if tick % hintEvery == 0 { await pushHints() }
    }

    /// Runs until the server has finished every job, then flushes the stream and sends the
    /// final "job finished" push — the moment a real backend would notify the device.
    public func runToCompletion(maxTicks: Int = 500) async {
        await acquire()
        defer { release() }
        await startLocked()
        var remaining = max(0, maxTicks)
        while remaining > 0, await !serverFinished() {
            await stepLocked()
            remaining -= 1
        }
        await finishLocked()
    }

    /// Flushes in-flight stream events and sends a final hint for every job.
    public func finish() async {
        await acquire()
        defer { release() }
        await finishLocked()
    }

    private func finishLocked() async {
        await relay.receive(link.drain(), via: .stream)
        await pushHints()
    }

    /// Simulates the OS killing the app: the in-memory relay and every in-flight stream
    /// event are lost; a fresh relay restores from the store and resumes.
    public func killAndRelaunch() async {
        await acquire()
        defer { release() }
        _ = link.drain()
        await relay.retire() // a dead process sends nothing more; its saves are also fenced by epoch
        relay = Self.makeRelay(server: server, store: store, policy: policy, counter: keyCounter)
        relaunches += 1
        await relay.restore()
        await relay.resume()
    }

    public func cancel(_ id: JobID) async {
        await acquire()
        defer { release() }
        await relay.requestCancel(id)
    }

    public func snapshot() async -> SimulationSnapshot {
        await acquire()
        defer { release() }
        var rows: [SimulatedJob] = []
        let projections = await relay.allProjections()
        for projection in projections {
            let id = projection.state.jobID
            let request = await relay.request(of: id)
            let log = await server.log(of: id)
            rows.append(SimulatedJob(
                prompt: request?.prompt ?? "",
                tokenBudget: request?.tokenBudget ?? 0,
                projection: projection,
                serverHighWater: await server.highWater(of: id),
                serverTerminal: await server.isTerminal(id),
                divergences: ConvergenceCheck.divergences(device: projection.state, serverLog: log)))
        }
        let finished = await serverFinished() && rows.allSatisfy { $0.projection.isTerminal }
        return SimulationSnapshot(tick: tick, profile: link.profile, jobs: rows,
                                  health: await relay.health, server: await server.stats,
                                  pendingSubmissions: await relay.pendingSubmissions().count,
                                  relaunches: relaunches, isFinished: finished)
    }

    private func pushHints() async {
        for id in await server.jobIDs() {
            await relay.receiveHint(id, highWater: await server.highWater(of: id))
        }
    }

    private func serverFinished() async -> Bool {
        let ids = await server.jobIDs()
        for id in ids where await !server.isTerminal(id) { return false }
        return !ids.isEmpty
    }
}

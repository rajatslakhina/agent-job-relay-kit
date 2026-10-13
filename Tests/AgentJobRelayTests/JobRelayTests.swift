import XCTest
@testable import AgentJobRelay

/// Records every key and every backoff delay it is asked about; fails the first N submits.
actor RecordingTransport: JobTransport {
    let server: ScriptedAgentServer
    private var failuresRemaining: Int
    private(set) var keys: [IdempotencyKey] = []
    init(server: ScriptedAgentServer, failures: Int) {
        self.server = server
        self.failuresRemaining = failures
    }
    func setFailures(_ count: Int) { failuresRemaining = count }
    func submit(_ request: JobRequest, key: IdempotencyKey) async throws -> JobID {
        keys.append(key)
        if failuresRemaining > 0 {
            failuresRemaining -= 1
            throw SimulatedTransportError(message: "offline")
        }
        return try await server.submit(request, key: key)
    }
    func fetchEvents(_ job: JobID, after seq: UInt64, limit: Int) async throws -> [JobEvent] {
        try await server.fetchEvents(job, after: seq, limit: limit)
    }
    func cancel(_ job: JobID) async throws { try await server.cancel(job) }
}

/// Blocks every fetch until `release()`, so tests can stack concurrent reconciles on one fetch.
actor GatedTransport: JobTransport {
    let log: [JobEvent]
    /// How many log events the "server" has released. Read when a fetch *starts*, so a fetch
    /// blocked in flight answers with the log as it was when the request was sent.
    private var available: Int
    private(set) var fetches = 0
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    init(log: [JobEvent], available: Int? = nil) {
        self.log = log
        self.available = available ?? log.count
    }
    func setAvailable(_ count: Int) { available = count }
    var waiting: Int { waiters.count }
    func submit(_ request: JobRequest, key: IdempotencyKey) async throws -> JobID { Fixture.job }
    func fetchEvents(_ job: JobID, after seq: UInt64, limit: Int) async throws -> [JobEvent] {
        fetches += 1
        let released = log.prefix(max(0, available))
        if !isOpen { await withCheckedContinuation { waiters.append($0) } }
        return Array(released.filter { $0.seq > seq }.prefix(limit))
    }
    func cancel(_ job: JobID) async throws {}
    func release() {
        isOpen = true
        let pending = waiters
        waiters.removeAll()
        pending.forEach { $0.resume() }
    }
}

actor DelayRecorder {
    private(set) var delays: [Double] = []
    func record(_ delay: Double) { delays.append(delay) }
}

final class JobRelayTests: XCTestCase {
    private func makeRelay(_ transport: any JobTransport, store: InMemoryRelayStore = InMemoryRelayStore(),
                           policy: RelayPolicy = RelayPolicy(), keys: [String] = (1...20).map { "k\($0)" }) -> JobRelay {
        let box = KeyBox(keys)
        return JobRelay(transport: transport, store: store, policy: policy,
                        makeKey: { box.next() }, sleep: { _ in }, randomUnit: { 0.5 })
    }

    func testLostAcknowledgementIsRetriedWithTheSameKeyAndCreatesOneJob() async throws {
        let server = ScriptedAgentServer(lostAcks: 1)
        let relay = makeRelay(server)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let stats = await server.stats
        XCTAssertEqual(stats.submitCalls, 2)
        XCTAssertEqual(stats.jobsCreated, 1, "the retry must not start (and bill) a second agent run")
        XCTAssertEqual(id, JobID("job-1"))
        let pending = await relay.pendingSubmissions()
        XCTAssertEqual(pending, [])
    }

    func testExhaustedSubmissionSurvivesRelaunchAndResumesWithItsOriginalKey() async throws {
        let server = ScriptedAgentServer()
        let transport = RecordingTransport(server: server, failures: 100)
        let store = InMemoryRelayStore()
        let first = makeRelay(transport, store: store, policy: RelayPolicy(retry: RetryPolicy(maxAttempts: 3)))
        do {
            _ = try await first.submit(JobRequest(prompt: "p", tokenBudget: 0))
            XCTFail("expected exhaustion")
        } catch let error as RelayError {
            XCTAssertEqual(error, .submissionExhausted(IdempotencyKey("k1")))
        }
        let persisted = await store.load()
        XCTAssertEqual(persisted?.pending.map(\.key), [IdempotencyKey("k1")], "an exhausted submission stays pending")
        XCTAssertEqual(persisted?.pending.first?.attempts, 3)

        await transport.setFailures(0)
        let second = makeRelay(transport, store: store, keys: ["must-not-be-used"])
        await second.restore()
        await second.resume()
        let keys = await transport.keys
        XCTAssertEqual(Set(keys), [IdempotencyKey("k1")])
        XCTAssertEqual(keys.count, 4)
        let projections = await second.allProjections()
        XCTAssertEqual(projections.map(\.state.jobID), [JobID("job-1")])
        let pending = await second.pendingSubmissions()
        XCTAssertEqual(pending, [])
        let created = await server.stats.jobsCreated
        XCTAssertEqual(created, 1)
    }

    func testBackoffDelaysFollowTheRetryPolicy() async {
        let recorder = DelayRecorder()
        let transport = RecordingTransport(server: ScriptedAgentServer(), failures: 100)
        let relay = JobRelay(transport: transport, store: InMemoryRelayStore(),
                             policy: RelayPolicy(retry: RetryPolicy(maxAttempts: 4, baseDelay: 1, maxDelay: 3)),
                             makeKey: { IdempotencyKey("k") }, sleep: { await recorder.record($0) },
                             randomUnit: { 0.5 })
        _ = try? await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let delays = await recorder.delays
        XCTAssertEqual(delays, [0.5, 1.0, 1.5], "full jitter at u=0.5 over ceilings 1, 2, min(3, 4); no sleep after the last attempt")
    }

    func testConcurrentReconcilesCoalesceIntoOneFetch() async throws {
        let transport = GatedTransport(log: Fixture.completedLog())
        let relay = makeRelay(transport)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let first = Task { await relay.receiveHint(id, highWater: 18) }
        while await transport.waiting == 0 { await Task.yield() }
        var others: [Task<Void, Never>] = []
        for _ in 0..<5 { others.append(Task { await relay.receiveHint(id, highWater: 18) }) }
        for _ in 0..<200 { await Task.yield() }
        await transport.release()
        await first.value
        for task in others { await task.value }
        let fetches = await transport.fetches
        XCTAssertEqual(fetches, 1, "five hints arriving mid-fetch must not start five more fetches")
        let health = await relay.health
        XCTAssertEqual(health.coalescedReconciles, 5)
        let projection = await relay.projection(of: id)
        XCTAssertEqual(projection?.state.terminal, .completed)
    }

    func testHintTriggersPagedBackfillToConvergence() async throws {
        let server = ScriptedAgentServer()
        let relay = makeRelay(server, policy: RelayPolicy(pageSize: 4))
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let released = await server.advance(id, by: 100)
        XCTAssertEqual(released.count, 18)
        await relay.receiveHint(id, highWater: 18)
        let projection = await relay.projection(of: id)
        XCTAssertEqual(projection?.state.terminal, .completed)
        XCTAssertEqual(projection?.ledger.appliedByChannel[.backfill], 18)
        let fetches = await server.stats.fetchCalls
        XCTAssertEqual(fetches, 5, "ceil(18 / 4) pages")
        let log = await server.log(of: id)
        XCTAssertEqual(ConvergenceCheck.divergences(device: try XCTUnwrap(projection).state, serverLog: log), [])
    }

    func testHintAheadOfTheServerStopsInsteadOfSpinning() async throws {
        let server = ScriptedAgentServer()
        let relay = makeRelay(server)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        await relay.receiveHint(id, highWater: 50) // server has released nothing yet
        let fetches = await server.stats.fetchCalls
        XCTAssertEqual(fetches, 1)
    }

    func testBudgetCeilingCancelsOnceAndServerDecidesTheOutcome() async throws {
        let server = ScriptedAgentServer()
        let relay = makeRelay(server)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 4_000))
        await relay.receive(await server.advance(id, by: 8), via: .stream) // 420 + 1,850 + 2,100 = 4,370
        let cancelling = await relay.projection(of: id)
        XCTAssertEqual(cancelling?.status, .cancelling)
        await relay.receive(await server.advance(id, by: 8), via: .stream)
        let ended = await relay.projection(of: id)
        XCTAssertEqual(ended?.status, .cancelled)
        let stats = await server.stats
        XCTAssertEqual(stats.cancelCalls, 1)
        let health = await relay.health
        XCTAssertEqual(health.autoCancels, 1)
    }

    func testCancelAfterServerFinishedIsANoOp() async throws {
        let server = ScriptedAgentServer()
        let relay = makeRelay(server)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        await relay.receive(await server.advance(id, by: 100), via: .stream)
        await relay.requestCancel(id)
        let stats = await server.stats
        XCTAssertEqual(stats.cancelCalls, 0)
        let projection = await relay.projection(of: id)
        XCTAssertEqual(projection?.status, .completed)
    }

    func testRelaunchRestoresCursorAndProbesForEventsReleasedWhileDead() async throws {
        let server = ScriptedAgentServer()
        let store = InMemoryRelayStore()
        let first = makeRelay(server, store: store)
        let id = try await first.submit(JobRequest(prompt: "p", tokenBudget: 0))
        await first.receive(await server.advance(id, by: 6), via: .stream)
        await first.requestCancel(id)
        _ = await server.advance(id, by: 100) // server emits `.cancelled` while the app is dead

        let second = makeRelay(server, store: store)
        await second.restore()
        let restored = await second.projection(of: id)
        XCTAssertEqual(restored?.state.appliedThrough, 6)
        XCTAssertEqual(restored?.status, .cancelling, "cancel intent was persisted")
        await second.resume()
        let caughtUp = await second.projection(of: id)
        XCTAssertEqual(caughtUp?.status, .cancelled)
        XCTAssertEqual(caughtUp?.ledger.appliedByChannel[.backfill], 1)
    }

    func testStoreKeepsTheNewestGenerationWhenSavesArriveOutOfOrder() async {
        let store = InMemoryRelayStore()
        var newer = RelayCheckpoint()
        newer.generation = 5
        var older = RelayCheckpoint()
        older.generation = 4
        older.pending = [PendingSubmission(key: IdempotencyKey("stale"), request: JobRequest(prompt: "", tokenBudget: 0), attempts: 0)]
        await store.save(newer)
        await store.save(older)
        let loaded = await store.load()
        XCTAssertEqual(loaded?.generation, 5)
        XCTAssertEqual(loaded?.pending, [])
        let ignored = await store.staleSavesIgnored
        XCTAssertEqual(ignored, 1)
    }

    func testEventsAndHintsForUnknownJobsAreCountedNotCrashed() async {
        let relay = makeRelay(ScriptedAgentServer())
        await relay.receive([Fixture.usage(1, 5, JobID("ghost"))])
        await relay.receiveHint(JobID("ghost"), highWater: 3)
        let health = await relay.health
        XCTAssertEqual(health.eventsForUnknownJobs, 2)
    }
}

/// Answers `submit` by first reading the store, so the test sees exactly what a crash at
/// the moment the request leaves the device would leave behind.
actor DurabilityProbeTransport: JobTransport {
    let store: InMemoryRelayStore
    private(set) var keysDurableAtSend: [Bool] = []
    private var cancelFailures: Int
    private(set) var cancelCalls = 0
    init(store: InMemoryRelayStore, cancelFailures: Int = 0) {
        self.store = store
        self.cancelFailures = cancelFailures
    }
    func submit(_ request: JobRequest, key: IdempotencyKey) async throws -> JobID {
        let pending = await store.load()?.pending ?? []
        keysDurableAtSend.append(pending.contains { $0.key == key })
        return JobID("job-\(keysDurableAtSend.count)")
    }
    func fetchEvents(_ job: JobID, after seq: UInt64, limit: Int) async throws -> [JobEvent] { [] }
    func cancel(_ job: JobID) async throws {
        cancelCalls += 1
        if cancelFailures > 0 {
            cancelFailures -= 1
            throw SimulatedTransportError(message: "cancel failed")
        }
    }
}

final class KeyBox: @unchecked Sendable {
    // Test-only key source shared with the relay's actor; guarded by a lock.
    private let lock = NSLock()
    private var keys: [String]
    init(_ keys: [String]) { self.keys = keys }
    func next() -> IdempotencyKey {
        lock.lock()
        defer { lock.unlock() }
        guard !keys.isEmpty else { return IdempotencyKey("exhausted") }
        return IdempotencyKey(keys.removeFirst())
    }
}

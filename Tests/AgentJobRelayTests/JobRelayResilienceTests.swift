import XCTest
@testable import AgentJobRelay

/// Fencing, retirement, cancel retry, coalescing liveness and retention.
final class JobRelayResilienceTests: XCTestCase {
    private func makeRelay(_ transport: any JobTransport, store: InMemoryRelayStore = InMemoryRelayStore(),
                           policy: RelayPolicy = RelayPolicy()) -> JobRelay {
        let box = KeyBox((1...20).map { "r\($0)" })
        return JobRelay(transport: transport, store: store, policy: policy,
                        makeKey: { box.next() }, sleep: { _ in }, randomUnit: { 0.5 })
    }

    func testKeyIsDurableBeforeTheFirstNetworkAttempt() async throws {
        let store = InMemoryRelayStore()
        let transport = DurabilityProbeTransport(store: store)
        let relay = makeRelay(transport, store: store)
        _ = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let durable = await transport.keysDurableAtSend
        XCTAssertEqual(durable, [true], "a crash while the first request is in flight must leave the key on disk")
    }

    func testHintThatJoinsAnEmptyFetchTriggersAnotherPass() async throws {
        // The server has released nothing when the first fetch is sent. While it is in
        // flight, the server releases 6 events and a second hint arrives. The first fetch
        // comes back empty (no progress), so only the coalesced "go again" pass can catch up.
        let transport = GatedTransport(log: Fixture.completedLog(), available: 0)
        let relay = makeRelay(transport)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let first = Task { await relay.receiveHint(id, highWater: 6) }
        while await transport.waiting == 0 { await Task.yield() }
        await transport.setAvailable(6)
        await relay.receiveHint(id, highWater: 6) // coalesced into the running reconcile
        await transport.release()
        await first.value
        let fetches = await transport.fetches
        XCTAssertEqual(fetches, 2)
        let projection = await relay.projection(of: id)
        XCTAssertEqual(projection?.state.appliedThrough, 6, "the wake-up that arrived mid-fetch must not be lost")
    }

    func testRelaunchedInstanceFencesOutTheOldOneEvenIfItKeepsRunning() async throws {
        let server = ScriptedAgentServer()
        let store = InMemoryRelayStore()
        let old = makeRelay(server, store: store)
        let id = try await old.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let fresh = makeRelay(server, store: store)
        await fresh.restore()
        let claimed = await store.load()
        XCTAssertEqual(claimed?.epoch, 1, "restore claims a new epoch immediately")

        // The superseded instance was never retired (think: a background task's relay).
        await old.receive(await server.advance(id, by: 4), via: .stream)
        let afterOld = await store.load()
        XCTAssertEqual(afterOld?.jobs.first?.state.appliedThrough, 0, "old-epoch saves are rejected")
        let ignored = await store.staleSavesIgnored
        XCTAssertGreaterThan(ignored, 0)

        await fresh.requestCancel(id)
        let afterFresh = await store.load()
        XCTAssertEqual(afterFresh?.jobs.first?.cancelRequested, true, "the new instance's saves still land")
    }

    func testRetiredRelayIgnoresEverything() async throws {
        let server = ScriptedAgentServer()
        let store = InMemoryRelayStore()
        let relay = makeRelay(server, store: store)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let savesBefore = await store.saveCount
        await relay.retire()
        await relay.receive(await server.advance(id, by: 4))
        await relay.receiveHint(id, highWater: 4)
        await relay.requestCancel(id)
        do {
            _ = try await relay.submit(JobRequest(prompt: "q", tokenBudget: 0))
            XCTFail("a retired relay must not submit")
        } catch let error as RelayError {
            XCTAssertEqual(error, .retired)
        }
        let savesAfter = await store.saveCount
        XCTAssertEqual(savesAfter, savesBefore)
        let projection = await relay.projection(of: id)
        XCTAssertEqual(projection?.state.appliedThrough, 0)
        let health = await relay.health
        XCTAssertEqual(health.callsAfterRetire, 4)
    }

    func testFailedCancelIsRetriedWithoutARelaunch() async throws {
        let store = InMemoryRelayStore()
        let transport = DurabilityProbeTransport(store: store, cancelFailures: 1)
        let relay = makeRelay(transport, store: store)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        await relay.requestCancel(id)
        let health = await relay.health
        XCTAssertEqual(health.cancelFailures, 1)
        await relay.receive([Fixture.usage(1, 10, id)])
        let calls = await transport.cancelCalls
        XCTAssertEqual(calls, 2, "the next ingest re-sends the cancel the user asked for")
        await relay.receive([Fixture.usage(2, 10, id)])
        let callsAfterSuccess = await transport.cancelCalls
        XCTAssertEqual(callsAfterSuccess, 2, "and stops once one succeeded")
    }

    func testTerminalJobsArePrunedOldestFirstAndLiveJobsAreKept() async throws {
        let server = ScriptedAgentServer()
        let relay = makeRelay(server, policy: RelayPolicy(maxRetainedTerminalJobs: 1))
        let first = try await relay.submit(JobRequest(prompt: "a", tokenBudget: 0))
        let second = try await relay.submit(JobRequest(prompt: "b", tokenBudget: 0))
        let live = try await relay.submit(JobRequest(prompt: "c", tokenBudget: 0))
        await relay.receive(await server.advance(first, by: 100))
        await relay.receive(await server.advance(second, by: 100))
        let ids = await relay.allProjections().map(\.state.jobID)
        XCTAssertEqual(ids, [second, live])
        let health = await relay.health
        XCTAssertEqual(health.prunedTerminalJobs, 1)
        await relay.forget(live) // not terminal: refused
        await relay.forget(second)
        let remaining = await relay.allProjections().map(\.state.jobID)
        XCTAssertEqual(remaining, [live])
    }

    func testResumeDuringSubmitsFirstSaveCannotHijackTheKey() async throws {
        let server = ScriptedAgentServer()
        let store = GatedStore()
        let gatedRelay = JobRelay(transport: server, store: store, makeKey: { IdempotencyKey("only") },
                                  sleep: { _ in }, randomUnit: { 0.5 })
        let submit = Task { try await gatedRelay.submit(JobRequest(prompt: "p", tokenBudget: 0)) }
        while await store.waiting == 0 { await Task.yield() }
        let resume = Task { await gatedRelay.resume() } // runs while submit is suspended in persist()
        for _ in 0..<200 { await Task.yield() }
        await store.open()
        await resume.value
        let id = try await submit.value
        XCTAssertEqual(id, JobID("job-1"), "the caller that owns the key gets the job")
        let stats = await server.stats
        XCTAssertEqual(stats.submitCalls, 1)
        XCTAssertEqual(stats.jobsCreated, 1)
    }

    func testEventBeyondTheWindowTriggersAnImmediateFetch() async throws {
        let server = ScriptedAgentServer()
        let relay = makeRelay(server, policy: RelayPolicy(reorderWindow: 2))
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let released = await server.advance(id, by: 6)
        let last = try XCTUnwrap(released.last)
        await relay.receive([last]) // seq 6 with nothing applied: beyond a window of 2
        let projection = await relay.projection(of: id)
        XCTAssertEqual(projection?.state.appliedThrough, 6, "the hole is fetched, not waited for")
        XCTAssertEqual(projection?.ledger.discardedBeyondWindow, 1)
    }

    func testBudgetTripsExactlyAtTheCeiling() async throws {
        let server = ScriptedAgentServer()
        let relay = makeRelay(server)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 2_270)) // 420 + 1,850
        await relay.receive(await server.advance(id, by: 5)) // through usage 420
        let under = await server.stats.cancelCalls
        XCTAssertEqual(under, 0)
        await relay.receive(await server.advance(id, by: 1)) // usage 1,850 → exactly 2,270
        let at = await server.stats.cancelCalls
        XCTAssertEqual(at, 1)
    }

    func testRepeatedCancelRequestsSendOneCancel() async throws {
        let server = ScriptedAgentServer()
        let relay = makeRelay(server)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        await relay.requestCancel(id)
        await relay.requestCancel(id)
        await relay.receive(await server.advance(id, by: 0))
        let calls = await server.stats.cancelCalls
        XCTAssertEqual(calls, 1)
    }

    func testRetireDuringAnInFlightFetchDiscardsItsResult() async throws {
        let transport = GatedTransport(log: Fixture.completedLog())
        let relay = makeRelay(transport)
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let reconcile = Task { await relay.receiveHint(id, highWater: 18) }
        while await transport.waiting == 0 { await Task.yield() }
        await relay.retire()
        await transport.release()
        await reconcile.value
        let projection = await relay.projection(of: id)
        XCTAssertEqual(projection?.state.appliedThrough, 0, "a retired instance must not apply what it fetched")
    }

    func testEachReconcileIsBoundedByMaxBackfillPages() async throws {
        let server = ScriptedAgentServer()
        let relay = makeRelay(server, policy: RelayPolicy(pageSize: 1, maxBackfillPages: 2))
        let id = try await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        _ = await server.advance(id, by: 100)
        await relay.receiveHint(id, highWater: 18)
        let fetches = await server.stats.fetchCalls
        XCTAssertEqual(fetches, 2, "one reconcile may not fetch more than maxBackfillPages pages")
        let projection = await relay.projection(of: id)
        XCTAssertEqual(projection?.state.appliedThrough, 2)
    }

    func testFreshInstallRestoreClaimsAnEpochImmediately() async {
        let store = InMemoryRelayStore()
        let relay = makeRelay(ScriptedAgentServer(), store: store)
        await relay.restore()
        let checkpoint = await store.load()
        XCTAssertEqual(checkpoint?.epoch, 1)
    }

    func testAbandoningAPendingSubmissionBoundsThePendingList() async {
        let transport = RecordingTransport(server: ScriptedAgentServer(), failures: 100)
        let store = InMemoryRelayStore()
        let relay = makeRelay(transport, store: store, policy: RelayPolicy(retry: RetryPolicy(maxAttempts: 1)))
        _ = try? await relay.submit(JobRequest(prompt: "p", tokenBudget: 0))
        let before = await relay.pendingSubmissions()
        XCTAssertEqual(before.count, 1)
        guard let key = before.first?.key else { return }
        await relay.abandonPendingSubmission(key)
        let pending = await relay.pendingSubmissions()
        XCTAssertEqual(pending, [])
        let persisted = await store.load()
        XCTAssertEqual(persisted?.pending, [])
    }
}

/// A store whose saves block until `open()`, to hold a relay inside `persist()`.
actor GatedStore: RelayStore {
    private var checkpoint: RelayCheckpoint?
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    var waiting: Int { waiters.count }
    func load() async -> RelayCheckpoint? { checkpoint }
    func save(_ checkpoint: RelayCheckpoint) async {
        if !isOpen { await withCheckedContinuation { waiters.append($0) } }
        if let current = self.checkpoint, checkpoint.ranks(below: current) { return }
        self.checkpoint = checkpoint
    }
    func open() {
        isOpen = true
        let pending = waiters
        waiters.removeAll()
        pending.forEach { $0.resume() }
    }
}

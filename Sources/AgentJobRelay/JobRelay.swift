import Foundation

/// Tunables for the relay. Every value is clamped so a bad config cannot trap or spin.
public struct RelayPolicy: Hashable, Sendable {
    public let reorderWindow: Int
    public let pageSize: Int
    /// Upper bound on fetches per `reconcile` call, so a server that keeps growing its log
    /// cannot hold the relay in one loop forever.
    public let maxBackfillPages: Int
    public let retry: RetryPolicy
    /// Terminal jobs kept (oldest pruned first). Live jobs are never pruned, so memory and
    /// checkpoint size are bounded by live jobs + this number, not by every job ever run.
    public let maxRetainedTerminalJobs: Int

    public init(reorderWindow: Int = 64, pageSize: Int = 50, maxBackfillPages: Int = 20,
                retry: RetryPolicy = RetryPolicy(), maxRetainedTerminalJobs: Int = 50) {
        self.maxRetainedTerminalJobs = max(0, maxRetainedTerminalJobs)
        self.reorderWindow = max(1, reorderWindow)
        self.pageSize = max(1, pageSize)
        self.maxBackfillPages = max(1, maxBackfillPages)
        self.retry = retry
    }
}

/// Operational counters for the relay itself (as opposed to per-job delivery ledgers).
public struct RelayHealth: Hashable, Sendable {
    public internal(set) var submitAttempts = 0
    public internal(set) var submitFailures = 0
    public internal(set) var backfillFetches = 0
    public internal(set) var backfillFailures = 0
    /// `reconcile` requests that joined an in-flight backfill instead of starting a new one.
    public internal(set) var coalescedReconciles = 0
    public internal(set) var autoCancels = 0
    public internal(set) var cancelFailures = 0
    public internal(set) var eventsForUnknownJobs = 0
    public internal(set) var prunedTerminalJobs = 0
    /// Calls ignored because `retire()` was called on this instance. (An instance superseded
    /// by a newer epoch does not know it; the store fences its saves instead.)
    public internal(set) var callsAfterRetire = 0
}

public enum RelayError: Error, Hashable, Sendable {
    /// Retries ran out. The submission stays pending with its key; `resume()` retries it
    /// later without risking a duplicate job.
    case submissionExhausted(IdempotencyKey)
    case submissionInFlight(IdempotencyKey)
    /// This instance was retired; use the relay that replaced it.
    case retired
    /// The caller abandoned this pending submission while it was being retried.
    case submissionAbandoned(IdempotencyKey)
}

/// The client half of a long-running, server-side agent job.
///
/// The phone's job is not to run the agent; it is to (1) submit exactly one job per user
/// intent even across retries and process death, (2) turn three unreliable inputs — a
/// foreground stream, silent push hints and paged fetches — into one ordered, exactly-once
/// projection, and (3) never show a result as more certain than the server says it is.
///
/// Concurrency: every mutation happens on this actor, and **no local copy of a projection
/// is held across an `await`**. After each suspension the code re-reads `jobs[id]`, because
/// a stream batch or a hint may have advanced the job while the fetch was in flight.
public actor JobRelay {
    private struct Entry: Sendable {
        let request: JobRequest
        var projection: JobProjection
    }

    private let transport: any JobTransport
    private let store: any RelayStore
    private let policy: RelayPolicy
    private let makeKey: @Sendable () -> IdempotencyKey
    private let sleep: @Sendable (Double) async -> Void
    private let randomUnit: @Sendable () -> Double

    private var pending: [PendingSubmission] = []
    private var jobs: [JobID: Entry] = [:]
    private var order: [JobID] = []
    private var generation: UInt64 = 0
    private var epoch: UInt64 = 0
    private var isRetired = false

    private var submitting: Set<IdempotencyKey> = []
    private var backfilling: Set<JobID> = []
    private var backfillAgain: Set<JobID> = []
    private var cancelIssued: Set<JobID> = []

    public private(set) var health = RelayHealth()

    /// Default backoff sleeper. NaN, infinite or negative delays become 0 instead of trapping
    /// in the `Double` → integer conversion.
    public static let systemSleep: @Sendable (Double) async -> Void = { seconds in
        let nanos = Saturating.int(seconds * 1_000_000_000)
        try? await Task.sleep(nanoseconds: UInt64(max(0, nanos)))
    }

    public init(
        transport: any JobTransport,
        store: any RelayStore,
        policy: RelayPolicy = RelayPolicy(),
        makeKey: @escaping @Sendable () -> IdempotencyKey = { IdempotencyKey(UUID().uuidString) },
        sleep: @escaping @Sendable (Double) async -> Void = JobRelay.systemSleep,
        randomUnit: @escaping @Sendable () -> Double = { Double.random(in: 0..<1) }
    ) {
        self.transport = transport
        self.store = store
        self.policy = policy
        self.makeKey = makeKey
        self.sleep = sleep
        self.randomUnit = randomUnit
    }

    // MARK: - Lifecycle

    /// Loads the last checkpoint. Call once, before anything else, after a (re)launch.
    ///
    /// Restoring claims a new fencing epoch and persists it immediately, so any older relay
    /// instance still holding in-flight saves is fenced out from this point on.
    public func restore() async {
        guard !isRetired else { return noteRetiredCall() }
        let checkpoint = await store.load()
        epoch = (checkpoint?.epoch ?? epoch) &+ 1
        generation = max(generation, checkpoint?.generation ?? 0)
        guard let checkpoint else {
            await persist()
            return
        }
        pending = checkpoint.pending
        for job in checkpoint.jobs where jobs[job.state.jobID] == nil {
            let projection = JobProjection(restoring: job.state, cancelRequested: job.cancelRequested,
                                           reorderWindow: policy.reorderWindow)
            jobs[job.state.jobID] = Entry(request: job.request, projection: projection)
            order.append(job.state.jobID)
        }
        await persist()
    }

    /// Stops this instance: every later call is ignored and nothing more is persisted.
    /// Use it when handing the store to a new instance in the same process.
    public func retire() {
        isRetired = true
    }

    private func noteRetiredCall() {
        Saturating.increment(&health.callsAfterRetire)
    }

    /// Drops a terminal job from memory and from the checkpoint. Live jobs are kept.
    public func forget(_ id: JobID) async {
        guard !isRetired else { return noteRetiredCall() }
        guard let projection = jobs[id]?.projection, projection.isTerminal else { return }
        remove(id)
        await persist()
    }

    private func remove(_ id: JobID) {
        jobs[id] = nil
        order.removeAll { $0 == id }
        cancelIssued.remove(id)
        backfillAgain.remove(id)
    }

    /// Finishes whatever a previous process left undone: resubmits pending submissions with
    /// their original keys, re-sends cancels the user asked for, and catches every live job up.
    public func resume() async {
        guard !isRetired else { return noteRetiredCall() }
        for submission in pending where !submitting.contains(submission.key) {
            _ = try? await drive(submission.key)
        }
        for id in order {
            guard let projection = jobs[id]?.projection, !projection.isTerminal else { continue }
            if projection.cancelRequested { await sendCancel(id) }
            // After a relaunch the device has no evidence of how far the log has grown,
            // so probe with one fetch even though `isBehind` is false.
            await reconcile(id, probe: true)
        }
    }

    // MARK: - Submission

    /// Submits a job. The idempotency key is persisted *before* the first attempt.
    public func submit(_ request: JobRequest) async throws -> JobID {
        guard !isRetired else {
            noteRetiredCall()
            throw RelayError.retired
        }
        let submission = PendingSubmission(key: makeKey(), request: request, attempts: 0)
        pending.append(submission)
        // Claim the key *before* the first suspension point. Otherwise a `resume()` that runs
        // while `persist()` is suspended would see an unclaimed pending key, drive it itself,
        // and this call would report failure for a job that exists and is being billed.
        submitting.insert(submission.key)
        defer { submitting.remove(submission.key) }
        await persist()
        return try await attempt(submission.key)
    }

    /// Drives a pending key that nobody else is driving (used by `resume()`).
    private func drive(_ key: IdempotencyKey) async throws -> JobID {
        guard !submitting.contains(key) else { throw RelayError.submissionInFlight(key) }
        submitting.insert(key)
        defer { submitting.remove(key) }
        return try await attempt(key)
    }

    /// The retry loop. Callers must already hold `key` in `submitting`, so no other path
    /// can drive or resolve it concurrently.
    private func attempt(_ key: IdempotencyKey) async throws -> JobID {
        for attempt in 0..<policy.retry.maxAttempts {
            guard !isRetired else { throw RelayError.retired }
            // Only `abandonPendingSubmission` can remove a claimed key from `pending`.
            guard let index = pending.firstIndex(where: { $0.key == key }) else {
                throw RelayError.submissionAbandoned(key)
            }
            Saturating.increment(&pending[index].attempts)
            let request = pending[index].request
            Saturating.increment(&health.submitAttempts)
            do {
                let id = try await transport.submit(request, key: key)
                pending.removeAll { $0.key == key }
                if jobs[id] == nil {
                    jobs[id] = Entry(request: request,
                                     projection: JobProjection(jobID: id, reorderWindow: policy.reorderWindow))
                    order.append(id)
                }
                await persist()
                return id
            } catch {
                Saturating.increment(&health.submitFailures)
                if attempt + 1 < policy.retry.maxAttempts {
                    await sleep(policy.retry.delay(forAttempt: attempt, unit: randomUnit()))
                }
            }
        }
        await persist()
        throw RelayError.submissionExhausted(key)
    }

    // MARK: - Ingest

    /// Feeds a batch from the foreground stream (or any other push-style source).
    public func receive(_ events: [JobEvent], via channel: Channel = .stream) async {
        guard !isRetired else { return noteRetiredCall() }
        var overflowed: Set<JobID> = []
        var touched: [JobID] = []
        for event in events {
            guard jobs[event.jobID] != nil else {
                Saturating.increment(&health.eventsForUnknownJobs)
                continue
            }
            let outcome = jobs[event.jobID]?.projection.ingest(event, via: channel)
            if case .beyondWindow = outcome { overflowed.insert(event.jobID) }
            if !touched.contains(event.jobID) { touched.append(event.jobID) }
        }
        for id in touched { await enforceIntent(id) }
        await persist()
        // A dropped-beyond-window event is never coming back on the stream; fetch it now.
        for id in touched where overflowed.contains(id) { await reconcile(id) }
    }

    /// A silent push said the server's log for `id` reaches `highWater`. Pushes carry only a
    /// cursor, never content: content arrives through `fetchEvents`, which is ordered and paged.
    public func receiveHint(_ id: JobID, highWater: UInt64) async {
        guard !isRetired else { return noteRetiredCall() }
        guard jobs[id] != nil else {
            Saturating.increment(&health.eventsForUnknownJobs)
            return
        }
        jobs[id]?.projection.noteHighWater(highWater)
        await reconcile(id)
    }

    /// Fetches until the job is caught up, terminal, or the page budget is spent.
    /// Concurrent calls for the same job coalesce into the one already running.
    /// `probe` forces one fetch even when the device has no evidence it is behind.
    public func reconcile(_ id: JobID, probe: Bool = false) async {
        guard !isRetired else { return noteRetiredCall() }
        guard jobs[id] != nil else { return }
        if backfilling.contains(id) {
            backfillAgain.insert(id)
            Saturating.increment(&health.coalescedReconciles)
            return
        }
        backfilling.insert(id)
        defer { backfilling.remove(id) }

        var pages = 0
        repeat {
            backfillAgain.remove(id)
            while pages < policy.maxBackfillPages,
                  let projection = jobs[id]?.projection,
                  !projection.isTerminal,
                  projection.isBehind || (probe && pages == 0) {
                let after = projection.state.appliedThrough
                pages += 1
                Saturating.increment(&health.backfillFetches)
                let page: [JobEvent]
                do {
                    page = try await transport.fetchEvents(id, after: after, limit: policy.pageSize)
                } catch {
                    Saturating.increment(&health.backfillFailures)
                    await persist()
                    return
                }
                guard !isRetired else { return }
                for event in page { jobs[id]?.projection.ingest(event, via: .backfill) }
                await enforceIntent(id)
                // No progress means the hint ran ahead of what the server will serve yet.
                // Stop instead of spinning; the next hint or stream batch will retry.
                guard let now = jobs[id]?.projection.state.appliedThrough, now > after else { break }
            }
        } while backfillAgain.contains(id) && pages < policy.maxBackfillPages
        await persist()
    }

    /// Catches every live job up — call on foreground, stream reconnect, or a stall timer.
    public func reconcileAll() async {
        for id in order { await reconcile(id) }
    }

    // MARK: - Cancellation

    /// Records the user's intent locally and durably, then tells the server. The projection
    /// shows `.cancelling` until the server's terminal event decides what actually happened.
    public func requestCancel(_ id: JobID) async {
        guard !isRetired else { return noteRetiredCall() }
        guard let projection = jobs[id]?.projection, !projection.isTerminal else { return }
        jobs[id]?.projection.requestCancel()
        await persist()
        await sendCancel(id)
    }

    private func sendCancel(_ id: JobID) async {
        guard !cancelIssued.contains(id) else { return }
        cancelIssued.insert(id)
        do {
            try await transport.cancel(id)
        } catch {
            Saturating.increment(&health.cancelFailures)
            cancelIssued.remove(id) // the next ingest, reconcile or `resume()` tries again
        }
    }

    /// Re-sends a cancel whose earlier attempt failed (the user's intent stands until the
    /// server answers), then applies the budget ceiling.
    private func enforceIntent(_ id: JobID) async {
        if let projection = jobs[id]?.projection, projection.cancelRequested,
           !projection.isTerminal, !cancelIssued.contains(id) {
            await sendCancel(id)
            return
        }
        guard let entry = jobs[id],
              entry.request.tokenBudget > 0,
              !entry.projection.isTerminal,
              !entry.projection.cancelRequested,
              entry.projection.state.tokensUsed >= entry.request.tokenBudget else { return }
        Saturating.increment(&health.autoCancels)
        await requestCancel(id)
    }

    // MARK: - Reads

    public func projection(of id: JobID) -> JobProjection? { jobs[id]?.projection }
    public func request(of id: JobID) -> JobRequest? { jobs[id]?.request }
    public func allProjections() -> [JobProjection] { order.compactMap { jobs[$0]?.projection } }
    public func pendingSubmissions() -> [PendingSubmission] { pending }

    /// Gives up on a submission whose outcome is unknown (e.g. retries exhausted and the user
    /// dismissed it). Bounds the pending list, which otherwise lives until a resume succeeds.
    /// Note the server may still have accepted it; abandoning only stops this device asking.
    public func abandonPendingSubmission(_ key: IdempotencyKey) async {
        guard !isRetired else { return noteRetiredCall() }
        guard pending.contains(where: { $0.key == key }) else { return }
        pending.removeAll { $0.key == key }
        await persist()
    }

    // MARK: - Persistence

    private func persist() async {
        guard !isRetired else { return }
        pruneTerminalJobs()
        generation &+= 1 // wrapping is unreachable in practice (2^64 saves) and must not trap
        var checkpoint = RelayCheckpoint()
        checkpoint.generation = generation
        checkpoint.epoch = epoch
        checkpoint.pending = pending
        checkpoint.jobs = order.compactMap { id in
            guard let entry = jobs[id] else { return nil }
            return RelayCheckpoint.Job(request: entry.request, state: entry.projection.state,
                                       cancelRequested: entry.projection.cancelRequested)
        }
        await store.save(checkpoint)
    }
}

extension JobRelay {
    /// Keeps at most `maxRetainedTerminalJobs` terminal jobs, dropping the oldest first.
    fileprivate func pruneTerminalJobs() {
        let terminal = order.filter { jobs[$0]?.projection.isTerminal == true }
        let excess = terminal.count - policy.maxRetainedTerminalJobs
        guard excess > 0 else { return }
        for id in terminal.prefix(excess) {
            remove(id)
            Saturating.increment(&health.prunedTerminalJobs)
        }
    }
}

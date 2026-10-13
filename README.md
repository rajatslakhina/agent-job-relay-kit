# AgentJobRelay

**The phone doesn't run the agent. It just has to never lie about what the agent did.**

Agent work is moving off the device. A research or coding agent now runs for minutes on a server, through several phases, revising its results as it goes, while the iPhone that started it gets suspended, loses its connection, and is killed by the OS. The hard part on the client isn't the network call. It's the bookkeeping:

- The user taps **Run** once. A connection reset after the server accepted the job must not start, and bill, a second agent run.
- Progress arrives over a foreground stream that drops, duplicates and reorders, plus silent pushes that wake the app, plus fetches on relaunch. Three unreliable channels have to add up to **one ordered, exactly-once view**.
- A job that failed halfway must not show its draft as the answer. A job the user cancelled, which finished anyway, must say so.

`AgentJobRelay` is the client half of that system: a Swift package with a pure reducer, an actor that orchestrates it, and a seeded end-to-end simulator that proves the two agree with the server under drop, duplicate, reorder and process death.

> Demo app: (added after the companion repo is pushed)

## Why this matters

Most "streaming AI" clients apply whatever the stream sends, in the order it arrives. Under real mobile conditions that shows a stale draft over the final answer, double-counts spend when an event is redelivered, and puts a **Completed** badge on a job whose log still has a hole in it. The test suite includes that naive reducer and shows the convergence check catching it on most chaotic deliveries (`testCheckRejectsArrivalOrderReducer`).

For an engineering lead the decisions here are platform decisions. Every team that ships an agent feature needs the same idempotency, ordering, cancellation and honesty rules, and getting them wrong shows up as duplicate charges and support tickets rather than as crashes.

## Architecture

```
            foreground stream ──┐         silent push (cursor only) ──┐
          (drops, dupes, reorder)│                                     │
                                 ▼                                     ▼
 ┌──────────────────────────── JobRelay (actor) ─────────────────────────────┐
 │ submit ─ key persisted BEFORE first attempt ─ retry w/ full jitter         │
 │ receive(events) ─────────► JobProjection.ingest  (pure, per job)           │
 │ receiveHint(highWater) ──► reconcile: paged fetchEvents, coalesced per job │
 │ requestCancel ─ intent persisted, server decides outcome                   │
 │ budget ceiling ─ auto-cancel once tokensUsed ≥ tokenBudget                 │
 │ persist() ─ (epoch, generation)-stamped RelayCheckpoint ─► RelayStore      │
 └────────────────────────────────────────────────────────────────────────────┘
                 │ JobTransport (submit / fetchEvents / cancel)
                 ▼
            agent backend  ◄── ScriptedAgentServer + ChaosLink in tests and the demo
```

| Type | Role |
|---|---|
| `JobEvent`, `Phase`, `ArtifactFragment`, `Terminal` | Wire model. `seq` is dense and 1-based per job, and the terminal event carries the last `seq`. |
| `JobProjection` | Pure, synchronous reducer: reorder buffer, deduplication, absorbing terminal, artifact confidence, `DeliveryLedger`. |
| `ConvergenceCheck` | Independent fold of the server log (not the reducer re-run) that lists every `Divergence`. |
| `JobRelay` | Actor: idempotent submission, ingest, coalesced backfill, cancellation, budget, epoch-fenced checkpointing, terminal-job retention. |
| `RetryPolicy`, `RelayPolicy` | Full-jitter backoff and clamped tunables. |
| `RelayStore`, `InMemoryRelayStore`, `RelayCheckpoint` | The durable-state port (you supply the on-disk implementation; `InMemoryRelayStore` is the reference used by tests and the demo), ranked by `(epoch, generation)`. |
| `ScriptedAgentServer`, `ChaosLink`, `SplitMix64`, `RelaySimulation` | Deterministic backend, lossy stream, and the end-to-end harness the demo drives. |

## Design decisions and trade-offs

**1. Apply in `seq` order with a bounded reorder buffer, not in arrival order.** An event that arrives ahead of a gap waits in a buffer of at most `reorderWindow` events. Anything further ahead is dropped and counted (`discardedBeyondWindow`), and the relay fetches the hole instead. *Trade-off:* a single lost event holds back everything behind it until a fetch fills it, which is head-of-line blocking by design. *Rejected:* applying out of order and "fixing up" later. That is the only way to show a Completed badge over a hole, and the tests show the naive reducer doing exactly that (`testCheckRejectsCompletedOverAHole`).

**2. Pushes carry a cursor, never content.** A silent push only says "the log reaches `seq` N". Content always comes through `fetchEvents`, which is ordered and paged. *Trade-off:* one extra round trip per wake. *Rejected:* payload-bearing pushes. They are size-limited, can be coalesced or dropped by the OS, and would be a fourth unordered channel to reconcile.

**3. The idempotency key is persisted before the network sees it.** `submit` writes a `PendingSubmission` to the store, then attempts. It claims the key in memory *before* that save suspends, so a concurrent `resume()` cannot drive the same key and leave the caller with a false failure. If retries run out, the entry stays, and `resume()` after a relaunch retries with the **same** key. `abandonPendingSubmission(_:)` lets the app give up on one explicitly, which bounds the pending list. *Trade-off:* one extra durable write per submission. *Rejected:* generating the key per attempt. The lost-acknowledgement case (the server accepted the job, the response died) then becomes a second, billed agent run.

**4. Fragments are whole revisions, not deltas.** *Trade-off:* more bytes per update. *Benefit:* a fragment is idempotent, a stale revision is detectable (`staleRevisions`), and a backfill after a long suspension only needs the newest revision.

**5. Honesty is part of the state, not the UI.** `Artifact.Confidence` is `.provisional`, `.final` or `.unconfirmed`. A job that fails or is cancelled turns its provisional results into `.unconfirmed` inside the reducer, so no screen can render a draft as the answer. `JobStatus.completedBeforeCancel` exists because "you cancelled it, but it finished first" is a different fact from "cancelled".

**6. Coalesce backfill per job without losing wake-ups. Re-read state after every `await`.** `reconcile` keeps one fetch loop per job. A hint that arrives mid-fetch sets a "go again" flag instead of starting a second fetch (`coalescedReconciles`). If the in-flight fetch comes back empty, the flag makes the loop run once more, so the hint that arrived meanwhile is not lost (`testHintThatJoinsAnEmptyFetchTriggersAnotherPass`). The actor never holds a copy of a projection across a suspension point. *Rejected:* a "fetch on every hint" approach, which turns a burst of pushes into a burst of identical requests (`testConcurrentReconcilesCoalesceIntoOneFetch`: 5 hints during a blocked fetch, 1 fetch).

**7. The client-side token budget is a brake, not a wall.** When `tokensUsed` reaches `tokenBudget`, the relay requests cancellation. If that request fails, it is re-sent on the next ingest or reconcile, not just after a relaunch. The device only learns about spend as fast as the stream delivers it. In the library's default simulation (2 server events per tick, seed 2026), the 5,000-token job stops at 7,670 tokens on a clean stream. On the flaky and hostile streams it reaches 8,870, which is **the job's entire scripted spend**: the usage report arrived too late and the brake saved nothing. Both figures are pinned by tests: `testDefaultScenarioOutcomes` pins the clean figure, which does not depend on the seed, and `testBudgetJobAtTheDemoSeedOnLossyStreams` pins the seed-2026 lossy one. That is why the server-side limit has to be the real enforcement.

**8. Saves are ranked by `(epoch, generation)`, which is a fencing token.** Two `persist()` calls from one actor can reach the store in either order across suspension points, so `generation` is monotonic and stores keep the higher one. A harder case: two relay instances share one store, such as the app and a background task, or a process that was superseded but still has saves in flight. `restore()` claims `epoch + 1` and persists it immediately. From then on, every save from an older instance ranks lower and is rejected, however late it lands (`testRelaunchedInstanceFencesOutTheOldOneEvenIfItKeepsRunning`). One window remains: an old save that lands between the new instance's `load()` and its epoch-claiming save is accepted and then superseded, so its contents are lost rather than merged. Closing that window needs compare-and-swap in the store, which this port does not require. `retire()` also stops an in-process instance outright. *Rejected:* "last write wins", which lets a stale instance silently undo a cancel.

**9. Bounded retention.** Live jobs are never dropped. Terminal jobs are kept up to `RelayPolicy.maxRetainedTerminalJobs` (default 50) and pruned oldest-first, and `forget(_:)` drops one explicitly. Memory and checkpoint size grow with live jobs and unresolved submissions, not with every job ever run.

### Crash-safety rules the code follows

- No force-unwraps or implicitly unwrapped optionals.
- Every server-supplied number reaches counters through `Saturating` (`Int.max`-derived, so it is 32-bit-safe).
- `seq == 0`, `seq == UInt64.max`, negative usage, `Int.max` usage, NaN/∞ retry inputs and negative or zero reorder windows, zero page sizes and negative page budgets are all tested.
- Each job's buffer is bounded by `reorderWindow`. The number of retained jobs is bounded by live jobs plus `maxRetainedTerminalJobs`. Each `reconcile` call is bounded by `maxBackfillPages`.

## Tests: 61 XCTest cases

| Suite | What it pins |
|---|---|
| `JobProjectionTests` (15) | Ordering, dedup, window bound, absorbing terminal, confidence downgrade, final immutability, phase monotonicity, boundary `seq`s, saturation, and the ledger conservation law across 200 seeds. |
| `ConvergenceCheckTests` (8) | Hand-pinned expected values. **Two deliberately broken reducers** (arrival-order and no-dedup) must be reported as diverged, and the real projection converges on the same 200 chaotic deliveries. |
| `JobRelayTests` (11) | Lost acknowledgement → one job. Exhausted submission → relaunch → same key. Backoff delays. **5 concurrent hints during a blocked fetch → exactly 1 fetch.** Paged backfill, hint-ahead-of-server stops, budget auto-cancel, cancel-after-finish, relaunch with persisted cancel intent, stale saves. |
| `JobRelayResilienceTests` (14) | **The key is in the store when the first request is sent**, and a `resume()` that runs while `submit` is suspended in that save cannot take over the key. A hint that joins an empty in-flight fetch still triggers a pass. A superseded instance's saves are fenced out by epoch, and a fresh install claims an epoch on `restore()`. A retired relay ignores everything, including the result of a fetch that was in flight. A failed cancel is retried without a relaunch, and repeated cancel requests send one. The budget trips exactly at the ceiling. An event beyond the window triggers an immediate fetch, and one reconcile never fetches more than `maxBackfillPages` pages. Terminal-job pruning keeps live jobs, and abandoning a pending submission bounds the pending list. |
| `RetryAndArithmeticTests` (7) | Backoff ceiling, hostile inputs, saturating helpers, clamping, chaos reproducibility. |
| `SimulationTests` (6) | End to end: 3 chaos profiles × 25 seeds converge with one job per intent. A double kill-and-relaunch mid-run. **Kills racing concurrent steps.** The default scenario's outcomes, with pinned figures. The budget job on lossy streams. An anti-vacuity check that the hostile stream really forces duplicates, reorders and backfills. |

## Build and test

```bash
git clone https://github.com/rajatslakhina/agent-job-relay-kit.git
cd agent-job-relay-kit
swift build -Xswiftc -warnings-as-errors
swift test
```

The library has no UIKit or SwiftUI dependency, so the same commands run on macOS and on Linux (Swift 6.0+). To see it running, use the companion demo app.

## Use it

```swift
.package(url: "https://github.com/rajatslakhina/agent-job-relay-kit.git", from: "1.0.0")
```

You supply two adapters: a `JobTransport` over your agent backend's HTTP API, and a durable `RelayStore` (a file, Core Data or SQLite) that honours `RelayCheckpoint.ranks(below:)`. The library ships only the in-memory reference store.

```swift
let relay = JobRelay(transport: YourAgentAPI(), store: YourDurableRelayStore())
await relay.restore()                 // on launch
await relay.resume()                  // finish what the last process left undone
let id = try await relay.submit(JobRequest(prompt: "Compare on-device vs cloud inference cost", tokenBudget: 20_000))

// Your stream client:      await relay.receive(events, via: .stream)
// Silent push handler:     await relay.receiveHint(id, highWater: cursor)
// Foreground / reconnect:  await relay.reconcileAll()
let view = await relay.projection(of: id)   // state, status, ledger
```

## Verification

_Written after CI reports._

import XCTest
@testable import AgentJobRelay

final class JobProjectionTests: XCTestCase {
    func testInOrderDeliveryAppliesEverythingAndCompletes() {
        var projection = JobProjection(jobID: Fixture.job)
        for event in Fixture.completedLog() {
            if case .applied = projection.ingest(event, via: .stream) { continue }
            XCTFail("event \(event.seq) was not applied")
        }
        XCTAssertEqual(projection.state.appliedThrough, 18)
        XCTAssertEqual(projection.state.tokensUsed, 8_870)
        XCTAssertEqual(projection.state.terminal, .completed)
        XCTAssertEqual(projection.state.phase?.name, "Reviewing")
        XCTAssertEqual(projection.state.artifacts.map(\.id), ["plan", "findings", "answer"])
        XCTAssertTrue(projection.state.artifacts.allSatisfy { $0.confidence == .final })
        XCTAssertEqual(projection.status, .completed)
        XCTAssertFalse(projection.isBehind)
    }

    func testRedeliveredUsageIsNotDoubleCounted() {
        var projection = JobProjection(jobID: Fixture.job)
        projection.ingest(Fixture.usage(1, 100), via: .stream)
        XCTAssertEqual(projection.ingest(Fixture.usage(1, 100), via: .backfill), .duplicate)
        XCTAssertEqual(projection.state.tokensUsed, 100)
        XCTAssertEqual(projection.ledger.duplicates, 1)
    }

    func testOutOfOrderEventsAreBufferedThenDrainedInOrderKeepingTheirChannel() {
        var projection = JobProjection(jobID: Fixture.job)
        XCTAssertEqual(projection.ingest(Fixture.fragment(2, "a", rev: 2, body: "new"), via: .backfill),
                       .buffered(missing: 1...1))
        XCTAssertEqual(projection.state.appliedThrough, 0)
        XCTAssertEqual(projection.firstGap, 1...1)
        XCTAssertEqual(projection.ingest(Fixture.fragment(1, "a", rev: 1, body: "old"), via: .stream), .applied(drained: 1))
        XCTAssertEqual(projection.state.artifacts.first?.body, "new", "rev 2 must win even though it arrived first")
        XCTAssertEqual(projection.ledger.reordered, 1)
        XCTAssertEqual(projection.ledger.appliedByChannel[.backfill], 1)
        XCTAssertEqual(projection.ledger.appliedByChannel[.stream], 1)
        XCTAssertNil(projection.firstGap)
    }

    func testEventsBeyondTheReorderWindowAreDroppedNotBuffered() {
        var projection = JobProjection(jobID: Fixture.job, reorderWindow: 3)
        XCTAssertEqual(projection.ingest(Fixture.usage(4, 1), via: .stream), .buffered(missing: 1...3))
        XCTAssertEqual(projection.ingest(Fixture.usage(5, 1), via: .stream), .beyondWindow(missing: 1...4))
        XCTAssertEqual(projection.bufferedCount, 1, "memory stays bounded by the window")
        XCTAssertEqual(projection.ledger.discardedBeyondWindow, 1)
        XCTAssertEqual(projection.knownHighWater, 5, "a dropped event still proves the log reaches its seq")
        XCTAssertEqual(projection.firstGap, 1...3, "the gap ends just before the lowest buffered event")
        XCTAssertTrue(projection.ledger.isConserved(buffered: projection.bufferedCount))
    }

    func testZeroOrNegativeWindowIsClampedToOne() {
        var projection = JobProjection(jobID: Fixture.job, reorderWindow: -5)
        XCTAssertEqual(projection.reorderWindow, 1)
        XCTAssertEqual(projection.ingest(Fixture.usage(2, 1), via: .stream), .buffered(missing: 1...1))
    }

    func testTerminalIsAbsorbingAndBufferedEventsPastItAreAccounted() {
        var projection = JobProjection(jobID: Fixture.job)
        projection.ingest(Fixture.usage(3, 50), via: .stream)        // buffered past the terminal
        projection.ingest(Fixture.terminal(2, .completed), via: .stream) // buffered
        projection.ingest(Fixture.usage(1, 10), via: .stream)         // applies 1, drains 2 (terminal)
        XCTAssertEqual(projection.state.terminal, .completed)
        XCTAssertEqual(projection.state.tokensUsed, 10, "nothing after the terminal event may change state")
        XCTAssertEqual(projection.ledger.afterTerminal, 1)
        XCTAssertEqual(projection.bufferedCount, 0)
        XCTAssertEqual(projection.ingest(Fixture.usage(9, 5), via: .stream), .afterTerminal)
        XCTAssertTrue(projection.ledger.isConserved(buffered: projection.bufferedCount))
    }

    func testFailureMarksProvisionalArtifactsUnconfirmedButKeepsFinalOnes() {
        var projection = JobProjection(jobID: Fixture.job)
        for event in Fixture.failedLog() { projection.ingest(event, via: .stream) }
        let byID = Dictionary(uniqueKeysWithValues: projection.state.artifacts.map { ($0.id, $0) })
        XCTAssertEqual(byID["plan"]?.confidence, .final)
        XCTAssertEqual(byID["findings"]?.confidence, .unconfirmed)
        XCTAssertNil(byID["answer"])
        XCTAssertEqual(projection.status, .failed(reason: "Tool call timed out: source 3 unreachable"))
    }

    func testFinalArtifactsAndOlderRevisionsCannotOverwrite() {
        var projection = JobProjection(jobID: Fixture.job)
        projection.ingest(Fixture.fragment(1, "a", rev: 3, body: "three"), via: .stream)
        projection.ingest(Fixture.fragment(2, "a", rev: 2, body: "two"), via: .stream)
        XCTAssertEqual(projection.state.artifacts.first?.body, "three")
        projection.ingest(Fixture.fragment(3, "a", rev: 4, body: "final", final: true), via: .stream)
        projection.ingest(Fixture.fragment(4, "a", rev: 5, body: "after final"), via: .stream)
        XCTAssertEqual(projection.state.artifacts.first?.body, "final")
        XCTAssertEqual(projection.ledger.staleRevisions, 2)
    }

    func testPhaseNeverMovesBackwards() {
        var projection = JobProjection(jobID: Fixture.job)
        projection.ingest(JobEvent(jobID: Fixture.job, seq: 1, payload: .phase(Phase(ordinal: 2, name: "Drafting"))), via: .stream)
        projection.ingest(JobEvent(jobID: Fixture.job, seq: 2, payload: .phase(Phase(ordinal: 0, name: "Planning"))), via: .stream)
        XCTAssertEqual(projection.state.phase?.name, "Drafting")
        XCTAssertEqual(projection.ledger.phaseRegressions, 1)
    }

    func testBoundarySequenceNumbersDoNotTrap() {
        var projection = JobProjection(jobID: Fixture.job)
        XCTAssertEqual(projection.ingest(Fixture.usage(0, 1), via: .stream), .duplicate)
        XCTAssertEqual(projection.ingest(Fixture.usage(UInt64.max, 1), via: .stream),
                       .beyondWindow(missing: 1...(UInt64.max - 1)))
        let state = JobState(jobID: Fixture.job)
        var nearTop = JobProjection(restoring: state, cancelRequested: false)
        nearTop.noteHighWater(UInt64.max)
        XCTAssertEqual(nearTop.firstGap, 1...UInt64.max)
    }

    func testHostileUsageValuesSaturateInsteadOfTrapping() {
        var projection = JobProjection(jobID: Fixture.job)
        projection.ingest(Fixture.usage(1, Int.max), via: .stream)
        projection.ingest(Fixture.usage(2, Int.max), via: .stream)
        projection.ingest(Fixture.usage(3, -1_000), via: .stream)
        XCTAssertEqual(projection.state.tokensUsed, Int.max)
    }

    func testEventsForAnotherJobAreRejected() {
        var projection = JobProjection(jobID: Fixture.job)
        XCTAssertEqual(projection.ingest(Fixture.usage(1, 5, JobID("other")), via: .stream), .wrongJob)
        XCTAssertEqual(projection.state.tokensUsed, 0)
        XCTAssertEqual(projection.ledger.wrongJob, 1)
    }

    func testCancelRequestedButServerCompletedFirstIsReportedHonestly() {
        var projection = JobProjection(jobID: Fixture.job)
        projection.requestCancel()
        XCTAssertEqual(projection.status, .cancelling)
        for event in Fixture.completedLog() { projection.ingest(event, via: .stream) }
        XCTAssertEqual(projection.status, .completedBeforeCancel)
    }

    func testLedgerIsConservedUnderChaoticDeliveryAcrossSeeds() {
        for seed in UInt64(1)...200 {
            var projection = JobProjection(jobID: Fixture.job, reorderWindow: 4)
            for event in Fixture.chaoticDelivery(of: Fixture.completedLog(), seed: seed) {
                projection.ingest(event, via: .stream)
                XCTAssertTrue(projection.ledger.isConserved(buffered: projection.bufferedCount), "seed \(seed)")
                XCTAssertLessThanOrEqual(projection.bufferedCount, 4, "seed \(seed)")
            }
        }
    }

    func testConservationCheckRejectsAMiscountedLedger() {
        var projection = JobProjection(jobID: Fixture.job)
        projection.ingest(Fixture.usage(2, 1), via: .stream)
        XCTAssertTrue(projection.ledger.isConserved(buffered: 1))
        XCTAssertFalse(projection.ledger.isConserved(buffered: 0), "an event that vanished from the buffer must break the law")
    }
}

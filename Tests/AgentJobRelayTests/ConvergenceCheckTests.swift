import XCTest
@testable import AgentJobRelay

final class ConvergenceCheckTests: XCTestCase {
    func testExpectedStateMatchesHandComputedValues() {
        // Pinned by hand from the script, not derived from JobProjection.
        let expected = ConvergenceCheck.expectedState(jobID: Fixture.job, serverLog: Fixture.completedLog())
        XCTAssertEqual(expected.appliedThrough, 18)
        XCTAssertEqual(expected.tokensUsed, 420 + 1_850 + 2_100 + 3_300 + 1_200)
        XCTAssertEqual(expected.terminal, .completed)
        XCTAssertEqual(expected.artifacts.map(\.revision), [2, 3, 3])
        XCTAssertEqual(expected.artifacts.last?.body,
                       "Answer on fixture, citing all three sources; caveat on the disputed figure.")
    }

    func testExpectedStateStopsAtTheFirstHole() {
        var log = Fixture.completedLog()
        log.remove(at: 5) // seq 6 missing
        let expected = ConvergenceCheck.expectedState(jobID: Fixture.job, serverLog: log)
        XCTAssertEqual(expected.appliedThrough, 5)
        XCTAssertNil(expected.terminal)
    }

    func testProjectionConvergesUnderChaoticDeliveryAcrossSeeds() {
        for seed in UInt64(1)...200 {
            var projection = JobProjection(jobID: Fixture.job)
            for event in Fixture.chaoticDelivery(of: Fixture.completedLog(), seed: seed) {
                projection.ingest(event, via: .stream)
            }
            XCTAssertEqual(ConvergenceCheck.divergences(device: projection.state, serverLog: Fixture.completedLog()), [],
                           "seed \(seed)")
        }
    }

    /// The check must catch the reducer most stream clients actually ship.
    func testCheckRejectsArrivalOrderReducer() {
        var caught = 0
        for seed in UInt64(1)...200 {
            var broken = ArrivalOrderReducer(jobID: Fixture.job)
            for event in Fixture.chaoticDelivery(of: Fixture.completedLog(), seed: seed) { broken.ingest(event) }
            if !ConvergenceCheck.divergences(device: broken.state, serverLog: Fixture.completedLog()).isEmpty { caught += 1 }
        }
        XCTAssertGreaterThan(caught, 150, "arrival-order application diverges on most chaotic deliveries")
    }

    func testCheckRejectsCompletedOverAHole() {
        // Terminal arrives, one usage event is lost: the broken reducer shows "completed"
        // with the wrong spend. The real projection refuses to apply the terminal at all.
        let log = Fixture.completedLog()
        var delivery = log
        delivery.remove(at: 15) // seq 16: usage 1,200
        var broken = ArrivalOrderReducer(jobID: Fixture.job)
        var real = JobProjection(jobID: Fixture.job)
        for event in delivery {
            broken.ingest(event)
            real.ingest(event, via: .stream)
        }
        XCTAssertEqual(broken.state.terminal, .completed)
        XCTAssertTrue(ConvergenceCheck.divergences(device: broken.state, serverLog: log)
            .contains(.tokens(device: 7_670, server: 8_870)))
        XCTAssertNil(real.state.terminal, "a completed badge is never shown over a hole")
        XCTAssertEqual(real.firstGap, 16...16)
    }

    func testCheckRejectsReducerWithoutDeduplication() {
        var broken = NoDedupReducer(jobID: Fixture.job)
        let log = Fixture.completedLog()
        for event in log + [log[1]] { broken.ingest(event) }
        XCTAssertTrue(ConvergenceCheck.divergences(device: broken.state, serverLog: log)
            .contains(.tokens(device: 8_870 + 420, server: 8_870)))
    }

    func testCheckReportsMissingUnexpectedAndConfidenceDifferences() {
        var device = ConvergenceCheck.expectedState(jobID: Fixture.job, serverLog: Fixture.failedLog())
        XCTAssertEqual(ConvergenceCheck.divergences(device: device, serverLog: Fixture.failedLog()), [])
        device.artifacts[1].confidence = .provisional
        device.artifacts.append(Artifact(id: "ghost", revision: 1, body: "", confidence: .final))
        let divergences = ConvergenceCheck.divergences(device: device, serverLog: Fixture.failedLog())
        XCTAssertTrue(divergences.contains(.artifactConfidence(id: "findings", device: .provisional, server: .unconfirmed)))
        XCTAssertTrue(divergences.contains(.artifactUnexpected(id: "ghost")))
        device.artifacts.removeAll()
        XCTAssertTrue(ConvergenceCheck.divergences(device: device, serverLog: Fixture.failedLog())
            .contains(.artifactMissing(id: "plan")))
    }

    func testEmptyLogOnlyMatchesAnEmptyDevice() {
        XCTAssertEqual(ConvergenceCheck.divergences(device: JobState(jobID: Fixture.job), serverLog: []), [])
        // A device that claims progress the server never logged must be reported.
        let ahead = ConvergenceCheck.expectedState(jobID: Fixture.job, serverLog: Fixture.completedLog())
        XCTAssertTrue(ConvergenceCheck.divergences(device: ahead, serverLog: [])
            .contains(.appliedThrough(device: 18, server: 0)))
    }
}

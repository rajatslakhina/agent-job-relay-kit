import XCTest
@testable import AgentJobRelay

final class RetryAndArithmeticTests: XCTestCase {
    func testCeilingDoublesThenCaps() {
        let policy = RetryPolicy(maxAttempts: 5, baseDelay: 0.5, maxDelay: 4)
        XCTAssertEqual((0..<6).map { policy.ceiling(forAttempt: $0) }, [0.5, 1, 2, 4, 4, 4])
    }

    func testHostileInputsNeverTrapOrProduceNaN() {
        let policy = RetryPolicy(maxAttempts: -3, baseDelay: .nan, maxDelay: .infinity)
        XCTAssertEqual(policy.maxAttempts, 1)
        XCTAssertEqual(policy.delay(forAttempt: Int.max, unit: .nan), 0)
        let normal = RetryPolicy(baseDelay: 1, maxDelay: 10)
        XCTAssertEqual(normal.ceiling(forAttempt: Int.max), 10)
        XCTAssertEqual(normal.ceiling(forAttempt: -7), 1)
        XCTAssertEqual(normal.delay(forAttempt: 2, unit: 7), 4, "unit is clamped to 1")
    }

    func testSaturatingHelpers() {
        XCTAssertEqual(Saturating.add(Int.max, 1), Int.max)
        XCTAssertEqual(Saturating.add(Int.min, -1), Int.min)
        XCTAssertEqual(Saturating.distance(from: 9, to: 3), 0)
        XCTAssertEqual(Saturating.clampToInt(UInt64.max), Int.max)
        XCTAssertEqual(Saturating.int(.nan), 0)
        XCTAssertEqual(Saturating.int(.infinity), Int.max)
        XCTAssertEqual(Saturating.int(-.infinity), Int.min)
        XCTAssertEqual(Saturating.int(1e300), Int.max)
        XCTAssertEqual(Saturating.int(-2.9), -2)
    }

    func testJobRequestClampsNegativeBudget() {
        XCTAssertEqual(JobRequest(prompt: "", tokenBudget: -10).tokenBudget, 0)
    }

    func testRelayPolicyClampsDegenerateValues() {
        let policy = RelayPolicy(reorderWindow: 0, pageSize: 0, maxBackfillPages: -1)
        XCTAssertEqual([policy.reorderWindow, policy.pageSize, policy.maxBackfillPages], [1, 1, 1])
    }

    func testChaosProfileClampsRates() {
        let profile = ChaosProfile(name: "x", dropRate: 2, duplicateRate: -1, delayRate: .nan, maxDelayTicks: 0)
        XCTAssertEqual([profile.dropRate, profile.duplicateRate, profile.delayRate], [1, 0, 0])
        XCTAssertEqual(profile.maxDelayTicks, 1)
    }

    func testChaosLinkIsReproducibleAndDeliversEverythingWhenClean() {
        let log = Fixture.completedLog()
        var a = ChaosLink(profile: .hostile, seed: 9)
        var b = ChaosLink(profile: .hostile, seed: 9)
        XCTAssertEqual(a.transmit(log), b.transmit(log))
        var c = ChaosLink(profile: .hostile, seed: 10)
        var d = ChaosLink(profile: .hostile, seed: 9)
        XCTAssertNotEqual(c.transmit(log), d.transmit(log), "the seed must actually change the run")
        XCTAssertEqual(ChaosProfile(name: "x", dropRate: 0, duplicateRate: 0, delayRate: 1, maxDelayTicks: Int.max).maxDelayTicks, 1_000)
        var clean = ChaosLink(profile: .clean, seed: 1)
        XCTAssertEqual(clean.transmit(log), log)
        XCTAssertEqual(clean.inFlight, 0)
    }
}

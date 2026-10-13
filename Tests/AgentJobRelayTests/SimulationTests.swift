import XCTest
@testable import AgentJobRelay

/// End-to-end: scripted backend + chaotic stream + push hints + relaunches.
final class SimulationTests: XCTestCase {
    func testEveryProfileConvergesAcrossSeeds() async {
        for profile in ChaosProfile.all {
            for seed in UInt64(1)...25 {
                let simulation = RelaySimulation(profile: profile, seed: seed)
                await simulation.runToCompletion()
                let snapshot = await simulation.snapshot()
                let label = "\(profile.name) seed \(seed)"
                XCTAssertTrue(snapshot.isFinished, label)
                XCTAssertTrue(snapshot.allConverged, "\(label): \(snapshot.jobs.map(\.divergences))")
                XCTAssertTrue(snapshot.allConserved, label)
                XCTAssertEqual(snapshot.server.jobsCreated, 3, label)
                XCTAssertEqual(snapshot.server.lostAcks, 1, label)
                XCTAssertEqual(snapshot.pendingSubmissions, 0, label)
            }
        }
    }

    func testRelaunchMidRunStillConverges() async {
        for seed in UInt64(1)...25 {
            let simulation = RelaySimulation(profile: .hostile, seed: seed)
            for _ in 0..<4 { await simulation.step() }
            await simulation.killAndRelaunch()
            for _ in 0..<3 { await simulation.step() }
            await simulation.killAndRelaunch()
            await simulation.runToCompletion()
            let snapshot = await simulation.snapshot()
            XCTAssertEqual(snapshot.relaunches, 2)
            XCTAssertTrue(snapshot.allConverged, "seed \(seed): \(snapshot.jobs.map(\.divergences))")
            XCTAssertEqual(snapshot.server.jobsCreated, 3, "seed \(seed)")
        }
    }

    func testDefaultScenarioOutcomes() async {
        let simulation = RelaySimulation(profile: .clean, seed: 1)
        await simulation.runToCompletion()
        let jobs = await simulation.snapshot().jobs
        XCTAssertEqual(jobs.map(\.projection.status), [
            .completed,
            .failed(reason: "Tool call timed out: source 3 unreachable"),
            .cancelled,
        ])
        XCTAssertEqual(jobs[1].projection.state.artifacts.map(\.confidence), [.final, .unconfirmed])
        // Pinned: the README quotes these figures. On a clean stream the device sees the
        // 3,300-token usage event the tick it is released and cancels; the next tick ends the job.
        XCTAssertEqual(jobs[2].projection.state.tokensUsed, 7_670)
        let snapshot = await simulation.snapshot()
        XCTAssertEqual(snapshot.server.submitCalls, 4)
        XCTAssertEqual(snapshot.health.autoCancels, 1)
    }

    func testBudgetJobAtTheDemoSeedOnLossyStreams() async {
        // The README states the brake did not save anything here: 8,870 is the whole script.
        for profile in [ChaosProfile.flaky, .hostile] {
            let simulation = RelaySimulation(profile: profile, seed: 2026)
            await simulation.runToCompletion()
            let jobs = await simulation.snapshot().jobs
            XCTAssertEqual(jobs[2].projection.state.tokensUsed, 8_870, profile.name)
            XCTAssertEqual(jobs[2].projection.status, .cancelled, profile.name)
        }
    }

    func testKillRacingInFlightStepsStillConverges() async {
        // Kill-and-relaunch fired concurrently with steps: the simulation's FIFO lock and the
        // relay's epoch fencing must keep the old instance from corrupting the new one.
        for seed in UInt64(1)...15 {
            let simulation = RelaySimulation(profile: .hostile, seed: seed)
            await withTaskGroup(of: Void.self) { group in
                for index in 0..<8 {
                    group.addTask {
                        if index % 3 == 2 { await simulation.killAndRelaunch() } else { await simulation.step() }
                    }
                }
            }
            await simulation.runToCompletion()
            let snapshot = await simulation.snapshot()
            XCTAssertTrue(snapshot.allConverged, "seed \(seed): \(snapshot.jobs.map(\.divergences))")
            XCTAssertEqual(snapshot.server.jobsCreated, 3, "seed \(seed)")
            XCTAssertEqual(snapshot.relaunches, 2, "seed \(seed)")
        }
    }

    func testHostileStreamActuallyExercisesTheRecoveryPaths() async {
        // Guards against a vacuous end-to-end test: if the chaos stopped dropping or
        // duplicating, the convergence assertions above would pass without proving anything.
        let simulation = RelaySimulation(profile: .hostile, seed: 3)
        await simulation.runToCompletion()
        let snapshot = await simulation.snapshot()
        let ledgers = snapshot.jobs.map(\.projection.ledger)
        XCTAssertGreaterThan(ledgers.map(\.duplicates).reduce(0, +), 0)
        XCTAssertGreaterThan(ledgers.map { $0.appliedByChannel[.backfill] ?? 0 }.reduce(0, +), 0)
        XCTAssertGreaterThan(ledgers.map(\.reordered).reduce(0, +), 0)
        XCTAssertGreaterThan(snapshot.health.backfillFetches, 0)
    }
}

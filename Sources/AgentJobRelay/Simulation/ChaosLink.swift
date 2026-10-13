/// How badly the foreground stream misbehaves. Rates are probabilities in [0, 1].
public struct ChaosProfile: Hashable, Sendable {
    public let name: String
    public let dropRate: Double
    public let duplicateRate: Double
    public let delayRate: Double
    /// A delayed event arrives 1...maxDelayTicks ticks later — i.e. out of order. Clamped to 1...1,000.
    public let maxDelayTicks: Int

    public init(name: String, dropRate: Double, duplicateRate: Double, delayRate: Double, maxDelayTicks: Int) {
        func clamp(_ value: Double) -> Double { value.isFinite ? min(max(0, value), 1) : 0 }
        self.name = name
        self.dropRate = clamp(dropRate)
        self.duplicateRate = clamp(duplicateRate)
        self.delayRate = clamp(delayRate)
        // Capped so `tick + lag` can never overflow; a 1,000-tick reorder is already absurd.
        self.maxDelayTicks = min(max(1, maxDelayTicks), 1_000)
    }

    public static let clean = ChaosProfile(name: "Clean", dropRate: 0, duplicateRate: 0, delayRate: 0, maxDelayTicks: 1)
    public static let flaky = ChaosProfile(name: "Flaky", dropRate: 0.15, duplicateRate: 0.2, delayRate: 0.25, maxDelayTicks: 3)
    public static let hostile = ChaosProfile(name: "Hostile", dropRate: 0.35, duplicateRate: 0.4, delayRate: 0.4, maxDelayTicks: 5)
    public static let all: [ChaosProfile] = [.clean, .flaky, .hostile]
}

/// A seeded, lossy, duplicating, reordering pipe standing in for a mobile network stream.
public struct ChaosLink: Sendable {
    public private(set) var profile: ChaosProfile
    private var rng: SplitMix64
    private var tick = 0
    private var delayed: [(due: Int, event: JobEvent)] = []

    public init(profile: ChaosProfile, seed: UInt64) {
        self.profile = profile
        self.rng = SplitMix64(seed: seed)
    }

    public var inFlight: Int { delayed.count }

    /// Sends this tick's `events`; returns what the device receives this tick.
    public mutating func transmit(_ events: [JobEvent]) -> [JobEvent] {
        Saturating.increment(&tick)
        var delivered: [JobEvent] = []
        let due = delayed.filter { $0.due <= tick }
        delayed.removeAll { $0.due <= tick }
        delivered += due.map(\.event)

        for event in events {
            if rng.unit() < profile.dropRate { continue }
            if rng.unit() < profile.delayRate {
                let lag = 1 + Int(rng.next() % UInt64(profile.maxDelayTicks)) // maxDelayTicks >= 1
                delayed.append((due: Saturating.add(tick, lag), event: event))
            } else {
                delivered.append(event)
            }
            if rng.unit() < profile.duplicateRate { delivered.append(event) }
        }
        return delivered
    }

    /// Everything still in flight, delivered at once (the stream flushing on reconnect).
    public mutating func drain() -> [JobEvent] {
        defer { delayed.removeAll() }
        return delayed.sorted { $0.due < $1.due }.map(\.event)
    }
}

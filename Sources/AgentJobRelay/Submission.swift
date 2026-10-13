import Foundation

/// Client-generated key that makes job submission safe to retry.
///
/// The key is created and **persisted before the first network attempt**. If the app is
/// killed after the server accepted the job but before the response arrived, the relaunch
/// resubmits with the same key and the server returns the existing job instead of
/// starting (and billing) a second one.
public struct IdempotencyKey: Hashable, Sendable, Codable, CustomStringConvertible {
    public let rawValue: String
    public init(_ rawValue: String) { self.rawValue = rawValue }
    public var description: String { rawValue }
}

/// What the user asked the agent to do.
public struct JobRequest: Hashable, Sendable, Codable {
    public let prompt: String
    /// Client-side spend ceiling; `0` disables it. When the projection's `tokensUsed` reaches
    /// it, the relay requests cancellation. The server's own limit is the real enforcement; this one
    /// exists so the device stops a runaway job without waiting for a human.
    public let tokenBudget: Int

    public init(prompt: String, tokenBudget: Int) {
        self.prompt = prompt
        self.tokenBudget = max(0, tokenBudget)
    }
}

/// A submission whose outcome the device does not know yet.
public struct PendingSubmission: Hashable, Sendable, Codable {
    public let key: IdempotencyKey
    public let request: JobRequest
    public internal(set) var attempts: Int
}

/// Exponential backoff with full jitter: `delay = random(0 ... min(cap, base * 2^attempt))`.
///
/// Full jitter rather than equal jitter: after a backend blip, thousands of devices retry
/// at once, and full jitter spreads them across the whole window.
public struct RetryPolicy: Hashable, Sendable, Codable {
    public let maxAttempts: Int
    public let baseDelay: Double
    public let maxDelay: Double

    public init(maxAttempts: Int = 4, baseDelay: Double = 0.5, maxDelay: Double = 30) {
        self.maxAttempts = max(1, maxAttempts)
        // NaN or negative inputs collapse to 0 rather than poisoning every delay.
        self.baseDelay = baseDelay.isFinite ? max(0, baseDelay) : 0
        self.maxDelay = maxDelay.isFinite ? max(0, maxDelay) : 0
    }

    /// Upper bound of the jitter window for a 0-based retry `attempt`.
    public func ceiling(forAttempt attempt: Int) -> Double {
        // Clamp the exponent so 2^attempt stays finite; the cap dominates long before 62.
        let exponent = Double(min(max(0, attempt), 62))
        let raw = baseDelay * pow(2, exponent)
        return raw.isFinite ? min(maxDelay, raw) : maxDelay
    }

    /// `unit` is a uniform sample in [0, 1); injected so tests are deterministic.
    public func delay(forAttempt attempt: Int, unit: Double) -> Double {
        let clamped = unit.isFinite ? min(max(0, unit), 1) : 0
        return ceiling(forAttempt: attempt) * clamped
    }
}

/// Everything the relay must survive a process kill with.
public struct RelayCheckpoint: Hashable, Sendable, Codable {
    public struct Job: Hashable, Sendable, Codable {
        public let request: JobRequest
        public let state: JobState
        public let cancelRequested: Bool
    }

    /// Monotonic save counter. Two saves issued from one actor can reach the store in
    /// either order across suspension points, so stores keep the highest generation they
    /// have seen rather than the last call that arrived.
    public var generation: UInt64 = 0
    /// Fencing token. Every `JobRelay.restore()` claims `epoch + 1`, and stores rank
    /// checkpoints by `(epoch, generation)`. A relay instance that was superseded — the
    /// previous process, or a second instance in a background task or extension sharing the
    /// same store — can then never overwrite the newer instance's state, however late its
    /// in-flight saves land.
    public var epoch: UInt64 = 0
    public var pending: [PendingSubmission] = []
    public var jobs: [Job] = []

    public init() {}

    /// `true` when `self` is older than `other` and must not replace it.
    public func ranks(below other: RelayCheckpoint) -> Bool {
        (epoch, generation) < (other.epoch, other.generation)
    }
}

/// Durable storage for the relay's checkpoint (file, Core Data, keychain-backed — the relay
/// does not care). `save` is awaited before any network call that depends on it.
/// Implementations must ignore a checkpoint that ranks below the stored one by
/// `(epoch, generation)` — see `RelayCheckpoint.ranks(below:)`.
public protocol RelayStore: Sendable {
    func load() async -> RelayCheckpoint?
    func save(_ checkpoint: RelayCheckpoint) async
}

/// A store that survives "relaunches" within one process — used by tests and the demo to
/// simulate a kill-and-relaunch without a filesystem.
public actor InMemoryRelayStore: RelayStore {
    public private(set) var saveCount = 0
    private var checkpoint: RelayCheckpoint?

    public init(_ checkpoint: RelayCheckpoint? = nil) { self.checkpoint = checkpoint }

    public func load() async -> RelayCheckpoint? { checkpoint }

    public private(set) var staleSavesIgnored = 0

    public func save(_ checkpoint: RelayCheckpoint) async {
        if let current = self.checkpoint, checkpoint.ranks(below: current) {
            staleSavesIgnored += 1
            return
        }
        self.checkpoint = checkpoint
        saveCount += 1
    }
}

/// The network boundary. A production implementation wraps the agent backend's HTTP API;
/// the stream itself is fed into the relay by the app's networking layer via `receive`.
public protocol JobTransport: Sendable {
    /// Must be idempotent on `key`: the same key returns the same `JobID`.
    func submit(_ request: JobRequest, key: IdempotencyKey) async throws -> JobID
    /// Events with `seq > after`, ascending, at most `limit` of them.
    func fetchEvents(_ job: JobID, after seq: UInt64, limit: Int) async throws -> [JobEvent]
    func cancel(_ job: JobID) async throws
}

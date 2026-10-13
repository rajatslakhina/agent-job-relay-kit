import Foundation

/// Server-assigned identity of a long-running agent job.
public struct JobID: Hashable, Sendable, Codable, CustomStringConvertible {
    public let rawValue: String
    public init(_ rawValue: String) { self.rawValue = rawValue }
    public var description: String { rawValue }
}

/// A phase of a multi-phase agent job (e.g. planning → researching → drafting → reviewing).
///
/// Phases are ordered by `ordinal`, not by name, so a backend can rename or add phases
/// without the client hard-coding a list. A projection never moves to a lower ordinal.
public struct Phase: Hashable, Sendable, Codable, Comparable {
    public let ordinal: Int
    public let name: String
    public init(ordinal: Int, name: String) {
        self.ordinal = ordinal
        self.name = name
    }
    public static func < (lhs: Phase, rhs: Phase) -> Bool { lhs.ordinal < rhs.ordinal }
}

/// One revision of a result the agent is producing.
///
/// Fragments carry the *whole* body of a revision rather than a delta. That costs bandwidth,
/// but it makes a fragment idempotent and lets a backfill skip straight to the newest revision.
public struct ArtifactFragment: Hashable, Sendable, Codable {
    public let artifactID: String
    public let revision: Int
    public let body: String
    /// The agent will not revise this artifact again.
    public let isFinal: Bool

    public init(artifactID: String, revision: Int, body: String, isFinal: Bool) {
        self.artifactID = artifactID
        self.revision = revision
        self.body = body
        self.isFinal = isFinal
    }
}

/// How a job ended, as decided by the server. The client never invents a terminal state.
public enum Terminal: Hashable, Sendable, Codable {
    case completed
    case failed(reason: String)
    case cancelled
}

/// One entry in a job's server-side event log.
///
/// Contract with the backend: `seq` is dense and 1-based per job (1, 2, 3, …), and the
/// terminal event, if any, carries the highest `seq` the job will ever emit.
public struct JobEvent: Hashable, Sendable, Codable {
    public enum Payload: Hashable, Sendable, Codable {
        case phase(Phase)
        case fragment(ArtifactFragment)
        /// Incremental model tokens consumed since the previous usage event.
        case usage(tokens: Int)
        case terminal(Terminal)
    }

    public let jobID: JobID
    public let seq: UInt64
    public let payload: Payload

    public init(jobID: JobID, seq: UInt64, payload: Payload) {
        self.jobID = jobID
        self.seq = seq
        self.payload = payload
    }
}

/// The path an event took to reach the device. Every channel is at-least-once and
/// unordered with respect to the others; the projection is what makes them agree.
public enum Channel: String, Hashable, Sendable, Codable, CaseIterable {
    /// Long-lived server-sent stream while the app is in the foreground.
    case stream
    /// Paged fetch issued by the relay to fill a gap or catch up after suspension.
    case backfill
}

/// Saturating arithmetic used anywhere a server-supplied number reaches a counter.
///
/// A token-usage event with a hostile or buggy value must not trap the app, so every
/// accumulation goes through these helpers instead of `+`. The ceiling is `Int.max`
/// of the running platform (32-bit on watchOS), never a hard-coded 64-bit literal.
enum Saturating {
    static func add(_ lhs: Int, _ rhs: Int) -> Int {
        let (sum, overflow) = lhs.addingReportingOverflow(rhs)
        if overflow { return rhs > 0 ? Int.max : Int.min }
        return sum
    }

    static func increment(_ value: inout Int) {
        value = add(value, 1)
    }

    /// `lhs - rhs` for sequence numbers, clamped to 0 when `rhs >= lhs`.
    static func distance(from rhs: UInt64, to lhs: UInt64) -> UInt64 {
        lhs > rhs ? lhs - rhs : 0
    }

    /// Converts a `UInt64` distance to `Int`, saturating at `Int.max`.
    static func clampToInt(_ value: UInt64) -> Int {
        value > UInt64(Int.max) ? Int.max : Int(value)
    }

    /// Converts a `Double` to `Int` without trapping on NaN, infinity or out-of-range values.
    static func int(_ value: Double) -> Int {
        guard value.isFinite else { return value.isNaN ? 0 : (value > 0 ? Int.max : Int.min) }
        if value >= Double(Int.max) { return Int.max }
        if value <= Double(Int.min) { return Int.min }
        return Int(value)
    }
}

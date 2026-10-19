package node

import (
	"math"
	"time"
)

const (
	// LeaseDeletePenaltyUnits is how many penalty units one lease deletion adds.
	LeaseDeletePenaltyUnits int32 = 1
	// LeaseDeletePenaltyHalfLife is the time for the accumulated penalty to decay to half.
	LeaseDeletePenaltyHalfLife = 6 * time.Hour
	// LeaseDeletePenaltyStep is the effective-latency cost of one penalty unit.
	LeaseDeletePenaltyStep = 500 * time.Millisecond
)

// EffectivePenaltyNs returns the decayed penalty expressed as equivalent latency in nanoseconds.
// It returns 0 when there is no penalty. Non-positive age clamps to 0.
func EffectivePenaltyNs(units int32, updatedNs int64, now time.Time) float64 {
	if units <= 0 || updatedNs <= 0 {
		return 0
	}
	age := now.Sub(time.Unix(0, updatedNs))
	if age < 0 {
		age = 0
	}
	decay := math.Pow(0.5, age.Seconds()/LeaseDeletePenaltyHalfLife.Seconds())
	return float64(units) * float64(LeaseDeletePenaltyStep) * decay
}

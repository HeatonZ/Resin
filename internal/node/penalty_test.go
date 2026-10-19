package node

import (
	"math"
	"testing"
	"time"
)

func TestEffectivePenaltyNs(t *testing.T) {
	now := time.Unix(1700000000, 0)

	tests := []struct {
		name      string
		units     int32
		updatedNs int64
		want      float64
	}{
		{
			name:      "zero units",
			units:     0,
			updatedNs: now.UnixNano(),
			want:      0,
		},
		{
			name:      "negative units",
			units:     -3,
			updatedNs: now.UnixNano(),
			want:      0,
		},
		{
			name:      "zero updated timestamp",
			units:     2,
			updatedNs: 0,
			want:      0,
		},
		{
			name:      "negative updated timestamp",
			units:     2,
			updatedNs: -1,
			want:      0,
		},
		{
			name:      "age zero equals units times step",
			units:     4,
			updatedNs: now.UnixNano(),
			want:      4 * float64(LeaseDeletePenaltyStep),
		},
		{
			name:      "age one half life halves the penalty",
			units:     1,
			updatedNs: now.Add(-LeaseDeletePenaltyHalfLife).UnixNano(),
			want:      float64(LeaseDeletePenaltyStep) / 2,
		},
		{
			name:      "age two half lives quarters the penalty",
			units:     8,
			updatedNs: now.Add(-2 * LeaseDeletePenaltyHalfLife).UnixNano(),
			want:      8 * float64(LeaseDeletePenaltyStep) / 4,
		},
		{
			name:      "future timestamp clamps to age zero",
			units:     2,
			updatedNs: now.Add(time.Hour).UnixNano(),
			want:      2 * float64(LeaseDeletePenaltyStep),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := EffectivePenaltyNs(tc.units, tc.updatedNs, now)
			if math.Abs(got-tc.want) > 1e-6 {
				t.Fatalf("EffectivePenaltyNs(%d, %d) = %v, want %v", tc.units, tc.updatedNs, got, tc.want)
			}
		})
	}
}

package routing

import (
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

const maxLowLatencyIdleSamples = 16

var ErrNoAvailableNodes = errors.New("no available nodes")

var randomRouteRNGPool = sync.Pool{
	New: func() any {
		return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	},
}

// randomRoute selects a routable node using P2C with latency/load scoring.
// The cooldown policy scans the routable view to sample only nodes outside its
// one-hour allocation window when any such node exists.
// Post-pick race handling (node removed right after selection) is handled by
// the caller in RouteRequest.
func randomRoute(
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
	targetDomain string,
	authorities []string,
	p2cWindow time.Duration,
	histories ...*NodeAllocationHistory,
) (node.Hash, error) {
	view := plat.View()
	size := view.Size()
	if size == 0 {
		return node.Zero, ErrNoAvailableNodes
	}

	rng := randomRouteRNGPool.Get().(*rand.Rand)
	defer randomRouteRNGPool.Put(rng)

	if plat.AllocationPolicy == platform.AllocationPolicyPreferLowLatencyAndIdle && len(histories) > 0 {
		var freshFirst, freshSecond node.Hash
		var freshCount int
		var liveFirst, liveSecond node.Hash
		var liveCount int
		var attempts int
		seen := make(map[node.Hash]struct{}, maxLowLatencyIdleSamples)
		nowNs := time.Now().UnixNano()
		for attempts = 0; attempts < maxLowLatencyIdleSamples && attempts < size; attempts++ {
			candidate, ok := view.RandomPick(rng)
			if !ok {
				break
			}
			if _, duplicate := seen[candidate]; duplicate {
				continue
			}
			seen[candidate] = struct{}{}
			if _, ok := pool.GetEntry(candidate); !ok {
				continue
			}
			liveCount++
			samplePair(rng, candidate, liveCount, &liveFirst, &liveSecond)
			if histories[0].WasRecentlyAllocated(candidate, nowNs) {
				continue
			}
			freshCount++
			samplePair(rng, candidate, freshCount, &freshFirst, &freshSecond)
		}
		if freshCount == 1 {
			return freshFirst, nil
		}
		if freshCount >= 2 {
			return chooseByScore(freshFirst, freshSecond, plat, stats, pool, targetDomain, authorities, p2cWindow)
		}
		if liveCount == 1 {
			return liveFirst, nil
		}
		if liveCount >= 2 {
			return chooseByScore(liveFirst, liveSecond, plat, stats, pool, targetDomain, authorities, p2cWindow)
		}
		return node.Zero, ErrNoAvailableNodes
	}

	// Sample k distinct candidates. k defaults to 3 (see candidateSamples) so the
	// score gets one more chance per allocation to reject a slow or penalised node;
	// RESIN_P2C_CANDIDATES=2 restores the plain pairwise P2C behaviour.
	k := candidateSamples()
	if size < k {
		k = size
	}
	candidates := sampleCandidates(view, rng, k)
	if len(candidates) == 0 {
		return node.Zero, ErrNoAvailableNodes
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}

	return chooseBest(candidates, plat, stats, pool, targetDomain, authorities, p2cWindow)
}

func samplePair(rng *rand.Rand, candidate node.Hash, count int, first, second *node.Hash) {
	switch count {
	case 1:
		*first = candidate
	case 2:
		*second = candidate
	default:
		if index := rng.IntN(count); index < 2 {
			if index == 0 {
				*first = candidate
			} else {
				*second = candidate
			}
		}
	}
}

func chooseByScore(
	h1, h2 node.Hash,
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
	targetDomain string,
	authorities []string,
	p2cWindow time.Duration,
) (node.Hash, error) {
	lat1, lat2 := compareLatencies(h1, h2, pool, targetDomain, authorities, p2cWindow)
	s1 := calculateScore(h1, lat1, plat, stats, pool)
	s2 := calculateScore(h2, lat2, plat, stats, pool)
	if s1 < s2 {
		return h1, nil
	}
	return h2, nil // favor h2 on tie
}

// compareLatencies determines the latency values for h1 and h2.
// Implements the 3-level comparison logic:
// 1. Target domain present in both and recent.
// 2. Common authority domains present in both and recent.
// 3. Fallback to 0 (empty) for both.
func compareLatencies(
	h1, h2 node.Hash,
	pool PoolAccessor,
	target string,
	authorities []string,
	window time.Duration,
) (time.Duration, time.Duration) {
	e1, ok1 := pool.GetEntry(h1)
	e2, ok2 := pool.GetEntry(h2)
	if !ok1 || !ok2 || e1.LatencyTable == nil || e2.LatencyTable == nil {
		return 0, 0
	}

	now := time.Now()

	// 1. Target domain check.
	// target can be empty if extracted domain is invalid/empty, handle gracefully.
	lat1, ok1 := lookupRecentDomainLatency(e1, target, now, window)
	lat2, ok2 := lookupRecentDomainLatency(e2, target, now, window)
	if ok1 && ok2 {
		return lat1, lat2
	}

	// 2. Authority intersection check.
	lat1, lat2, ok := averageComparableAuthorityLatencies(e1, e2, authorities, now, window)
	if ok {
		return lat1, lat2
	}

	// 3. Fallback.
	return 0, 0
}

func isRecent(t time.Time, now time.Time, window time.Duration) bool {
	return now.Sub(t) <= window
}

// calculateScore computes the score for a node based on platform allocation policy.
// Lower is better.
func calculateScore(
	h node.Hash,
	latency time.Duration,
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
) float64 {
	entry, _ := pool.GetEntry(h)
	// If entry is nil (race), treat as high load/latency?
	// But we hold ref via pool, only deletion removes it.
	// Assuming existence since we just picked it from view.

	// Lease count from stats.
	var leaseCount int64
	if entry != nil {
		ip := entry.GetEgressIP()
		if ip.IsValid() {
			leaseCount = stats.Get(ip)
		}
	}

	// If latency is 0 (empty/incompatible), score = LeaseCount strictly.
	var base float64
	if latency <= 0 {
		base = float64(leaseCount)
	} else {
		// Policy-based scoring.
		switch plat.AllocationPolicy {
		case platform.AllocationPolicyPreferLowLatency:
			base = float64(latency)
		case platform.AllocationPolicyPreferLowLatencyAndIdle:
			base = float64(latency)
		case platform.AllocationPolicyPreferIdleIP:
			base = float64(leaseCount)
		case platform.AllocationPolicyBalanced:
			fallthrough
		default:
			// (LeaseCount + 1) * Latency
			base = float64(leaseCount+1) * float64(latency)
		}
	}

	// Lease-deletion penalty: graded, time-decayed, applied in the same
	// (nanosecond-latency) dimension as the policy score. Lower is better, so
	// a penalised node loses P2C comparisons but is not hard-excluded.
	// Under AllocationPolicyBalanced the base is latency*leaseCount, so the
	// additive penalty is only a coarse de-weighting there.
	var penalty float64
	if entry != nil {
		penalty = node.EffectivePenaltyNs(entry.PenaltyUnits.Load(), entry.PenaltyUpdated.Load(), time.Now())
	}
	return base + penalty
}

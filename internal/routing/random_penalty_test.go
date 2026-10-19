package routing

import (
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

// newPenaltyScorePool builds a one-node pool usable by calculateScore.
func newPenaltyScorePool(t *testing.T) (*topology.GlobalNodePool, node.Hash) {
	t.Helper()

	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "url", true, false)
	subMgr.Register(sub)

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	raw := []byte(`{"type":"ss","server":"7.7.7.7","port":443}`)
	h := node.HashFromRawOptions(raw)
	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	sub.SwapManagedNodes(mn)
	pool.AddNodeFromSub(h, raw, "s1")

	if _, ok := pool.GetEntry(h); !ok {
		t.Fatalf("node %s missing after add", h.Hex())
	}
	return pool, h
}

func newPenaltyScorePlatform() *platform.Platform {
	plat := platform.NewPlatform("plat-1", "Default", nil, nil)
	plat.AllocationPolicy = platform.AllocationPolicyPreferLowLatencyAndIdle
	return plat
}

// TestCalculateScore_IncludesLeasePenalty proves the allocator actually sees the
// penalty: a penalised node must score strictly worse (higher) by exactly the
// equivalent latency of one unit, so P2C prefers its peers.
func TestCalculateScore_IncludesLeasePenalty(t *testing.T) {
	pool, h := newPenaltyScorePool(t)
	plat := newPenaltyScorePlatform()
	stats := NewIPLoadStats()
	const latency = 200 * time.Millisecond

	before := calculateScore(h, latency, plat, stats, pool)

	pool.ApplyNodePenalty(h, node.LeaseDeletePenaltyUnits)

	after := calculateScore(h, latency, plat, stats, pool)

	want := before + float64(node.LeaseDeletePenaltyStep)
	if after != want {
		t.Fatalf("score after penalty = %v, want %v (before %v + step %v)",
			after, want, before, float64(node.LeaseDeletePenaltyStep))
	}
	if after <= before {
		t.Fatalf("penalty did not raise the score: %v -> %v", before, after)
	}
}

// TestCalculateScore_PenaltyAppliesOnZeroLatencyPath guards the fallback branch:
// nodes without a measured latency must still be de-prioritised after a penalty.
func TestCalculateScore_PenaltyAppliesOnZeroLatencyPath(t *testing.T) {
	pool, h := newPenaltyScorePool(t)
	plat := newPenaltyScorePlatform()
	stats := NewIPLoadStats()

	before := calculateScore(h, 0, plat, stats, pool)
	pool.ApplyNodePenalty(h, node.LeaseDeletePenaltyUnits)
	after := calculateScore(h, 0, plat, stats, pool)

	if before != 0 {
		t.Fatalf("baseline zero-latency score = %v, want 0", before)
	}
	if after != float64(node.LeaseDeletePenaltyStep) {
		t.Fatalf("zero-latency score after penalty = %v, want %v", after, float64(node.LeaseDeletePenaltyStep))
	}
}

// TestCalculateScore_PenaltyDecays guards the decay: a penalty stamped one
// half-life ago must cost half as much, so a node is never permanently pinned to
// the bottom of the ranking.
func TestCalculateScore_PenaltyDecays(t *testing.T) {
	pool, h := newPenaltyScorePool(t)
	plat := newPenaltyScorePlatform()
	stats := NewIPLoadStats()
	const latency = 200 * time.Millisecond

	base := calculateScore(h, latency, plat, stats, pool)
	pool.ApplyNodePenalty(h, node.LeaseDeletePenaltyUnits)
	fresh := calculateScore(h, latency, plat, stats, pool) - base

	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("node entry vanished")
	}
	entry.PenaltyUpdated.Store(time.Now().Add(-node.LeaseDeletePenaltyHalfLife).UnixNano())

	aged := calculateScore(h, latency, plat, stats, pool) - base

	want := fresh / 2
	if diff := aged - want; diff > 1e-6 || diff < -1e-6 {
		t.Fatalf("penalty after one half-life = %v, want %v (fresh %v)", aged, want, fresh)
	}
}

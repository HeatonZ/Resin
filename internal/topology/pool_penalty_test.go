package topology

import (
	"net/netip"
	"testing"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/subscription"
)

// TestApplyNodePenalty_SingleNodeOnly verifies a lease-deletion penalty lands on
// exactly one node: a sibling sharing the same egress IP must stay untouched.
// Penalising by egress IP instead of node hash would be a behaviour change.
func TestApplyNodePenalty_SingleNodeOnly(t *testing.T) {
	pool, subMgr := newHealthTestPool(3)
	sub := subMgr.Lookup("s1")

	a := addTestNode(pool, sub, `{"type":"ss","server":"1.1.1.1","port":443}`)
	b := addTestNode(pool, sub, `{"type":"ss","server":"2.2.2.2","port":443}`)

	// Same egress IP on both nodes: the penalty must not spread by IP.
	for _, h := range []node.Hash{a, b} {
		entry, ok := pool.GetEntry(h)
		if !ok {
			t.Fatalf("node %s missing after add", h.Hex())
		}
		entry.SetEgressIP(netip.MustParseAddr("198.2.214.3"))
	}

	pool.ApplyNodePenalty(a, node.LeaseDeletePenaltyUnits)

	entryA, _ := pool.GetEntry(a)
	entryB, _ := pool.GetEntry(b)
	if got := entryA.PenaltyUnits.Load(); got != node.LeaseDeletePenaltyUnits {
		t.Fatalf("penalised node units = %d, want %d", got, node.LeaseDeletePenaltyUnits)
	}
	if got := entryB.PenaltyUnits.Load(); got != 0 {
		t.Fatalf("same-egress sibling units = %d, want 0 (penalty must be per node, not per egress IP)", got)
	}
	if entryA.PenaltyUpdated.Load() == 0 {
		t.Fatal("penalised node has units but no PenaltyUpdated timestamp")
	}
	if entryB.PenaltyUpdated.Load() != 0 {
		t.Fatal("sibling got a PenaltyUpdated timestamp it should never have received")
	}
}

// TestApplyNodePenalty_SurvivesProbeSuccess guards the reason this penalty is a
// separate field from failure_count: a successful probe resets failure_count
// (probe targets are reachable even from a 403-blocked egress), so a penalty
// stored there would evaporate within one probe cycle.
func TestApplyNodePenalty_SurvivesProbeSuccess(t *testing.T) {
	pool, subMgr := newHealthTestPool(3)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","server":"3.3.3.3","port":443}`)

	pool.RecordResult(h, false)
	pool.RecordResult(h, false)
	pool.ApplyNodePenalty(h, node.LeaseDeletePenaltyUnits)

	// Probe succeeds against a reachable target: failure_count resets, circuit closes.
	pool.RecordResult(h, true)

	entry, _ := pool.GetEntry(h)
	if got := entry.FailureCount.Load(); got != 0 {
		t.Fatalf("failure_count = %d, want 0 after probe success", got)
	}
	if got := entry.PenaltyUnits.Load(); got != node.LeaseDeletePenaltyUnits {
		t.Fatalf("penalty after probe success = %d, want %d (must not be cleared like failure_count)",
			got, node.LeaseDeletePenaltyUnits)
	}
}

// TestApplyNodePenalty_NotifiesDynamicChange guards the persistence wiring.
// cache.db flushes are driven by the engine's dirty set, which is only populated
// by this callback; without it the penalty would live in memory only and be lost
// on the next restart.
func TestApplyNodePenalty_NotifiesDynamicChange(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "url", true, false)
	subMgr.Register(sub)

	var notified []node.Hash
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeDynamicChanged: func(hash node.Hash) {
			notified = append(notified, hash)
		},
	})
	h := addTestNode(pool, sub, `{"type":"ss","server":"4.4.4.4","port":443}`)

	notified = nil
	pool.ApplyNodePenalty(h, node.LeaseDeletePenaltyUnits)

	for _, got := range notified {
		if got == h {
			return
		}
	}
	t.Fatalf("OnNodeDynamicChanged not fired for penalised node %s; the penalty would never reach cache.db", h.Hex())
}

// TestApplyNodePenalty_NoOps covers the guard cases: non-positive units and an
// unknown hash must be silent no-ops rather than creating state or panicking.
func TestApplyNodePenalty_NoOps(t *testing.T) {
	pool, subMgr := newHealthTestPool(3)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","server":"5.5.5.5","port":443}`)

	pool.ApplyNodePenalty(h, 0)
	pool.ApplyNodePenalty(h, -3)

	entry, _ := pool.GetEntry(h)
	if got := entry.PenaltyUnits.Load(); got != 0 {
		t.Fatalf("units = %d after non-positive penalties, want 0", got)
	}
	if entry.PenaltyUpdated.Load() != 0 {
		t.Fatal("non-positive penalty still stamped PenaltyUpdated")
	}

	unknown := node.HashFromRawOptions([]byte(`{"type":"ss","server":"6.6.6.6","port":443}`))
	pool.ApplyNodePenalty(unknown, node.LeaseDeletePenaltyUnits) // must not panic
}

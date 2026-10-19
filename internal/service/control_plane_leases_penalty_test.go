package service

import (
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/subscription"
)

// TestDeleteLease_RecordsPenaltyOnBoundNode is the end-to-end wiring test for
// the lease-deletion penalty: deleting a lease through the control plane must
// penalise exactly the node that lease was bound to, and leave a sibling node
// (even one on the same egress IP) untouched.
func TestDeleteLease_RecordsPenaltyOnBoundNode(t *testing.T) {
	t.Setenv("RESIN_LEASE_PENALTY", "")

	cp, plat := newLeaseInheritanceTestService()
	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	cp.SubMgr.Register(sub)

	// Two nodes deliberately sharing one egress IP: the penalty must not spread.
	target := addRoutableNodeForSubscription(t, cp.Pool, sub,
		[]byte(`{"type":"ss","server":"10.0.0.1","port":443}`), "198.2.214.3")
	sibling := addRoutableNodeForSubscription(t, cp.Pool, sub,
		[]byte(`{"type":"ss","server":"10.0.0.2","port":443}`), "198.2.214.3")

	now := time.Now().UnixNano()
	seedLease(t, cp, model.Lease{
		PlatformID:     plat.ID,
		Account:        "acct-penalty",
		NodeHash:       target.Hex(),
		EgressIP:       "198.2.214.3",
		CreatedAtNs:    now - int64(time.Minute),
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	})

	if err := cp.DeleteLease(plat.ID, "acct-penalty"); err != nil {
		t.Fatalf("DeleteLease: %v", err)
	}

	entryTarget, ok := cp.Pool.GetEntry(target)
	if !ok {
		t.Fatalf("target node %s missing", target.Hex())
	}
	entrySibling, ok := cp.Pool.GetEntry(sibling)
	if !ok {
		t.Fatalf("sibling node %s missing", sibling.Hex())
	}

	if got := entryTarget.PenaltyUnits.Load(); got != node.LeaseDeletePenaltyUnits {
		t.Fatalf("bound node units = %d, want %d", got, node.LeaseDeletePenaltyUnits)
	}
	if entryTarget.PenaltyUpdated.Load() == 0 {
		t.Fatal("bound node penalised without a PenaltyUpdated timestamp")
	}
	if got := entrySibling.PenaltyUnits.Load(); got != 0 {
		t.Fatalf("sibling node units = %d, want 0 (same egress IP must not be penalised)", got)
	}
}

// TestDeleteLease_AccumulatesPenaltyPerDeletion guards that repeated deletions
// keep stacking on the same node rather than saturating after the first one.
func TestDeleteLease_AccumulatesPenaltyPerDeletion(t *testing.T) {
	t.Setenv("RESIN_LEASE_PENALTY", "")

	cp, plat := newLeaseInheritanceTestService()
	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	cp.SubMgr.Register(sub)

	h := addRoutableNodeForSubscription(t, cp.Pool, sub,
		[]byte(`{"type":"ss","server":"10.0.0.3","port":443}`), "203.0.113.5")

	for i := 0; i < 3; i++ {
		now := time.Now().UnixNano()
		seedLease(t, cp, model.Lease{
			PlatformID:     plat.ID,
			Account:        "acct-stack",
			NodeHash:       h.Hex(),
			EgressIP:       "203.0.113.5",
			CreatedAtNs:    now - int64(time.Minute),
			ExpiryNs:       now + int64(time.Hour),
			LastAccessedNs: now,
		})
		if err := cp.DeleteLease(plat.ID, "acct-stack"); err != nil {
			t.Fatalf("DeleteLease #%d: %v", i+1, err)
		}
	}

	entry, _ := cp.Pool.GetEntry(h)
	want := node.LeaseDeletePenaltyUnits * 3
	if got := entry.PenaltyUnits.Load(); got != want {
		t.Fatalf("accumulated units = %d, want %d", got, want)
	}
}

// TestDeleteLease_PenaltyDisabledByEnv guards the kill switch: with
// RESIN_LEASE_PENALTY=0 the deletion must still succeed but record nothing.
func TestDeleteLease_PenaltyDisabledByEnv(t *testing.T) {
	t.Setenv("RESIN_LEASE_PENALTY", "0")

	cp, plat := newLeaseInheritanceTestService()
	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	cp.SubMgr.Register(sub)

	h := addRoutableNodeForSubscription(t, cp.Pool, sub,
		[]byte(`{"type":"ss","server":"10.0.0.4","port":443}`), "203.0.113.6")

	now := time.Now().UnixNano()
	seedLease(t, cp, model.Lease{
		PlatformID:     plat.ID,
		Account:        "acct-off",
		NodeHash:       h.Hex(),
		EgressIP:       "203.0.113.6",
		CreatedAtNs:    now - int64(time.Minute),
		ExpiryNs:       now + int64(time.Hour),
		LastAccessedNs: now,
	})

	if err := cp.DeleteLease(plat.ID, "acct-off"); err != nil {
		t.Fatalf("DeleteLease with penalty disabled: %v", err)
	}

	entry, _ := cp.Pool.GetEntry(h)
	if got := entry.PenaltyUnits.Load(); got != 0 {
		t.Fatalf("units = %d with RESIN_LEASE_PENALTY=0, want 0", got)
	}
}

package state

import (
	"testing"

	"github.com/Resinat/Resin/internal/model"
)

// TestCacheRepo_NodesDynamic_PenaltyRoundTrip guards the penalty columns added
// by migration 000003. A mismatch between the column list, the placeholder
// count and the Exec argument order would otherwise only surface as a runtime
// flush error in production, silently dropping the penalty.
func TestCacheRepo_NodesDynamic_PenaltyRoundTrip(t *testing.T) {
	repo := newTestCacheRepo(t)

	in := []model.NodeDynamic{
		{Hash: "h1", EgressIP: "1.1.1.1", PenaltyUnits: 3, PenaltyUpdatedAtNs: 111},
		{Hash: "h2", EgressIP: "2.2.2.2"}, // zero penalty must round-trip too
	}
	if err := repo.BulkUpsertNodesDynamic(in); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}

	got := map[string]model.NodeDynamic{}
	loaded, err := repo.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	for _, n := range loaded {
		got[n.Hash] = n
	}
	if len(got) != len(in) {
		t.Fatalf("expected %d nodes, got %d", len(in), len(got))
	}
	for _, want := range in {
		g, ok := got[want.Hash]
		if !ok {
			t.Fatalf("node %s missing after load", want.Hash)
		}
		if g.PenaltyUnits != want.PenaltyUnits || g.PenaltyUpdatedAtNs != want.PenaltyUpdatedAtNs {
			t.Fatalf("node %s penalty round-trip: got (%d,%d) want (%d,%d)",
				want.Hash, g.PenaltyUnits, g.PenaltyUpdatedAtNs, want.PenaltyUnits, want.PenaltyUpdatedAtNs)
		}
		if g.EgressIP != want.EgressIP {
			t.Fatalf("node %s egress round-trip: got %q want %q", want.Hash, g.EgressIP, want.EgressIP)
		}
	}

	// The ON CONFLICT path must also carry the penalty columns.
	in[0].PenaltyUnits = 7
	in[0].PenaltyUpdatedAtNs = 222
	if err := repo.BulkUpsertNodesDynamic(in[:1]); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic (update): %v", err)
	}
	loaded, err = repo.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic (after update): %v", err)
	}
	for _, n := range loaded {
		if n.Hash == "h1" && (n.PenaltyUnits != 7 || n.PenaltyUpdatedAtNs != 222) {
			t.Fatalf("ON CONFLICT update lost penalty: got (%d,%d) want (7,222)",
				n.PenaltyUnits, n.PenaltyUpdatedAtNs)
		}
	}
}

package routing

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/testutil"
)

// newLatencyEntry builds a routable entry whose very first cloudflare.com sample
// is latency, so the EWMA (and therefore the score) is deterministic.
func newLatencyEntry(t *testing.T, raw, ip string, latency time.Duration) (node.Hash, *node.NodeEntry) {
	t.Helper()
	rawOpts := json.RawMessage(raw)
	h := node.HashFromRawOptions(rawOpts)
	e := node.NewNodeEntry(h, rawOpts, time.Now(), 16)
	e.AddSubscriptionID("sub-test")

	parsedIP, err := netip.ParseAddr(ip)
	if err != nil {
		t.Fatalf("parse ip %q: %v", ip, err)
	}
	e.SetEgressIP(parsedIP)

	e.LatencyTable.Update("cloudflare.com", latency, 10*time.Minute)
	waitForDomainLatency(t, e, "cloudflare.com")

	ob := testutil.NewNoopOutbound()
	e.Outbound.Store(&ob)
	return h, e
}

func TestCandidateSamples(t *testing.T) {
	cases := []struct {
		env  string
		set  bool
		want int
	}{
		{set: false, want: defaultCandidateSamples},
		{env: "", set: true, want: defaultCandidateSamples},
		{env: "2", set: true, want: 2},
		{env: "5", set: true, want: 5},
		{env: "0", set: true, want: 1},
		{env: "-3", set: true, want: 1},
		{env: "99", set: true, want: maxCandidateSamples},
		{env: "abc", set: true, want: defaultCandidateSamples},
	}
	for _, tc := range cases {
		if tc.set {
			t.Setenv("RESIN_P2C_CANDIDATES", tc.env)
		} else {
			t.Setenv("RESIN_P2C_CANDIDATES", "")
		}
		if got := candidateSamples(); got != tc.want {
			t.Fatalf("candidateSamples(env=%q, set=%v) = %d, want %d", tc.env, tc.set, got, tc.want)
		}
	}
}

func TestChooseBestPicksLowestScore(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-choose-best", "Plat-Choose-Best", nil, nil)
	plat.AllocationPolicy = platform.AllocationPolicyBalanced

	hashes := make([]node.Hash, 0, 3)
	for i, latency := range []time.Duration{400 * time.Millisecond, 90 * time.Millisecond, 250 * time.Millisecond} {
		h, entry := newLatencyEntry(t, fmt.Sprintf(`{"id":"choose-%d"}`, i), fmt.Sprintf("198.51.100.%d", 10+i), latency)
		pool.addEntry(h, entry)
		hashes = append(hashes, h)
	}

	best, err := chooseBest(hashes, plat, NewIPLoadStats(), pool, "example.com", []string{"cloudflare.com"}, time.Minute)
	if err != nil {
		t.Fatalf("chooseBest: %v", err)
	}
	if best != hashes[1] {
		t.Fatalf("chooseBest picked the wrong candidate, want the 90ms one")
	}

	// A slower candidate must not displace the winner.
	slow, slowEntry := newLatencyEntry(t, `{"id":"choose-slow"}`, "198.51.100.20", 900*time.Millisecond)
	pool.addEntry(slow, slowEntry)
	best, err = chooseBest(append(hashes, slow), plat, NewIPLoadStats(), pool, "example.com", []string{"cloudflare.com"}, time.Minute)
	if err != nil {
		t.Fatalf("chooseBest: %v", err)
	}
	if best != hashes[1] {
		t.Fatalf("chooseBest picked a slower candidate over the 90ms one")
	}
}

func TestRandomRouteSamplesConfiguredCandidateCount(t *testing.T) {
	t.Setenv("RESIN_P2C_CANDIDATES", "3")

	basePool := newRouterTestPool()
	plat := platform.NewPlatform("plat-candidate-count", "Plat-Candidate-Count", nil, nil)
	basePool.addPlatform(plat)
	for i := 0; i < 64; i++ {
		h, entry := newRoutableEntry(t, fmt.Sprintf(`{"id":"count-%d"}`, i), fmt.Sprintf("203.0.113.%d", i+1))
		basePool.addEntry(h, entry)
	}
	basePool.rebuildPlatformView(plat)

	counted := &getEntryCountPool{inner: basePool}
	if _, err := randomRoute(plat, NewIPLoadStats(), counted, "example.com", []string{"cloudflare.com"}, time.Minute); err != nil {
		t.Fatalf("randomRoute: %v", err)
	}
	// One lookup resolves candidate latencies, one scores each candidate.
	if want := 6; counted.getCalls != want {
		t.Fatalf("getEntry calls = %d, want %d for 3 candidates", counted.getCalls, want)
	}
}

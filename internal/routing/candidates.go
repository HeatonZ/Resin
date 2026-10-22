package routing

import (
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

const (
	// defaultCandidateSamples is how many distinct nodes one allocation compares.
	// 2 is plain P2C; 3 samples one more candidate per allocation to give the
	// latency/penalty score more chances to reject a bad node.
	defaultCandidateSamples = 3
	// maxCandidateSamples bounds the env override so a typo cannot turn one
	// allocation into a pool-wide scan.
	maxCandidateSamples = 16
)

// candidateSamples reports how many distinct candidates an allocation samples.
// RESIN_P2C_CANDIDATES overrides the default; invalid values fall back to it and
// valid ones are clamped to [1, maxCandidateSamples].
func candidateSamples() int {
	raw, ok := os.LookupEnv("RESIN_P2C_CANDIDATES")
	if !ok || raw == "" {
		return defaultCandidateSamples
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return defaultCandidateSamples
	}
	if n < 1 {
		n = 1
	}
	if n > maxCandidateSamples {
		n = maxCandidateSamples
	}
	return n
}

// sampleCandidates picks up to k distinct routable nodes from the view. The
// attempt budget keeps a tiny or heavily duplicated view from spinning.
func sampleCandidates(view platform.ReadOnlyView, rng *rand.Rand, k int) []node.Hash {
	if view == nil || k <= 0 {
		return nil
	}
	out := make([]node.Hash, 0, k)
	seen := make(map[node.Hash]struct{}, k)
	for attempts := 0; attempts < k*4 && len(out) < k; attempts++ {
		candidate, ok := view.RandomPick(rng)
		if !ok {
			break
		}
		if _, duplicate := seen[candidate]; duplicate {
			continue
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	return out
}

// candidateLatencies resolves one comparable latency per candidate, mirroring the
// pairwise rules of compareLatencies:
//  1. target domain present and recent for every candidate;
//  2. otherwise domains present and recent for every candidate, averaged per node;
//  3. otherwise zero for all (score then falls back to load terms alone).
func candidateLatencies(
	hashes []node.Hash,
	pool PoolAccessor,
	target string,
	authorities []string,
	window time.Duration,
) []time.Duration {
	out := make([]time.Duration, len(hashes))
	if len(hashes) == 0 {
		return out
	}
	entries := make([]*node.NodeEntry, len(hashes))
	for i, h := range hashes {
		entry, ok := pool.GetEntry(h)
		if !ok || entry == nil || entry.LatencyTable == nil {
			return out
		}
		entries[i] = entry
	}

	now := time.Now()

	// 1. Target domain for every candidate.
	targetOK := true
	targetLatencies := make([]time.Duration, len(entries))
	for i, entry := range entries {
		latency, ok := lookupRecentDomainLatency(entry, target, now, window)
		if !ok {
			targetOK = false
			break
		}
		targetLatencies[i] = latency
	}
	if targetOK {
		return targetLatencies
	}

	// 2. Domains shared by every candidate.
	sums := make([]time.Duration, len(entries))
	var count int64
	for _, domain := range authorities {
		latencies := make([]time.Duration, len(entries))
		all := true
		for i, entry := range entries {
			latency, ok := lookupRecentDomainLatency(entry, domain, now, window)
			if !ok {
				all = false
				break
			}
			latencies[i] = latency
		}
		if !all {
			continue
		}
		for i := range sums {
			sums[i] += latencies[i]
		}
		count++
	}
	if count == 0 {
		return out
	}
	for i := range sums {
		out[i] = time.Duration(int64(sums[i]) / count)
	}
	return out
}

// chooseBest returns the candidate with the lowest score. Ties favour the later
// candidate, matching the historical pairwise P2C tie-break.
func chooseBest(
	candidates []node.Hash,
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
	targetDomain string,
	authorities []string,
	p2cWindow time.Duration,
) (node.Hash, error) {
	if len(candidates) == 0 {
		return node.Zero, ErrNoAvailableNodes
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	latencies := candidateLatencies(candidates, pool, targetDomain, authorities, p2cWindow)
	best := candidates[0]
	bestScore := calculateScore(candidates[0], latencies[0], plat, stats, pool)
	for i := 1; i < len(candidates); i++ {
		score := calculateScore(candidates[i], latencies[i], plat, stats, pool)
		if score <= bestScore {
			best = candidates[i]
			bestScore = score
		}
	}
	return best, nil
}

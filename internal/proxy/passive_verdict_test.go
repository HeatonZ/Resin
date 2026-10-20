package proxy

import (
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// countingRecorder captures passive verdicts so tests can assert that an
// ambiguous status records nothing at all rather than recording success.
type countingRecorder struct {
	mu    sync.Mutex
	calls []bool
}

func (c *countingRecorder) RecordResult(hash node.Hash, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, success)
}

func (c *countingRecorder) RecordLatency(hash node.Hash, rawTarget string, latency *time.Duration) {
}

func (c *countingRecorder) snapshot() []bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]bool(nil), c.calls...)
}

func TestPassiveVerdictForStatus(t *testing.T) {
	ignore := []int{403}

	cases := []struct {
		name   string
		status int
		want   passiveVerdict
	}{
		{"403 is ambiguous and must not be evidence", 403, passiveNoEvidence},
		{"200 is a real success", 200, passiveSuccess},
		{"502 still counts as success for passive feedback", 502, passiveSuccess},
		{"429 is not in the ignore list", 429, passiveSuccess},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := passiveVerdictForStatus(tc.status, ignore); got != tc.want {
				t.Fatalf("status %d: got verdict %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

func TestPassiveVerdictForStatusEmptyIgnoreList(t *testing.T) {
	// An explicitly empty list disables the rule, restoring the legacy
	// behaviour where any completed exchange counts as success.
	if got := passiveVerdictForStatus(403, nil); got != passiveSuccess {
		t.Fatalf("empty ignore list: got %v, want passiveSuccess", got)
	}
}

func TestRecordPassiveVerdictNoEvidenceRecordsNothing(t *testing.T) {
	rec := &countingRecorder{}
	route := routing.RouteResult{}

	recordPassiveVerdictAsync(rec, route, passiveNoEvidence)

	// Recording is async elsewhere, so give a spawned goroutine every chance
	// to run before asserting nothing happened.
	time.Sleep(50 * time.Millisecond)
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("passiveNoEvidence must record nothing, got %d call(s): %v", len(got), got)
	}
}

func TestRecordPassiveVerdictFailureRecordsFailure(t *testing.T) {
	rec := &countingRecorder{}
	route := routing.RouteResult{}

	recordPassiveVerdictAsync(rec, route, passiveFailure)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := rec.snapshot(); len(got) == 1 {
			if got[0] {
				t.Fatalf("want a failure record, got success")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("want exactly one failure record, got %v", rec.snapshot())
}

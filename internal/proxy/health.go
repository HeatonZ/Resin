package proxy

import (
	"context"
	"errors"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// Feedback always identifies the exact outbound mode used by the request.
type HealthRecorder interface {
	ApplyFeedback(node.Hash, uint64, node.Feedback) bool
}

func recordPassiveResultAsync(health HealthRecorder, route routing.RouteResult, success bool, relay node.RelayInfo) {
	if health != nil {
		go health.ApplyFeedback(route.NodeHash, route.NodeVersion, node.Feedback{Result: &success, PlatformID: route.PlatformID, Relay: relay})
	}
}
func recordPassiveFailureAsync(health HealthRecorder, route routing.RouteResult, err error, relay node.RelayInfo) {
	if health != nil && !errors.Is(err, routing.ErrNoAvailableRelay) && !errors.Is(err, context.Canceled) {
		failed := false
		go health.ApplyFeedback(route.NodeHash, route.NodeVersion, node.Feedback{Result: &failed, Err: err, PlatformID: route.PlatformID, Relay: relay})
	}
}
func recordLatencyForVersion(health HealthRecorder, hash node.Hash, version uint64, domain string, latency *time.Duration) {
	if health != nil {
		health.ApplyFeedback(hash, version, node.Feedback{Target: domain, Latency: latency})
	}
}

// passiveVerdict describes what a completed upstream exchange says about the
// health of the node that carried it.
type passiveVerdict int

const (
	// passiveSuccess: the node carried the exchange end to end.
	passiveSuccess passiveVerdict = iota
	// passiveFailure: the node failed the exchange.
	passiveFailure
	// passiveNoEvidence: the exchange completed but says nothing about the
	// node. Record nothing so FailureCount and the circuit breaker keep their
	// current values; recording success would clear a real failure streak and
	// recording failure would blame the node for someone else's mistake.
	passiveNoEvidence
)

// passiveVerdictForStatus maps an upstream status code to the verdict passive
// feedback should record. ignore lists status codes that are ambiguous about
// node health (see RuntimeConfig.PassiveIgnoreStatusCodes).
func passiveVerdictForStatus(status int, ignore []int) passiveVerdict {
	for _, code := range ignore {
		if code == status {
			return passiveNoEvidence
		}
	}
	return passiveSuccess
}

// recordPassiveVerdictAsync reports a passive verdict. passiveNoEvidence is a
// no-op by design: it exists so call sites can state "this exchange proved
// nothing" explicitly instead of silently falling through to success.
// relay carries the chain provenance of the exchange so a verdict still credits
// the relay that carried it.
func recordPassiveVerdictAsync(health HealthRecorder, route routing.RouteResult, verdict passiveVerdict, relay node.RelayInfo) {
	switch verdict {
	case passiveNoEvidence:
		return
	case passiveFailure:
		recordPassiveResultAsync(health, route, false, relay)
	default:
		recordPassiveResultAsync(health, route, true, relay)
	}
}

package proxy

import (
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// HealthRecorder abstracts passive health feedback reporting.
// topology.GlobalNodePool satisfies this interface.
type HealthRecorder interface {
	RecordResult(hash node.Hash, success bool)
	RecordLatency(hash node.Hash, rawTarget string, latency *time.Duration)
}

type passiveHealthRecorder interface {
	RecordPassiveResult(platformID string, hash node.Hash, success bool)
}

func recordPassiveResultAsync(health HealthRecorder, route routing.RouteResult, success bool) {
	if health == nil {
		return
	}
	if recorder, ok := health.(passiveHealthRecorder); ok {
		go recorder.RecordPassiveResult(route.PlatformID, route.NodeHash, success)
		return
	}
	go health.RecordResult(route.NodeHash, success)
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
func recordPassiveVerdictAsync(health HealthRecorder, route routing.RouteResult, verdict passiveVerdict) {
	switch verdict {
	case passiveNoEvidence:
		return
	case passiveFailure:
		recordPassiveResultAsync(health, route, false)
	default:
		recordPassiveResultAsync(health, route, true)
	}
}

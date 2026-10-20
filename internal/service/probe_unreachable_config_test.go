package service

import "testing"

// TestValidateRuntimeConfig_RejectOnlyUnreachableIsWritable pins the new field
// into the PATCH whitelist. Without it the setting is rejected at save time and
// the operator has no way to relax the probe on a credentialed-less probe.
func TestValidateRuntimeConfig_RejectOnlyUnreachableIsWritable(t *testing.T) {
	cfg := newDefaultCfg()
	cfg.ProbeRejectOnlyUnreachable = true
	if err := validateRuntimeConfig(cfg); err != nil {
		t.Fatalf("connectivity-only mode must be a valid config: %v", err)
	}

	// It is a plain bool, so it must not disturb the status-code rules that
	// remain in effect when the mode is off.
	cfg = newDefaultCfg()
	cfg.ProbeRejectOnlyUnreachable = false
	cfg.ProbeRejectStatusCodes = []int{403}
	if err := validateRuntimeConfig(cfg); err != nil {
		t.Fatalf("status-code rules must stay valid alongside the flag: %v", err)
	}
}

// TestProbeRejectOnlyUnreachableInWhitelist guards against the field being
// added to the struct but forgotten in the patch whitelist, which would make it
// silently un-settable via the API.
func TestProbeRejectOnlyUnreachableInWhitelist(t *testing.T) {
	if !runtimeConfigAllowedFields["probe_reject_only_unreachable"] {
		t.Fatal("probe_reject_only_unreachable must be patchable")
	}
}

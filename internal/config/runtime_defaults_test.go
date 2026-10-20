package config

import (
	"encoding/json"
	"testing"
)

// TestApplyMissingDefaults_LegacyRowAdoptsProbeRules covers the upgrade path:
// a config row written by a version that predates probe_reject_status_codes
// must adopt the default codes, otherwise the new rules silently stay off.
func TestApplyMissingDefaults_LegacyRowAdoptsProbeRules(t *testing.T) {
	legacy := `{"max_consecutive_failures":3,"latency_test_url":"https://www.gstatic.com/generate_204"}`

	cfg := &RuntimeConfig{}
	if err := json.Unmarshal([]byte(legacy), cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.ProbeRejectStatusCodes != nil {
		t.Fatal("precondition: field should be nil before defaults are applied")
	}

	cfg.ApplyMissingDefaults()

	want := []int{403, 429, 500, 502, 503, 504}
	if len(cfg.ProbeRejectStatusCodes) != len(want) {
		t.Fatalf("got %v, want %v", cfg.ProbeRejectStatusCodes, want)
	}
	for i, code := range want {
		if cfg.ProbeRejectStatusCodes[i] != code {
			t.Fatalf("got %v, want %v", cfg.ProbeRejectStatusCodes, want)
		}
	}
}

// TestApplyMissingDefaults_PreservesExplicitEmpty is the other half of the
// contract: an operator who stored an empty list meant "disable status-code
// rejection", and that must survive a restart.
func TestApplyMissingDefaults_PreservesExplicitEmpty(t *testing.T) {
	row := `{"probe_reject_status_codes":[]}`

	cfg := &RuntimeConfig{}
	if err := json.Unmarshal([]byte(row), cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.ProbeRejectStatusCodes == nil {
		t.Fatal("precondition: explicit [] should decode to a non-nil empty slice")
	}

	cfg.ApplyMissingDefaults()

	if len(cfg.ProbeRejectStatusCodes) != 0 {
		t.Fatalf("explicit empty list must stay empty, got %v", cfg.ProbeRejectStatusCodes)
	}
}

// TestApplyMissingDefaults_KeepsConfiguredValues makes sure an operator's own
// list is never overwritten.
func TestApplyMissingDefaults_KeepsConfiguredValues(t *testing.T) {
	cfg := &RuntimeConfig{ProbeRejectStatusCodes: []int{403}}
	cfg.ApplyMissingDefaults()

	if len(cfg.ProbeRejectStatusCodes) != 1 || cfg.ProbeRejectStatusCodes[0] != 403 {
		t.Fatalf("configured values must not be overwritten, got %v", cfg.ProbeRejectStatusCodes)
	}
}

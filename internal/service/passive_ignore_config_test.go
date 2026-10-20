package service

import (
	"encoding/json"
	"testing"

	"github.com/Resinat/Resin/internal/config"
)

// TestPassiveIgnoreStatusCodesIsPatchable guards the hot-reload path: an
// operator must be able to change the rule without restarting, and a missing
// whitelist entry would surface as "unknown or read-only field".
func TestPassiveIgnoreStatusCodesIsPatchable(t *testing.T) {
	if !runtimeConfigAllowedFields["passive_ignore_status_codes"] {
		t.Fatal("passive_ignore_status_codes must be patchable at runtime")
	}

	cfg := config.NewDefaultRuntimeConfig()
	patch := json.RawMessage(`{"passive_ignore_status_codes":[403,429]}`)
	if serr := parseRuntimeConfigPatch(patch, cfg); serr != nil {
		t.Fatalf("patch rejected: %v", serr)
	}
	if len(cfg.PassiveIgnoreStatusCodes) != 2 || cfg.PassiveIgnoreStatusCodes[0] != 403 {
		t.Fatalf("patch not applied, got %v", cfg.PassiveIgnoreStatusCodes)
	}
}

func TestPassiveIgnoreStatusCodesRejectsOutOfRange(t *testing.T) {
	cfg := config.NewDefaultRuntimeConfig()
	cfg.PassiveIgnoreStatusCodes = []int{999}
	if err := validateRuntimeConfig(cfg); err == nil {
		t.Fatal("out-of-range status code must be rejected")
	}
}

// TestPassiveIgnoreStatusCodesDefaultAppliedToOldConfig covers upgrades: a
// config row persisted before this field existed unmarshals it as nil, and
// without ApplyMissingDefaults the rule would silently stay off.
func TestPassiveIgnoreStatusCodesDefaultAppliedToOldConfig(t *testing.T) {
	var cfg config.RuntimeConfig
	if err := json.Unmarshal([]byte(`{"max_consecutive_failures":2}`), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.PassiveIgnoreStatusCodes != nil {
		t.Fatalf("precondition failed: want nil before defaults, got %v", cfg.PassiveIgnoreStatusCodes)
	}
	cfg.ApplyMissingDefaults()
	if len(cfg.PassiveIgnoreStatusCodes) != 1 || cfg.PassiveIgnoreStatusCodes[0] != 403 {
		t.Fatalf("defaults not applied, got %v", cfg.PassiveIgnoreStatusCodes)
	}
}

// TestPassiveIgnoreStatusCodesEmptySlicePreserved verifies the operator can
// disable the rule: JSON distinguishes absent (nil, take default) from an
// explicit empty list (deliberate "off").
func TestPassiveIgnoreStatusCodesEmptySlicePreserved(t *testing.T) {
	var cfg config.RuntimeConfig
	if err := json.Unmarshal([]byte(`{"passive_ignore_status_codes":[]}`), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	cfg.ApplyMissingDefaults()
	if len(cfg.PassiveIgnoreStatusCodes) != 0 {
		t.Fatalf("explicit empty list must be preserved, got %v", cfg.PassiveIgnoreStatusCodes)
	}
}

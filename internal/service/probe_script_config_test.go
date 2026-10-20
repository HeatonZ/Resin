package service

import (
	"strings"
	"testing"
)

// TestValidateRuntimeConfig_ProbeScriptsRejectSyntaxErrors pins the save-time
// guarantee: a script that does not parse must be rejected here, because a
// persisted broken script would silently fall back to default probe behaviour
// and the operator would believe their logic is live.
func TestValidateRuntimeConfig_ProbeScriptsRejectSyntaxErrors(t *testing.T) {
	cfg := newDefaultCfg()
	cfg.ProbeScriptBuild = `(function( {`
	if err := validateRuntimeConfig(cfg); err == nil {
		t.Fatal("expected syntax error to be rejected at save time")
	} else if !strings.Contains(err.Error(), "probe_script_build") {
		t.Fatalf("error should name the offending field, got: %v", err)
	}

	cfg = newDefaultCfg()
	cfg.ProbeScriptJudge = `function(){ return {ok: }`
	if err := validateRuntimeConfig(cfg); err == nil {
		t.Fatal("expected judge syntax error to be rejected")
	} else if !strings.Contains(err.Error(), "probe_script_judge") {
		t.Fatalf("error should name the offending field, got: %v", err)
	}
}

// TestValidateRuntimeConfig_ProbeScriptsAcceptValid checks the happy path and
// that empty scripts stay valid (the off switch).
func TestValidateRuntimeConfig_ProbeScriptsAcceptValid(t *testing.T) {
	cfg := newDefaultCfg()
	cfg.ProbeScriptBuild = `(function(ctx){ return {url: "https://x/y", method: "POST"}; })(ctx)`
	cfg.ProbeScriptJudge = `(function(ctx, statusCode, body){ return {ok: statusCode === 200, reason: ""}; })(ctx, statusCode, body)`
	cfg.ProbeScriptEnabled = true
	if err := validateRuntimeConfig(cfg); err != nil {
		t.Fatalf("valid scripts rejected: %v", err)
	}

	cfg = newDefaultCfg()
	cfg.ProbeScriptBuild = "   "
	cfg.ProbeScriptJudge = ""
	if err := validateRuntimeConfig(cfg); err != nil {
		t.Fatalf("empty scripts must stay valid: %v", err)
	}
}

// TestValidateRuntimeConfig_ProbeScriptSizeBound keeps an unbounded script from
// being stored: it runs on every probe cycle across the whole pool.
func TestValidateRuntimeConfig_ProbeScriptSizeBound(t *testing.T) {
	cfg := newDefaultCfg()
	cfg.ProbeScriptBuild = "// " + strings.Repeat("a", maxProbeScriptLen)
	if err := validateRuntimeConfig(cfg); err == nil {
		t.Fatal("expected oversized script to be rejected")
	}
}

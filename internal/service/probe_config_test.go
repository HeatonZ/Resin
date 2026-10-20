package service

import (
	"net/http"
	"strings"
	"testing"

	"github.com/Resinat/Resin/internal/config"
)

// TestValidateRuntimeConfig_ProbeRejectStatusCodes pins the range check: an
// out-of-range code would silently never match and quietly disable rejection.
func TestValidateRuntimeConfig_ProbeRejectStatusCodes(t *testing.T) {
	cfg := newDefaultCfg()
	cfg.ProbeRejectStatusCodes = []int{403, 429, 502}
	if err := validateRuntimeConfig(cfg); err != nil {
		t.Fatalf("valid codes rejected: %v", err)
	}

	cfg = newDefaultCfg()
	cfg.ProbeRejectStatusCodes = []int{403, 999}
	if err := validateRuntimeConfig(cfg); err == nil {
		t.Error("expected error for out-of-range status code 999")
	}

	cfg = newDefaultCfg()
	cfg.ProbeRejectStatusCodes = []int{0}
	if err := validateRuntimeConfig(cfg); err == nil {
		t.Error("expected error for out-of-range status code 0")
	}

	// Empty must stay valid: it is the documented way to restore legacy
	// permissive behaviour without a code change.
	cfg = newDefaultCfg()
	cfg.ProbeRejectStatusCodes = nil
	if err := validateRuntimeConfig(cfg); err != nil {
		t.Fatalf("empty reject list should be valid, got: %v", err)
	}
}

// TestValidateRuntimeConfig_ProbeRequestTemplate covers the operator-supplied
// probe template: a probe must never be able to mutate upstream state, and an
// unbounded body would be replayed through every node in the pool.
func TestValidateRuntimeConfig_ProbeRequestTemplate(t *testing.T) {
	cases := []struct {
		name    string
		tpl     config.ProbeRequestTemplate
		wantErr bool
	}{
		{name: "zero template allowed", tpl: config.ProbeRequestTemplate{}, wantErr: false},
		{
			name: "POST with body allowed",
			tpl: config.ProbeRequestTemplate{
				Method:  http.MethodPost,
				Body:    `{"model":"x"}`,
				Headers: map[string]string{"Content-Type": "application/json"},
			},
			wantErr: false,
		},
		{name: "GET allowed", tpl: config.ProbeRequestTemplate{Method: http.MethodGet}, wantErr: false},
		{name: "HEAD allowed", tpl: config.ProbeRequestTemplate{Method: http.MethodHead}, wantErr: false},
		// Mutating verbs must be refused: a probe runs continuously against
		// every node, so DELETE/PUT would be an unbounded write amplifier.
		{name: "DELETE refused", tpl: config.ProbeRequestTemplate{Method: http.MethodDelete}, wantErr: true},
		{name: "PUT refused", tpl: config.ProbeRequestTemplate{Method: http.MethodPut}, wantErr: true},
		{name: "PATCH refused", tpl: config.ProbeRequestTemplate{Method: http.MethodPatch}, wantErr: true},
		{
			name:    "oversized body refused",
			tpl:     config.ProbeRequestTemplate{Method: http.MethodPost, Body: strings.Repeat("a", maxProbeRequestBodyLen+1)},
			wantErr: true,
		},
		{
			name: "oversized header value refused",
			tpl: config.ProbeRequestTemplate{
				Method:  http.MethodGet,
				Headers: map[string]string{"X-Big": strings.Repeat("a", maxProbeRequestHeaderLen+1)},
			},
			wantErr: true,
		},
		{
			name:    "empty header name refused",
			tpl:     config.ProbeRequestTemplate{Method: http.MethodGet, Headers: map[string]string{"  ": "v"}},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newDefaultCfg()
			cfg.ProbeRequest = tc.tpl
			err := validateRuntimeConfig(cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

// TestValidateRuntimeConfig_ProbeRejectBodySubstrings bounds markers so a
// pathological entry cannot be stored, and keeps normal markers valid.
func TestValidateRuntimeConfig_ProbeRejectBodySubstrings(t *testing.T) {
	cfg := newDefaultCfg()
	cfg.ProbeRejectBodySubstrings = []string{"free tier can only be used from within opencode", "abusive traffic"}
	if err := validateRuntimeConfig(cfg); err != nil {
		t.Fatalf("normal markers rejected: %v", err)
	}

	cfg = newDefaultCfg()
	cfg.ProbeRejectBodySubstrings = []string{strings.Repeat("a", maxProbeRejectMarkerLen+1)}
	if err := validateRuntimeConfig(cfg); err == nil {
		t.Error("expected error for oversized marker")
	}
}

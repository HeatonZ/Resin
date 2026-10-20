package config

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestProbeScriptSecretsNeverSerialize is the load-bearing safety property of
// the credential channel. A probe needs a real credential to tell a blocked
// egress from a request with no identity, but the config struct is what
// GET /system/config serialises and what gets persisted to state.db. If the
// secrets ever gained a JSON tag, every credential would be readable through
// the API and recoverable from a database dump.
func TestProbeScriptSecretsNeverSerialize(t *testing.T) {
	cfg := NewDefaultRuntimeConfig()
	cfg.ProbeScriptSecrets = map[string]string{"apikey": "sk-live-do-not-leak"}

	blob, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(blob), "sk-live-do-not-leak") {
		t.Fatal("credential leaked into the serialised config")
	}
	if strings.Contains(string(blob), "probe_script_secrets") {
		t.Fatal("the secrets field itself must not appear in the serialised config")
	}
}

// TestProbeScriptSecretsSurviveConfigRoundTrip documents the deliberate
// consequence of json:"-": secrets are process state, not persisted config, so
// they must be re-injected from the environment on every start. A round trip
// through JSON is expected to drop them, and callers must not rely on it to
// carry credentials.
func TestProbeScriptSecretsSurviveConfigRoundTrip(t *testing.T) {
	cfg := NewDefaultRuntimeConfig()
	cfg.ProbeScriptSecrets = map[string]string{"apikey": "sk-live"}

	blob, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var restored RuntimeConfig
	if err := json.Unmarshal(blob, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(restored.ProbeScriptSecrets) != 0 {
		t.Fatalf("secrets must not survive a JSON round trip, got %d entries", len(restored.ProbeScriptSecrets))
	}
}

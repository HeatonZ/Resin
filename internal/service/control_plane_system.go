package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/probescript"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/topology"
)

// ServiceError wraps an error with a code for API response mapping.
type ServiceError struct {
	Code    string // INVALID_ARGUMENT, NOT_FOUND, CONFLICT, INTERNAL
	Message string
	Err     error
}

func (e *ServiceError) Error() string { return e.Message }
func (e *ServiceError) Unwrap() error { return e.Err }

func invalidArg(msg string) *ServiceError {
	return &ServiceError{Code: "INVALID_ARGUMENT", Message: msg}
}

func notFound(msg string) *ServiceError {
	return &ServiceError{Code: "NOT_FOUND", Message: msg}
}

func conflict(msg string) *ServiceError {
	return &ServiceError{Code: "CONFLICT", Message: msg}
}

func internal(msg string, err error) *ServiceError {
	return &ServiceError{Code: "INTERNAL", Message: msg, Err: err}
}

// --- ControlPlaneService ---

// ControlPlaneService provides all control plane operations.
// Handlers call its methods; business logic lives here, not in handlers.
type ControlPlaneService struct {
	Engine          *state.StateEngine
	Pool            *topology.GlobalNodePool
	SubMgr          *topology.SubscriptionManager
	Scheduler       *topology.SubscriptionScheduler
	Router          *routing.Router
	GeoIP           *geoip.Service
	ProbeMgr        *probe.ProbeManager
	MatcherRuntime  *proxy.AccountMatcherRuntime
	RuntimeCfg      *atomic.Pointer[config.RuntimeConfig]
	EnvCfg          *config.EnvConfig
	EndpointRuntime EndpointRuntime

	configMu      sync.Mutex
	configVersion int
	endpointMu    sync.RWMutex
}

// ------------------------------------------------------------------
// System Config
// ------------------------------------------------------------------

// runtimeConfigAllowedFields is the set of JSON field names that can be patched.
var runtimeConfigAllowedFields = map[string]bool{
	"request_log_enabled":                      true,
	"reverse_proxy_log_detail_enabled":         true,
	"reverse_proxy_log_req_headers_max_bytes":  true,
	"reverse_proxy_log_req_body_max_bytes":     true,
	"reverse_proxy_log_resp_headers_max_bytes": true,
	"reverse_proxy_log_resp_body_max_bytes":    true,
	"max_consecutive_failures":                 true,
	"max_latency_test_interval":                true,
	"max_authority_latency_test_interval":      true,
	"max_egress_test_interval":                 true,
	"latency_test_url":                         true,
	"latency_authorities":                      true,
	"probe_reject_status_codes":                true,
	"probe_reject_only_unreachable":            true,
	"probe_request":                            true,
	"probe_reject_body_substrings":             true,
	"probe_script_build":                       true,
	"probe_script_judge":                       true,
	"probe_script_enabled":                     true,
	"p2c_latency_window":                       true,
	"latency_decay_window":                     true,
	"cache_flush_interval":                     true,
	"cache_flush_dirty_threshold":              true,
}

var platformPatchAllowedFields = map[string]bool{
	"name":                                 true,
	"sticky_ttl":                           true,
	"regex_filters":                        true,
	"region_filters":                       true,
	"max_reference_latency_ms":             true,
	"reverse_proxy_miss_action":            true,
	"reverse_proxy_empty_account_behavior": true,
	"reverse_proxy_fixed_account_header":   true,
	"allocation_policy":                    true,
	"passive_circuit_breaker_disabled":     true,
}

var subscriptionPatchAllowedFields = map[string]bool{
	"name":                       true,
	"url":                        true,
	"content":                    true,
	"update_interval":            true,
	"enabled":                    true,
	"ephemeral":                  true,
	"incremental_alive_nodes":    true,
	"ephemeral_node_evict_delay": true,
}

func parseRuntimeConfigPatch(patchJSON json.RawMessage, out *config.RuntimeConfig) *ServiceError {
	var rawPatch map[string]json.RawMessage
	if err := json.Unmarshal(patchJSON, &rawPatch); err != nil {
		return invalidArg("invalid JSON: " + err.Error())
	}
	if len(rawPatch) == 0 {
		return invalidArg("empty patch")
	}
	for key, raw := range rawPatch {
		if !runtimeConfigAllowedFields[key] {
			return invalidArg(fmt.Sprintf("unknown or read-only field: %q", key))
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return invalidArg(fmt.Sprintf("null value not allowed for field: %q", key))
		}
	}

	dec := json.NewDecoder(bytes.NewReader(patchJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return invalidArg("validation failed: " + err.Error())
	}
	return nil
}

func copyRuntimeConfig(cfg *config.RuntimeConfig) *config.RuntimeConfig {
	if cfg == nil {
		return config.NewDefaultRuntimeConfig()
	}
	out := *cfg
	out.LatencyAuthorities = append([]string(nil), cfg.LatencyAuthorities...)
	out.ProbeRejectStatusCodes = append([]int(nil), cfg.ProbeRejectStatusCodes...)
	out.ProbeRejectBodySubstrings = append([]string(nil), cfg.ProbeRejectBodySubstrings...)
	// Strings are immutable in Go, so the script fields copy by value safely;
	// no extra work is needed beyond the struct copy above.
	if len(cfg.ProbeRequest.Headers) > 0 {
		headers := make(map[string]string, len(cfg.ProbeRequest.Headers))
		for k, v := range cfg.ProbeRequest.Headers {
			headers[k] = v
		}
		out.ProbeRequest.Headers = headers
	}
	return &out
}

// PatchRuntimeConfig applies a constrained partial patch to the runtime config.
// This is not RFC 7396 JSON Merge Patch: patch must be a non-empty object and
// null values are rejected.
// Pipeline: validate → persist → atomic swap.
func (s *ControlPlaneService) PatchRuntimeConfig(patchJSON json.RawMessage) (*config.RuntimeConfig, error) {
	s.configMu.Lock()
	defer s.configMu.Unlock()

	// 3. Deep-copy current config → apply patch.
	previousCfg := copyRuntimeConfig(s.RuntimeCfg.Load())
	newCfg := copyRuntimeConfig(previousCfg)
	if verr := parseRuntimeConfigPatch(patchJSON, newCfg); verr != nil {
		return nil, verr
	}

	// 4. Additional validation.
	if err := validateRuntimeConfig(newCfg); err != nil {
		return nil, err
	}

	// On process start, initialize local configVersion from persisted state
	// so PATCH keeps monotonically increasing versions across restarts.
	if s.configVersion == 0 && s.Engine != nil {
		_, persistedVersion, err := s.Engine.GetSystemConfig()
		if err != nil {
			return nil, internal("load persisted config version", err)
		}
		if persistedVersion > s.configVersion {
			s.configVersion = persistedVersion
		}
	}

	// 5. Persist.
	newVersion := s.configVersion + 1
	if err := s.Engine.SaveSystemConfig(newCfg, newVersion, time.Now().UnixNano()); err != nil {
		return nil, internal("persist config", err)
	}

	// 6. Atomic swap.
	s.RuntimeCfg.Store(newCfg)
	s.configVersion = newVersion
	if s.Pool != nil && !slices.Equal(previousCfg.LatencyAuthorities, newCfg.LatencyAuthorities) {
		s.Pool.RebuildAllPlatforms()
	}

	return newCfg, nil
}

func validateRuntimeConfig(cfg *config.RuntimeConfig) *ServiceError {
	latencyURL := strings.TrimSpace(cfg.LatencyTestURL)
	u, verr := parseHTTPAbsoluteURL("latency_test_url", latencyURL)
	if verr != nil {
		return verr
	}
	latencyDomain := strings.ToLower(netutil.ExtractDomain(u.Host))
	if cfg.MaxConsecutiveFailures < 0 {
		return invalidArg("max_consecutive_failures: must be non-negative")
	}
	// Status codes must be plausible HTTP codes; an out-of-range value would
	// silently never match and disable rejection for that code.
	for _, code := range cfg.ProbeRejectStatusCodes {
		if code < 100 || code > 599 {
			return invalidArg("probe_reject_status_codes: each entry must be a valid HTTP status code (100-599)")
		}
	}
	// The probe template is operator-supplied input that is sent verbatim
	// through every node, so bound it: a huge body would multiply across the
	// whole pool on every probe cycle.
	if err := validateProbeRequestTemplate(cfg.ProbeRequest); err != nil {
		return err.(*ServiceError)
	}
	for _, marker := range cfg.ProbeRejectBodySubstrings {
		if len(marker) > maxProbeRejectMarkerLen {
			return invalidArg("probe_reject_body_substrings: each marker must be at most 256 bytes")
		}
	}
	// A script that does not compile must be rejected at save time, not
	// discovered later as a silently disabled probe.
	if err := validateProbeScripts(cfg); err != nil {
		return err
	}
	if cfg.CacheFlushDirtyThreshold < 0 {
		return invalidArg("cache_flush_dirty_threshold: must be non-negative")
	}
	// Request log bytes fields must be non-negative.
	if cfg.ReverseProxyLogReqHeadersMaxBytes < 0 {
		return invalidArg("reverse_proxy_log_req_headers_max_bytes: must be non-negative")
	}
	if cfg.ReverseProxyLogReqBodyMaxBytes < 0 {
		return invalidArg("reverse_proxy_log_req_body_max_bytes: must be non-negative")
	}
	if cfg.ReverseProxyLogRespHeadersMaxBytes < 0 {
		return invalidArg("reverse_proxy_log_resp_headers_max_bytes: must be non-negative")
	}
	if cfg.ReverseProxyLogRespBodyMaxBytes < 0 {
		return invalidArg("reverse_proxy_log_resp_body_max_bytes: must be non-negative")
	}
	minProbeInterval := 30 * time.Second
	// Probe intervals must be at least 30s (DESIGN.md).
	if time.Duration(cfg.MaxLatencyTestInterval) < minProbeInterval {
		return invalidArg("max_latency_test_interval: must be >= 30s")
	}
	if time.Duration(cfg.MaxAuthorityLatencyTestInterval) < minProbeInterval {
		return invalidArg("max_authority_latency_test_interval: must be >= 30s")
	}
	if time.Duration(cfg.MaxEgressTestInterval) < minProbeInterval {
		return invalidArg("max_egress_test_interval: must be >= 30s")
	}
	if cfg.P2CLatencyWindow < 0 {
		return invalidArg("p2c_latency_window: must be non-negative")
	}
	if cfg.LatencyDecayWindow < 0 {
		return invalidArg("latency_decay_window: must be non-negative")
	}
	minCacheFlushInterval := 5 * time.Second
	if time.Duration(cfg.CacheFlushInterval) < minCacheFlushInterval {
		return invalidArg("cache_flush_interval: must be >= 5s")
	}

	// LatencyTestURL domain must be in LatencyAuthorities.
	// If absent, append it instead of returning an error.
	if latencyDomain != "" {
		found := false
		for _, authority := range cfg.LatencyAuthorities {
			if strings.EqualFold(strings.TrimSpace(authority), latencyDomain) {
				found = true
				break
			}
		}
		if !found {
			cfg.LatencyAuthorities = append(cfg.LatencyAuthorities, latencyDomain)
		}
	}
	return nil
}

// maxProbeScriptLen bounds an operator script. The scripts run on every probe
// cycle across the whole pool; an unbounded script is a self-inflicted DoS.
const maxProbeScriptLen = 16 * 1024

// validateProbeScripts compiles the operator scripts so a syntax error is
// caught at save time. Without this, a broken script would only show up as a
// probe that quietly falls back to default behaviour.
func validateProbeScripts(cfg *config.RuntimeConfig) *ServiceError {
	for name, src := range map[string]string{
		"probe_script_build": cfg.ProbeScriptBuild,
		"probe_script_judge": cfg.ProbeScriptJudge,
	} {
		if len(src) > maxProbeScriptLen {
			return invalidArg(name + ": script must be at most 16384 bytes")
		}
		if strings.TrimSpace(src) == "" {
			continue
		}
		if err := probescript.Compile(src); err != nil {
			return invalidArg(name + ": " + err.Error())
		}
	}
	return nil
}

const (
	// maxProbeRequestBodyLen bounds the probe request body. The template is
	// replayed through every probed node, so an unbounded body would be
	// multiplied across the whole pool on each cycle.
	maxProbeRequestBodyLen = 8 * 1024
	// maxProbeRequestHeaderLen bounds a single header value.
	maxProbeRequestHeaderLen = 4 * 1024
	// maxProbeRejectMarkerLen bounds one body marker.
	maxProbeRejectMarkerLen = 256
)

// allowedProbeMethods are the methods a probe may use. Restricted to the verbs
// a read-only health check needs: a probe must never mutate upstream state.
var allowedProbeMethods = map[string]bool{
	http.MethodGet:  true,
	http.MethodHead: true,
	http.MethodPost: true,
}

// validateProbeRequestTemplate rejects templates that are unsafe or unbounded.
func validateProbeRequestTemplate(tpl config.ProbeRequestTemplate) error {
	if tpl.IsZero() {
		return nil
	}
	if tpl.Method != "" {
		method := strings.ToUpper(strings.TrimSpace(tpl.Method))
		if !allowedProbeMethods[method] {
			return invalidArg("probe_request.method: only GET, HEAD and POST are allowed")
		}
	}
	if len(tpl.Body) > maxProbeRequestBodyLen {
		return invalidArg("probe_request.body: must be at most 8192 bytes")
	}
	for name, value := range tpl.Headers {
		if strings.TrimSpace(name) == "" {
			return invalidArg("probe_request.headers: header name must not be empty")
		}
		if len(value) > maxProbeRequestHeaderLen {
			return invalidArg("probe_request.headers: each header value must be at most 4096 bytes")
		}
	}
	return nil
}

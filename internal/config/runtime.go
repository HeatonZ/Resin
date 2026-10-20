package config

import "time"

// ProbeRequestTemplate describes a replayed HTTP probe request. The zero value
// means "plain GET with no body", preserving legacy behaviour.
type ProbeRequestTemplate struct {
	// Method is the HTTP method; empty means GET.
	Method string `json:"method"`
	// Body is the request body sent verbatim; empty means no body.
	Body string `json:"body"`
	// Headers are extra request headers (Content-Type, auth, client identity).
	Headers map[string]string `json:"headers"`
}

// IsZero reports whether the template is unset, so callers can fall back to the
// legacy plain-GET probe without special-casing each field.
func (t ProbeRequestTemplate) IsZero() bool {
	return t.Method == "" && t.Body == "" && len(t.Headers) == 0
}

// RuntimeConfig holds all hot-updatable global settings.
// These are persisted in the database and served via GET /system/config.
type RuntimeConfig struct {
	// Request log
	RequestLogEnabled                  bool `json:"request_log_enabled"`
	ReverseProxyLogDetailEnabled       bool `json:"reverse_proxy_log_detail_enabled"`
	ReverseProxyLogReqHeadersMaxBytes  int  `json:"reverse_proxy_log_req_headers_max_bytes"`
	ReverseProxyLogReqBodyMaxBytes     int  `json:"reverse_proxy_log_req_body_max_bytes"`
	ReverseProxyLogRespHeadersMaxBytes int  `json:"reverse_proxy_log_resp_headers_max_bytes"`
	ReverseProxyLogRespBodyMaxBytes    int  `json:"reverse_proxy_log_resp_body_max_bytes"`

	// Health check
	MaxConsecutiveFailures          int      `json:"max_consecutive_failures"`
	MaxLatencyTestInterval          Duration `json:"max_latency_test_interval"`
	MaxAuthorityLatencyTestInterval Duration `json:"max_authority_latency_test_interval"`
	MaxEgressTestInterval           Duration `json:"max_egress_test_interval"`

	// Probe
	LatencyTestURL     string   `json:"latency_test_url"`
	LatencyAuthorities []string `json:"latency_authorities"`
	// ProbeRejectStatusCodes lists HTTP status codes that must be treated as a
	// failed probe even though the request completed. A node behind an upstream
	// gate answers 403/429/5xx over a perfectly working TCP/TLS path; without
	// this, every gated node looks healthy. Empty disables status-based
	// rejection and restores the legacy "any response is healthy" behaviour.
	ProbeRejectStatusCodes []int `json:"probe_reject_status_codes"`
	// ProbeRejectOnlyUnreachable limits probe failure to connectivity problems.
	//
	// A 403 does not prove the egress is unusable: the free tier also answers
	// 403 when the request lacks a real client identity, so a healthy node and a
	// blocked one are indistinguishable without credentials. When true, only
	// transport-level failures (dial timeout, connection reset, TLS error) fail
	// the probe; a completed HTTP exchange counts as reachable regardless of the
	// status code. Use this when the probe cannot carry credentials.
	ProbeRejectOnlyUnreachable bool `json:"probe_reject_only_unreachable"`
	// ProbeRequest is an optional HTTP probe template. A bare GET of a website
	// root answers 200 for every node, so a gated node looks identical to a
	// healthy one; replaying the real business request (method, body, client
	// headers) is what actually reproduces what traffic does. Empty means the
	// legacy plain GET.
	ProbeRequest ProbeRequestTemplate `json:"probe_request"`
	// ProbeRejectBodySubstrings rejects a probe whose body contains any of
	// these substrings (case-insensitive). Gates that answer 200 with an error
	// document need this; a healthy 200 body never contains them.
	ProbeRejectBodySubstrings []string `json:"probe_reject_body_substrings"`
	// ProbeScriptBuild is a JavaScript snippet returning {url, method, headers,
	// body}. It replaces the declarative probe_request template, which cannot
	// express logic (fetch a token first, branch on node attributes, ...).
	// Empty = use probe_request or the legacy plain GET.
	ProbeScriptBuild string `json:"probe_script_build"`
	// ProbeScriptJudge is a JavaScript snippet receiving (ctx, statusCode, body)
	// and returning {ok, reason}. It overrides the status/body-marker rules so
	// arbitrary upstreams can be judged. Empty = built-in rules only.
	ProbeScriptJudge string `json:"probe_script_judge"`
	// ProbeScriptEnabled gates both scripts. Off by default: an operator script
	// runs across the whole pool, so it must be an explicit opt-in.
	ProbeScriptEnabled bool `json:"probe_script_enabled"`
	// ProbeScriptSecrets are key/value pairs exposed to the scripts as
	// ctx.secrets. This is where upstream credentials belong: the probe needs
	// them to tell "this egress is blocked" from "this request carries no
	// identity", and the two are otherwise byte-identical.
	//
	// json:"-" keeps them out of GET /system/config and out of the persisted
	// config blob, so a credential never leaks through the API or a state.db
	// dump. They are supplied once per process via RESIN_PROBE_SECRET_<NAME>
	// and are write-only: the PATCH response reports names, never values.
	ProbeScriptSecrets map[string]string `json:"-"`

	// P2C
	P2CLatencyWindow   Duration `json:"p2c_latency_window"`
	LatencyDecayWindow Duration `json:"latency_decay_window"`

	// Persistence
	CacheFlushInterval       Duration `json:"cache_flush_interval"`
	CacheFlushDirtyThreshold int      `json:"cache_flush_dirty_threshold"`
}

// ApplyMissingDefaults fills defaults into fields that were absent from
// persisted JSON. Older config rows predate fields added later (for example
// probe_reject_status_codes), and unmarshalling leaves those as nil. Without
// this, an upgrade silently disables the new rules instead of adopting their
// defaults.
//
// Only nil slices are filled. An explicitly stored empty slice is a deliberate
// operator choice ("disable this rule"), and JSON distinguishes the two, so it
// must be preserved.
func (c *RuntimeConfig) ApplyMissingDefaults() {
	def := NewDefaultRuntimeConfig()
	if c.ProbeRejectStatusCodes == nil {
		c.ProbeRejectStatusCodes = def.ProbeRejectStatusCodes
	}
	if c.LatencyAuthorities == nil {
		c.LatencyAuthorities = def.LatencyAuthorities
	}
}

// NewDefaultRuntimeConfig returns a RuntimeConfig populated with the default
// values specified in DESIGN.md §运行时全局设置项.
func NewDefaultRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		RequestLogEnabled:                  true,
		ReverseProxyLogDetailEnabled:       false,
		ReverseProxyLogReqHeadersMaxBytes:  4096,
		ReverseProxyLogReqBodyMaxBytes:     1024,
		ReverseProxyLogRespHeadersMaxBytes: 1024,
		ReverseProxyLogRespBodyMaxBytes:    1024,

		MaxConsecutiveFailures:          3,
		MaxLatencyTestInterval:          Duration(1 * time.Hour),
		MaxAuthorityLatencyTestInterval: Duration(3 * time.Hour),
		MaxEgressTestInterval:           Duration(24 * time.Hour),

		LatencyTestURL:     "https://www.gstatic.com/generate_204",
		LatencyAuthorities: []string{"gstatic.com", "google.com", "cloudflare.com", "github.com"},
		// A 403/429/5xx answer means the upstream gate rejected this egress;
		// counting it as a healthy probe is what let fully-gated nodes stay in
		// rotation. Defaults are the codes observed from the free-tier gate.
		ProbeRejectStatusCodes: []int{403, 429, 500, 502, 503, 504},

		P2CLatencyWindow:   Duration(10 * time.Minute),
		LatencyDecayWindow: Duration(10 * time.Minute),

		CacheFlushInterval:       Duration(5 * time.Minute),
		CacheFlushDirtyThreshold: 1000,
	}
}

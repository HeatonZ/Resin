package probescript

import (
	"strings"
	"testing"
	"time"
)

const buildScript = `
(function(ctx){
	return {
		url: "https://opencode.ai/inference/v1/chat/completions",
		method: "POST",
		headers: {
			"Content-Type": "application/json",
			"x-opencode-client": "cli"
		},
		body: JSON.stringify({
			model: "claude-sonnet-4",
			messages: [{role: "user", content: "hi"}],
			max_tokens: 1
		})
	};
})(ctx)
`

const judgeScript = `
(function(ctx, statusCode, body){
	if (statusCode === 403 || statusCode === 429) {
		return {ok: false, reason: "gated status " + statusCode};
	}
	if (statusCode >= 500) {
		return {ok: false, reason: "upstream 5xx"};
	}
	var b = (body || "").toLowerCase();
	if (b.indexOf("free tier can only be used") >= 0) {
		return {ok: false, reason: "free tier gate"};
	}
	if (b.indexOf("abusive traffic") >= 0) {
		return {ok: false, reason: "abusive traffic gate"};
	}
	return {ok: true, reason: "healthy"};
})(ctx, statusCode, body)
`

func TestEngine_BuildRequest(t *testing.T) {
	e := New(buildScript, "")
	if !e.Enabled() {
		t.Fatalf("engine should be enabled, err=%v", e.Err())
	}

	req, err := e.BuildRequest(Context{NodeHash: "abc", BaseURL: "https://x"})
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if req.Method != "POST" {
		t.Fatalf("method = %q, want POST", req.Method)
	}
	if req.URL != "https://opencode.ai/inference/v1/chat/completions" {
		t.Fatalf("url = %q", req.URL)
	}
	if req.Headers["x-opencode-client"] != "cli" {
		t.Fatalf("client header missing: %v", req.Headers)
	}
	if !strings.Contains(req.Body, `"max_tokens":1`) {
		t.Fatalf("body not built as expected: %q", req.Body)
	}
}

func TestEngine_Judge(t *testing.T) {
	e := New("", judgeScript)
	if !e.ShouldJudge() {
		t.Fatalf("judge should be enabled, err=%v", e.Err())
	}

	cases := []struct {
		name   string
		status int
		body   string
		wantOK bool
	}{
		{name: "403 gated", status: 403, body: "", wantOK: false},
		{name: "429 rate limited", status: 429, body: "", wantOK: false},
		{name: "502 bad gateway", status: 502, body: "", wantOK: false},
		{name: "200 free tier gate", status: 200, body: `{"error":{"message":"free tier can only be used from within opencode"}}`, wantOK: false},
		{name: "200 abusive traffic", status: 200, body: `abusive traffic from this IP address`, wantOK: false},
		{name: "healthy 200", status: 200, body: `{"id":"chatcmpl-1","choices":[]}`, wantOK: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := e.Judge(Context{}, tc.status, []byte(tc.body))
			if err != nil {
				t.Fatalf("judge: %v", err)
			}
			if res.OK != tc.wantOK {
				t.Fatalf("ok = %v, want %v (reason=%q)", res.OK, tc.wantOK, res.Reason)
			}
		})
	}
}

// TestEngine_TimeoutInterruptsRunawayScript is the safety property that matters
// most: a `while(true)` must not wedge a probe worker forever. goja has no
// context support, so this only works because the engine interrupts on a timer.
func TestEngine_TimeoutInterruptsRunawayScript(t *testing.T) {
	e := New(`(function(){ while(true){} })()`, "")

	done := make(chan error, 1)
	go func() {
		start := time.Now()
		_, err := e.BuildRequest(Context{})
		elapsed := time.Since(start)
		if elapsed > defaultTimeout+2*time.Second {
			t.Errorf("runaway script took %s, timeout not enforced", elapsed)
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout error, got nil")
		}
		if !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("expected timeout error, got: %v", err)
		}
	case <-time.After(defaultTimeout + 10*time.Second):
		t.Fatal("engine never returned: runaway script wedged the worker")
	}
}

// TestEngine_SandboxHasNoEscapeHatches pins the security boundary: the VM must
// expose no filesystem, network, process or module system.
func TestEngine_SandboxHasNoEscapeHatches(t *testing.T) {
	for _, probe := range []string{
		`(function(){ return {url: "https://x/t=" + typeof require, method:"GET"} })()`,
		`(function(){ return {url: "https://x/t=" + typeof process, method:"GET"} })()`,
		`(function(){ return {url: "https://x/t=" + typeof fetch, method:"GET"} })()`,
		`(function(){ return {url: "https://x/t=" + typeof module, method:"GET"} })()`,
		`(function(){ return {url: "https://x/t=" + typeof XMLHttpRequest, method:"GET"} })()`,
		`(function(){ return {url: "https://x/t=" + typeof globalThis.process, method:"GET"} })()`,
		`(function(){ return {url: "https://x/t=" + new Function("return typeof process")(), method:"GET"} })()`,
	} {
		e := New(probe, "")
		req, err := e.BuildRequest(Context{})
		if err != nil {
			// A script that throws while introspecting is equally fine: it
			// proves the binding is absent.
			continue
		}
		if want := "https://x/t=undefined"; req.URL != want {
			t.Fatalf("sandbox leak: %q resolved to %q, want %q", probe, req.URL, want)
		}
	}
}

// TestEngine_RejectsUnsafeRequest covers the amplification guardrails: a probe
// is replayed across every node in the pool, so mutating verbs and oversized
// bodies must be refused even if a script produces them.
func TestEngine_RejectsUnsafeRequest(t *testing.T) {
	cases := []struct {
		name   string
		script string
	}{
		{name: "DELETE refused", script: `(function(){ return {url:"https://x/y", method:"DELETE"} })()`},
		{name: "PUT refused", script: `(function(){ return {url:"https://x/y", method:"PUT"} })()`},
		{name: "PATCH refused", script: `(function(){ return {url:"https://x/y", method:"PATCH"} })()`},
		{name: "non-http url refused", script: `(function(){ return {url:"file:///etc/passwd", method:"GET"} })()`},
		{name: "empty url refused", script: `(function(){ return {url:"", method:"GET"} })()`},
		{name: "oversized body refused", script: `(function(){ return {url:"https://x/y", method:"POST", body: "a".repeat(9000)} })()`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := New(tc.script, "")
			if _, err := e.BuildRequest(Context{}); err == nil {
				t.Fatal("expected request to be rejected")
			}
		})
	}
}

// TestEngine_CompileErrorDoesNotPanic verifies a typo in an operator script
// surfaces as an error instead of taking the probe down.
func TestEngine_CompileErrorDoesNotPanic(t *testing.T) {
	e := New(`(function( {`, "")
	if e.Enabled() {
		t.Fatal("broken script must not enable the engine")
	}
	if e.Err() == nil {
		t.Fatal("expected a compile error to be reported")
	}
	if _, err := e.BuildRequest(Context{}); err == nil {
		t.Fatal("disabled engine must return an error, not a zero request")
	}
}

// TestEngine_RuntimeErrorSurfaces verifies a throwing script reports an error
// rather than silently producing a zero-value request that would look like a
// probe failure against every node.
func TestEngine_RuntimeErrorSurfaces(t *testing.T) {
	e := New(`(function(){ throw new Error("boom"); })()`, "")
	_, err := e.BuildRequest(Context{})
	if err == nil {
		t.Fatal("expected runtime error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected original message, got: %v", err)
	}
}

// TestEngine_ContextFieldsVisible confirms the script can read the node context
// it is documented to receive.
func TestEngine_ContextFieldsVisible(t *testing.T) {
	e := New(`(function(ctx){ return {url: "https://x/" + ctx.nodeHash + "?tag=" + ctx.nodeTag, method:"GET"} })(ctx)`, "")
	req, err := e.BuildRequest(Context{NodeHash: "deadbeef", NodeTag: "hk-01"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if req.URL != "https://x/deadbeef?tag=hk-01" {
		t.Fatalf("context not visible: %q", req.URL)
	}
}

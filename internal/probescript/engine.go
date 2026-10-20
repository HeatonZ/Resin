package probescript

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
)

// Request is the probe request a script produced. It is intentionally narrow:
// the script decides *what to send*, never *where the bytes go*, so a bad
// script cannot turn the probe into a tunnel or a scanner.
type Request struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// Result is what a script judges after the request came back.
type Result struct {
	// OK reports whether this probe attempt counts as healthy.
	OK bool `json:"ok"`
	// Reason is a short human-readable explanation shown in probe logs.
	Reason string `json:"reason"`
}

// Context is the read-only data handed to the script. Deliberately limited to
// non-secret facts about the node: the script runs across the whole pool, so
// anything put here is amplified across every node.
type Context struct {
	NodeHash  string            `json:"nodeHash"`
	NodeTag   string            `json:"nodeTag"`
	Platform  string            `json:"platform"`
	EgressIP  string            `json:"egressIP"`
	ProxyType string            `json:"proxyType"`
	NowUnix   int64             `json:"nowUnix"`
	BaseURL   string            `json:"baseURL"`
	Env       map[string]string `json:"env"`
}

// asMap flattens the context into the exact shape a script sees. Keeping this
// explicit means adding a Go field never silently changes the JS API.
func (c Context) asMap() map[string]any {
	m := map[string]any{
		"nodeHash":  c.NodeHash,
		"nodeTag":   c.NodeTag,
		"platform":  c.Platform,
		"egressIP":  c.EgressIP,
		"proxyType": c.ProxyType,
		"nowUnix":   c.NowUnix,
		"baseURL":   c.BaseURL,
	}
	if len(c.Env) > 0 {
		env := make(map[string]any, len(c.Env))
		for k, v := range c.Env {
			env[k] = v
		}
		m["env"] = env
	}
	return m
}

const (
	// defaultTimeout bounds one script evaluation. A probe cycle sweeps
	// thousands of nodes, so a runaway loop must not wedge the worker.
	defaultTimeout = 2 * time.Second
	// maxBodyBytes bounds the request body a script may produce; the probe
	// replays it through every node in the pool.
	maxBodyBytes = 8 * 1024
)

var allowedMethods = map[string]bool{
	"GET":  true,
	"HEAD": true,
	"POST": true,
}

// Compile checks that an operator script parses, without running it. Used at
// config-save time so a syntax error cannot be persisted.
func Compile(src string) error {
	if strings.TrimSpace(src) == "" {
		return nil
	}
	if _, err := goja.Compile("probe_script", src, false); err != nil {
		return err
	}
	return nil
}

// Engine executes operator-supplied probe scripts. It is safe for concurrent
// use: each evaluation gets its own VM, since goja runtimes are single-threaded.
type Engine struct {
	mu       sync.RWMutex
	prog     *goja.Program // compiled BuildRequest script, nil when disabled
	judge    *goja.Program // compiled Judge script, nil when disabled
	srcBuild string
	srcJudge string
	initErr  error
}

// New compiles the two scripts once. A compile error is reported but does not
// disable the probe: the caller falls back to the built-in request so a typo in
// a script cannot take health checking offline.
func New(buildSrc, judgeSrc string) *Engine {
	e := &Engine{srcBuild: buildSrc, srcJudge: judgeSrc}
	if strings.TrimSpace(buildSrc) != "" {
		prog, err := goja.Compile("probe_build", buildSrc, false)
		if err != nil {
			e.initErr = fmt.Errorf("compile probe build script: %w", err)
			return e
		}
		e.prog = prog
	}
	if strings.TrimSpace(judgeSrc) != "" {
		prog, err := goja.Compile("probe_judge", judgeSrc, false)
		if err != nil {
			e.initErr = fmt.Errorf("compile probe judge script: %w", err)
			return e
		}
		e.judge = prog
	}
	return e
}

// Enabled reports whether a request-building script is active.
func (e *Engine) Enabled() bool { return e != nil && e.prog != nil }

// ShouldJudge reports whether a response-judging script is active.
func (e *Engine) ShouldJudge() bool { return e != nil && e.judge != nil }

// Err returns the compile error, if the scripts failed to load.
func (e *Engine) Err() error { return e.initErr }

// BuildRequest runs the request-building script under a hard timeout.
func (e *Engine) BuildRequest(ctx Context) (Request, error) {
	if !e.Enabled() {
		return Request{}, fmt.Errorf("probe script engine disabled")
	}
	vm := goja.New()
	// The sandbox is the absence of bindings: no require, no fetch, no fs, no
	// process. Only the values set below exist inside the VM.
	//
	// Passed as a map rather than a Go struct: goja exposes Go struct fields by
	// their Go names, so a script writing ctx.nodeHash would silently get
	// undefined. A map keeps the JS-facing shape explicit and stable.
	if err := vm.Set("ctx", ctx.asMap()); err != nil {
		return Request{}, err
	}

	raw, err := runWithTimeout(vm, e.prog, defaultTimeout)
	if err != nil {
		return Request{}, err
	}

	// Read fields explicitly: goja's struct decoding ignores `json` tags, so
	// ExportTo(&Request{}) leaves every field at its zero value and a healthy
	// result looks like an empty request.
	if raw == nil || goja.IsUndefined(raw) || goja.IsNull(raw) {
		return Request{}, fmt.Errorf("probe script returned no value")
	}
	obj := raw.ToObject(vm)
	req := Request{
		URL:    stringProp(obj, "url"),
		Method: stringProp(obj, "method"),
		Body:   stringProp(obj, "body"),
	}
	if hv := obj.Get("headers"); hv != nil && !goja.IsUndefined(hv) && !goja.IsNull(hv) {
		headers := make(map[string]string)
		if err := vm.ExportTo(hv, &headers); err != nil {
			return Request{}, fmt.Errorf("probe script headers unusable: %w", err)
		}
		req.Headers = headers
	}
	return validateRequest(req)
}

// Judge runs the response-judging script under a hard timeout.
func (e *Engine) Judge(ctx Context, statusCode int, body []byte) (Result, error) {
	if !e.ShouldJudge() {
		return Result{}, fmt.Errorf("probe judge script disabled")
	}
	vm := goja.New()
	if err := vm.Set("ctx", ctx.asMap()); err != nil {
		return Result{}, err
	}
	if err := vm.Set("statusCode", statusCode); err != nil {
		return Result{}, err
	}
	// Bodies can be large (an HTML homepage is 66KB). Bound what the script can
	// see so one pathological response cannot balloon memory per node.
	if err := vm.Set("body", truncateForScript(body, 64*1024)); err != nil {
		return Result{}, err
	}

	raw, err := runWithTimeout(vm, e.judge, defaultTimeout)
	if err != nil {
		return Result{}, err
	}

	if raw == nil || goja.IsUndefined(raw) || goja.IsNull(raw) {
		return Result{}, fmt.Errorf("probe judge script returned no value")
	}
	obj := raw.ToObject(vm)
	return Result{
		OK:     obj.Get("ok").ToBoolean(),
		Reason: obj.Get("reason").String(),
	}, nil
}

// runWithTimeout evaluates prog with an interrupt deadline. goja has no context
// support, so the deadline is enforced by interrupting from a watchdog
// goroutine — without this a `while(true)` in a script hangs a probe worker
// permanently.
func runWithTimeout(vm *goja.Runtime, prog *goja.Program, d time.Duration) (goja.Value, error) {
	timer := time.AfterFunc(d, func() {
		vm.Interrupt("probe script timed out")
	})
	defer timer.Stop()

	val, err := vm.RunProgram(prog)
	if err != nil {
		if _, ok := err.(*goja.InterruptedError); ok {
			return nil, fmt.Errorf("probe script timed out after %s", d)
		}
		return nil, fmt.Errorf("probe script error: %w", err)
	}
	return val, nil
}

// stringProp reads a string property defensively. goja's Object.Get returns nil
// for a missing property, and calling String() on nil panics — a script that
// omits an optional field must not crash the probe.
func stringProp(obj *goja.Object, name string) string {
	v := obj.Get(name)
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return ""
	}
	return v.String()
}

func truncateForScript(body []byte, limit int) string {
	if len(body) <= limit {
		return string(body)
	}
	return string(body[:limit])
}

func validateRequest(req Request) (Request, error) {
	req.URL = strings.TrimSpace(req.URL)
	if req.URL == "" {
		return Request{}, fmt.Errorf("probe script produced empty url")
	}
	if !strings.HasPrefix(req.URL, "http://") && !strings.HasPrefix(req.URL, "https://") {
		return Request{}, fmt.Errorf("probe script url must be http(s), got %q", req.URL)
	}
	req.Method = strings.ToUpper(strings.TrimSpace(req.Method))
	if req.Method == "" {
		req.Method = "GET"
	}
	if !allowedMethods[req.Method] {
		return Request{}, fmt.Errorf("probe script method %q not allowed (GET/HEAD/POST only)", req.Method)
	}
	if len(req.Body) > maxBodyBytes {
		return Request{}, fmt.Errorf("probe script body exceeds %d bytes", maxBodyBytes)
	}
	return req, nil
}

// MarshalRequest is a helper for tests and logging.
func MarshalRequest(req Request) string {
	b, err := json.Marshal(req)
	if err != nil {
		return "{}"
	}
	return string(b)
}

package netutil

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

const defaultOutboundUserAgent = "Resin/1.0"

// headerHasKey reports whether the header map already carries name, ignoring
// case: a script may spell it "user-agent" and http.Header.Set would then
// normalise it into the same slot the default would overwrite.
func headerHasKey(h map[string]string, name string) bool {
	for k := range h {
		if strings.EqualFold(k, name) {
			return true
		}
	}
	return false
}

type ConnLifecycleOp uint8

const (
	ConnLifecycleOpen ConnLifecycleOp = iota
	ConnLifecycleClose
)

// OutboundHTTPOptions controls outbound-backed HTTP execution behavior.
type OutboundHTTPOptions struct {
	// RequireStatusOK enforces HTTP 200 status; otherwise any status is accepted.
	RequireStatusOK bool
	// AcceptStatus, when non-nil, overrides RequireStatusOK: the response is
	// accepted only when AcceptStatus(statusCode) returns true. Probes use this
	// to reject gate responses (403/429/5xx) that still arrive over a working
	// TCP/TLS path — a node that answers 502 is not healthy just because it
	// answered.
	AcceptStatus func(statusCode int) bool
	// AcceptBody, when non-nil, gets the (already read) response body and can
	// reject it. Status alone cannot tell a healthy node from a gated one when
	// the gate answers 200 with an error document in the body, so a probe that
	// replays a real request needs to inspect the payload too.
	AcceptBody func(body []byte) bool
	// OnStatus receives the HTTP status code as soon as headers arrive. Callers
	// that judge the response themselves (a script) need the real code; the
	// return values only carry body and latency.
	OnStatus func(statusCode int)
	// Method overrides the request method; empty means GET.
	Method string
	// Body is the request body sent with the request (nil for GET).
	Body []byte
	// Header carries extra request headers (e.g. Content-Type, auth, client
	// identity headers a real client would send). The User-Agent set below
	// wins over any User-Agent supplied here.
	Header map[string]string
	// UserAgent overrides the request User-Agent when non-empty.
	UserAgent string
	// OnConnLifecycle is called with open/close lifecycle events to track connection
	// lifecycle for metrics. Set by probe callers to count outbound connections;
	// left nil for download callers (GeoIP, subscription) to exclude from stats.
	OnConnLifecycle func(op ConnLifecycleOp)
}

// HTTPGetViaOutbound executes an HTTP GET through the provided outbound.
// Timeout and cancellation are controlled solely by ctx.
func HTTPGetViaOutbound(
	ctx context.Context,
	outbound adapter.Outbound,
	url string,
	opts OutboundHTTPOptions,
) ([]byte, time.Duration, error) {
	if outbound == nil {
		return nil, 0, fmt.Errorf("outbound fetch: outbound is nil")
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := outbound.DialContext(ctx, network, M.ParseSocksaddr(addr))
			if err != nil {
				return nil, err
			}
			if opts.OnConnLifecycle != nil {
				opts.OnConnLifecycle(ConnLifecycleOpen)
				return &connCloseHook{Conn: conn, onClose: func() { opts.OnConnLifecycle(ConnLifecycleClose) }}, nil
			}
			return conn, nil
		},
		DisableKeepAlives: true,
		ForceAttemptHTTP2: true,
	}

	client := &http.Client{
		Transport: transport,
	}

	method := opts.Method
	if method == "" {
		method = http.MethodGet
	}
	var bodyReader io.Reader
	if len(opts.Body) > 0 {
		bodyReader = bytes.NewReader(opts.Body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, 0, err
	}

	userAgent := opts.UserAgent
	if userAgent == "" {
		userAgent = defaultOutboundUserAgent
	}
	for k, v := range opts.Header {
		req.Header.Set(k, v)
	}
	// The script's own User-Agent must win: upstreams that gate on client
	// identity read this header, so forcing the default here would silently
	// turn every scripted request back into a generic one. Header names from a
	// script are arbitrary case, so compare case-insensitively.
	if !headerHasKey(opts.Header, "User-Agent") {
		req.Header.Set("User-Agent", userAgent)
	}

	var start time.Time
	var latency time.Duration
	trace := &httptrace.ClientTrace{
		TLSHandshakeStart: func() { start = time.Now() },
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			if err == nil {
				latency = time.Since(start)
			}
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(ctx, trace))

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if opts.OnStatus != nil {
		opts.OnStatus(resp.StatusCode)
	}

	if opts.AcceptStatus != nil {
		if !opts.AcceptStatus(resp.StatusCode) {
			return nil, latency, fmt.Errorf("outbound fetch: rejected status %d from %s", resp.StatusCode, url)
		}
	} else if opts.RequireStatusOK && resp.StatusCode != http.StatusOK {
		return nil, latency, fmt.Errorf("outbound fetch: unexpected status %d from %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, latency, err
	}

	if opts.AcceptBody != nil && !opts.AcceptBody(body) {
		return nil, latency, fmt.Errorf("outbound fetch: rejected body from %s (len=%d)", url, len(body))
	}

	return body, latency, nil
}

// connCloseHook wraps a net.Conn and calls onClose exactly once on Close.
type connCloseHook struct {
	net.Conn
	onClose   func()
	closeOnce sync.Once
	closeErr  error
}

func (c *connCloseHook) Close() error {
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
		c.closeErr = c.Conn.Close()
	})
	return c.closeErr
}

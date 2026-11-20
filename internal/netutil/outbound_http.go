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
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/node"
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

// OutboundHTTPResult retains the actual connection's relay on success and failure.
type OutboundHTTPResult struct {
	Body    []byte
	Latency time.Duration
	Relay   node.RelayInfo
}

// HTTPGetViaOutbound executes an HTTP GET through the provided outbound.
// Timeout and cancellation are controlled solely by ctx.
func HTTPGetViaOutbound(
	ctx context.Context,
	outbound adapter.Outbound,
	url string,
	opts OutboundHTTPOptions,
) (result OutboundHTTPResult, err error) {
	if outbound == nil {
		return result, fmt.Errorf("outbound fetch: outbound is nil")
	}
	var relay atomic.Pointer[node.RelayInfo]
	var latency atomic.Int64
	defer func() {
		result.Latency = time.Duration(latency.Load())
		if info := relay.Load(); info != nil {
			result.Relay = *info
			err = info.WrapError(err)
		}
	}()

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := outbound.DialContext(WithoutHTTPTrace(ctx), network, M.ParseSocksaddr(addr))
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
		return result, err
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

	var start atomic.Pointer[time.Time]
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { relay.Store(nil) },
		GotConn: func(conn httptrace.GotConnInfo) {
			info := node.RelayInfoFromConn(conn.Conn)
			relay.Store(&info)
		},
		TLSHandshakeStart: func() { now := time.Now(); start.Store(&now) },
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			if began := start.Load(); err == nil && began != nil {
				latency.Store(int64(time.Since(*began)))
			}
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(ctx, trace))

	resp, err := client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()

	if opts.OnStatus != nil {
		opts.OnStatus(resp.StatusCode)
	}

	if opts.AcceptStatus != nil {
		if !opts.AcceptStatus(resp.StatusCode) {
			return result, fmt.Errorf("outbound fetch: rejected status %d from %s", resp.StatusCode, url)
		}
	} else if opts.RequireStatusOK && resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("outbound fetch: unexpected status %d from %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}

	if opts.AcceptBody != nil && !opts.AcceptBody(body) {
		return result, fmt.Errorf("outbound fetch: rejected body from %s (len=%d)", url, len(body))
	}

	result.Body = body
	return result, nil
}

// connCloseHook wraps a net.Conn and calls onClose exactly once on Close.
type connCloseHook struct {
	net.Conn
	onClose   func()
	closeOnce sync.Once
	closeErr  error
}

func (c *connCloseHook) Upstream() any { return c.Conn }

func (c *connCloseHook) Close() error {
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
		c.closeErr = c.Conn.Close()
	})
	return c.closeErr
}

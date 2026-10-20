package netutil

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Resinat/Resin/internal/testutil"
)

// TestHTTPGetViaOutbound_ProbeTemplateReplaysBusinessRequest pins the reason the
// template exists: a bare GET of a website root returns 200 for every node, so
// a gated node is indistinguishable from a healthy one. Replaying the real
// method/body/headers is what actually exposes the gate.
func TestHTTPGetViaOutbound_ProbeTemplateReplaysBusinessRequest(t *testing.T) {
	type captured struct {
		method  string
		body    string
		header  http.Header
		hasBody bool
	}
	got := make(chan captured, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- captured{method: r.Method, body: string(b), header: r.Header.Clone(), hasBody: len(b) > 0}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}

	_, _, err = HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		Method: http.MethodPost,
		Body:   []byte(`{"model":"x","messages":[]}`),
		Header: map[string]string{
			"Content-Type":      "application/json",
			"x-opencode-client": "cli",
		},
	})
	if err != nil {
		t.Fatalf("probe template request failed: %v", err)
	}

	c := <-got
	if c.method != http.MethodPost {
		t.Fatalf("method = %q, want POST", c.method)
	}
	if c.body != `{"model":"x","messages":[]}` {
		t.Fatalf("body = %q, want the template body verbatim", c.body)
	}
	if c.header.Get("Content-Type") != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", c.header.Get("Content-Type"))
	}
	if c.header.Get("x-opencode-client") != "cli" {
		t.Fatalf("client identity header not forwarded, got %q", c.header.Get("x-opencode-client"))
	}
}

// TestHTTPGetViaOutbound_AcceptBodyRejectsGateDocument covers the 200-with-error
// case: the transport succeeded and the status is 200, yet the body proves the
// node is gated. Without body inspection this node counts as healthy forever.
func TestHTTPGetViaOutbound_AcceptBodyRejectsGateDocument(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"error":{"type":"server_error","message":"free tier can only be used from within opencode"}}`))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}

	markers := []string{"free tier can only be used", "abusive traffic"}
	_, _, err = HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		AcceptBody: func(body []byte) bool {
			lower := strings.ToLower(string(body))
			for _, m := range markers {
				if strings.Contains(lower, m) {
					return false
				}
			}
			return true
		},
	})
	if err == nil {
		t.Fatal("expected gate document to be rejected, got nil error")
	}
	if !strings.Contains(err.Error(), "rejected body") {
		t.Fatalf("expected body rejection error, got: %v", err)
	}
}

// TestHTTPGetViaOutbound_HealthyBodyPassesMarkerCheck guards against a marker
// list so broad that it rejects healthy traffic — the failure mode that would
// burn the whole node pool at once.
func TestHTTPGetViaOutbound_HealthyBodyPassesMarkerCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"chatcmpl-1","choices":[{"message":{"content":"hi"}}]}`))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}

	body, _, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		AcceptBody: func(b []byte) bool {
			return !strings.Contains(strings.ToLower(string(b)), "free tier can only be used")
		},
	})
	if err != nil {
		t.Fatalf("healthy body must pass, got: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("expected body to be returned")
	}
}

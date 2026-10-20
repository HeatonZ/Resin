package netutil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Resinat/Resin/internal/testutil"
)

// TestHTTPGetViaOutbound_AcceptStatusRejectsGatedCodes covers the regression
// that let fully-gated egresses stay in rotation: a node behind an upstream
// gate answers 403/429/5xx over a working TCP/TLS path, so the transport
// succeeds while every real request through it fails. AcceptStatus must turn
// those into probe failures.
func TestHTTPGetViaOutbound_AcceptStatusRejectsGatedCodes(t *testing.T) {
	reject := map[int]bool{403: true, 429: true, 500: true, 502: true, 503: true, 504: true}
	accept := func(code int) bool { return !reject[code] }

	cases := []struct {
		name      string
		status    int
		wantErr   bool
		wantInMsg string
		wantBody  string
	}{
		{name: "403 gate rejected", status: http.StatusForbidden, wantErr: true, wantInMsg: "rejected status 403"},
		{name: "429 rate limited rejected", status: http.StatusTooManyRequests, wantErr: true, wantInMsg: "rejected status 429"},
		{name: "502 bad gateway rejected", status: http.StatusBadGateway, wantErr: true, wantInMsg: "rejected status 502"},
		{name: "200 accepted", status: http.StatusOK, wantErr: false, wantBody: "body"},
		// 204 carries no body by definition; only the accept decision matters.
		{name: "204 accepted", status: http.StatusNoContent, wantErr: false, wantBody: ""},
		{name: "401 accepted (not a gate signal)", status: http.StatusUnauthorized, wantErr: false, wantBody: "body"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("body"))
			}))
			defer srv.Close()

			ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
			if err != nil {
				t.Fatalf("build outbound: %v", err)
			}

			body, _, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
				AcceptStatus: accept,
			})

			if tc.wantErr {
				if err == nil {
					t.Fatalf("status %d: expected rejection, got nil error (body=%q)", tc.status, string(body))
				}
				if tc.wantInMsg != "" && !strings.Contains(err.Error(), tc.wantInMsg) {
					t.Fatalf("status %d: expected %q in error, got: %v", tc.status, tc.wantInMsg, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("status %d: expected success, got error: %v", tc.status, err)
			}
			if string(body) != tc.wantBody {
				t.Fatalf("status %d: body = %q, want %q", tc.status, string(body), tc.wantBody)
			}
		})
	}
}

// TestHTTPGetViaOutbound_AcceptStatusOverridesRequireStatusOK pins the
// precedence rule: when AcceptStatus is set it is authoritative, so a caller
// can accept a non-200 code even if RequireStatusOK was also passed.
func TestHTTPGetViaOutbound_AcceptStatusOverridesRequireStatusOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}

	_, _, err = HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: true,
		AcceptStatus:    func(code int) bool { return code == http.StatusNoContent },
	})
	if err != nil {
		t.Fatalf("AcceptStatus should take precedence over RequireStatusOK, got: %v", err)
	}
}

// TestHTTPGetViaOutbound_NilAcceptStatusKeepsLegacyBehaviour guards backward
// compatibility: with no AcceptStatus the old "any response is accepted"
// behaviour must remain, so other callers (subscription/GeoIP downloads) are
// unaffected by the probe change.
func TestHTTPGetViaOutbound_NilAcceptStatusKeepsLegacyBehaviour(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("legacy-body"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}

	body, _, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{})
	if err != nil {
		t.Fatalf("nil AcceptStatus should keep legacy permissive behaviour, got: %v", err)
	}
	if string(body) != "legacy-body" {
		t.Fatalf("body = %q, want %q", string(body), "legacy-body")
	}
}

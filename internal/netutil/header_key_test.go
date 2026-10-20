package netutil

import "testing"

// TestHeaderHasKeyIsCaseInsensitive guards the User-Agent precedence rule. A
// script writes header names as plain strings, so "user-agent" and
// "User-Agent" must both be recognised as already-set; otherwise the default
// silently overwrites the client identity the probe depends on.
func TestHeaderHasKeyIsCaseInsensitive(t *testing.T) {
	cases := []struct {
		headers map[string]string
		want    bool
	}{
		{map[string]string{"User-Agent": "opencode/1.18.31"}, true},
		{map[string]string{"user-agent": "opencode/1.18.31"}, true},
		{map[string]string{"USER-AGENT": "opencode/1.18.31"}, true},
		{map[string]string{"Authorization": "Bearer x"}, false},
		{nil, false},
	}
	for _, tc := range cases {
		if got := headerHasKey(tc.headers, "User-Agent"); got != tc.want {
			t.Errorf("headerHasKey(%v) = %v, want %v", tc.headers, got, tc.want)
		}
	}
}

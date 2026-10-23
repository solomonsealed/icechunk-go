package icechunk

import (
	"strings"
	"testing"
)

// The cases of upstream's attribution tests (icechunk-storage
// src/attribution.rs), with this module's product token.
func TestAttributionUserAgent(t *testing.T) {
	labels := func(a Attribution) requestLabels {
		t.Helper()
		l, err := newRequestLabels(a)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	none := labels(Attribution{})
	valueOf := func(ua, field string) string {
		_, rest, _ := strings.Cut(ua, field+"=")
		v, _, _ := strings.Cut(rest, ";")
		return strings.TrimSuffix(v, ")")
	}
	for _, c := range []struct {
		got, want string
	}{
		{none.userAgent("", false, nil, false), "icechunk/2.3.0"},
		{labels(Attribution{Client: "weatherlib/0.9", Workload: "nightly-ingest", Principal: "u_123"}).userAgent("", false, nil, false),
			"weatherlib/0.9 icechunk/2.3.0 (workload=nightly-ingest; principal=u_123)"},
		{labels(Attribution{Workload: "w"}).userAgent("g/temperature", true, []uint32{0, 1, 2}, true),
			"icechunk/2.3.0 (workload=w; array=g/temperature; chunk=0/1/2)"},
		{none.userAgent("a", true, nil, false), "icechunk/2.3.0 (array=a)"},
		{none.userAgent("", true, []uint32{}, true), "icechunk/2.3.0 (array=; chunk=)"}, // a root scalar array
		{none.userAgent("a(b)c\\d;e=f%gé h", true, nil, false), "icechunk/2.3.0 (array=a%28b%29c%5Cd%3Be%3Df%25g%C3%A9 h)"},
		{labels(Attribution{Principal: "p"}).userAgent("", false, nil, false), "icechunk/2.3.0 (principal=p)"},
	} {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}

	keep := (maxArrayBytes - 3) / 2
	long := strings.Repeat("h", 150) + "MIDDLE" + strings.Repeat("t", 150)
	if v := valueOf(none.userAgent(long, true, nil, false), "array"); v != strings.Repeat("h", keep)+"..."+strings.Repeat("t", keep) {
		t.Errorf("long array: %q", v)
	}
	v := valueOf(none.userAgent(strings.Repeat("é", 100), true, nil, false), "array")
	head, tail, _ := strings.Cut(v, "...")
	if len(head)%3 != 0 || len(tail)%3 != 0 || !strings.HasPrefix(tail, "%") {
		t.Errorf("truncation split an escape: %q", v)
	}
	if got := truncateMiddle(strings.Repeat("a", 125)+"%41"+strings.Repeat("b", 200)+"%42"+strings.Repeat("c", 125), maxArrayBytes); got != strings.Repeat("a", 125)+"..."+strings.Repeat("c", 125) {
		t.Errorf("escapes at both cut points: %q", got)
	}
	if got := truncateMiddle(strings.Repeat("A", 50)+"%41"+strings.Repeat("B", 10), 25); got != strings.Repeat("A", 11)+"..."+strings.Repeat("B", 10) {
		t.Errorf("orphaned hex digit: %q", got)
	}
	coords := make([]uint32, 40)
	for i := range coords {
		coords[i] = 1_000_000 + uint32(i)
	}
	v = valueOf(none.userAgent("", false, coords, true), "chunk")
	if len(v) != 2*((maxChunkBytes-3)/2)+3 || !strings.HasPrefix(v, "1000000/1000001/") || !strings.HasSuffix(v, "/1000038/1000039") {
		t.Errorf("long chunk: %q", v)
	}

	for _, bad := range []string{"a(b", "a)b", "a\\b", "a;b", "a=b", "a\"b", "aé", "a\nb", strings.Repeat("x", maxLabelBytes+1)} {
		if _, err := newRequestLabels(Attribution{Workload: bad}); err == nil {
			t.Errorf("workload %q accepted", bad)
		}
		if _, err := newRequestLabels(Attribution{Principal: bad}); err == nil {
			t.Errorf("principal %q accepted", bad)
		}
	}
	for _, ok := range []string{strings.Repeat("x", maxLabelBytes), "spaces are fine"} {
		if _, err := newRequestLabels(Attribution{Workload: ok}); err != nil {
			t.Errorf("workload %q: %v", ok, err)
		}
	}
	for _, bad := range []string{"a b", "a/", "/1", "a/b/c", "a(b)", "é"} {
		if _, err := newRequestLabels(Attribution{Client: bad}); err == nil {
			t.Errorf("client %q accepted", bad)
		}
	}
	for _, ok := range []string{"weatherlib", "weatherlib/0.9.1", "my-lib_2/1.0-rc1"} {
		if _, err := newRequestLabels(Attribution{Client: ok}); err != nil {
			t.Errorf("client %q: %v", ok, err)
		}
	}
}

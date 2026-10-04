package icechunk

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/solomonsealed/icechunk-go/storage"
)

// Every repo info file in the fixtures (current files and their backups)
// survives parse → encode → parse unchanged.
func TestRepoDocRoundTrip(t *testing.T) {
	var files []string
	for _, pattern := range []string{"testdata/*/*/repo", "testdata/*/*/overwritten/repo.*"} {
		m, _ := filepath.Glob(pattern)
		files = append(files, m...)
	}
	if len(files) < 10 {
		t.Fatalf("only %d repo files found", len(files))
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := parseRepoDoc(raw)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		enc, err := doc.encode()
		if err != nil {
			t.Fatalf("%s: encode: %v", f, err)
		}
		again, err := parseRepoDoc(enc)
		if err != nil {
			t.Fatalf("%s: reparse: %v", f, err)
		}
		if !reflect.DeepEqual(doc, again) {
			t.Errorf("%s: document changed in round trip", f)
		}
		// The public decoder agrees too.
		a, err1 := parseRepoInfo(raw)
		b, err2 := parseRepoInfo(enc)
		if err1 != nil || err2 != nil {
			t.Fatalf("%s: %v %v", f, err1, err2)
		}
		a.Implementation, b.Implementation = "", ""
		if !reflect.DeepEqual(a, b) {
			t.Errorf("%s: RepoInfo differs after re-encoding", f)
		}
	}
	t.Logf("%d repo files round-tripped", len(files))
	_ = context.Background
	_ = storage.ErrNotFound
	_ = strings.Contains
}

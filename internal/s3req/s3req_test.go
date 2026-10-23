package s3req

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/solomonsealed/icechunk-go/storage"
)

// Example "GET Bucket (List Objects)" from the AWS SigV4 documentation
// (Authenticating Requests: Using the Authorization Header).
func TestSignAWSExample(t *testing.T) {
	c := &Config{
		Bucket:          "examplebucket",
		Region:          "us-east-1",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	r := Request{Method: "GET", Header: map[string]string{}}
	sign(c, &r, "examplebucket.s3.amazonaws.com", "/", "max-keys=2&prefix=J", time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC))
	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;x-amz-content-sha256;x-amz-date, " +
		"Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
	if got := r.Header["authorization"]; got != want {
		t.Fatalf("authorization =\n%s\nwant\n%s", got, want)
	}
}

func TestTargets(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		cfg  Config
		key  string
		want string
	}{
		{Config{Bucket: "b", Region: "eu-west-1", Anonymous: true}, "snapshots/X", "https://b.s3.eu-west-1.amazonaws.com/snapshots/X"},
		{Config{Bucket: "b", Prefix: "repos/r1/", Anonymous: true}, "repo", "https://b.s3.us-east-1.amazonaws.com/repos/r1/repo"},
		{Config{Bucket: "b", ForcePathStyle: true, Anonymous: true}, "repo", "https://s3.us-east-1.amazonaws.com/b/repo"},
		{Config{Bucket: "b", Endpoint: "https://acct.r2.cloudflarestorage.com", Anonymous: true}, "refs/tag.it works!/ref.json", "https://acct.r2.cloudflarestorage.com/b/refs/tag.it%20works%21/ref.json"},
		{Config{Bucket: "b", Endpoint: "http://localhost:9000/", Anonymous: true}, "repo", "http://localhost:9000/b/repo"},
		{Config{Bucket: "b", Prefix: "/", Anonymous: true}, "snapshots/X", "https://b.s3.us-east-1.amazonaws.com//snapshots/X"},
	} {
		r, err := Get(&tc.cfg, tc.key, nil, now)
		if err != nil || r.URL != tc.want {
			t.Errorf("Get(%q) URL = %q, %v; want %q", tc.key, r.URL, err, tc.want)
		}
	}
	r, _ := List(&Config{Bucket: "b", Prefix: "p", Anonymous: true}, "refs/", "", now)
	if want := "https://b.s3.us-east-1.amazonaws.com/?list-type=2&prefix=p%2Frefs%2F"; r.URL != want {
		t.Errorf("List URL = %q, want %q", r.URL, want)
	}
}

func TestGetHeaders(t *testing.T) {
	h := GetHeaders(&storage.GetOptions{
		Range:             &storage.ByteRange{Offset: 10, Length: 5},
		IfMatch:           "abc",
		IfUnmodifiedSince: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
	})
	if h["range"] != "bytes=10-14" || h["if-match"] != `"abc"` || h["if-unmodified-since"] != "Tue, 02 Jan 2024 03:04:05 GMT" {
		t.Errorf("headers = %v", h)
	}
}

func TestPutHeaders(t *testing.T) {
	c := &Config{Bucket: "b", Region: "us-east-1", AccessKeyID: "AK", SecretAccessKey: "SK"}
	r, err := Put(c, "repo", []byte("x"), &storage.PutOptions{IfMatch: `"v"`, StorageClass: "STANDARD_IA",
		Metadata: map[string]string{"icFileType": "repo-info", storage.WriteIDMetadataKey: "w"}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if r.Header["if-match"] != `"v"` || r.Header["x-amz-storage-class"] != "STANDARD_IA" ||
		r.Header["x-amz-meta-icfiletype"] != "repo-info" || r.Header[WriteIDHeader] != "w" {
		t.Errorf("headers = %v", r.Header)
	}
	if !strings.Contains(r.Header["authorization"], "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-meta-icechunkwriteid;x-amz-meta-icfiletype;x-amz-storage-class,") {
		t.Errorf("authorization = %s", r.Header["authorization"])
	}
	if ConditionalWriteID(&storage.PutOptions{IfNotExists: true, Metadata: map[string]string{storage.WriteIDMetadataKey: "w"}}) != "w" ||
		ConditionalWriteID(&storage.PutOptions{Metadata: map[string]string{storage.WriteIDMetadataKey: "w"}}) != "" ||
		ConditionalWriteID(&storage.PutOptions{IfMatch: "v"}) != "" || ConditionalWriteID(nil) != "" {
		t.Error("ConditionalWriteID")
	}
}

func TestReadBack(t *testing.T) {
	for _, c := range []struct {
		status          int
		stored, etag    string
		version         string
		ours, wantError bool
	}{
		{200, "w", `"e1"`, "e1", true, false},
		{200, "other", `"e1"`, "", false, false},
		{200, "", `"e1"`, "", false, false},
		{404, "", "", "", false, false},
		{200, "w", "", "", false, true},
		{500, "", "", "", false, true},
		{403, "w", `"e1"`, "", false, true},
	} {
		v, ours, err := ReadBack("k", "w", c.status, c.stored, c.etag)
		if v != c.version || ours != c.ours || (err != nil) != c.wantError {
			t.Errorf("ReadBack(%d, %q, %q) = %q, %v, %v", c.status, c.stored, c.etag, v, ours, err)
		}
	}
}

func TestCopy(t *testing.T) {
	c := &Config{Bucket: "b", Prefix: "repos/r", Endpoint: "http://localhost:9000", AccessKeyID: "AK", SecretAccessKey: "SK"}
	r, err := Copy(c, "repo", "overwritten/repo.1.X", &storage.PutOptions{IfMatch: `"e1"`, StorageClass: "STANDARD_IA"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if r.Method != "PUT" || r.URL != "http://localhost:9000/b/repos/r/overwritten/repo.1.X" || r.Header["x-amz-copy-source"] != "/b/repos/r/repo" ||
		r.Header["x-amz-copy-source-if-match"] != `"e1"` || r.Header["x-amz-storage-class"] != "STANDARD_IA" ||
		!strings.Contains(r.Header["authorization"], "x-amz-copy-source;x-amz-copy-source-if-match;") {
		t.Errorf("request = %+v", r)
	}
	for _, tc := range []struct {
		status  int
		body    string
		version string
		err     error
	}{
		{200, `<CopyObjectResult><LastModified>x</LastModified><ETag>&quot;e2&quot;</ETag></CopyObjectResult>`, "e2", nil},
		{412, ``, "", storage.ErrPreconditionFailed},
		{404, ``, "", storage.ErrNotFound},
		{200, `<Error><Code>InternalError</Code></Error>`, "", nil},
		{500, `oops`, "", nil},
	} {
		v, err := CheckCopy("repo", "dst", tc.status, []byte(tc.body))
		if v != tc.version || (tc.err != nil && !errors.Is(err, tc.err)) || (tc.version == "" && err == nil) {
			t.Errorf("CheckCopy(%d, %s) = %q, %v", tc.status, tc.body, v, err)
		}
	}
}

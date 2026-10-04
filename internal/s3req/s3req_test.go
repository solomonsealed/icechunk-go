package s3req

import (
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

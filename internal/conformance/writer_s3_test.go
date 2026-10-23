package conformance

import (
	"context"
	"errors"
	"os"
	"testing"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/storage/httpstore"
	zarr "github.com/solomonsealed/zarr-go"
)

// s3Settings are the storage settings both writers use in the S3 check.
var s3Settings = icechunk.StorageSettings{StorageClass: "STANDARD_IA", ChunksStorageClass: "INTELLIGENT_TIERING"}

// TestWriteToS3 writes, for testdata/check_s3_objects.py, a repository to
// <bucket>/go on an S3 endpoint as that script writes one to <bucket>/py
// with icechunk-python: the script then compares their objects' storage
// classes and metadata. If <bucket>/py exists, it also commits on top of
// it, for the script to verify. It runs only when ICECHUNK_S3_ENDPOINT is
// set (with ICECHUNK_S3_BUCKET, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY).
func TestWriteToS3(t *testing.T) {
	endpoint := os.Getenv("ICECHUNK_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set ICECHUNK_S3_ENDPOINT to run")
	}
	ctx := context.Background()
	st := func(prefix string) *httpstore.S3 {
		return httpstore.NewS3(storage.S3Config{
			Bucket: os.Getenv("ICECHUNK_S3_BUCKET"), Prefix: prefix, Endpoint: endpoint, Region: "us-east-1",
			AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		}, nil)
	}
	write := func(repo *icechunk.Repository, path, message string) {
		t.Helper()
		s, err := repo.WritableSession(ctx, "main")
		if err != nil {
			t.Fatal(err)
		}
		arr, err := s.CreateArray(ctx, path, zarr.ArraySpec{Shape: []uint64{200}, ChunkShape: []uint64{100}, DataType: "float64",
			FillValue: 0.0, Codecs: []zarr.CodecSpec{{Name: "bytes", Configuration: []byte(`{"endian": "little"}`)}}})
		if err != nil {
			t.Fatal(err)
		}
		vals := make([]float64, 200)
		for i := range vals {
			vals[i] = float64(i) + 1
		}
		nd, _ := zarr.FromSlice([]uint64{200}, vals)
		if err := arr.Write(ctx, []uint64{0}, nd); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Commit(ctx, message, nil); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := st("go").Get(ctx, "repo", storage.RangeOf(0, 1)); err == nil {
		t.Fatalf("%s/go already holds a repository", os.Getenv("ICECHUNK_S3_BUCKET"))
	}
	repo, err := icechunk.Create(ctx, st("go"), &icechunk.Options{StorageSettings: s3Settings})
	if err != nil {
		t.Fatal(err)
	}
	write(repo, "/a", "write")
	main, err := repo.LookupBranch(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateTag(ctx, "t", main); err != nil {
		t.Fatal(err)
	}

	py, err := icechunk.Open(ctx, st("py"), &icechunk.Options{StorageSettings: s3Settings})
	if errors.Is(err, icechunk.ErrRepositoryNotFound) {
		t.Log("no repository at py: run check_s3_objects.py first to have Go commit on top of one")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	write(py, "/go", "go commit")
}

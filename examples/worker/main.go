//go:build js && wasm

// Command worker is a Cloudflare Worker, written in Go, that serves an
// Icechunk repository over HTTP using the serve package: a JSON API
// (refs, history, nodes, array slices) and a read-only Zarr v3 store at
// /zarr/<ref>/<key> that any Zarr client can read.
//
// Configuration (wrangler.toml bindings and vars):
//
//	REPO_BUCKET         R2 bucket binding holding the repository, or
//	REPO_URL            base URL of the repository (public bucket, CDN, ...),
//	                    or s3://bucket/prefix for S3-compatible APIs, signed
//	                    with the AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
//	                    secrets (anonymous without them); S3_ENDPOINT and
//	                    S3_REGION select non-AWS services such as GCS or MinIO
//	REPO_PREFIX         key prefix of the repository inside REPO_BUCKET
//	DEFAULT_REF         ref used when a request names none (default "main")
//	REPO_INFO_TTL       how long branch/tag lookups are cached, e.g. "10s"
//	CACHE_MB            snapshot/manifest cache size in MiB (default 32)
//	VIRTUAL_CONTAINERS  JSON object mapping virtual chunk URL prefixes to
//	                    "https://..." base URLs or "r2:<BINDING>[/prefix]"
//	WRITE_TOKEN         secret enabling the write endpoints (POST /arrays/...,
//	                    PUT /array/...) for "Authorization: Bearer <token>"
//	CREATE_IF_MISSING   "true" creates the repository if it does not exist
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/cfworker"
	"github.com/solomonsealed/icechunk-go/serve"
	"github.com/solomonsealed/icechunk-go/storage"
)

// The repository handle lives as long as the isolate, so decoded snapshots
// and manifests are cached across requests.
var svc atomic.Pointer[serve.Service]

// service returns the shared service, opening the repository on first use.
// Concurrent cold requests each open it rather than waiting on one another:
// in a Worker, a request must never wait for I/O started by another request
// (see cfworker.Serve).
func service(ctx context.Context, env cfworker.Env) (*serve.Service, error) {
	if s := svc.Load(); s != nil {
		return s, nil
	}
	s, err := newService(ctx, env)
	if err != nil {
		return nil, err
	}
	svc.CompareAndSwap(nil, s)
	return svc.Load(), nil
}

func newService(ctx context.Context, env cfworker.Env) (*serve.Service, error) {
	var st storage.Storage
	switch {
	case env.Binding("REPO_BUCKET").Truthy():
		st = cfworker.NewR2(env.Binding("REPO_BUCKET"), env.Var("REPO_PREFIX"))
	case strings.HasPrefix(env.Var("REPO_URL"), "s3://"):
		bucket, prefix, _ := strings.Cut(strings.TrimPrefix(env.Var("REPO_URL"), "s3://"), "/")
		st = cfworker.NewS3(storage.S3Config{
			Bucket:          bucket,
			Prefix:          prefix,
			Endpoint:        env.Var("S3_ENDPOINT"),
			Region:          env.Var("S3_REGION"),
			AccessKeyID:     env.Var("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: env.Var("AWS_SECRET_ACCESS_KEY"),
			SessionToken:    env.Var("AWS_SESSION_TOKEN"),
			Anonymous:       env.Var("AWS_ACCESS_KEY_ID") == "",
		})
	case env.Var("REPO_URL") != "":
		st = cfworker.NewHTTP(env.Var("REPO_URL"), nil)
	default:
		return nil, fmt.Errorf("configure a REPO_BUCKET R2 binding or a REPO_URL variable")
	}
	// Isolates have 128 MB of memory: keep the snapshot/manifest cache small.
	opts := &icechunk.Options{RepoInfoTTL: 10 * time.Second, CacheBytes: 32 << 20}
	if mb := env.Var("CACHE_MB"); mb != "" {
		n, err := strconv.Atoi(mb)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("CACHE_MB: invalid value %q", mb)
		}
		opts.CacheBytes = int64(n) << 20
	}
	if ttl := env.Var("REPO_INFO_TTL"); ttl != "" {
		d, err := time.ParseDuration(ttl)
		if err != nil {
			return nil, fmt.Errorf("REPO_INFO_TTL: %w", err)
		}
		opts.RepoInfoTTL = d
	}
	if vc := env.Var("VIRTUAL_CONTAINERS"); vc != "" {
		var m map[string]string
		if err := json.Unmarshal([]byte(vc), &m); err != nil {
			return nil, fmt.Errorf("VIRTUAL_CONTAINERS: %w", err)
		}
		opts.VirtualChunkContainers = map[string]storage.Storage{}
		for prefix, target := range m {
			if rest, ok := strings.CutPrefix(target, "r2:"); ok {
				binding, p, _ := strings.Cut(rest, "/")
				opts.VirtualChunkContainers[prefix] = cfworker.NewR2(env.Binding(binding), p)
			} else {
				opts.VirtualChunkContainers[prefix] = cfworker.NewHTTP(target, nil)
			}
		}
	}
	repo, err := icechunk.Open(ctx, st, opts)
	if errors.Is(err, icechunk.ErrRepositoryNotFound) && env.Var("CREATE_IF_MISSING") == "true" {
		repo, err = icechunk.Create(ctx, st, opts)
		if errors.Is(err, icechunk.ErrAlreadyExists) { // a concurrent request created it
			repo, err = icechunk.Open(ctx, st, opts)
		}
	}
	if err != nil {
		return nil, err
	}
	return &serve.Service{Repo: repo, DefaultRef: env.Var("DEFAULT_REF"), WriteToken: env.Var("WRITE_TOKEN")}, nil
}

func main() {
	cfworker.Serve(func(ctx context.Context, r *cfworker.Request) (*cfworker.Response, error) {
		s, err := service(ctx, r.Env)
		if err != nil {
			return nil, err
		}
		req := &serve.Request{Method: r.Method, URL: r.URL, Header: r.Header}
		if r.Method == "POST" || r.Method == "PUT" {
			if req.Body, err = r.Body(ctx); err != nil {
				return nil, err
			}
		}
		resp := s.Handle(ctx, req)
		return &cfworker.Response{Status: resp.Status, Header: resp.Header, Body: resp.Body}, nil
	})
}

// Command icechunk-go inspects and serves Icechunk repositories.
//
//	icechunk-go info   <repo>
//	icechunk-go log    [-ref main] [-n 20] <repo>
//	icechunk-go ls     [-ref main] <repo>
//	icechunk-go cat    [-ref main] <repo> <zarr-key>
//	icechunk-go read   [-ref main] [-slice 0:10,5] [-json] <repo> <array-path>
//	icechunk-go chunks [-ref main] [-n 100] <repo> <array-path>
//	icechunk-go serve  [-addr :8080] [-write-token T] <repo>
//	icechunk-go create <repo>
//	icechunk-go branch <repo> <name> [<snapshot-or-ref>]   (-delete to delete)
//	icechunk-go tag    <repo> <name> [<snapshot-or-ref>]   (-delete to delete)
//
// <repo> is a local path, file://, http(s)://, s3://bucket/prefix,
// gs://bucket/prefix or r2://bucket/prefix. S3-style URLs read credentials
// from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN and
// AWS_REGION / AWS_ENDPOINT_URL; r2:// also needs R2_ACCOUNT_ID. Without
// credentials, requests are anonymous. -ref accepts a branch, tag or
// snapshot id. -virtual prefix=target (repeatable) maps virtual chunk
// locations to storage, e.g. -virtual s3://bucket/=./local-copy.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/serve"
	"github.com/solomonsealed/icechunk-go/serve/nethttp"
	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/storage/httpstore"
	zarr "github.com/solomonsealed/zarr-go"
)

type virtualFlag map[string]string

func (v virtualFlag) String() string { return fmt.Sprint(map[string]string(v)) }
func (v virtualFlag) Set(s string) error {
	prefix, target, ok := strings.Cut(s, "=")
	if !ok || prefix == "" || target == "" {
		return fmt.Errorf("expected prefix=target, got %q", s)
	}
	v[prefix] = target
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, strings.TrimSpace(`
usage: icechunk-go <command> [flags] <repo> [args]

commands:
  info     spec version, branches, tags, status and config
  log      commit history of a ref
  ls       arrays and groups with shape, dtype and chunking
  cat      raw value of a Zarr key (e.g. group/array/zarr.json, array/c/0/0)
  read     decoded values of an array (optionally a -slice)
  chunks   chunk references of an array
  serve    HTTP API + read-only Zarr store (same as the Cloudflare Worker)
  create   create an empty repository (spec v2)
  branch   create (or -delete) a branch at a snapshot, branch or tag
  tag      create (or -delete) a tag at a snapshot, branch or tag

<repo>: path | file:// | http(s):// | s3://bucket/prefix | gs://bucket/prefix | r2://bucket/prefix
Run "icechunk-go <command> -h" for command flags.`))
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	ref := fs.String("ref", "main", "branch, tag or snapshot id")
	n := fs.Int("n", 0, "maximum number of entries (log, chunks)")
	sel := fs.String("slice", "", "selection such as 0:10,5,: (read)")
	asJSON := fs.Bool("json", false, "print JSON (read)")
	addr := fs.String("addr", "localhost:8080", "listen address (serve)")
	writeToken := fs.String("write-token", "", "enable write endpoints with this bearer token (serve)")
	del := fs.Bool("delete", false, "delete instead of create (branch, tag)")
	virtual := virtualFlag{}
	fs.Var(virtual, "virtual", "prefix=target mapping of virtual chunk locations (repeatable)")
	fs.Parse(os.Args[2:])
	args := fs.Args()
	if len(args) < 1 {
		usage()
	}
	ctx := context.Background()
	opts := &icechunk.Options{VirtualChunkContainers: map[string]storage.Storage{}}
	for prefix, target := range virtual {
		st, err := openStorage(target)
		check(err)
		opts.VirtualChunkContainers[prefix] = st
	}
	st, err := openStorage(args[0])
	check(err)
	if cmd == "create" {
		_, err := icechunk.Create(ctx, st, opts)
		check(err)
		fmt.Println("created", args[0])
		return
	}
	repo, err := icechunk.Open(ctx, st, opts)
	check(err)

	switch cmd {
	case "info":
		info(ctx, repo)
	case "log":
		sess := session(ctx, repo, *ref)
		i := 0
		for si, err := range repo.Ancestry(ctx, icechunk.AtSnapshot(sess.SnapshotID())) {
			check(err)
			fmt.Printf("%s  %s  %s\n", si.ID, si.FlushedAt.Format("2006-01-02 15:04:05Z07:00"), si.Message)
			if len(si.Metadata) > 0 {
				b, _ := json.Marshal(si.Metadata)
				fmt.Printf("    metadata: %s\n", b)
			}
			if i++; *n > 0 && i >= *n {
				break
			}
		}
	case "ls":
		ls(session(ctx, repo, *ref))
	case "cat":
		need(args, 2)
		b, err := session(ctx, repo, *ref).Store().Get(ctx, args[1])
		check(err)
		os.Stdout.Write(b)
	case "read":
		need(args, 2)
		read(ctx, session(ctx, repo, *ref), args[1], *sel, *asJSON)
	case "chunks":
		need(args, 2)
		i := 0
		for e, err := range session(ctx, repo, *ref).ChunkRefs(ctx, args[1]) {
			check(err)
			switch e.Ref.Kind {
			case icechunk.InlineChunk:
				fmt.Printf("%v  inline   %d bytes\n", e.Coords, e.Ref.Size())
			case icechunk.NativeChunk:
				fmt.Printf("%v  native   %d bytes  chunks/%s @%d\n", e.Coords, e.Ref.Length, e.Ref.ID, e.Ref.Offset)
			case icechunk.VirtualChunk:
				fmt.Printf("%v  virtual  %d bytes  %s @%d\n", e.Coords, e.Ref.Length, e.Ref.Location, e.Ref.Offset)
			}
			if i++; *n > 0 && i >= *n {
				break
			}
		}
	case "branch", "tag":
		need(args, 2)
		name := args[1]
		switch {
		case *del && cmd == "branch":
			check(repo.DeleteBranch(ctx, name))
		case *del:
			check(repo.DeleteTag(ctx, name))
		default:
			target := "main"
			if len(args) > 2 {
				target = args[2]
			}
			id := session(ctx, repo, target).SnapshotID()
			if cmd == "branch" {
				check(repo.CreateBranch(ctx, name, id))
			} else {
				check(repo.CreateTag(ctx, name, id))
			}
			fmt.Printf("%s %s -> %s\n", cmd, name, id)
		}
	case "serve":
		svc := &serve.Service{Repo: repo, DefaultRef: *ref, WriteToken: *writeToken}
		fmt.Fprintf(os.Stderr, "serving on http://%s  (try /, /nodes, /array/<path>?slice=..., /zarr/%s/zarr.json)\n", *addr, *ref)
		check(http.ListenAndServe(*addr, nethttp.Handler(svc)))
	default:
		usage()
	}
}

func need(args []string, n int) {
	if len(args) < n {
		usage()
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func session(ctx context.Context, repo *icechunk.Repository, ref string) *icechunk.Session {
	for _, v := range []icechunk.Version{icechunk.AtBranch(ref), icechunk.AtTag(ref)} {
		s, err := repo.ReadonlySession(ctx, v)
		if err == nil {
			return s
		}
		if !errors.Is(err, icechunk.ErrRefNotFound) {
			check(err)
		}
	}
	id, err := icechunk.ParseSnapshotID(ref)
	if err != nil {
		check(fmt.Errorf("%q is not a branch, tag or snapshot id", ref))
	}
	s, err := repo.ReadonlySession(ctx, icechunk.AtSnapshot(id))
	check(err)
	return s
}

func info(ctx context.Context, repo *icechunk.Repository) {
	fmt.Printf("spec version: %d\n", repo.SpecVersion())
	if repo.SpecVersion() >= 2 {
		ri, err := repo.RepoInfo(ctx)
		check(err)
		fmt.Printf("written by:   %s\nstatus:       %s\nsnapshots:    %d\n", ri.Implementation, ri.Status.Availability, len(ri.Snapshots))
	}
	for _, kind := range []struct {
		name   string
		list   func(context.Context) ([]string, error)
		lookup func(context.Context, string) (icechunk.SnapshotID, error)
	}{{"branches", repo.ListBranches, repo.LookupBranch}, {"tags", repo.ListTags, repo.LookupTag}} {
		names, err := kind.list(ctx)
		if err != nil {
			fmt.Printf("%s: (%v)\n", kind.name, err)
			continue
		}
		fmt.Printf("%s:\n", kind.name)
		for _, name := range names {
			id, err := kind.lookup(ctx, name)
			check(err)
			fmt.Printf("  %-24s %s\n", name, id)
		}
	}
	if repo.SpecVersion() >= 2 {
		cfg, err := repo.Config(ctx)
		check(err)
		if cfg != nil {
			b, _ := json.MarshalIndent(cfg, "", "  ")
			fmt.Printf("config: %s\n", b)
		}
	}
}

func ls(s *icechunk.Session) {
	nodes, err := s.Nodes()
	check(err)
	fmt.Printf("snapshot %s\n", s.SnapshotID())
	for _, n := range nodes {
		depth := strings.Count(n.Path, "/")
		if n.Path == "/" {
			depth = 0
		}
		name := n.Path[strings.LastIndex(n.Path, "/")+1:]
		indent := strings.Repeat("  ", depth)
		if n.Type == icechunk.GroupNode {
			fmt.Printf("%s%s/\n", indent, name)
			continue
		}
		md, err := zarr.ParseMetadata(n.ZarrMetadata)
		if err != nil {
			fmt.Printf("%s%s  (unreadable metadata: %v)\n", indent, name, err)
			continue
		}
		var codecs []string
		for _, c := range md.Codecs {
			codecs = append(codecs, c.Name)
		}
		chunking := fmt.Sprint(md.ChunkShape)
		if md.ChunkShape == nil {
			chunking = "rectilinear"
		}
		fmt.Printf("%s%s  %s %v  chunks %s  [%s]\n", indent, name, md.DataType.Name, md.Shape, chunking, strings.Join(codecs, ", "))
	}
}

func read(ctx context.Context, s *icechunk.Session, path, sel string, asJSON bool) {
	arr, err := s.OpenArray(ctx, path)
	check(err)
	selectors, err := zarr.ParseSelector(sel)
	check(err)
	nd, err := arr.Get(ctx, selectors...)
	check(err)
	shape := nd.Shape
	if asJSON {
		b, err := json.Marshal(nd)
		check(err)
		fmt.Println(string(b))
		return
	}
	vals, err := nd.Values()
	check(err)
	fmt.Printf("%s %v\n", nd.DataType.Name, shape)
	printNested(os.Stdout, vals, shape)
}

// printNested prints values with one line per innermost row.
func printNested(w io.Writer, vals any, shape []uint64) {
	b, _ := json.Marshal(vals)
	var flat []any
	if json.Unmarshal(b, &flat) != nil {
		fmt.Fprintln(w, vals)
		return
	}
	if len(shape) == 0 {
		fmt.Fprintln(w, flat[0])
		return
	}
	row := int(shape[len(shape)-1])
	if row == 0 {
		fmt.Fprintln(w, "[]")
		return
	}
	for i := 0; i < len(flat); i += row {
		fmt.Fprintln(w, flat[i:min(i+row, len(flat))])
	}
}

// openStorage turns a repository location into a Storage.
func openStorage(loc string) (storage.Storage, error) {
	scheme, rest, hasScheme := strings.Cut(loc, "://")
	if !hasScheme {
		return storage.NewLocal(loc), nil
	}
	env := os.Getenv
	s3 := func(bucketPath, endpoint, region string) storage.Storage {
		bucket, prefix, _ := strings.Cut(bucketPath, "/")
		cfg := storage.S3Config{
			Bucket: bucket, Prefix: prefix, Endpoint: endpoint, Region: region,
			AccessKeyID: env("AWS_ACCESS_KEY_ID"), SecretAccessKey: env("AWS_SECRET_ACCESS_KEY"), SessionToken: env("AWS_SESSION_TOKEN"),
		}
		var o *httpstore.Options
		if cfg.AccessKeyID == "" {
			cfg.Anonymous = true
			o = &httpstore.Options{NotFoundStatus: []int{403}}
		}
		return httpstore.NewS3(cfg, o)
	}
	switch scheme {
	case "file":
		return storage.NewLocal(rest), nil
	case "http", "https":
		return httpstore.New(loc, nil), nil
	case "s3":
		return s3(rest, env("AWS_ENDPOINT_URL"), env("AWS_REGION")), nil
	case "gs", "gcs":
		return s3(rest, "https://storage.googleapis.com", "auto"), nil
	case "r2":
		acct := env("R2_ACCOUNT_ID")
		if acct == "" {
			return nil, fmt.Errorf("r2:// needs R2_ACCOUNT_ID (and R2 access keys in AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)")
		}
		return s3(rest, "https://"+acct+".r2.cloudflarestorage.com", "auto"), nil
	}
	known := []string{"file", "http", "https", "s3", "gs", "r2"}
	sort.Strings(known)
	return nil, fmt.Errorf("unsupported scheme %q (supported: %s)", scheme, strings.Join(known, ", "))
}

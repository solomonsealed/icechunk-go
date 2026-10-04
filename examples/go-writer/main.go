// Command go-writer appends one day of synthetic temperature data to an
// Icechunk repository per run, the Go counterpart of
// examples/python-writer/write_repo.py.
//
//	go run ./examples/go-writer -local ./demo-repo
//	R2_ACCOUNT_ID=... AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//	  go run ./examples/go-writer -r2-bucket icechunk-repos -prefix demo
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
	"github.com/solomonsealed/icechunk-go/storage/httpstore"
	zarr "github.com/solomonsealed/zarr-go"
)

const ny, nx = 90, 180

func main() {
	local := flag.String("local", "", "local directory for the repository")
	bucket := flag.String("r2-bucket", "", "R2 bucket (uses R2_ACCOUNT_ID, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
	prefix := flag.String("prefix", "demo", "key prefix inside the bucket")
	flag.Parse()
	ctx := context.Background()

	var st storage.Storage
	switch {
	case *local != "":
		st = storage.NewLocal(*local)
	case *bucket != "":
		st = httpstore.NewS3(storage.S3Config{
			Bucket: *bucket, Prefix: *prefix, Region: "auto",
			Endpoint:        "https://" + os.Getenv("R2_ACCOUNT_ID") + ".r2.cloudflarestorage.com",
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		}, nil)
	default:
		log.Fatal("use -local DIR or -r2-bucket NAME")
	}

	repo, err := icechunk.Open(ctx, st, nil)
	if errors.Is(err, icechunk.ErrRepositoryNotFound) {
		repo, err = icechunk.Create(ctx, st, nil)
	}
	check(err)
	s, err := repo.WritableSession(ctx, "main")
	check(err)

	temp, err := s.OpenArray(ctx, "/temperature")
	if errors.Is(err, icechunk.ErrNodeNotFound) {
		temp, err = createArrays(ctx, s)
	}
	check(err)

	// Grow the time axis by one day, then write it.
	day := temp.Shape()[0]
	meta, err := zarr.ArraySpec{
		Shape: []uint64{day + 1, ny, nx}, ChunkShape: []uint64{1, ny, nx}, DataType: "float32",
		FillValue: math.NaN(), DimensionNames: []string{"time", "lat", "lon"},
		Attributes: map[string]any{"units": "degC"},
	}.Metadata()
	check(err)
	check(s.SetMetadata(ctx, "/temperature", meta))
	temp, err = s.OpenArray(ctx, "/temperature")
	check(err)
	values := make([]float32, ny*nx)
	for y := 0; y < ny; y++ {
		lat := -89 + 178*float64(y)/(ny-1)
		for x := 0; x < nx; x++ {
			lon := -179 + 358*float64(x)/(nx-1)
			values[y*nx+x] = float32(25*math.Cos(lat*math.Pi/180) - 5 + 3*math.Sin((lon+10*float64(day))*math.Pi/180))
		}
	}
	nd, err := zarr.FromSlice([]uint64{1, ny, nx}, values)
	check(err)
	check(temp.Write(ctx, []uint64{day, 0, 0}, nd))

	id, err := s.Commit(ctx, fmt.Sprintf("add day %d", day), &icechunk.CommitOptions{
		Metadata: map[string]any{"written_at": time.Now().UTC().Format(time.RFC3339), "day": day},
		Rebase:   true,
	})
	check(err)
	fmt.Printf("committed day %d as snapshot %s\n", day, id)
}

// createArrays sets up the coordinates and an empty temperature array.
func createArrays(ctx context.Context, s *icechunk.Session) (*zarr.Array, error) {
	if err := s.CreateGroup(ctx, "/", map[string]any{"title": "icechunk-go demo"}); err != nil {
		return nil, err
	}
	for _, c := range []struct {
		name   string
		n      int
		lo, hi float64
	}{{"lat", ny, -89, 89}, {"lon", nx, -179, 179}} {
		arr, err := s.CreateArray(ctx, "/"+c.name, zarr.ArraySpec{Shape: []uint64{uint64(c.n)}, DataType: "float32", DimensionNames: []string{c.name}})
		if err != nil {
			return nil, err
		}
		if err := arr.Write(ctx, []uint64{0}, linspace(c.lo, c.hi, c.n)); err != nil {
			return nil, err
		}
	}
	return s.CreateArray(ctx, "/temperature", zarr.ArraySpec{
		Shape: []uint64{0, ny, nx}, ChunkShape: []uint64{1, ny, nx}, DataType: "float32",
		FillValue: math.NaN(), DimensionNames: []string{"time", "lat", "lon"},
		Attributes: map[string]any{"units": "degC"},
	})
}

func linspace(a, b float64, n int) *zarr.NDArray {
	v := make([]float32, n)
	for i := range v {
		v[i] = float32(a + (b-a)*float64(i)/float64(n-1))
	}
	nd, err := zarr.FromSlice([]uint64{uint64(n)}, v)
	check(err)
	return nd
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

package storage

// S3Config describes a repository stored under a prefix of an
// S3-compatible bucket (AWS S3, Cloudflare R2's S3 API, MinIO, GCS
// interoperability, Tigris, ...). It is used by storage/httpstore.NewS3 and
// cfworker.NewS3.
type S3Config struct {
	Bucket string
	// Prefix is the key prefix of the repository root within the bucket.
	// The special value "/" reads repositories created at the bucket root by
	// Icechunk < 2.1, whose keys start with a slash (upstream's
	// legacy_rooted_keys layout).
	Prefix string
	// Region defaults to "us-east-1"; use "auto" for Cloudflare R2.
	Region string
	// Endpoint overrides the AWS endpoint, e.g.
	// "https://<account>.r2.cloudflarestorage.com" or "http://localhost:9000".
	// Custom endpoints are always addressed path-style.
	Endpoint string
	// ForcePathStyle addresses AWS buckets as s3.<region>.amazonaws.com/<bucket>.
	ForcePathStyle bool
	// Anonymous sends unsigned requests (public buckets). Requests are also
	// unsigned when AccessKeyID is empty.
	Anonymous       bool
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

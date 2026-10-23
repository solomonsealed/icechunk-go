package icechunk

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/solomonsealed/icechunk-go/internal/format"
	"github.com/solomonsealed/icechunk-go/storage"
)

// UserAgentProduct is the product token that identifies this module in the
// User-Agent header of every request a repository makes (upstream sends
// "icechunk/<version>").
const UserAgentProduct = "icechunk-go/" + format.Version

// Attribution labels the requests a repository makes to its object store,
// as upstream's Attribution (icechunk 2.3.0) does. Storages that make HTTP
// requests send each request's attribution as its User-Agent header (see
// storage.WithUserAgent), so that the bucket owner can break traffic down
// in access logs. A chunk read of an attributed repository sends
//
//	weatherlib/0.9 icechunk-go/0.1.0 (workload=nightly-ingest; principal=u_123; array=g/temperature; chunk=0/1/2)
//
// The repository adds the array path and chunk coordinates itself: chunk
// reads and writes (virtual chunks included) carry both, manifest reads and
// writes the array, and other requests neither. Unattributed repositories
// send the same, without the client and the labels.
type Attribution struct {
	// Client is which software is talking to the bucket: the product token
	// of the application or library embedding icechunk-go, "name" or
	// "name/version" in HTTP token characters, e.g. "weatherlib/0.9".
	Client string
	// Workload is what the requests are for: the job, pipeline or
	// deployment running them, e.g. "nightly-ingest". Printable ASCII, at
	// most 128 bytes, none of ( ) \ ; = and ".
	Workload string
	// Principal is on whose behalf the requests are made: a user id,
	// service account or tenant, for services that act for many users.
	// Same rules as Workload.
	Principal string
}

// Limits of attribution values, in bytes, as upstream's.
const (
	maxLabelBytes = 128 // any Attribution field
	maxArrayBytes = 256 // the rendered array= value, after encoding
	maxChunkBytes = 64  // the rendered chunk= value
)

// requestLabels is the per-repository part of the User-Agent header,
// rendered once: the product tokens, and the comment fields (empty when no
// label is set).
type requestLabels struct {
	products, fields string
}

func newRequestLabels(a Attribution) (requestLabels, error) {
	if err := validateClient(a.Client); err != nil {
		return requestLabels{}, err
	}
	l := requestLabels{products: UserAgentProduct}
	if a.Client != "" {
		l.products = a.Client + " " + UserAgentProduct
	}
	var fields []string
	for _, f := range []struct{ name, value string }{{"workload", a.Workload}, {"principal", a.Principal}} {
		if f.value == "" {
			continue
		}
		if err := validateLabel(f.name, f.value); err != nil {
			return requestLabels{}, err
		}
		fields = append(fields, f.name+"="+f.value)
	}
	l.fields = strings.Join(fields, "; ")
	return l, nil
}

func validateLabel(field, value string) error {
	if len(value) > maxLabelBytes {
		return fmt.Errorf("icechunk: attribution %s is longer than %d bytes", field, maxLabelBytes)
	}
	for _, c := range value {
		if c < ' ' || c > '~' || strings.ContainsRune(`()\;="`, c) {
			return fmt.Errorf("icechunk: attribution %s contains the forbidden character %q", field, c)
		}
	}
	return nil
}

func isTokenChar(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)
}

func validateClient(value string) error {
	if value == "" {
		return nil
	}
	if len(value) > maxLabelBytes {
		return fmt.Errorf("icechunk: attribution client is longer than %d bytes", maxLabelBytes)
	}
	token := func(s string) bool {
		return s != "" && strings.IndexFunc(s, func(c rune) bool { return !isTokenChar(c) }) < 0
	}
	name, version, hasVersion := strings.Cut(value, "/")
	if !token(name) || hasVersion && !token(version) {
		return fmt.Errorf("icechunk: attribution client must be name or name/version in token characters, got %q", value)
	}
	return nil
}

// userAgent renders the User-Agent of one request: the product tokens, then
// a parenthesized comment with the labels, the array path (without its
// leading "/") and the chunk coordinates, when there is anything to say.
func (l requestLabels) userAgent(array string, hasArray bool, chunk []uint32, hasChunk bool) string {
	var parts []string
	if l.fields != "" {
		parts = append(parts, l.fields)
	}
	if hasArray {
		parts = append(parts, "array="+truncateMiddle(encodeComment(array), maxArrayBytes))
	}
	if hasChunk {
		coords := make([]string, len(chunk))
		for i, c := range chunk {
			coords[i] = strconv.FormatUint(uint64(c), 10)
		}
		parts = append(parts, "chunk="+truncateMiddle(strings.Join(coords, "/"), maxChunkBytes))
	}
	if len(parts) == 0 {
		return l.products
	}
	return l.products + " (" + strings.Join(parts, "; ") + ")"
}

// encodeComment percent-encodes the bytes a header comment cannot carry
// (controls and non-ASCII) and the comment's separators: ( ) \ ; = and %.
// "/" and space pass through.
func encodeComment(value string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 0x20 || c >= 0x7F || strings.IndexByte(`()\;=%`, c) >= 0 {
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&15])
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// truncateMiddle keeps the first and last (limit-3)/2 bytes of an ASCII
// value longer than limit, joined by "...", moving the cut points outward
// off any %XX escape they would split.
func truncateMiddle(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	keep := (limit - 3) / 2
	escapeBefore := func(i int) bool { return strings.IndexByte(value[max(i-2, 0):i], '%') >= 0 }
	head := keep
	for head > 0 && escapeBefore(head) {
		head--
	}
	tail := len(value) - keep
	for tail < len(value) && escapeBefore(tail) {
		tail++
	}
	return value[:head] + "..." + value[tail:]
}

// req returns ctx for a request about no array in particular (snapshots,
// transaction logs, the repo info file, refs), unless the caller already
// attributed it to an array or chunk.
func (r *Repository) req(ctx context.Context) context.Context {
	if storage.UserAgent(ctx) != "" {
		return ctx
	}
	return storage.WithUserAgent(ctx, r.labels.userAgent("", false, nil, false))
}

// reqArray returns ctx for a request about the array at path (manifests).
func (r *Repository) reqArray(ctx context.Context, path string) context.Context {
	return storage.WithUserAgent(ctx, r.labels.userAgent(strings.TrimPrefix(path, "/"), true, nil, false))
}

// reqChunk returns ctx for a request about one chunk of the array at path.
func (r *Repository) reqChunk(ctx context.Context, path string, coords []uint32) context.Context {
	return storage.WithUserAgent(ctx, r.labels.userAgent(strings.TrimPrefix(path, "/"), true, coords, true))
}

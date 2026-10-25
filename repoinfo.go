package icechunk

import (
	"bytes"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/solomonsealed/icechunk-go/internal/fbs"
	"github.com/solomonsealed/icechunk-go/internal/format"
)

// recoverFormat converts a panic raised while walking a malformed flatbuffer
// (the Go flatbuffers runtime does not verify buffers) into an ErrFormat error.
func recoverFormat(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("%w: corrupt flatbuffer: %v", ErrFormat, r)
	}
}

// vecLen guards allocations sized by a vector length read from a flatbuffer:
// every element occupies at least 4 bytes (an offset or a scalar), so a
// length beyond len(buf)/4 can only come from a corrupt file. It panics,
// which recoverFormat turns into an ErrFormat error.
func vecLen(n int, buf []byte) int {
	if n < 0 || n > len(buf)/4 {
		panic(fmt.Sprintf("vector length %d exceeds buffer size %d", n, len(buf)))
	}
	return n
}

// SnapshotInfo summarizes one commit.
type SnapshotInfo struct {
	ID SnapshotID
	// ParentID is nil for the repository's initial snapshot.
	ParentID  *SnapshotID
	FlushedAt time.Time
	Message   string
	// Metadata holds the commit properties (JSON-compatible values).
	Metadata map[string]any
}

// RepoStatus is the availability state recorded in the repo info file.
type RepoStatus struct {
	// Availability is "online", "read-only" or "offline".
	Availability string
	SetAt        time.Time
	Reason       string
}

// Update is one entry of the repository's operations log (spec v2).
type Update struct {
	// Kind is the update type, e.g. "NewCommit", "TagCreated", "BranchReset".
	Kind      string
	UpdatedAt time.Time
	// Name is the branch or tag the update refers to, if any.
	Name string
	// SnapshotID is the new snapshot for commits; zero otherwise.
	SnapshotID SnapshotID
	// PreviousSnapshotID is set for deletes, resets and amends.
	PreviousSnapshotID SnapshotID
	BackupPath         string
}

// RepoInfo is the decoded `repo` file: the single entry point of a spec v2
// repository holding refs, the snapshot graph, config and the ops log.
type RepoInfo struct {
	SpecVersion    int
	Implementation string
	Branches       map[string]SnapshotID
	Tags           map[string]SnapshotID
	DeletedTags    []string
	// Snapshots is sorted by id; parents are resolved into ParentID.
	Snapshots []SnapshotInfo
	Status    RepoStatus
	// Metadata is the repository-level user metadata.
	Metadata map[string]any
	// Config is the repository configuration as stored (decoded FlexBuffer).
	Config any
	// EnabledFeatureFlags / DisabledFeatureFlags are explicit overrides.
	EnabledFeatureFlags  []uint16
	DisabledFeatureFlags []uint16
	// Updates is the recent operations log, newest first.
	Updates []Update
	// RepoBeforeUpdates points at the previous repo file holding older updates.
	RepoBeforeUpdates string
}

func microsToTime(us uint64) time.Time {
	return time.UnixMicro(int64(us)).UTC()
}

func idFrom12(o *fbs.ObjectId12) SnapshotID {
	var id ObjectID12
	if o != nil {
		copy(id[:], o.Bytes())
	}
	return id
}

func decodeMetadataItems(buf []byte, n int, get func(*fbs.MetadataItem, int) bool, specVersion uint8) (map[string]any, error) {
	return decodeMetadata(rawItems(vecLen(n, buf), get), specVersion)
}

func decodeMetadata(items []rawMetadataItem, specVersion uint8) (map[string]any, error) {
	decode := format.DecodeFlexBuffer
	if specVersion == 1 {
		decode = format.DecodeMsgpack
	}
	out := make(map[string]any, len(items))
	for _, it := range items {
		v, err := decode(it.value)
		if err != nil {
			return nil, fmt.Errorf("decoding metadata %q: %w", it.name, err)
		}
		out[it.name] = v
	}
	return out, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// parseRepoInfo decodes a `repo` file (header + compressed flatbuffer).
func parseRepoInfo(raw []byte) (info *RepoInfo, err error) {
	defer recoverFormat(&err)
	h, doc, err := decodeRepoDoc(raw, false)
	if err != nil {
		return nil, err
	}
	info = &RepoInfo{
		SpecVersion:    int(doc.specVersion),
		Implementation: h.Implementation,
		Branches:       doc.branches,
		Tags:           doc.tags,
		DeletedTags:    doc.deletedTags,
		Snapshots:      make([]SnapshotInfo, len(doc.snapshots)),
		Status: RepoStatus{
			Availability: availabilityName(doc.status.availability),
			SetAt:        microsToTime(doc.status.setAt),
			Reason:       deref(doc.status.reason),
		},
		EnabledFeatureFlags:  doc.enabledFlags,
		DisabledFeatureFlags: doc.disabledFlags,
		RepoBeforeUpdates:    deref(doc.repoBeforeUpdates),
	}
	if info.SpecVersion == 0 {
		info.SpecVersion = int(h.SpecVersion)
	}
	for i, s := range doc.snapshots {
		md, err := decodeMetadata(s.metadata, 2)
		if err != nil {
			return nil, err
		}
		info.Snapshots[i] = SnapshotInfo{ID: s.id, ParentID: s.parent, FlushedAt: microsToTime(s.flushedAt), Message: s.message, Metadata: md}
	}
	if info.Metadata, err = decodeMetadata(doc.metadata, 2); err != nil {
		return nil, err
	}
	if len(doc.config) > 0 {
		if info.Config, err = format.DecodeFlexBuffer(doc.config); err != nil {
			return nil, fmt.Errorf("decoding repo config: %w", err)
		}
	}
	for _, u := range doc.updates {
		kind := fbs.EnumNamesUpdateType[u.typ]
		if kind == "" {
			kind = fmt.Sprintf("Unknown(%d)", u.typ)
		}
		info.Updates = append(info.Updates, Update{
			Kind:               strings.TrimSuffix(kind, "Update"),
			UpdatedAt:          microsToTime(u.updatedAt),
			Name:               u.name,
			SnapshotID:         u.newSnap,
			PreviousSnapshotID: u.prevSnap,
			BackupPath:         deref(u.backupPath),
		})
	}
	return info, nil
}

func availabilityName(a fbs.RepoAvailability) string {
	switch a {
	case fbs.RepoAvailabilityOnline:
		return "online"
	case fbs.RepoAvailabilityReadOnly:
		return "read-only"
	case fbs.RepoAvailabilityOffline:
		return "offline"
	}
	return fmt.Sprintf("unknown(%d)", a)
}

// snapshot returns the info for id using binary search over the sorted list.
func (ri *RepoInfo) snapshot(id SnapshotID) (*SnapshotInfo, bool) {
	i, ok := slices.BinarySearchFunc(ri.Snapshots, id, func(s SnapshotInfo, id SnapshotID) int { return bytes.Compare(s.ID[:], id[:]) })
	if !ok {
		return nil, false
	}
	return &ri.Snapshots[i], true
}

// BranchNames returns the branch names in sorted order.
func (ri *RepoInfo) BranchNames() []string { return sortedKeys(ri.Branches) }

// TagNames returns the tag names in sorted order.
func (ri *RepoInfo) TagNames() []string { return sortedKeys(ri.Tags) }

func sortedKeys(m map[string]SnapshotID) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

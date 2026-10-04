package icechunk

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

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
	vecLen(n, buf)
	if n == 0 {
		return map[string]any{}, nil
	}
	out := make(map[string]any, n)
	var item fbs.MetadataItem
	for i := 0; i < n; i++ {
		get(&item, i)
		var v any
		var err error
		if specVersion == 1 {
			v, err = format.DecodeMsgpack(item.ValueBytes())
		} else {
			v, err = format.DecodeFlexBuffer(item.ValueBytes())
		}
		if err != nil {
			return nil, fmt.Errorf("decoding metadata %q: %w", item.Name(), err)
		}
		out[string(item.Name())] = v
	}
	return out, nil
}

// parseRepoInfo decodes a `repo` file (header + compressed flatbuffer).
func parseRepoInfo(raw []byte) (info *RepoInfo, err error) {
	defer recoverFormat(&err)
	h, buf, err := format.Decode(raw, format.FileTypeRepoInfo)
	if err != nil {
		return nil, err
	}
	root := fbs.GetRootAsRepo(buf, 0)
	info = &RepoInfo{
		SpecVersion:       int(root.SpecVersion()),
		Implementation:    h.Implementation,
		Branches:          map[string]SnapshotID{},
		Tags:              map[string]SnapshotID{},
		RepoBeforeUpdates: string(root.RepoBeforeUpdates()),
	}
	if info.SpecVersion == 0 {
		info.SpecVersion = int(h.SpecVersion)
	}

	n := vecLen(root.SnapshotsLength(), buf)
	info.Snapshots = make([]SnapshotInfo, n)
	parents := make([]int32, n)
	var si fbs.SnapshotInfo
	for i := 0; i < n; i++ {
		root.Snapshots(&si, i)
		md, err := decodeMetadataItems(buf, si.MetadataLength(), si.Metadata, 2)
		if err != nil {
			return nil, err
		}
		info.Snapshots[i] = SnapshotInfo{
			ID:        idFrom12(si.Id(nil)),
			FlushedAt: microsToTime(si.FlushedAt()),
			Message:   string(si.Message()),
			Metadata:  md,
		}
		parents[i] = si.ParentOffset()
	}
	for i, p := range parents {
		if p >= 0 {
			if int(p) >= n {
				return nil, fmt.Errorf("%w: snapshot parent index %d out of range", ErrFormat, p)
			}
			pid := info.Snapshots[p].ID
			info.Snapshots[i].ParentID = &pid
		}
	}

	var ref fbs.Ref
	snapAt := func(idx uint32) (SnapshotID, error) {
		if int(idx) >= n {
			return SnapshotID{}, fmt.Errorf("%w: ref snapshot index %d out of range", ErrFormat, idx)
		}
		return info.Snapshots[idx].ID, nil
	}
	for i := 0; i < root.BranchesLength(); i++ {
		root.Branches(&ref, i)
		id, err := snapAt(ref.SnapshotIndex())
		if err != nil {
			return nil, err
		}
		info.Branches[string(ref.Name())] = id
	}
	for i := 0; i < root.TagsLength(); i++ {
		root.Tags(&ref, i)
		id, err := snapAt(ref.SnapshotIndex())
		if err != nil {
			return nil, err
		}
		info.Tags[string(ref.Name())] = id
	}
	for i := 0; i < root.DeletedTagsLength(); i++ {
		info.DeletedTags = append(info.DeletedTags, string(root.DeletedTags(i)))
	}

	if st := root.Status(nil); st != nil {
		info.Status = RepoStatus{
			Availability: availabilityName(st.Availability()),
			SetAt:        microsToTime(st.SetAt()),
			Reason:       string(st.LimitedAvailabilityReason()),
		}
	}
	if info.Metadata, err = decodeMetadataItems(buf, root.MetadataLength(), root.Metadata, 2); err != nil {
		return nil, err
	}
	if cfg := root.ConfigBytes(); len(cfg) > 0 {
		if info.Config, err = format.DecodeFlexBuffer(cfg); err != nil {
			return nil, fmt.Errorf("decoding repo config: %w", err)
		}
	}
	for i := 0; i < root.EnabledFeatureFlagsLength(); i++ {
		info.EnabledFeatureFlags = append(info.EnabledFeatureFlags, root.EnabledFeatureFlags(i))
	}
	for i := 0; i < root.DisabledFeatureFlagsLength(); i++ {
		info.DisabledFeatureFlags = append(info.DisabledFeatureFlags, root.DisabledFeatureFlags(i))
	}
	var up fbs.Update
	for i := 0; i < root.LatestUpdatesLength(); i++ {
		root.LatestUpdates(&up, i)
		info.Updates = append(info.Updates, decodeUpdate(&up))
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

func decodeUpdate(up *fbs.Update) Update {
	u := Update{
		Kind:       fbs.EnumNamesUpdateType[up.UpdateTypeType()],
		UpdatedAt:  microsToTime(up.UpdatedAt()),
		BackupPath: string(up.BackupPath()),
	}
	if u.Kind == "" {
		u.Kind = fmt.Sprintf("Unknown(%d)", up.UpdateTypeType())
	}
	u.Kind = strings.TrimSuffix(u.Kind, "Update")
	var t flatbuffers.Table
	if !up.UpdateType(&t) {
		return u
	}
	switch up.UpdateTypeType() {
	case fbs.UpdateTypeTagCreatedUpdate:
		var x fbs.TagCreatedUpdate
		x.Init(t.Bytes, t.Pos)
		u.Name = string(x.Name())
	case fbs.UpdateTypeTagDeletedUpdate:
		var x fbs.TagDeletedUpdate
		x.Init(t.Bytes, t.Pos)
		u.Name, u.PreviousSnapshotID = string(x.Name()), idFrom12(x.PreviousSnapId(nil))
	case fbs.UpdateTypeBranchCreatedUpdate:
		var x fbs.BranchCreatedUpdate
		x.Init(t.Bytes, t.Pos)
		u.Name = string(x.Name())
	case fbs.UpdateTypeBranchDeletedUpdate:
		var x fbs.BranchDeletedUpdate
		x.Init(t.Bytes, t.Pos)
		u.Name, u.PreviousSnapshotID = string(x.Name()), idFrom12(x.PreviousSnapId(nil))
	case fbs.UpdateTypeBranchResetUpdate:
		var x fbs.BranchResetUpdate
		x.Init(t.Bytes, t.Pos)
		u.Name, u.PreviousSnapshotID = string(x.Name()), idFrom12(x.PreviousSnapId(nil))
	case fbs.UpdateTypeNewCommitUpdate:
		var x fbs.NewCommitUpdate
		x.Init(t.Bytes, t.Pos)
		u.Name, u.SnapshotID = string(x.Branch()), idFrom12(x.NewSnapId(nil))
	case fbs.UpdateTypeCommitAmendedUpdate:
		var x fbs.CommitAmendedUpdate
		x.Init(t.Bytes, t.Pos)
		u.Name = string(x.Branch())
		u.SnapshotID, u.PreviousSnapshotID = idFrom12(x.NewSnapId(nil)), idFrom12(x.PreviousSnapId(nil))
	case fbs.UpdateTypeNewDetachedSnapshotUpdate:
		var x fbs.NewDetachedSnapshotUpdate
		x.Init(t.Bytes, t.Pos)
		u.SnapshotID = idFrom12(x.NewSnapId(nil))
	}
	return u
}

// snapshot returns the info for id using binary search over the sorted list.
func (ri *RepoInfo) snapshot(id SnapshotID) (*SnapshotInfo, bool) {
	i := sort.Search(len(ri.Snapshots), func(i int) bool {
		return bytes.Compare(ri.Snapshots[i].ID[:], id[:]) >= 0
	})
	if i < len(ri.Snapshots) && ri.Snapshots[i].ID == id {
		return &ri.Snapshots[i], true
	}
	return nil, false
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

package icechunk

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/solomonsealed/icechunk-go/internal/fbs"
	"github.com/solomonsealed/icechunk-go/internal/format"
)

// repoDoc is a lossless model of a repo info file, used to produce updated
// versions of it. Fields the writer does not interpret (metadata values,
// config, extra) are carried as raw bytes so that rewriting never changes
// them, and every update type of the ops log round-trips.
type repoDoc struct {
	specVersion       uint8
	tags              map[string]SnapshotID
	branches          map[string]SnapshotID
	deletedTags       []string
	snapshots         []docSnapshot // sorted by id
	status            docStatus
	metadata          []rawMetadataItem
	updates           []docUpdate // newest first
	repoBeforeUpdates *string
	config            []byte
	hasConfig         bool
	enabledFlags      []uint16
	hasEnabledFlags   bool
	disabledFlags     []uint16
	hasDisabledFlags  bool
	extra             []byte
}

type rawMetadataItem struct {
	name  string
	value []byte
}

type docSnapshot struct {
	id        SnapshotID
	parent    *SnapshotID
	flushedAt uint64 // microseconds
	message   string
	metadata  []rawMetadataItem
	pruned    []SnapshotID // nil when absent
}

type docStatus struct {
	availability fbs.RepoAvailability
	setAt        uint64
	reason       *string
}

type docUpdate struct {
	typ        fbs.UpdateType
	updatedAt  uint64
	backupPath *string
	// Union payload; which fields are meaningful depends on typ.
	name        string // tag/branch name, or branch of commit updates
	prevSnap    SnapshotID
	newSnap     SnapshotID
	fromVersion uint8
	toVersion   uint8
	status      docStatus
	flagID      uint16
	flagValue   bool
	flagIsSet   bool
}

const defaultUpdatesPerRepoFile = 1000

func rawItems(n int, get func(*fbs.MetadataItem, int) bool) []rawMetadataItem {
	out := make([]rawMetadataItem, n)
	var it fbs.MetadataItem
	for i := range out {
		get(&it, i)
		out[i] = rawMetadataItem{name: string(it.Name()), value: append([]byte(nil), it.ValueBytes()...)}
	}
	return out
}

func strPtr(b []byte) *string {
	if b == nil {
		return nil
	}
	s := string(b)
	return &s
}

// parseRepoDoc decodes a complete repo info file.
func parseRepoDoc(raw []byte) (doc *repoDoc, err error) {
	defer recoverFormat(&err)
	_, buf, err := format.Decode(raw, format.FileTypeRepoInfo)
	if err != nil {
		return nil, err
	}
	root := fbs.GetRootAsRepo(buf, 0)
	doc = &repoDoc{
		specVersion:       root.SpecVersion(),
		tags:              map[string]SnapshotID{},
		branches:          map[string]SnapshotID{},
		repoBeforeUpdates: strPtr(root.RepoBeforeUpdates()),
		extra:             append([]byte(nil), root.ExtraBytes()...),
	}
	n := vecLen(root.SnapshotsLength(), buf)
	doc.snapshots = make([]docSnapshot, n)
	var si fbs.SnapshotInfo
	parents := make([]int32, n)
	for i := 0; i < n; i++ {
		root.Snapshots(&si, i)
		ds := docSnapshot{
			id:        idFrom12(si.Id(nil)),
			flushedAt: si.FlushedAt(),
			message:   string(si.Message()),
			metadata:  rawItems(vecLen(si.MetadataLength(), buf), si.Metadata),
		}
		if pn := si.PrunedAncestorTxLogsLength(); si.HasPrunedAncestorTxLogs() {
			ds.pruned = make([]SnapshotID, vecLen(pn, buf))
			var o fbs.ObjectId12
			for j := range ds.pruned {
				si.PrunedAncestorTxLogs(&o, j)
				ds.pruned[j] = idFrom12(&o)
			}
		}
		doc.snapshots[i] = ds
		parents[i] = si.ParentOffset()
	}
	for i, p := range parents {
		if p >= 0 {
			if int(p) >= n {
				return nil, fmt.Errorf("%w: parent index out of range", ErrFormat)
			}
			pid := doc.snapshots[p].id
			doc.snapshots[i].parent = &pid
		}
	}
	var ref fbs.Ref
	for i := 0; i < root.BranchesLength(); i++ {
		root.Branches(&ref, i)
		if int(ref.SnapshotIndex()) >= n {
			return nil, fmt.Errorf("%w: branch index out of range", ErrFormat)
		}
		doc.branches[string(ref.Name())] = doc.snapshots[ref.SnapshotIndex()].id
	}
	for i := 0; i < root.TagsLength(); i++ {
		root.Tags(&ref, i)
		if int(ref.SnapshotIndex()) >= n {
			return nil, fmt.Errorf("%w: tag index out of range", ErrFormat)
		}
		doc.tags[string(ref.Name())] = doc.snapshots[ref.SnapshotIndex()].id
	}
	for i := 0; i < root.DeletedTagsLength(); i++ {
		doc.deletedTags = append(doc.deletedTags, string(root.DeletedTags(i)))
	}
	if st := root.Status(nil); st != nil {
		doc.status = parseDocStatus(st)
	}
	doc.metadata = rawItems(vecLen(root.MetadataLength(), buf), root.Metadata)
	if root.HasConfig() {
		doc.hasConfig = true
		doc.config = append([]byte{}, root.ConfigBytes()...)
	}
	if root.HasEnabledFeatureFlags() {
		doc.hasEnabledFlags = true
		for i := 0; i < root.EnabledFeatureFlagsLength(); i++ {
			doc.enabledFlags = append(doc.enabledFlags, root.EnabledFeatureFlags(i))
		}
	}
	if root.HasDisabledFeatureFlags() {
		doc.hasDisabledFlags = true
		for i := 0; i < root.DisabledFeatureFlagsLength(); i++ {
			doc.disabledFlags = append(doc.disabledFlags, root.DisabledFeatureFlags(i))
		}
	}
	var up fbs.Update
	for i := 0; i < vecLen(root.LatestUpdatesLength(), buf); i++ {
		root.LatestUpdates(&up, i)
		du, err := parseDocUpdate(&up)
		if err != nil {
			return nil, err
		}
		doc.updates = append(doc.updates, du)
	}
	return doc, nil
}

func parseDocStatus(st *fbs.RepoStatus) docStatus {
	return docStatus{availability: st.Availability(), setAt: st.SetAt(), reason: strPtr(st.LimitedAvailabilityReason())}
}

func parseDocUpdate(up *fbs.Update) (docUpdate, error) {
	du := docUpdate{typ: up.UpdateTypeType(), updatedAt: up.UpdatedAt(), backupPath: strPtr(up.BackupPath())}
	var t flatbuffers.Table
	if !up.UpdateType(&t) {
		return du, fmt.Errorf("%w: ops log entry without payload", ErrFormat)
	}
	switch du.typ {
	case fbs.UpdateTypeRepoInitializedUpdate, fbs.UpdateTypeConfigChangedUpdate, fbs.UpdateTypeMetadataChangedUpdate,
		fbs.UpdateTypeGCRanUpdate, fbs.UpdateTypeExpirationRanUpdate:
	case fbs.UpdateTypeRepoMigratedUpdate:
		var x fbs.RepoMigratedUpdate
		x.Init(t.Bytes, t.Pos)
		du.fromVersion, du.toVersion = x.FromVersion(), x.ToVersion()
	case fbs.UpdateTypeRepoStatusChangedUpdate:
		var x fbs.RepoStatusChangedUpdate
		x.Init(t.Bytes, t.Pos)
		if st := x.Status(nil); st != nil {
			du.status = parseDocStatus(st)
		}
	case fbs.UpdateTypeTagCreatedUpdate:
		var x fbs.TagCreatedUpdate
		x.Init(t.Bytes, t.Pos)
		du.name = string(x.Name())
	case fbs.UpdateTypeTagDeletedUpdate:
		var x fbs.TagDeletedUpdate
		x.Init(t.Bytes, t.Pos)
		du.name, du.prevSnap = string(x.Name()), idFrom12(x.PreviousSnapId(nil))
	case fbs.UpdateTypeBranchCreatedUpdate:
		var x fbs.BranchCreatedUpdate
		x.Init(t.Bytes, t.Pos)
		du.name = string(x.Name())
	case fbs.UpdateTypeBranchDeletedUpdate:
		var x fbs.BranchDeletedUpdate
		x.Init(t.Bytes, t.Pos)
		du.name, du.prevSnap = string(x.Name()), idFrom12(x.PreviousSnapId(nil))
	case fbs.UpdateTypeBranchResetUpdate:
		var x fbs.BranchResetUpdate
		x.Init(t.Bytes, t.Pos)
		du.name, du.prevSnap = string(x.Name()), idFrom12(x.PreviousSnapId(nil))
	case fbs.UpdateTypeNewCommitUpdate:
		var x fbs.NewCommitUpdate
		x.Init(t.Bytes, t.Pos)
		du.name, du.newSnap = string(x.Branch()), idFrom12(x.NewSnapId(nil))
	case fbs.UpdateTypeCommitAmendedUpdate:
		var x fbs.CommitAmendedUpdate
		x.Init(t.Bytes, t.Pos)
		du.name = string(x.Branch())
		du.prevSnap, du.newSnap = idFrom12(x.PreviousSnapId(nil)), idFrom12(x.NewSnapId(nil))
	case fbs.UpdateTypeNewDetachedSnapshotUpdate:
		var x fbs.NewDetachedSnapshotUpdate
		x.Init(t.Bytes, t.Pos)
		du.newSnap = idFrom12(x.NewSnapId(nil))
	case fbs.UpdateTypeFeatureFlagChangedUpdate:
		var x fbs.FeatureFlagChangedUpdate
		x.Init(t.Bytes, t.Pos)
		du.flagID, du.flagValue, du.flagIsSet = x.Id(), x.NewValue(), x.IsSet()
	default:
		// An update type from a newer spec: refuse to rewrite the file
		// rather than silently dropping history.
		return du, fmt.Errorf("%w: unknown ops log update type %d (written by a newer Icechunk?)", ErrFormat, du.typ)
	}
	return du, nil
}

// updatesLimit is the repository's num_updates_per_repo_info_file setting
// (default 1000): how many ops-log entries one repo info file keeps.
func (d *repoDoc) updatesLimit() int {
	if d.hasConfig {
		if cfg, err := format.DecodeFlexBuffer(d.config); err == nil {
			if m, ok := cfg.(map[string]any); ok {
				switch v := m["num_updates_per_repo_info_file"].(type) {
				case uint64:
					if v > 0 {
						return int(v)
					}
				case int64:
					if v > 0 {
						return int(v)
					}
				}
			}
		}
	}
	return defaultUpdatesPerRepoFile
}

// snapshotIndex returns the index of id in the sorted snapshot list.
func (d *repoDoc) snapshotIndex(id SnapshotID) (int, bool) {
	i := sort.Search(len(d.snapshots), func(i int) bool { return bytes.Compare(d.snapshots[i].id[:], id[:]) >= 0 })
	return i, i < len(d.snapshots) && d.snapshots[i].id == id
}

// nextUpdateTime returns now, or later if needed for the ops log to stay
// strictly increasing (upstream rejects non-increasing timestamps).
func (d *repoDoc) nextUpdateTime(now time.Time) uint64 {
	t := uint64(now.UnixMicro())
	if len(d.updates) > 0 && t <= d.updates[0].updatedAt {
		t = d.updates[0].updatedAt + 1
	}
	return t
}

// pushUpdate adds u to the ops log the way upstream does: the previous
// newest entry gets the backup path of the file being replaced, and once
// the log exceeds the per-file limit, older entries are left in the backup
// chain (repo_before_updates).
func (d *repoDoc) pushUpdate(u docUpdate, backupPath string, limit int) {
	all := make([]docUpdate, 0, len(d.updates)+1)
	all = append(all, u)
	for i, prev := range d.updates {
		if i == 0 {
			bp := backupPath
			prev.backupPath = &bp
		}
		all = append(all, prev)
	}
	rbu := d.repoBeforeUpdates
	var kept []docUpdate
	for _, e := range all {
		if len(kept) >= limit && e.backupPath != nil {
			rbu = e.backupPath
			break
		}
		kept = append(kept, e)
	}
	d.updates, d.repoBeforeUpdates = kept, rbu
}

func (d *repoDoc) addSnapshot(s docSnapshot) error {
	i, found := d.snapshotIndex(s.id)
	if found {
		return fmt.Errorf("icechunk: duplicate snapshot id %s", s.id)
	}
	d.snapshots = append(d.snapshots, docSnapshot{})
	copy(d.snapshots[i+1:], d.snapshots[i:])
	d.snapshots[i] = s
	return nil
}

// ---------------------------------------------------------------------------
// Encoding

func sortedNames(m map[string]SnapshotID) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func createID12(b *flatbuffers.Builder, id ObjectID12) flatbuffers.UOffsetT {
	return fbs.CreateObjectId12(b, id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7], id[8], id[9], id[10], id[11])
}

func createID8(b *flatbuffers.Builder, id NodeID) flatbuffers.UOffsetT {
	return fbs.CreateObjectId8(b, id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7])
}

// offsetVector writes a vector of table/string offsets.
func offsetVector(b *flatbuffers.Builder, offs []flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	b.StartVector(4, len(offs), 4)
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

func metadataVector(b *flatbuffers.Builder, items []rawMetadataItem) flatbuffers.UOffsetT {
	sorted := append([]rawMetadataItem(nil), items...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].name < sorted[j].name })
	offs := make([]flatbuffers.UOffsetT, len(sorted))
	for i, it := range sorted {
		name := b.CreateSharedString(it.name)
		value := b.CreateByteVector(it.value)
		fbs.MetadataItemStart(b)
		fbs.MetadataItemAddName(b, name)
		fbs.MetadataItemAddValue(b, value)
		offs[i] = fbs.MetadataItemEnd(b)
	}
	return offsetVector(b, offs)
}

func encodeDocStatus(b *flatbuffers.Builder, st docStatus) flatbuffers.UOffsetT {
	var reason flatbuffers.UOffsetT
	if st.reason != nil {
		reason = b.CreateString(*st.reason)
	}
	fbs.RepoStatusStart(b)
	fbs.RepoStatusAddAvailability(b, st.availability)
	fbs.RepoStatusAddSetAt(b, st.setAt)
	if st.reason != nil {
		fbs.RepoStatusAddLimitedAvailabilityReason(b, reason)
	}
	return fbs.RepoStatusEnd(b)
}

func encodeDocUpdate(b *flatbuffers.Builder, u docUpdate) flatbuffers.UOffsetT {
	var payload flatbuffers.UOffsetT
	switch u.typ {
	case fbs.UpdateTypeRepoInitializedUpdate:
		fbs.RepoInitializedUpdateStart(b)
		payload = fbs.RepoInitializedUpdateEnd(b)
	case fbs.UpdateTypeConfigChangedUpdate:
		fbs.ConfigChangedUpdateStart(b)
		payload = fbs.ConfigChangedUpdateEnd(b)
	case fbs.UpdateTypeMetadataChangedUpdate:
		fbs.MetadataChangedUpdateStart(b)
		payload = fbs.MetadataChangedUpdateEnd(b)
	case fbs.UpdateTypeGCRanUpdate:
		fbs.GCRanUpdateStart(b)
		payload = fbs.GCRanUpdateEnd(b)
	case fbs.UpdateTypeExpirationRanUpdate:
		fbs.ExpirationRanUpdateStart(b)
		payload = fbs.ExpirationRanUpdateEnd(b)
	case fbs.UpdateTypeRepoMigratedUpdate:
		fbs.RepoMigratedUpdateStart(b)
		fbs.RepoMigratedUpdateAddFromVersion(b, u.fromVersion)
		fbs.RepoMigratedUpdateAddToVersion(b, u.toVersion)
		payload = fbs.RepoMigratedUpdateEnd(b)
	case fbs.UpdateTypeRepoStatusChangedUpdate:
		st := encodeDocStatus(b, u.status)
		fbs.RepoStatusChangedUpdateStart(b)
		fbs.RepoStatusChangedUpdateAddStatus(b, st)
		payload = fbs.RepoStatusChangedUpdateEnd(b)
	case fbs.UpdateTypeTagCreatedUpdate:
		name := b.CreateString(u.name)
		fbs.TagCreatedUpdateStart(b)
		fbs.TagCreatedUpdateAddName(b, name)
		payload = fbs.TagCreatedUpdateEnd(b)
	case fbs.UpdateTypeTagDeletedUpdate:
		name := b.CreateString(u.name)
		fbs.TagDeletedUpdateStart(b)
		fbs.TagDeletedUpdateAddName(b, name)
		fbs.TagDeletedUpdateAddPreviousSnapId(b, createID12(b, u.prevSnap))
		payload = fbs.TagDeletedUpdateEnd(b)
	case fbs.UpdateTypeBranchCreatedUpdate:
		name := b.CreateString(u.name)
		fbs.BranchCreatedUpdateStart(b)
		fbs.BranchCreatedUpdateAddName(b, name)
		payload = fbs.BranchCreatedUpdateEnd(b)
	case fbs.UpdateTypeBranchDeletedUpdate:
		name := b.CreateString(u.name)
		fbs.BranchDeletedUpdateStart(b)
		fbs.BranchDeletedUpdateAddName(b, name)
		fbs.BranchDeletedUpdateAddPreviousSnapId(b, createID12(b, u.prevSnap))
		payload = fbs.BranchDeletedUpdateEnd(b)
	case fbs.UpdateTypeBranchResetUpdate:
		name := b.CreateString(u.name)
		fbs.BranchResetUpdateStart(b)
		fbs.BranchResetUpdateAddName(b, name)
		fbs.BranchResetUpdateAddPreviousSnapId(b, createID12(b, u.prevSnap))
		payload = fbs.BranchResetUpdateEnd(b)
	case fbs.UpdateTypeNewCommitUpdate:
		branch := b.CreateString(u.name)
		fbs.NewCommitUpdateStart(b)
		fbs.NewCommitUpdateAddBranch(b, branch)
		fbs.NewCommitUpdateAddNewSnapId(b, createID12(b, u.newSnap))
		payload = fbs.NewCommitUpdateEnd(b)
	case fbs.UpdateTypeCommitAmendedUpdate:
		branch := b.CreateString(u.name)
		fbs.CommitAmendedUpdateStart(b)
		fbs.CommitAmendedUpdateAddBranch(b, branch)
		fbs.CommitAmendedUpdateAddPreviousSnapId(b, createID12(b, u.prevSnap))
		fbs.CommitAmendedUpdateAddNewSnapId(b, createID12(b, u.newSnap))
		payload = fbs.CommitAmendedUpdateEnd(b)
	case fbs.UpdateTypeNewDetachedSnapshotUpdate:
		fbs.NewDetachedSnapshotUpdateStart(b)
		fbs.NewDetachedSnapshotUpdateAddNewSnapId(b, createID12(b, u.newSnap))
		payload = fbs.NewDetachedSnapshotUpdateEnd(b)
	case fbs.UpdateTypeFeatureFlagChangedUpdate:
		fbs.FeatureFlagChangedUpdateStart(b)
		fbs.FeatureFlagChangedUpdateAddId(b, u.flagID)
		fbs.FeatureFlagChangedUpdateAddNewValue(b, u.flagValue)
		fbs.FeatureFlagChangedUpdateAddIsSet(b, u.flagIsSet)
		payload = fbs.FeatureFlagChangedUpdateEnd(b)
	}
	var backup flatbuffers.UOffsetT
	if u.backupPath != nil {
		backup = b.CreateString(*u.backupPath)
	}
	fbs.UpdateStart(b)
	fbs.UpdateAddUpdateTypeType(b, u.typ)
	fbs.UpdateAddUpdateType(b, payload)
	fbs.UpdateAddUpdatedAt(b, u.updatedAt)
	if u.backupPath != nil {
		fbs.UpdateAddBackupPath(b, backup)
	}
	return fbs.UpdateEnd(b)
}

// encode serializes the document as a complete repo info file.
func (d *repoDoc) encode() ([]byte, error) {
	if _, ok := d.branches["main"]; !ok {
		return nil, fmt.Errorf("icechunk: a repository must have a main branch")
	}
	b := flatbuffers.NewBuilder(4096)
	index := func(id SnapshotID) (uint32, error) {
		i, ok := d.snapshotIndex(id)
		if !ok {
			return 0, fmt.Errorf("icechunk: ref points to unknown snapshot %s", id)
		}
		return uint32(i), nil
	}
	refs := func(m map[string]SnapshotID) (flatbuffers.UOffsetT, error) {
		names := sortedNames(m)
		offs := make([]flatbuffers.UOffsetT, len(names))
		for i, name := range names {
			idx, err := index(m[name])
			if err != nil {
				return 0, err
			}
			n := b.CreateString(name)
			fbs.RefStart(b)
			fbs.RefAddName(b, n)
			fbs.RefAddSnapshotIndex(b, idx)
			offs[i] = fbs.RefEnd(b)
		}
		return offsetVector(b, offs), nil
	}
	tags, err := refs(d.tags)
	if err != nil {
		return nil, err
	}
	branches, err := refs(d.branches)
	if err != nil {
		return nil, err
	}
	deleted := append([]string(nil), d.deletedTags...)
	sort.Strings(deleted)
	delOffs := make([]flatbuffers.UOffsetT, len(deleted))
	for i, name := range deleted {
		delOffs[i] = b.CreateString(name)
	}
	deletedTags := offsetVector(b, delOffs)

	snapOffs := make([]flatbuffers.UOffsetT, len(d.snapshots))
	for i, s := range d.snapshots {
		parent := int32(-1)
		if s.parent != nil {
			pi, ok := d.snapshotIndex(*s.parent)
			if !ok {
				return nil, fmt.Errorf("icechunk: snapshot %s has unknown parent %s", s.id, *s.parent)
			}
			parent = int32(pi)
		}
		md := metadataVector(b, s.metadata)
		msg := b.CreateString(s.message)
		var pruned flatbuffers.UOffsetT
		if s.pruned != nil {
			fbs.SnapshotInfoStartPrunedAncestorTxLogsVector(b, len(s.pruned))
			for j := len(s.pruned) - 1; j >= 0; j-- {
				createID12(b, s.pruned[j])
			}
			pruned = b.EndVector(len(s.pruned))
		}
		fbs.SnapshotInfoStart(b)
		fbs.SnapshotInfoAddId(b, createID12(b, s.id))
		fbs.SnapshotInfoAddParentOffset(b, parent)
		fbs.SnapshotInfoAddFlushedAt(b, s.flushedAt)
		fbs.SnapshotInfoAddMessage(b, msg)
		fbs.SnapshotInfoAddMetadata(b, md)
		if s.pruned != nil {
			fbs.SnapshotInfoAddPrunedAncestorTxLogs(b, pruned)
		}
		snapOffs[i] = fbs.SnapshotInfoEnd(b)
	}
	snapshots := offsetVector(b, snapOffs)
	status := encodeDocStatus(b, d.status)
	metadata := metadataVector(b, d.metadata)
	u16s := func(vals []uint16) flatbuffers.UOffsetT {
		b.StartVector(2, len(vals), 2)
		for i := len(vals) - 1; i >= 0; i-- {
			b.PrependUint16(vals[i])
		}
		return b.EndVector(len(vals))
	}
	var enabled, disabled flatbuffers.UOffsetT
	if d.hasEnabledFlags {
		enabled = u16s(d.enabledFlags)
	}
	if d.hasDisabledFlags {
		disabled = u16s(d.disabledFlags)
	}
	upOffs := make([]flatbuffers.UOffsetT, len(d.updates))
	for i, u := range d.updates {
		upOffs[i] = encodeDocUpdate(b, u)
	}
	updates := offsetVector(b, upOffs)
	var rbu, config, extra flatbuffers.UOffsetT
	if d.repoBeforeUpdates != nil {
		rbu = b.CreateString(*d.repoBeforeUpdates)
	}
	if d.hasConfig {
		config = b.CreateByteVector(d.config)
	}
	if len(d.extra) > 0 {
		extra = b.CreateByteVector(d.extra)
	}

	fbs.RepoStart(b)
	fbs.RepoAddSpecVersion(b, d.specVersion)
	fbs.RepoAddTags(b, tags)
	fbs.RepoAddBranches(b, branches)
	fbs.RepoAddDeletedTags(b, deletedTags)
	fbs.RepoAddSnapshots(b, snapshots)
	fbs.RepoAddStatus(b, status)
	fbs.RepoAddMetadata(b, metadata)
	fbs.RepoAddLatestUpdates(b, updates)
	if d.repoBeforeUpdates != nil {
		fbs.RepoAddRepoBeforeUpdates(b, rbu)
	}
	if d.hasConfig {
		fbs.RepoAddConfig(b, config)
	}
	if d.hasEnabledFlags {
		fbs.RepoAddEnabledFeatureFlags(b, enabled)
	}
	if d.hasDisabledFlags {
		fbs.RepoAddDisabledFeatureFlags(b, disabled)
	}
	if len(d.extra) > 0 {
		fbs.RepoAddExtra(b, extra)
	}
	b.FinishWithFileIdentifier(fbs.RepoEnd(b), []byte("Ichk"))
	return format.Encode(format.FileTypeRepoInfo, d.specVersion, b.FinishedBytes())
}

// featureFlagEnabled resolves a flag from the explicit overrides and the
// flag's default (all currently defined flags default to enabled).
func (d *repoDoc) featureFlagEnabled(id uint16) bool {
	for _, f := range d.disabledFlags {
		if f == id {
			return false
		}
	}
	return true
}

package icechunk

import (
	"cmp"
	"crypto/rand"
	"fmt"
	"strconv"

	"github.com/solomonsealed/icechunk-go/internal/format"
	"github.com/solomonsealed/icechunk-go/storage"
)

// StorageSettings are the object store settings of a repository's
// configuration (upstream's RepositoryConfig.storage) that the writer
// applies: storage classes, and whether objects carry user metadata.
// Repositories apply the settings persisted in their configuration,
// overridden field by field by those of Options.StorageSettings, as
// upstream merges the config given to Repository.open over the persisted
// one. Empty or nil fields leave the persisted value in place.
type StorageSettings struct {
	// StorageClass is the storage class of the objects written, passed to
	// the store as is (e.g. "STANDARD_IA" on S3), unless one of the classes
	// below overrides it. The repo info file and its backups always use it.
	StorageClass string
	// MetadataStorageClass is the storage class of snapshots, manifests and
	// transaction logs.
	MetadataStorageClass string
	// ChunksStorageClass is the storage class of chunks.
	ChunksStorageClass string
	// UnsafeUseMetadata is whether objects carry user metadata (true when
	// unset). Snapshots, manifests, transaction logs and the repo info file
	// then carry upstream's icspecver, icclient, icfiletype and iccompalg,
	// and conditional writes of the repo info file a write id, so that a
	// write whose response was lost is recognised (see
	// storage.PutOptions.Metadata). Upstream persists false for
	// repositories it creates on local files, which keep no metadata.
	UnsafeUseMetadata *bool
}

// writeSettings are the settings in effect for writes.
type writeSettings struct {
	storageClass, metadataClass, chunksClass string
	metadata                                 bool
}

// resolveWriteSettings merges override over the storage settings of a
// repository's stored configuration (the "storage" entry of RepoInfo.Config).
func resolveWriteSettings(config any, override StorageSettings) writeSettings {
	var s StorageSettings
	if cfg, ok := config.(map[string]any); ok {
		if st, ok := cfg["storage"].(map[string]any); ok {
			s.StorageClass, _ = st["storage_class"].(string)
			s.MetadataStorageClass, _ = st["metadata_storage_class"].(string)
			s.ChunksStorageClass, _ = st["chunks_storage_class"].(string)
			if b, ok := st["unsafe_use_metadata"].(bool); ok {
				s.UnsafeUseMetadata = &b
			}
		}
	}
	s.StorageClass = cmp.Or(override.StorageClass, s.StorageClass)
	s.MetadataStorageClass = cmp.Or(override.MetadataStorageClass, s.MetadataStorageClass)
	s.ChunksStorageClass = cmp.Or(override.ChunksStorageClass, s.ChunksStorageClass)
	if override.UnsafeUseMetadata != nil {
		s.UnsafeUseMetadata = override.UnsafeUseMetadata
	}
	return writeSettings{
		storageClass:  s.StorageClass,
		metadataClass: cmp.Or(s.MetadataStorageClass, s.StorageClass),
		chunksClass:   cmp.Or(s.ChunksStorageClass, s.StorageClass),
		metadata:      s.UnsafeUseMetadata == nil || *s.UnsafeUseMetadata,
	}
}

// writeSettings returns the settings in effect for the repository's writes.
func (r *Repository) writeSettings() writeSettings {
	r.mu.Lock()
	info := r.info
	r.mu.Unlock()
	var config any
	if info != nil {
		config = info.Config
	}
	return resolveWriteSettings(config, r.opts.StorageSettings)
}

// The file types of upstream's icfiletype object metadata.
const (
	fileTypeSnapshot       = "snapshot"
	fileTypeManifest       = "manifest"
	fileTypeTransactionLog = "transaction-log"
	fileTypeRepoInfo       = "repo-info"
)

// objectMetadata is the user metadata upstream stamps on the files it
// writes, with this module's implementation name (as in file headers) as
// the client.
func objectMetadata(fileType string, specVersion uint8) map[string]string {
	return map[string]string{
		"icspecver":  strconv.Itoa(int(specVersion)),
		"icclient":   format.ImplementationName,
		"icfiletype": fileType,
		"iccompalg":  "zstd",
	}
}

// chunk returns the options of a chunk write.
func (w writeSettings) chunk() *storage.PutOptions {
	if w.chunksClass == "" {
		return nil
	}
	return &storage.PutOptions{StorageClass: w.chunksClass}
}

// file returns the options of a write of a snapshot, manifest or
// transaction log.
func (w writeSettings) file(fileType string) *storage.PutOptions {
	o := &storage.PutOptions{StorageClass: w.metadataClass}
	if w.metadata {
		o.Metadata = objectMetadata(fileType, writeSpecVersion)
	}
	return o
}

// repoInfo returns the options of a write of the repo info file, or of its
// backup, with the conditions of cond. Conditional writes carry a fresh
// write id. Backups are copied server side where the storage can (see
// storage.Copier), keeping the original's metadata as upstream's do; a
// backup written instead gets the metadata of a repo info file, without a
// write id.
func (w writeSettings) repoInfo(specVersion uint8, cond storage.PutOptions) *storage.PutOptions {
	cond.StorageClass = w.storageClass
	if w.metadata {
		cond.Metadata = objectMetadata(fileTypeRepoInfo, specVersion)
		if cond.IfMatch != "" || cond.IfNotExists {
			cond.Metadata[storage.WriteIDMetadataKey] = newWriteID()
		}
	}
	return &cond
}

// newWriteID returns a random (version 4) UUID, as upstream's write ids.
func newWriteID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

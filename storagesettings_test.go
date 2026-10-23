package icechunk

import (
	"regexp"
	"testing"

	"github.com/solomonsealed/icechunk-go/storage"
)

// Settings merge as upstream's Settings::merge: the config given to Open
// over the persisted one, field by field, and the metadata and chunks
// classes fall back to the storage class.
func TestResolveWriteSettings(t *testing.T) {
	on, off := true, false
	persisted := map[string]any{"storage": map[string]any{
		"storage_class": "STANDARD_IA", "metadata_storage_class": nil, "chunks_storage_class": "INTELLIGENT_TIERING",
		"unsafe_use_metadata": false, "unsafe_use_conditional_update": false,
	}}
	for _, c := range []struct {
		name     string
		config   any
		override StorageSettings
		want     writeSettings
	}{
		{"none", nil, StorageSettings{}, writeSettings{metadata: true}},
		{"no storage entry", map[string]any{"inline_chunk_threshold_bytes": uint64(64)}, StorageSettings{}, writeSettings{metadata: true}},
		{"persisted", persisted, StorageSettings{},
			writeSettings{storageClass: "STANDARD_IA", metadataClass: "STANDARD_IA", chunksClass: "INTELLIGENT_TIERING"}},
		{"overridden", persisted, StorageSettings{StorageClass: "ONEZONE_IA", MetadataStorageClass: "GLACIER_IR", UnsafeUseMetadata: &on},
			writeSettings{storageClass: "ONEZONE_IA", metadataClass: "GLACIER_IR", chunksClass: "INTELLIGENT_TIERING", metadata: true}},
		{"override only", nil, StorageSettings{ChunksStorageClass: "X", UnsafeUseMetadata: &off},
			writeSettings{chunksClass: "X"}},
	} {
		if got := resolveWriteSettings(c.config, c.override); got != c.want {
			t.Errorf("%s: %+v, want %+v", c.name, got, c.want)
		}
	}

	w := writeSettings{storageClass: "S", metadataClass: "M", chunksClass: "C", metadata: true}
	if o := w.chunk(); o.StorageClass != "C" || o.Metadata != nil || o.IfMatch != "" {
		t.Errorf("chunk: %+v", o)
	}
	if o := (writeSettings{metadata: true}).chunk(); o != nil {
		t.Errorf("chunk without a class: %+v", o)
	}
	if o := w.file(fileTypeManifest); o.StorageClass != "M" || o.Metadata["icfiletype"] != "manifest" || o.Metadata["icspecver"] != "2" ||
		o.Metadata["icclient"] != "icechunk_go-0.1.0" || o.Metadata["iccompalg"] != "zstd" || len(o.Metadata) != 4 {
		t.Errorf("file: %+v", o)
	}
	uuid := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	a, b := w.repoInfo(2, storage.PutOptions{IfMatch: "v"}), w.repoInfo(2, storage.PutOptions{IfNotExists: true})
	if a.StorageClass != "S" || a.IfMatch != "v" || a.Metadata["icfiletype"] != "repo-info" || !uuid.MatchString(a.Metadata[storage.WriteIDMetadataKey]) ||
		!b.IfNotExists || a.Metadata[storage.WriteIDMetadataKey] == b.Metadata[storage.WriteIDMetadataKey] {
		t.Errorf("conditional repo info writes: %+v, %+v", a, b)
	}
	if o := w.repoInfo(2, storage.PutOptions{}); o.Metadata[storage.WriteIDMetadataKey] != "" || len(o.Metadata) != 4 {
		t.Errorf("repo info backup: %+v", o)
	}
	if o := (writeSettings{}).repoInfo(2, storage.PutOptions{IfMatch: "v"}); o.Metadata != nil {
		t.Errorf("repo info without metadata: %+v", o)
	}
}

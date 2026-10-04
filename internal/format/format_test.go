package format

import (
	"encoding/json"
	"reflect"
	"testing"
)

func normalizeJSON(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	json.Unmarshal(b, &out)
	return out
}

func TestFlexBufferRoundTrip(t *testing.T) {
	for _, v := range []any{
		nil, true, int64(-7), uint64(1 << 63), 0.25, "héllo", []any{},
		map[string]any{},
		map[string]any{"author": "go", "n": 42, "nested": map[string]any{"list": []any{1, "two", 3.5, false, nil}, "empty": map[string]any{}}},
		[]any{map[string]any{"a": []any{[]any{}}}, "x"},
	} {
		buf, err := EncodeFlexBuffer(v)
		if err != nil {
			t.Fatalf("encode %v: %v", v, err)
		}
		got, err := DecodeFlexBuffer(buf)
		if err != nil {
			t.Fatalf("decode %v: %v", v, err)
		}
		if !reflect.DeepEqual(normalizeJSON(t, got), normalizeJSON(t, v)) {
			t.Errorf("round trip %v -> %v", v, got)
		}
	}
}

func TestEncodeEnvelope(t *testing.T) {
	payload := []byte("flatbuffer payload")
	file, err := Encode(FileTypeManifest, 2, payload)
	if err != nil {
		t.Fatal(err)
	}
	h, body, err := Decode(file, FileTypeManifest)
	if err != nil || string(body) != string(payload) || h.Implementation != ImplementationName || h.SpecVersion != 2 {
		t.Fatalf("decode = %+v %q %v", h, body, err)
	}
}

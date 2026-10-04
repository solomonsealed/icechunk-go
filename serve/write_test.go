package serve

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	icechunk "github.com/solomonsealed/icechunk-go"
	"github.com/solomonsealed/icechunk-go/storage"
)

func do(t *testing.T, s *Service, method, target, token, body string) *Response {
	t.Helper()
	u, _ := url.Parse(target)
	h := map[string]string{}
	if token != "" {
		h["authorization"] = "Bearer " + token
	}
	return s.Handle(context.Background(), &Request{Method: method, URL: u, Header: h, Body: []byte(body)})
}

func TestWriteEndpoints(t *testing.T) {
	ctx := context.Background()
	repo, err := icechunk.Create(ctx, storage.NewMemory(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{Repo: repo}
	if r := do(t, s, "POST", "/arrays/a", "x", `{}`); r.Status != 405 {
		t.Errorf("writes without a token configured: %d", r.Status)
	}
	s.WriteToken = "secret"
	if r := do(t, s, "POST", "/arrays/a", "wrong", `{}`); r.Status != 401 {
		t.Errorf("bad token: %d", r.Status)
	}
	spec := `{"shape":[4,3],"chunk_shape":[2,2],"data_type":"float32","fill_value":"NaN",
		"dimension_names":["y",null],"attributes":{"units":"K"}}`
	if r := do(t, s, "POST", "/arrays/weather/temp?message=init", "secret", spec); r.Status != 201 {
		t.Fatalf("create: %d %s", r.Status, r.Body)
	}
	if r := do(t, s, "POST", "/arrays/weather/temp", "secret", spec); r.Status != 400 {
		t.Errorf("duplicate create: %d %s", r.Status, r.Body)
	}
	w := `{"start":[1,0],"shape":[2,3],"data":[1,2,3,4,null,"Infinity"]}`
	if r := do(t, s, "PUT", "/array/weather/temp", "secret", w); r.Status != 201 {
		t.Fatalf("write: %d %s", r.Status, r.Body)
	}
	r := get(t, s, "/array/weather/temp", nil)
	if string(r.Body) != `{"shape":[4,3],"dtype":"float32","data":[null,null,null,1,2,3,4,null,null,null,null,null]}` {
		t.Errorf("read back %s", r.Body)
	}
	if r := do(t, s, "PUT", "/array/weather/temp", "secret", `{"shape":[1],"data":[1]}`); r.Status != 400 {
		t.Errorf("bad write: %d %s", r.Status, r.Body)
	}
	var log []commitJSON
	json.Unmarshal(get(t, s, "/log", nil).Body, &log)
	if len(log) != 3 || log[0].Message != "write /weather/temp at [1 0]" || log[1].Message != "init" {
		t.Errorf("log = %+v", log)
	}
	// Parent groups were created like zarr-python does.
	if r := get(t, s, "/zarr/main/weather/zarr.json", nil); r.Status != 200 {
		t.Errorf("parent group: %d", r.Status)
	}
}

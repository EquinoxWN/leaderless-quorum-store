package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/EquinoxWN/leaderless-quorum-store/internal/replica"
)

func record(key, val string, c uint64) replica.Record {
	return replica.Record{Key: key, Value: []byte(val), Tag: replica.Tag{Counter: c, Writer: "w"}}
}

func TestLocalFaults(t *testing.T) {
	ctx := context.Background()
	l := NewLocal([]string{"a", "b"})
	if err := l.Write(ctx, "a", record("k", "v", 1)); err != nil {
		t.Fatal(err)
	}
	l.SetDown("a", true)
	if _, _, err := l.Read(ctx, "a", "k"); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("down read: %v", err)
	}
	if err := l.Write(ctx, "a", record("k", "v", 2)); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("down write: %v", err)
	}
	l.SetDown("a", false)
	l.SetDropWrites("a", true)
	if err := l.Write(ctx, "a", record("k", "v", 3)); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("dropped write: %v", err)
	}
	got, ok, err := l.Read(ctx, "a", "k")
	if err != nil || !ok || got.Tag.Counter != 1 {
		t.Fatalf("read while writes dropped = %+v %v %v, want counter 1", got, ok, err)
	}
	l.Heal()
	if err := l.Write(ctx, "a", record("k", "v", 4)); err != nil {
		t.Fatalf("after heal: %v", err)
	}
	if _, _, err := l.Read(ctx, "zz", "k"); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("unknown node: %v", err)
	}
	if err := l.Write(ctx, "b", record("", "v", 1)); !errors.Is(err, ErrRejected) {
		t.Fatalf("invalid record: %v", err)
	}
	if l.Replica("b").Len() != 0 {
		t.Fatal("rejected record stored")
	}
}

func TestLocalDelayHonoursContext(t *testing.T) {
	l := NewLocal([]string{"a"})
	l.SetDelay("a", time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := l.Read(ctx, "a", "k")
	if !errors.Is(err, ErrUnreachable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("delay ignored the context deadline")
	}
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	l.SetDelay("a", 0)
	if err := l.Write(cctx, "a", record("k", "v", 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled write: %v", err)
	}
}

func newServer(t *testing.T) (*HTTP, *replica.Replica, string) {
	t.Helper()
	rep := replica.New()
	srv := httptest.NewServer(NewHandler(rep))
	t.Cleanup(srv.Close)
	h, err := NewHTTP(map[string]string{"a": srv.URL + "/"}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return h, rep, srv.URL
}

func TestHTTPRoundTrip(t *testing.T) {
	h, rep, _ := newServer(t)
	ctx := context.Background()
	for _, key := range []string{"plain", "with/slash", "with space", "üñí?#%"} {
		if _, ok, err := h.Read(ctx, "a", key); err != nil || ok {
			t.Fatalf("%q before write: ok=%v err=%v", key, ok, err)
		}
		if err := h.Write(ctx, "a", record(key, "value of "+key, 7)); err != nil {
			t.Fatalf("%q write: %v", key, err)
		}
		got, ok, err := h.Read(ctx, "a", key)
		if err != nil || !ok || string(got.Value) != "value of "+key || got.Tag.Counter != 7 {
			t.Fatalf("%q read = %+v %v %v", key, got, ok, err)
		}
		if stored, _ := rep.Get(key); stored.Key != key {
			t.Fatalf("%q stored under %q", key, stored.Key)
		}
	}
	tomb := replica.Record{Key: "plain", Tag: replica.Tag{Counter: 8, Writer: "w"}, Deleted: true}
	if err := h.Write(ctx, "a", tomb); err != nil {
		t.Fatal(err)
	}
	got, ok, err := h.Read(ctx, "a", "plain")
	if err != nil || !ok || !got.Deleted {
		t.Fatalf("tombstone read = %+v %v %v", got, ok, err)
	}
}

func TestHTTPRejectsBadRequests(t *testing.T) {
	h, rep, base := newServer(t)
	ctx := context.Background()
	if err := h.Write(ctx, "a", record("k", "v", 0)); !errors.Is(err, ErrRejected) {
		t.Fatalf("client-side validation: %v", err)
	}
	if err := h.Write(ctx, "nope", record("k", "v", 1)); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("unknown node: %v", err)
	}
	cases := map[string]string{
		"not json":       `{`,
		"unknown field":  `{"key":"k","tag":{"counter":1,"writer":"w"},"admin":true}`,
		"key mismatch":   `{"key":"other","tag":{"counter":1,"writer":"w"}}`,
		"zero counter":   `{"key":"k","tag":{"counter":0,"writer":"w"}}`,
		"oversized body": `{"key":"k","value":"` + strings.Repeat("A", maxBody) + `"}`,
	}
	for name, body := range cases {
		req, err := http.NewRequest(http.MethodPut, base+"/v1/kv/k", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, resp.StatusCode)
		}
	}
	if rep.Len() != 0 {
		t.Fatalf("bad requests stored %d records", rep.Len())
	}
	resp, err := http.Get(base + "/v1/kv/" + strings.Repeat("k", replica.MaxKeyBytes+1))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("long key read: status %d", resp.StatusCode)
	}
	resp, err = http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
}

func TestHTTPClassifiesFailures(t *testing.T) {
	ctx := context.Background()
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/kv/garbage":
			_, _ = w.Write([]byte("not json"))
		case "/v1/kv/wrongkey":
			_, _ = w.Write([]byte(`{"key":"other","tag":{"counter":1,"writer":"w"}}`))
		case "/v1/kv/invalid":
			_, _ = w.Write([]byte(`{"key":"invalid","tag":{"counter":0,"writer":"w"}}`))
		case "/v1/kv/slow":
			time.Sleep(300 * time.Millisecond)
		default:
			http.Error(w, "boom", http.StatusInternalServerError)
		}
	}))
	defer broken.Close()
	h, err := NewHTTP(map[string]string{"b": broken.URL}, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"garbage", "wrongkey", "invalid", "slow", "fail"} {
		if _, _, err := h.Read(ctx, "b", key); !errors.Is(err, ErrUnreachable) {
			t.Errorf("read %s: %v, want ErrUnreachable", key, err)
		}
	}
	if err := h.Write(ctx, "b", record("k", "v", 1)); !errors.Is(err, ErrUnreachable) || !strings.Contains(err.Error(), "500") {
		t.Errorf("write to failing server: %v", err)
	}
	dead, err := NewHTTP(map[string]string{"c": "http://127.0.0.1:1"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := dead.Read(ctx, "c", "k"); !errors.Is(err, ErrUnreachable) {
		t.Errorf("closed port: %v", err)
	}
}

func TestNewHTTPValidatesConfig(t *testing.T) {
	bad := []map[string]string{
		nil,
		{"a": "127.0.0.1:7001"},
		{"a": "ftp://host:1"},
		{"a": "http://"},
		{"a": "http://host:1/path"},
		{"a": "http://host:1", "b": "::bad"},
	}
	for _, addrs := range bad {
		if _, err := NewHTTP(addrs, time.Second); err == nil {
			t.Errorf("NewHTTP(%v) accepted", addrs)
		}
	}
	if _, err := NewHTTP(map[string]string{"a": "http://h:1"}, 0); err == nil {
		t.Error("zero timeout accepted")
	}
}

package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/EquinoxWN/leaderless-quorum-store/internal/replica"
)

// maxBody bounds a JSON record on the wire (base64 value plus fields).
const maxBody = replica.MaxValueBytes*4/3 + 4096

// HTTP sends requests to replica nodes over HTTP/JSON; safe for concurrent use.
type HTTP struct {
	addrs  map[string]string
	client *http.Client
}

// NewHTTP maps node ids to base URLs like http://127.0.0.1:7001; timeout caps each request.
func NewHTTP(addrs map[string]string, timeout time.Duration) (*HTTP, error) {
	if len(addrs) == 0 {
		return nil, errors.New("transport: no node addresses")
	}
	if timeout <= 0 {
		return nil, errors.New("transport: timeout must be positive")
	}
	clean := make(map[string]string, len(addrs))
	for node, raw := range addrs {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || (u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("transport: node %q: address %q must be http(s)://host:port", node, raw)
		}
		clean[node] = strings.TrimSuffix(raw, "/")
	}
	return &HTTP{addrs: clean, client: &http.Client{Timeout: timeout}}, nil
}

// url builds the request URL for key on node.
func (h *HTTP) url(node, key string) (string, error) {
	base, ok := h.addrs[node]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownNode, node)
	}
	return base + "/v1/kv/" + url.PathEscape(key), nil
}

// statusErr classifies a non-success response: 4xx is permanent, the rest retryable.
func statusErr(node string, resp *http.Response) error {
	msg, err := io.ReadAll(io.LimitReader(resp.Body, 512))
	if err != nil {
		msg = []byte("(body unreadable: " + err.Error() + ")")
	}
	kind := ErrUnreachable
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		kind = ErrRejected
	}
	return fmt.Errorf("%w: %s: HTTP %d: %s", kind, node, resp.StatusCode, strings.TrimSpace(string(msg)))
}

// Read fetches key from node; a 404 means the replica has no record.
func (h *HTTP) Read(ctx context.Context, node, key string) (replica.Record, bool, error) {
	u, err := h.url(node, key)
	if err != nil {
		return replica.Record{}, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return replica.Record{}, false, fmt.Errorf("%w: %w", ErrRejected, err)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return replica.Record{}, false, fmt.Errorf("%w: %s: %w", ErrUnreachable, node, err)
	}
	defer closeBody(resp)
	switch resp.StatusCode {
	case http.StatusNotFound:
		return replica.Record{}, false, nil
	case http.StatusOK:
	default:
		return replica.Record{}, false, statusErr(node, resp)
	}
	var rec replica.Record
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return replica.Record{}, false, fmt.Errorf("%w: %s: bad response: %w", ErrUnreachable, node, err)
	}
	if rec.Key != key {
		return replica.Record{}, false, fmt.Errorf("%w: %s: response for key %q, asked %q", ErrUnreachable, node, rec.Key, key)
	}
	if err := rec.Validate(); err != nil {
		return replica.Record{}, false, fmt.Errorf("%w: %s: bad response: %w", ErrUnreachable, node, err)
	}
	return rec, true, nil
}

// Write stores rec on node.
func (h *HTTP) Write(ctx context.Context, node string, rec replica.Record) error {
	if err := rec.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	u, err := h.url(node, rec.Key)
	if err != nil {
		return err
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrUnreachable, node, err)
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusNoContent {
		return statusErr(node, resp)
	}
	return nil
}

// closeBody drains a little of the body so the connection can be reused, then closes it.
func closeBody(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)) // best effort: only affects connection reuse
	_ = resp.Body.Close()                                       // read side; nothing to recover
}

// NewHandler serves one replica over HTTP: GET and PUT /v1/kv/{key}, GET /healthz.
func NewHandler(rep *replica.Replica) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "ok\n") // client gone: nothing to do
	})
	mux.HandleFunc("GET /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if err := replica.ValidateKey(key); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rec, ok := rep.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rec) // client gone: nothing to do
	})
	mux.HandleFunc("PUT /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		var rec replica.Record
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rec); err != nil {
			http.Error(w, "bad body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if rec.Key != r.PathValue("key") {
			http.Error(w, "key in body does not match path", http.StatusBadRequest)
			return
		}
		if _, err := rep.Apply(rec); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

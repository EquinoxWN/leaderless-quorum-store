// Package replica holds one node's copy of the data: versioned records in memory.
package replica

import (
	"errors"
	"fmt"
	"sync"
	"unicode/utf8"
)

// Limits on what a replica accepts.
const (
	MaxKeyBytes    = 256
	MaxValueBytes  = 1 << 20
	MaxWriterBytes = 64
)

// ErrInvalid reports a record that breaks the limits above.
var ErrInvalid = errors.New("replica: invalid record")

// Tag orders versions of a key: higher Counter wins, Writer breaks ties.
type Tag struct {
	Counter uint64 `json:"counter"`
	Writer  string `json:"writer"`
}

// Less reports whether t is older than o.
func (t Tag) Less(o Tag) bool {
	if t.Counter != o.Counter {
		return t.Counter < o.Counter
	}
	return t.Writer < o.Writer
}

// IsZero reports whether t is the tag of a key never written.
func (t Tag) IsZero() bool { return t.Counter == 0 && t.Writer == "" }

// String formats the tag as counter@writer.
func (t Tag) String() string { return fmt.Sprintf("%d@%s", t.Counter, t.Writer) }

// Record is one version of a key; Deleted marks a tombstone.
type Record struct {
	Key     string `json:"key"`
	Value   []byte `json:"value,omitempty"`
	Tag     Tag    `json:"tag"`
	Deleted bool   `json:"deleted,omitempty"`
}

// ValidateKey checks a key against the replica limits.
func ValidateKey(key string) error {
	if key == "" || len(key) > MaxKeyBytes || !utf8.ValidString(key) {
		return fmt.Errorf("%w: key must be 1..%d bytes of UTF-8", ErrInvalid, MaxKeyBytes)
	}
	return nil
}

// ValidateWriter checks a writer (coordinator) id.
func ValidateWriter(w string) error {
	if w == "" || len(w) > MaxWriterBytes || !utf8.ValidString(w) {
		return fmt.Errorf("%w: writer id must be 1..%d bytes of UTF-8", ErrInvalid, MaxWriterBytes)
	}
	return nil
}

// Validate checks a record before it is stored or sent.
func (r Record) Validate() error {
	if err := ValidateKey(r.Key); err != nil {
		return err
	}
	if len(r.Value) > MaxValueBytes {
		return fmt.Errorf("%w: value is %d bytes, limit %d", ErrInvalid, len(r.Value), MaxValueBytes)
	}
	if r.Tag.Counter == 0 {
		return fmt.Errorf("%w: tag counter must be positive", ErrInvalid)
	}
	if r.Deleted && len(r.Value) > 0 {
		return fmt.Errorf("%w: tombstone carries a value", ErrInvalid)
	}
	return ValidateWriter(r.Tag.Writer)
}

// Replica is an in-memory store of the newest record per key; safe for concurrent use.
type Replica struct {
	mu   sync.RWMutex
	data map[string]Record
}

// New returns an empty replica.
func New() *Replica { return &Replica{data: make(map[string]Record)} }

// Apply stores rec if its tag is newer than the stored one; repeats are no-ops.
func (r *Replica) Apply(rec Record) (bool, error) {
	if err := rec.Validate(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.data[rec.Key]; ok && !cur.Tag.Less(rec.Tag) {
		return false, nil
	}
	rec.Value = append([]byte(nil), rec.Value...)
	r.data[rec.Key] = rec
	return true, nil
}

// Get returns a copy of the stored record for key.
func (r *Replica) Get(key string) (Record, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.data[key]
	if !ok {
		return Record{}, false
	}
	rec.Value = append([]byte(nil), rec.Value...)
	return rec, true
}

// Len returns the number of keys stored, tombstones included.
func (r *Replica) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.data)
}

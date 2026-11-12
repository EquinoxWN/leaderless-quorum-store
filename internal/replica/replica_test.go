package replica

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func rec(key, val string, counter uint64, writer string) Record {
	return Record{Key: key, Value: []byte(val), Tag: Tag{Counter: counter, Writer: writer}}
}

func TestTagOrder(t *testing.T) {
	cases := []struct {
		a, b Tag
		less bool
	}{
		{Tag{1, "a"}, Tag{2, "a"}, true},
		{Tag{2, "a"}, Tag{1, "z"}, false},
		{Tag{2, "a"}, Tag{2, "b"}, true},
		{Tag{2, "b"}, Tag{2, "a"}, false},
		{Tag{2, "a"}, Tag{2, "a"}, false},
		{Tag{}, Tag{1, "a"}, true},
	}
	for _, c := range cases {
		if got := c.a.Less(c.b); got != c.less {
			t.Errorf("%v.Less(%v) = %v, want %v", c.a, c.b, got, c.less)
		}
	}
	if !(Tag{}).IsZero() || (Tag{Counter: 1}).IsZero() || (Tag{Writer: "a"}).IsZero() {
		t.Error("IsZero wrong")
	}
	if s := (Tag{3, "w"}).String(); s != "3@w" {
		t.Errorf("String = %q", s)
	}
}

func TestApplyKeepsNewestAndIsIdempotent(t *testing.T) {
	r := New()
	mustApply(t, r, rec("k", "v2", 2, "a"), true)
	mustApply(t, r, rec("k", "v2", 2, "a"), false) // retry of the same write
	mustApply(t, r, rec("k", "v1", 1, "z"), false) // older write arrives late
	mustApply(t, r, rec("k", "v2b", 2, "b"), true) // same counter, higher writer wins
	got, ok := r.Get("k")
	if !ok || string(got.Value) != "v2b" || got.Tag != (Tag{2, "b"}) {
		t.Fatalf("Get = %+v, %v", got, ok)
	}
	if r.Len() != 1 {
		t.Fatalf("Len = %d", r.Len())
	}
	if _, ok := r.Get("missing"); ok {
		t.Fatal("missing key found")
	}
}

func mustApply(t *testing.T, r *Replica, rc Record, want bool) {
	t.Helper()
	got, err := r.Apply(rc)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Apply(%v) applied = %v, want %v", rc.Tag, got, want)
	}
}

func TestApplyAndGetCopyValues(t *testing.T) {
	r := New()
	in := rec("k", "abc", 1, "a")
	if _, err := r.Apply(in); err != nil {
		t.Fatal(err)
	}
	in.Value[0] = 'X'
	out, _ := r.Get("k")
	out.Value[1] = 'Y'
	again, _ := r.Get("k")
	if string(again.Value) != "abc" {
		t.Fatalf("stored value changed through caller slices: %q", again.Value)
	}
}

func TestApplyRejectsInvalidRecords(t *testing.T) {
	bad := map[string]Record{
		"empty key":       rec("", "v", 1, "a"),
		"long key":        rec(strings.Repeat("k", MaxKeyBytes+1), "v", 1, "a"),
		"non-utf8 key":    rec("\xff", "v", 1, "a"),
		"big value":       {Key: "k", Value: make([]byte, MaxValueBytes+1), Tag: Tag{1, "a"}},
		"zero counter":    rec("k", "v", 0, "a"),
		"empty writer":    rec("k", "v", 1, ""),
		"long writer":     rec("k", "v", 1, strings.Repeat("w", MaxWriterBytes+1)),
		"tombstone value": {Key: "k", Value: []byte("v"), Tag: Tag{1, "a"}, Deleted: true},
	}
	r := New()
	for name, rc := range bad {
		if _, err := r.Apply(rc); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	if r.Len() != 0 {
		t.Fatalf("invalid records stored: %d", r.Len())
	}
	ok := map[string]Record{
		"max key":   rec(strings.Repeat("k", MaxKeyBytes), "v", 1, "a"),
		"max value": {Key: "k", Value: make([]byte, MaxValueBytes), Tag: Tag{1, "a"}},
		"tombstone": {Key: "t", Tag: Tag{1, "a"}, Deleted: true},
	}
	for name, rc := range ok {
		if _, err := r.Apply(rc); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestConcurrentAppliesConvergeOnHighestTag(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := uint64(1); c <= 200; c++ {
				if _, err := r.Apply(rec("k", "v", c, string(rune('a'+w)))); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	got, _ := r.Get("k")
	if got.Tag != (Tag{200, "h"}) {
		t.Fatalf("final tag %v, want 200@h", got.Tag)
	}
}

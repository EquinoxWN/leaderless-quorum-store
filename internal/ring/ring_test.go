package ring

import (
	"errors"
	"strconv"
	"testing"
)

func nodes(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "node" + strconv.Itoa(i)
	}
	return out
}

func TestNewRejectsBadConfig(t *testing.T) {
	cases := map[string]struct {
		nodes  []string
		vnodes int
	}{
		"no nodes":       {nil, 8},
		"empty id":       {[]string{"a", ""}, 8},
		"duplicate":      {[]string{"a", "b", "a"}, 8},
		"zero vnodes":    {[]string{"a"}, 0},
		"too many vnode": {[]string{"a"}, MaxVirtualNodes + 1},
	}
	for name, c := range cases {
		if _, err := New(c.nodes, c.vnodes); !errors.Is(err, ErrConfig) {
			t.Errorf("%s: err = %v, want ErrConfig", name, err)
		}
	}
}

func TestPreferenceIsDistinctAndDeterministic(t *testing.T) {
	r, err := New(nodes(5), 32)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := New([]string{"node4", "node3", "node2", "node1", "node0"}, 32)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		key := "key" + strconv.Itoa(i)
		p, err := r.Preference(key, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(p) != 3 || p[0] == p[1] || p[1] == p[2] || p[0] == p[2] {
			t.Fatalf("%s: preference %v is not 3 distinct nodes", key, p)
		}
		q, err := r2.Preference(key, 3)
		if err != nil {
			t.Fatal(err)
		}
		for j := range p {
			if p[j] != q[j] {
				t.Fatalf("%s: placement depends on node order: %v vs %v", key, p, q)
			}
		}
	}
}

func TestPreferenceBounds(t *testing.T) {
	r, err := New(nodes(3), 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, -1, 4} {
		if _, err := r.Preference("k", n); !errors.Is(err, ErrConfig) {
			t.Errorf("n=%d: err = %v, want ErrConfig", n, err)
		}
	}
	p, err := r.Preference("k", 3)
	if err != nil || len(p) != 3 {
		t.Fatalf("n = all nodes: %v, %v", p, err)
	}
}

func TestKeysSpreadEvenly(t *testing.T) {
	r, err := New(nodes(8), 128)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	const keys = 40000
	for i := 0; i < keys; i++ {
		p, err := r.Preference("user:"+strconv.Itoa(i), 1)
		if err != nil {
			t.Fatal(err)
		}
		counts[p[0]]++
	}
	mean := keys / 8
	for n, c := range counts {
		if c < mean*7/10 || c > mean*13/10 {
			t.Errorf("%s owns %d keys, want within 30%% of %d", n, c, mean)
		}
	}
	if len(counts) != 8 {
		t.Fatalf("only %d of 8 nodes own keys", len(counts))
	}
}

func TestAddingANodeMovesFewKeys(t *testing.T) {
	before, err := New(nodes(8), 128)
	if err != nil {
		t.Fatal(err)
	}
	after, err := New(nodes(9), 128)
	if err != nil {
		t.Fatal(err)
	}
	moved := 0
	const keys = 20000
	for i := 0; i < keys; i++ {
		k := "k" + strconv.Itoa(i)
		a, _ := before.Preference(k, 1)
		b, _ := after.Preference(k, 1)
		if a[0] != b[0] {
			if b[0] != "node8" {
				t.Fatalf("%s moved from %s to %s, not to the new node", k, a[0], b[0])
			}
			moved++
		}
	}
	// The new node should take about 1/9 of the keys, far from a full reshuffle.
	if moved < keys/18 || moved > keys*2/9 {
		t.Fatalf("%d of %d keys moved, want about %d", moved, keys, keys/9)
	}
}

func TestNodesReturnsCopy(t *testing.T) {
	r, err := New([]string{"a", "b"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	ns := r.Nodes()
	ns[0] = "z"
	if r.Nodes()[0] != "a" {
		t.Fatal("Nodes exposed internal slice")
	}
}

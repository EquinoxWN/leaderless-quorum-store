// Package ring places keys on nodes with a consistent-hash ring of virtual nodes.
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
)

// MaxVirtualNodes bounds the ring size so a bad flag cannot exhaust memory.
const MaxVirtualNodes = 1024

// ErrConfig reports an invalid ring or placement request.
var ErrConfig = errors.New("ring: invalid configuration")

type point struct {
	hash uint64
	node string
}

// Ring is an immutable consistent-hash ring; safe for concurrent use.
type Ring struct {
	points []point
	nodes  []string
}

// New builds a ring with vnodes points per node.
func New(nodes []string, vnodes int) (*Ring, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("%w: no nodes", ErrConfig)
	}
	if vnodes < 1 || vnodes > MaxVirtualNodes {
		return nil, fmt.Errorf("%w: vnodes %d not in 1..%d", ErrConfig, vnodes, MaxVirtualNodes)
	}
	seen := make(map[string]bool, len(nodes))
	r := &Ring{points: make([]point, 0, len(nodes)*vnodes)}
	for _, n := range nodes {
		if n == "" {
			return nil, fmt.Errorf("%w: empty node id", ErrConfig)
		}
		if seen[n] {
			return nil, fmt.Errorf("%w: duplicate node %q", ErrConfig, n)
		}
		seen[n] = true
		r.nodes = append(r.nodes, n)
		for i := 0; i < vnodes; i++ {
			r.points = append(r.points, point{hash: hash(n + "#" + strconv.Itoa(i)), node: n})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		return r.points[i].node < r.points[j].node
	})
	return r, nil
}

// Nodes returns a copy of the node ids in the order given to New.
func (r *Ring) Nodes() []string { return append([]string(nil), r.nodes...) }

// Preference returns the n distinct nodes that hold key, walking clockwise.
func (r *Ring) Preference(key string, n int) ([]string, error) {
	if n < 1 || n > len(r.nodes) {
		return nil, fmt.Errorf("%w: need %d replicas, ring has %d nodes", ErrConfig, n, len(r.nodes))
	}
	h := hash(key)
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	out := make([]string, 0, n)
	taken := make(map[string]bool, n)
	for i := 0; len(out) < n; i++ {
		p := r.points[(start+i)%len(r.points)]
		if !taken[p.node] {
			taken[p.node] = true
			out = append(out, p.node)
		}
	}
	return out, nil
}

// hash maps a string to a ring position.
func hash(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

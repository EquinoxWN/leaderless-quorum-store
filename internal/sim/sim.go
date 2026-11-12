// Package sim runs the real coordinator against injected faults to show the R/W trade-off.
package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/EquinoxWN/leaderless-quorum-store/internal/quorum"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/ring"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/transport"
)

// MaxTrials bounds one run.
const MaxTrials = 100_000

// Row is the measured outcome for one N/R/W setting.
type Row struct {
	N, R, W       int
	Overlap       bool    // R + W > N
	Trials        int     // stale-read trials run
	Stale         int     // reads that returned the older value
	Expected      float64 // C(N-W, R) / C(N, R): chance a random read set misses the write set
	WriteTolerate int     // most nodes down with Put still succeeding
	ReadTolerate  int     // most nodes down with Get still succeeding
}

// StaleRate is Stale / Trials.
func (r Row) StaleRate() float64 {
	if r.Trials == 0 {
		return 0
	}
	return float64(r.Stale) / float64(r.Trials)
}

// cluster is an in-process cluster of exactly N nodes, so every key lives on all of them.
type cluster struct {
	nodes []string
	tr    *transport.Local
	coord *quorum.Coordinator
}

// newCluster builds the nodes, ring, transport and coordinator for one setting.
func newCluster(n, r, w int) (*cluster, error) {
	nodes := make([]string, n)
	for i := range nodes {
		nodes[i] = "n" + strconv.Itoa(i+1)
	}
	rg, err := ring.New(nodes, 16)
	if err != nil {
		return nil, err
	}
	tr := transport.NewLocal(nodes)
	coord, err := quorum.New("sim", rg, tr, quorum.Config{N: n, R: r, W: w, Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	return &cluster{nodes: nodes, tr: tr, coord: coord}, nil
}

// Measure runs trials stale-read trials and the availability sweep for one setting.
//
// A trial writes "old" to every replica, then writes "new" while writes to all but a
// random W replicas are lost, then reads while all but a random R replicas are down.
func Measure(ctx context.Context, n, r, w, trials int, rng *rand.Rand) (Row, error) {
	if trials < 1 || trials > MaxTrials {
		return Row{}, fmt.Errorf("sim: trials %d not in 1..%d", trials, MaxTrials)
	}
	c, err := newCluster(n, r, w)
	if err != nil {
		return Row{}, err
	}
	row := Row{N: n, R: r, W: w, Overlap: c.coord.Config().Overlaps(), Trials: trials, Expected: missChance(n, r, w)}
	for i := 0; i < trials; i++ {
		key := "k" + strconv.Itoa(i)
		if _, err := c.coord.Put(ctx, key, []byte("old")); err != nil {
			return Row{}, fmt.Errorf("sim: trial %d: initial write: %w", i, err)
		}
		c.coord.Wait()
		for _, node := range pick(rng, c.nodes, n-w) {
			c.tr.SetDropWrites(node, true)
		}
		if _, err := c.coord.Put(ctx, key, []byte("new")); err != nil {
			return Row{}, fmt.Errorf("sim: trial %d: write with %d replicas reachable: %w", i, w, err)
		}
		c.coord.Wait()
		c.tr.Heal()
		for _, node := range pick(rng, c.nodes, n-r) {
			c.tr.SetDown(node, true)
		}
		res, err := c.coord.Get(ctx, key)
		c.coord.Wait()
		c.tr.Heal()
		if err != nil {
			return Row{}, fmt.Errorf("sim: trial %d: read with %d replicas up: %w", i, r, err)
		}
		switch {
		case bytes.Equal(res.Value, []byte("old")):
			row.Stale++
		case !bytes.Equal(res.Value, []byte("new")):
			return Row{}, fmt.Errorf("sim: trial %d: read returned %q", i, res.Value)
		}
	}
	row.WriteTolerate, row.ReadTolerate, err = tolerance(ctx, c, rng)
	return row, err
}

// tolerance finds the most nodes that can be down while writes, and reads, still succeed.
func tolerance(ctx context.Context, c *cluster, rng *rand.Rand) (int, int, error) {
	wt, rt := -1, -1
	for down := 0; down <= len(c.nodes); down++ {
		for _, node := range pick(rng, c.nodes, down) {
			c.tr.SetDown(node, true)
		}
		key := "avail" + strconv.Itoa(down)
		_, werr := c.coord.Put(ctx, key, []byte("v"))
		_, rerr := c.coord.Get(ctx, key)
		c.coord.Wait()
		c.tr.Heal()
		for _, err := range []error{werr, rerr} {
			if err != nil && !errors.Is(err, quorum.ErrQuorum) {
				return 0, 0, err
			}
		}
		if werr == nil {
			wt = down
		}
		if rerr == nil {
			rt = down
		}
	}
	return wt, rt, nil
}

// pick returns k distinct nodes chosen at random.
func pick(rng *rand.Rand, nodes []string, k int) []string {
	p := rng.Perm(len(nodes))
	out := make([]string, 0, k)
	for _, i := range p[:k] {
		out = append(out, nodes[i])
	}
	return out
}

// missChance is the probability that R random replicas avoid W random ones.
func missChance(n, r, w int) float64 {
	return binom(n-w, r) / binom(n, r)
}

// binom is n choose k as a float (0 when k > n).
func binom(n, k int) float64 {
	if k < 0 || k > n {
		return 0
	}
	out := 1.0
	for i := 1; i <= k; i++ {
		out = out * float64(n-k+i) / float64(i)
	}
	return out
}

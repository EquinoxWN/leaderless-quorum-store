package quorum

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EquinoxWN/leaderless-quorum-store/internal/replica"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/ring"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/transport"
)

type cluster struct {
	nodes []string
	ring  *ring.Ring
	tr    *transport.Local
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	nodes := make([]string, size)
	for i := range nodes {
		nodes[i] = "n" + strconv.Itoa(i+1)
	}
	rg, err := ring.New(nodes, 32)
	if err != nil {
		t.Fatal(err)
	}
	return &cluster{nodes: nodes, ring: rg, tr: transport.NewLocal(nodes)}
}

func (c *cluster) coord(t *testing.T, id string, n, r, w int) *Coordinator {
	t.Helper()
	co, err := New(id, c.ring, c.tr, Config{N: n, R: r, W: w, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(co.Wait)
	return co
}

// holders lists the nodes whose replica stores key.
func (c *cluster) holders(key string) []string {
	var out []string
	for _, n := range c.nodes {
		if _, ok := c.tr.Replica(n).Get(key); ok {
			out = append(out, n)
		}
	}
	return out
}

func TestConfigValidate(t *testing.T) {
	good := Config{N: 3, R: 2, W: 2, Timeout: time.Second}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := []Config{
		{N: 0, R: 1, W: 1, Timeout: time.Second},
		{N: MaxReplicas + 1, R: 1, W: 1, Timeout: time.Second},
		{N: 3, R: 0, W: 1, Timeout: time.Second},
		{N: 3, R: 4, W: 1, Timeout: time.Second},
		{N: 3, R: 1, W: 0, Timeout: time.Second},
		{N: 3, R: 1, W: 4, Timeout: time.Second},
		{N: 3, R: 1, W: 1},
		{N: 3, R: 1, W: 1, Timeout: time.Second, Retries: -1},
		{N: 3, R: 1, W: 1, Timeout: time.Second, Retries: MaxRetries + 1, Backoff: time.Millisecond},
		{N: 3, R: 1, W: 1, Timeout: time.Second, Retries: 1},
	}
	for _, c := range bad {
		if err := c.Validate(); !errors.Is(err, ErrConfig) {
			t.Errorf("%+v: err = %v, want ErrConfig", c, err)
		}
	}
}

func TestNewRejectsBadArguments(t *testing.T) {
	c := newCluster(t, 3)
	cfg := Config{N: 3, R: 2, W: 2, Timeout: time.Second}
	if _, err := New("", c.ring, c.tr, cfg); !errors.Is(err, ErrConfig) {
		t.Errorf("empty id: %v", err)
	}
	if _, err := New("a", nil, c.tr, cfg); !errors.Is(err, ErrConfig) {
		t.Errorf("nil ring: %v", err)
	}
	if _, err := New("a", c.ring, nil, cfg); !errors.Is(err, ErrConfig) {
		t.Errorf("nil transport: %v", err)
	}
	if _, err := New("a", c.ring, c.tr, Config{N: 4, R: 2, W: 2, Timeout: time.Second}); !errors.Is(err, ErrConfig) {
		t.Errorf("N above ring size: %v", err)
	}
}

// Every R-subset meets every W-subset exactly when R + W > N (checked exhaustively).
func TestOverlapIsExactlyRPlusWGreaterThanN(t *testing.T) {
	for n := 1; n <= 7; n++ {
		full := 1<<n - 1
		for r := 1; r <= n; r++ {
			for w := 1; w <= n; w++ {
				always := true
				for rs := 1; rs <= full && always; rs++ {
					if popcount(rs) != r {
						continue
					}
					for ws := 1; ws <= full; ws++ {
						if popcount(ws) == w && rs&ws == 0 {
							always = false
							break
						}
					}
				}
				cfg := Config{N: n, R: r, W: w}
				if cfg.Overlaps() != always {
					t.Fatalf("N=%d R=%d W=%d: Overlaps()=%v, exhaustive=%v", n, r, w, cfg.Overlaps(), always)
				}
			}
		}
	}
}

func popcount(x int) int {
	c := 0
	for ; x > 0; x &= x - 1 {
		c++
	}
	return c
}

func TestPutGetDelete(t *testing.T) {
	c := newCluster(t, 3)
	co := c.coord(t, "c1", 3, 2, 2)
	ctx := context.Background()
	if res, err := co.Get(ctx, "k"); err != nil || res.Found {
		t.Fatalf("empty get = %+v, %v", res, err)
	}
	t1, err := co.Put(ctx, "k", []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	t2, err := co.Put(ctx, "k", []byte("v2"))
	if err != nil {
		t.Fatal(err)
	}
	if !t1.Less(t2) || t2.Writer != "c1" {
		t.Fatalf("tags not increasing: %v then %v", t1, t2)
	}
	res, err := co.Get(ctx, "k")
	if err != nil || !res.Found || string(res.Value) != "v2" || res.Tag != t2 || res.Replies < 2 {
		t.Fatalf("get = %+v, %v", res, err)
	}
	if _, err := co.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if res, err := co.Get(ctx, "k"); err != nil || res.Found || res.Value != nil {
		t.Fatalf("get after delete = %+v, %v", res, err)
	}
}

func TestEachKeyIsStoredOnExactlyNReplicas(t *testing.T) {
	c := newCluster(t, 6)
	co := c.coord(t, "c1", 3, 1, 1)
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		key := "key" + strconv.Itoa(i)
		if _, err := co.Put(ctx, key, []byte("v")); err != nil {
			t.Fatal(err)
		}
		co.Wait() // writes past the first W acks finish in the background
		want, err := co.Replicas(key)
		if err != nil {
			t.Fatal(err)
		}
		got := c.holders(key)
		if len(got) != 3 || strings.Join(got, ",") != strings.Join(sorted(want), ",") {
			t.Fatalf("%s stored on %v, preference list %v", key, got, want)
		}
	}
}

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestWriteNeedsWAcksAndReadNeedsRReplies(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ r, w int }{{1, 1}, {1, 3}, {2, 2}, {3, 1}, {2, 3}} {
		c := newCluster(t, 3)
		co := c.coord(t, "c1", 3, tc.r, tc.w)
		for down := 0; down <= 3; down++ {
			for i := 0; i < 3; i++ {
				c.tr.SetDown(c.nodes[i], i < down)
			}
			key := "k" + strconv.Itoa(down)
			_, werr := co.Put(ctx, key, []byte("v"))
			_, rerr := co.Get(ctx, key)
			co.Wait()
			up := 3 - down
			if wantOK := up >= max(tc.r, tc.w); (werr == nil) != wantOK {
				t.Errorf("R=%d W=%d, %d up: put err = %v, want ok=%v", tc.r, tc.w, up, werr, wantOK)
			}
			if wantOK := up >= tc.r; (rerr == nil) != wantOK {
				t.Errorf("R=%d W=%d, %d up: get err = %v, want ok=%v", tc.r, tc.w, up, rerr, wantOK)
			}
			for _, err := range []error{werr, rerr} {
				if err != nil && (!errors.Is(err, ErrQuorum) || !errors.Is(err, transport.ErrUnreachable)) {
					t.Errorf("quorum error lost its cause: %v", err)
				}
			}
		}
	}
}

func TestWriteQuorumFailureReportsCounts(t *testing.T) {
	c := newCluster(t, 3)
	co := c.coord(t, "c1", 3, 1, 3)
	c.tr.SetDropWrites("n2", true)
	_, err := co.Put(context.Background(), "k", []byte("v"))
	var qe *Error
	if !errors.As(err, &qe) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if !strings.HasPrefix(qe.Op, "write") || qe.Need != 3 || qe.Got > 2 || qe.Replicas != 3 || len(qe.Errs) != 1 {
		t.Fatalf("error = %+v", qe)
	}
	if !strings.Contains(err.Error(), "needs 3 of 3") {
		t.Fatalf("message: %v", err)
	}
}

// With R + W <= N a read can miss the latest acknowledged write.
func TestStaleReadWhenQuorumsDoNotOverlap(t *testing.T) {
	c := newCluster(t, 3)
	co := c.coord(t, "c1", 3, 1, 1)
	ctx := context.Background()
	if _, err := co.Put(ctx, "k", []byte("old")); err != nil {
		t.Fatal(err)
	}
	co.Wait()
	c.tr.SetDropWrites("n2", true)
	c.tr.SetDropWrites("n3", true)
	if _, err := co.Put(ctx, "k", []byte("new")); err != nil {
		t.Fatalf("W=1 write with one replica: %v", err)
	}
	co.Wait()
	c.tr.Heal()
	c.tr.SetDown("n1", true)
	c.tr.SetDown("n2", true)
	res, err := co.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Value) != "old" {
		t.Fatalf("read %q, expected the stale value with R=1 W=1", res.Value)
	}
}

// With R + W > N no read misses an acknowledged write, under random lost writes and down nodes.
func TestNoStaleReadWhenQuorumsOverlap(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	ctx := context.Background()
	for _, tc := range []struct{ n, r, w int }{{3, 2, 2}, {3, 1, 3}, {3, 3, 1}, {5, 3, 3}, {5, 2, 4}, {5, 1, 5}} {
		c := newCluster(t, tc.n)
		co := c.coord(t, "c1", tc.n, tc.r, tc.w)
		for i := 0; i < 300; i++ {
			key := "k" + strconv.Itoa(i%7)
			want := "v" + strconv.Itoa(i)
			for _, j := range rng.Perm(tc.n)[:tc.n-tc.w] {
				c.tr.SetDropWrites(c.nodes[j], true)
			}
			if _, err := co.Put(ctx, key, []byte(want)); err != nil {
				t.Fatalf("N=%d R=%d W=%d: put: %v", tc.n, tc.r, tc.w, err)
			}
			co.Wait()
			c.tr.Heal()
			for _, j := range rng.Perm(tc.n)[:tc.n-tc.r] {
				c.tr.SetDown(c.nodes[j], true)
			}
			res, err := co.Get(ctx, key)
			co.Wait()
			c.tr.Heal()
			if err != nil {
				t.Fatalf("N=%d R=%d W=%d: get: %v", tc.n, tc.r, tc.w, err)
			}
			if string(res.Value) != want {
				t.Fatalf("N=%d R=%d W=%d: read %q after writing %q", tc.n, tc.r, tc.w, res.Value, want)
			}
		}
	}
}

// A write through one coordinator then another is ordered by the version query, not by clocks.
func TestLaterWriteFromAnotherCoordinatorWins(t *testing.T) {
	c := newCluster(t, 3)
	a := c.coord(t, "zzz", 3, 2, 2) // high writer id: would win ties
	b := c.coord(t, "aaa", 3, 2, 2)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := a.Put(ctx, "k", []byte("from a")); err != nil {
			t.Fatal(err)
		}
	}
	c.tr.SetDropWrites("n1", true) // b's write misses one replica
	if _, err := b.Put(ctx, "k", []byte("from b")); err != nil {
		t.Fatal(err)
	}
	b.Wait()
	c.tr.Heal()
	for _, reader := range []*Coordinator{a, b} {
		res, err := reader.Get(ctx, "k")
		if err != nil || string(res.Value) != "from b" {
			t.Fatalf("read %q (%v), want the later write", res.Value, err)
		}
	}
	tag, err := a.Put(ctx, "k", []byte("a again"))
	if err != nil {
		t.Fatal(err)
	}
	if res, _ := b.Get(ctx, "k"); string(res.Value) != "a again" || res.Tag != tag {
		t.Fatalf("read %q %v, want a's newest write %v", res.Value, res.Tag, tag)
	}
}

func TestGetReturnsNewestOfRReplies(t *testing.T) {
	c := newCluster(t, 3)
	co := c.coord(t, "c1", 3, 3, 1)
	ctx := context.Background()
	if _, err := co.Put(ctx, "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	co.Wait()
	c.tr.SetDropWrites("n1", true)
	c.tr.SetDropWrites("n3", true)
	if _, err := co.Put(ctx, "k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	co.Wait()
	c.tr.Heal()
	res, err := co.Get(ctx, "k")
	if err != nil || string(res.Value) != "v2" || res.Replies != 3 {
		t.Fatalf("get = %+v, %v", res, err)
	}
}

func TestGetDoesNotWaitForSlowReplicasPastR(t *testing.T) {
	c := newCluster(t, 3)
	co := c.coord(t, "c1", 3, 2, 3)
	ctx := context.Background()
	if _, err := co.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	c.tr.SetDelay("n3", 2*time.Second)
	start := time.Now()
	res, err := co.Get(ctx, "k")
	if err != nil || string(res.Value) != "v" || res.Replies != 2 {
		t.Fatalf("get = %+v, %v", res, err)
	}
	if d := time.Since(start); d > time.Second/2 {
		t.Fatalf("get took %v, waited for the slow replica", d)
	}
}

func TestPutReturnsAfterWAcksAndFinishesTheRest(t *testing.T) {
	c := newCluster(t, 3)
	co := c.coord(t, "c1", 3, 1, 2)
	c.tr.SetDelay("n3", 200*time.Millisecond)
	start := time.Now()
	if _, err := co.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 150*time.Millisecond {
		t.Fatalf("put took %v, waited for the slow replica", d)
	}
	co.Wait()
	if got := c.holders("k"); len(got) != 3 {
		t.Fatalf("after Wait the value is on %v, want all 3", got)
	}
}

func TestTimeoutAndCancellation(t *testing.T) {
	c := newCluster(t, 3)
	co, err := New("c1", c.ring, c.tr, Config{N: 3, R: 2, W: 2, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer co.Wait()
	for _, n := range c.nodes {
		c.tr.SetDelay(n, time.Hour)
	}
	start := time.Now()
	if _, err := co.Get(context.Background(), "k"); !errors.Is(err, ErrQuorum) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("get with slow replicas: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("timeout not enforced")
	}
	c.tr.Heal()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := co.Put(ctx, "k", []byte("v")); !errors.Is(err, ErrQuorum) {
		t.Fatalf("put with cancelled context: %v", err)
	}
}

// flaky fails the first calls to each node with a retryable error.
type flaky struct {
	transport.Transport
	fails int32
	calls atomic.Int32
	err   error
}

func (f *flaky) Read(ctx context.Context, node, key string) (replica.Record, bool, error) {
	if f.calls.Add(1) <= f.fails {
		return replica.Record{}, false, f.err
	}
	return f.Transport.Read(ctx, node, key)
}

func TestRetriesAreBoundedAndSkipPermanentErrors(t *testing.T) {
	c := newCluster(t, 1)
	mk := func(fails int32, err error, retries int) (*Coordinator, *flaky) {
		f := &flaky{Transport: c.tr, fails: fails, err: err}
		co, e := New("c1", c.ring, f, Config{N: 1, R: 1, W: 1, Timeout: time.Second, Retries: retries, Backoff: time.Millisecond})
		if e != nil {
			t.Fatal(e)
		}
		return co, f
	}
	ctx := context.Background()
	co, f := mk(2, transport.ErrUnreachable, 2)
	if _, err := co.Get(ctx, "k"); err != nil || f.calls.Load() != 3 {
		t.Fatalf("2 transient failures, 2 retries: err=%v calls=%d", err, f.calls.Load())
	}
	co, f = mk(3, transport.ErrUnreachable, 2)
	if _, err := co.Get(ctx, "k"); !errors.Is(err, ErrQuorum) || f.calls.Load() != 3 {
		t.Fatalf("3 transient failures, 2 retries: err=%v calls=%d", err, f.calls.Load())
	}
	co, f = mk(5, transport.ErrRejected, 3)
	if _, err := co.Get(ctx, "k"); !errors.Is(err, transport.ErrRejected) || f.calls.Load() != 1 {
		t.Fatalf("permanent error: err=%v calls=%d", err, f.calls.Load())
	}
}

func TestInvalidInputIsRefused(t *testing.T) {
	c := newCluster(t, 3)
	co := c.coord(t, "c1", 3, 2, 2)
	ctx := context.Background()
	if _, err := co.Put(ctx, "", []byte("v")); !errors.Is(err, replica.ErrInvalid) {
		t.Errorf("empty key: %v", err)
	}
	if _, err := co.Put(ctx, "k", make([]byte, replica.MaxValueBytes+1)); !errors.Is(err, replica.ErrInvalid) {
		t.Errorf("big value: %v", err)
	}
	if _, err := co.Get(ctx, strings.Repeat("k", replica.MaxKeyBytes+1)); !errors.Is(err, replica.ErrInvalid) {
		t.Errorf("long key: %v", err)
	}
	for _, n := range c.nodes {
		if c.tr.Replica(n).Len() != 0 {
			t.Fatal("invalid input reached a replica")
		}
	}
}

// Concurrent writers and readers through several coordinators; replicas converge.
func TestConcurrentClientsConverge(t *testing.T) {
	c := newCluster(t, 5)
	coords := []*Coordinator{c.coord(t, "a", 3, 2, 2), c.coord(t, "b", 3, 2, 2), c.coord(t, "c", 3, 2, 2)}
	ctx := context.Background()
	var wg sync.WaitGroup
	for i, co := range coords {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if _, err := co.Put(ctx, "k"+strconv.Itoa(j%5), []byte(strconv.Itoa(i))); err != nil {
					t.Error(err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if _, err := co.Get(ctx, "k"+strconv.Itoa(j%5)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for _, co := range coords {
		co.Wait()
	}
	for j := 0; j < 5; j++ {
		key := "k" + strconv.Itoa(j)
		nodes, _ := coords[0].Replicas(key)
		var first replica.Tag
		for i, n := range nodes {
			rec, ok := c.tr.Replica(n).Get(key)
			if !ok {
				t.Fatalf("%s missing on %s", key, n)
			}
			if i == 0 {
				first = rec.Tag
			} else if rec.Tag != first {
				t.Fatalf("%s: replicas disagree after all writes finished: %v vs %v", key, first, rec.Tag)
			}
		}
	}
}

// Once too many replicas have failed for the quorum, the call returns without waiting for the rest.
func TestFailsFastWhenQuorumIsImpossible(t *testing.T) {
	c := newCluster(t, 3)
	co := c.coord(t, "c1", 3, 3, 1)
	c.tr.SetDown("n1", true)
	c.tr.SetDelay("n2", time.Hour)
	start := time.Now()
	if _, err := co.Get(context.Background(), "k"); !errors.Is(err, ErrQuorum) {
		t.Fatalf("err = %v", err)
	}
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Fatalf("get took %v, waited after R=3 became impossible", d)
	}
}

// Writes past the first W acks still reach every replica if the caller cancels afterwards.
func TestBackgroundWritesOutliveCallerCancel(t *testing.T) {
	c := newCluster(t, 3)
	co := c.coord(t, "c1", 3, 1, 2)
	c.tr.SetDelay("n3", 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := co.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	cancel()
	co.Wait()
	if got := c.holders("k"); len(got) != 3 {
		t.Fatalf("value on %v after caller cancel, want all 3", got)
	}
}

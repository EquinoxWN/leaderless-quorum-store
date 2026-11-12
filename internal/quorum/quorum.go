// Package quorum coordinates leaderless reads and writes over N replicas with R and W quorums.
package quorum

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/EquinoxWN/leaderless-quorum-store/internal/replica"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/ring"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/transport"
)

// Bounds on the configuration.
const (
	MaxReplicas = 16
	MaxRetries  = 5
	maxBackoff  = time.Second
)

// ErrConfig reports an invalid configuration; ErrQuorum a quorum that was not reached.
var (
	ErrConfig = errors.New("quorum: invalid configuration")
	ErrQuorum = errors.New("quorum: not reached")
)

// Config sets the replication factor and quorum sizes.
type Config struct {
	N, R, W int
	Timeout time.Duration // whole operation, per phase
	Retries int           // extra attempts per replica call on retryable errors
	Backoff time.Duration // first retry delay, doubled each attempt
}

// Validate checks 1 <= R, W <= N <= MaxReplicas and positive timings.
func (c Config) Validate() error {
	switch {
	case c.N < 1 || c.N > MaxReplicas:
		return fmt.Errorf("%w: N=%d not in 1..%d", ErrConfig, c.N, MaxReplicas)
	case c.R < 1 || c.R > c.N:
		return fmt.Errorf("%w: R=%d not in 1..N (%d)", ErrConfig, c.R, c.N)
	case c.W < 1 || c.W > c.N:
		return fmt.Errorf("%w: W=%d not in 1..N (%d)", ErrConfig, c.W, c.N)
	case c.Timeout <= 0:
		return fmt.Errorf("%w: timeout must be positive", ErrConfig)
	case c.Retries < 0 || c.Retries > MaxRetries:
		return fmt.Errorf("%w: retries=%d not in 0..%d", ErrConfig, c.Retries, MaxRetries)
	case c.Retries > 0 && c.Backoff <= 0:
		return fmt.Errorf("%w: backoff must be positive when retrying", ErrConfig)
	}
	return nil
}

// Overlaps reports whether every read quorum shares a replica with every write quorum.
func (c Config) Overlaps() bool { return c.R+c.W > c.N }

// Error describes a failed quorum, keeping every replica error.
type Error struct {
	Op        string
	Need, Got int
	Replicas  int
	Errs      []error
}

// Error formats the counts and the replica errors.
func (e *Error) Error() string {
	return fmt.Sprintf("quorum: %s needs %d of %d replicas, got %d: %v", e.Op, e.Need, e.Replicas, e.Got, errors.Join(e.Errs...))
}

// Is makes errors.Is(err, ErrQuorum) true.
func (e *Error) Is(target error) bool { return target == ErrQuorum }

// Unwrap exposes the replica errors to errors.Is and errors.As.
func (e *Error) Unwrap() []error { return e.Errs }

// Result is the outcome of a read.
type Result struct {
	Value   []byte
	Tag     replica.Tag
	Found   bool
	Replies int
}

// Coordinator runs the quorum protocol; safe for concurrent use.
type Coordinator struct {
	id    string
	ring  *ring.Ring
	tr    transport.Transport
	cfg   Config
	mu    sync.Mutex
	clock uint64
	bg    sync.WaitGroup
}

// New returns a coordinator whose writes carry id as the tag's writer.
func New(id string, rg *ring.Ring, tr transport.Transport, cfg Config) (*Coordinator, error) {
	if err := replica.ValidateWriter(id); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	if rg == nil || tr == nil {
		return nil, fmt.Errorf("%w: ring and transport are required", ErrConfig)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if n := len(rg.Nodes()); cfg.N > n {
		return nil, fmt.Errorf("%w: N=%d but the ring has %d nodes", ErrConfig, cfg.N, n)
	}
	return &Coordinator{id: id, ring: rg, tr: tr, cfg: cfg}, nil
}

// Config returns the coordinator's configuration.
func (c *Coordinator) Config() Config { return c.cfg }

// Replicas returns the N nodes that hold key.
func (c *Coordinator) Replicas(key string) ([]string, error) {
	if err := replica.ValidateKey(key); err != nil {
		return nil, err
	}
	return c.ring.Preference(key, c.cfg.N)
}

// Wait blocks until every background replica call has finished.
func (c *Coordinator) Wait() { c.bg.Wait() }

type reply struct {
	node  string
	rec   replica.Record
	found bool
	err   error
}

// call runs one replica request with bounded retries and exponential backoff.
func (c *Coordinator) call(ctx context.Context, fn func(context.Context) (replica.Record, bool, error)) (replica.Record, bool, error) {
	delay := c.cfg.Backoff
	for attempt := 0; ; attempt++ {
		rec, found, err := fn(ctx)
		if err == nil || attempt >= c.cfg.Retries || errors.Is(err, transport.ErrRejected) || errors.Is(err, transport.ErrUnknownNode) {
			return rec, found, err
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return rec, found, fmt.Errorf("%w (gave up retrying: %w)", err, ctx.Err())
		case <-t.C:
		}
		delay = min(delay*2, maxBackoff)
	}
}

// fanout sends fn to every node and returns once need calls succeed or too many fail.
// Calls still running when it returns keep their own context; cancelLeft decides whether
// they are cancelled (reads) or allowed to finish (writes, so all N replicas get the value).
func (c *Coordinator) fanout(ctx context.Context, op string, nodes []string, need int, cancelLeft bool,
	fn func(context.Context, string) (replica.Record, bool, error)) ([]reply, error) {
	base := ctx
	if !cancelLeft {
		base = context.WithoutCancel(ctx)
	}
	callCtx, cancel := context.WithTimeout(base, c.cfg.Timeout)
	results := make(chan reply, len(nodes))
	var calls sync.WaitGroup
	for _, node := range nodes {
		calls.Add(1)
		c.bg.Add(1)
		go func() {
			defer c.bg.Done()
			defer calls.Done()
			rec, found, err := c.call(callCtx, func(cctx context.Context) (replica.Record, bool, error) { return fn(cctx, node) })
			results <- reply{node: node, rec: rec, found: found, err: err}
		}()
	}
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		calls.Wait()
		cancel()
	}()
	if cancelLeft {
		defer cancel()
	}
	var ok []reply
	var errs []error
	for range nodes {
		select {
		case r := <-results:
			if r.err != nil {
				errs = append(errs, r.err)
			} else {
				ok = append(ok, r)
			}
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			return nil, &Error{Op: op, Need: need, Got: len(ok), Replicas: len(nodes), Errs: errs}
		}
		if len(ok) >= need {
			return ok, nil
		}
		if len(errs) > len(nodes)-need {
			break
		}
	}
	return nil, &Error{Op: op, Need: need, Got: len(ok), Replicas: len(nodes), Errs: errs}
}

// newest returns the reply with the highest tag among those that hold a record.
func newest(rs []reply) (replica.Record, bool) {
	var best replica.Record
	found := false
	for _, r := range rs {
		if r.found && (!found || best.Tag.Less(r.rec.Tag)) {
			best, found = r.rec, true
		}
	}
	return best, found
}

// Get reads key from the replicas and returns the newest version among the first R replies.
func (c *Coordinator) Get(ctx context.Context, key string) (Result, error) {
	nodes, err := c.Replicas(key)
	if err != nil {
		return Result{}, err
	}
	rs, err := c.fanout(ctx, "read "+key, nodes, c.cfg.R, true, func(cctx context.Context, node string) (replica.Record, bool, error) {
		return c.tr.Read(cctx, node, key)
	})
	if err != nil {
		return Result{}, err
	}
	rec, found := newest(rs)
	if found {
		c.observe(rec.Tag.Counter)
	}
	return Result{Value: rec.Value, Tag: rec.Tag, Found: found && !rec.Deleted, Replies: len(rs)}, nil
}

// Put writes value under key; it returns the new tag once W replicas acknowledge.
func (c *Coordinator) Put(ctx context.Context, key string, value []byte) (replica.Tag, error) {
	if len(value) > replica.MaxValueBytes {
		return replica.Tag{}, fmt.Errorf("%w: value is %d bytes, limit %d", replica.ErrInvalid, len(value), replica.MaxValueBytes)
	}
	return c.write(ctx, key, value, false)
}

// Delete writes a tombstone for key once W replicas acknowledge.
func (c *Coordinator) Delete(ctx context.Context, key string) (replica.Tag, error) {
	return c.write(ctx, key, nil, true)
}

// write asks R replicas for the newest tag, then sends a higher one to all N and waits for W.
func (c *Coordinator) write(ctx context.Context, key string, value []byte, deleted bool) (replica.Tag, error) {
	nodes, err := c.Replicas(key)
	if err != nil {
		return replica.Tag{}, err
	}
	rs, err := c.fanout(ctx, "version query "+key, nodes, c.cfg.R, true, func(cctx context.Context, node string) (replica.Record, bool, error) {
		return c.tr.Read(cctx, node, key)
	})
	if err != nil {
		return replica.Tag{}, err
	}
	var seen uint64
	if rec, found := newest(rs); found {
		seen = rec.Tag.Counter
	}
	tag := replica.Tag{Counter: c.next(seen), Writer: c.id}
	rec := replica.Record{Key: key, Value: append([]byte(nil), value...), Tag: tag, Deleted: deleted}
	if err := rec.Validate(); err != nil {
		return replica.Tag{}, err
	}
	_, err = c.fanout(ctx, "write "+key, nodes, c.cfg.W, false, func(cctx context.Context, node string) (replica.Record, bool, error) {
		return replica.Record{}, false, c.tr.Write(cctx, node, rec)
	})
	if err != nil {
		return replica.Tag{}, err
	}
	return tag, nil
}

// next advances the logical clock past seen and returns the new counter.
func (c *Coordinator) next(seen uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clock = max(c.clock, seen) + 1
	return c.clock
}

// observe moves the logical clock forward to a counter seen in a read.
func (c *Coordinator) observe(seen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clock = max(c.clock, seen)
}

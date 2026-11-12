// Package transport carries reads and writes from a coordinator to replicas.
package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/EquinoxWN/leaderless-quorum-store/internal/replica"
)

// Errors a transport returns; ErrRejected is permanent, the others may be retried.
var (
	ErrUnreachable = errors.New("transport: replica unreachable")
	ErrRejected    = errors.New("transport: request rejected")
	ErrUnknownNode = errors.New("transport: unknown node")
)

// Transport sends one request to one replica; every call honours ctx.
type Transport interface {
	Read(ctx context.Context, node, key string) (replica.Record, bool, error)
	Write(ctx context.Context, node string, rec replica.Record) error
}

// Local is an in-process transport with fault injection; safe for concurrent use.
type Local struct {
	mu       sync.Mutex
	replicas map[string]*replica.Replica
	down     map[string]bool
	dropW    map[string]bool
	delay    map[string]time.Duration
}

// NewLocal creates one empty replica per node id.
func NewLocal(nodes []string) *Local {
	l := &Local{
		replicas: make(map[string]*replica.Replica, len(nodes)),
		down:     map[string]bool{},
		dropW:    map[string]bool{},
		delay:    map[string]time.Duration{},
	}
	for _, n := range nodes {
		l.replicas[n] = replica.New()
	}
	return l
}

// Replica returns the store behind node, for inspection in tests.
func (l *Local) Replica(node string) *replica.Replica {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.replicas[node]
}

// SetDown makes every call to node fail as unreachable.
func (l *Local) SetDown(node string, down bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.down[node] = down
}

// SetDropWrites makes writes to node fail while reads still work (a lost message).
func (l *Local) SetDropWrites(node string, drop bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dropW[node] = drop
}

// SetDelay holds every call to node for d before it is served.
func (l *Local) SetDelay(node string, d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.delay[node] = d
}

// Heal clears every injected fault.
func (l *Local) Heal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.down = map[string]bool{}
	l.dropW = map[string]bool{}
	l.delay = map[string]time.Duration{}
}

// enter applies the injected faults for one call and returns the target replica.
func (l *Local) enter(ctx context.Context, node string, write bool) (*replica.Replica, error) {
	l.mu.Lock()
	rep, ok := l.replicas[node]
	down, drop, d := l.down[node], l.dropW[node], l.delay[node]
	l.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownNode, node)
	}
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %s: %w", ErrUnreachable, node, ctx.Err())
		case <-t.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrUnreachable, node, err)
	}
	if down || (write && drop) {
		return nil, fmt.Errorf("%w: %s (injected fault)", ErrUnreachable, node)
	}
	return rep, nil
}

// Read fetches key from node.
func (l *Local) Read(ctx context.Context, node, key string) (replica.Record, bool, error) {
	rep, err := l.enter(ctx, node, false)
	if err != nil {
		return replica.Record{}, false, err
	}
	rec, ok := rep.Get(key)
	return rec, ok, nil
}

// Write stores rec on node.
func (l *Local) Write(ctx context.Context, node string, rec replica.Record) error {
	rep, err := l.enter(ctx, node, true)
	if err != nil {
		return err
	}
	if _, err := rep.Apply(rec); err != nil {
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	return nil
}

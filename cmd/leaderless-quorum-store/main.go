// Command leaderless-quorum-store runs replica nodes, quorum reads and writes, and the R/W trade-off check.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/EquinoxWN/leaderless-quorum-store/internal/quorum"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/replica"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/ring"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/sim"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/transport"
)

const usage = `usage:
  leaderless-quorum-store serve -id ID [-addr 127.0.0.1:7001]
  leaderless-quorum-store put    -nodes ID=URL,... [-n 3 -r 2 -w 2 -id cli -timeout 2s] KEY VALUE
  leaderless-quorum-store get    -nodes ID=URL,... [-n 3 -r 2 -w 2 -id cli -timeout 2s] KEY
  leaderless-quorum-store delete -nodes ID=URL,... [-n 3 -r 2 -w 2 -id cli -timeout 2s] KEY
  leaderless-quorum-store tradeoffs [-n 3 -trials 2000 -seed 1]
`

// main runs the CLI until it finishes or receives an interrupt.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run dispatches a subcommand and returns the exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "serve":
		err = serve(ctx, args[1:], stdout)
	case "put", "get", "delete":
		err = client(ctx, args[0], args[1:], stdout)
	case "tradeoffs":
		err = tradeoffs(ctx, args[1:], stdout)
	default:
		err = fmt.Errorf("%w: unknown command %q", errUsage, args[0])
	}
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		if errors.Is(err, errUsage) {
			fmt.Fprint(stderr, usage)
			return 2
		}
		return 1
	}
	return 0
}

var errUsage = errors.New("bad usage")

// newFlags returns a flag set that reports errors instead of exiting.
func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

// serve runs one replica node over HTTP until ctx is cancelled.
func serve(ctx context.Context, args []string, stdout io.Writer) error {
	fs := newFlags("serve")
	id := fs.String("id", "", "node id")
	addr := fs.String("addr", "127.0.0.1:7001", "listen address")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%w: unexpected arguments %v", errUsage, fs.Args())
	}
	if err := replica.ValidateWriter(*id); err != nil {
		return fmt.Errorf("%w: -id: %w", errUsage, err)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           transport.NewHandler(replica.New()),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	fmt.Fprintf(stdout, "node %s serving on http://%s\n", *id, ln.Addr())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	fmt.Fprintf(stdout, "node %s stopped\n", *id)
	return nil
}

// parseNodes reads "a=http://host:port,b=..." into ids and addresses.
func parseNodes(s string) ([]string, map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil, fmt.Errorf("%w: -nodes is required", errUsage)
	}
	var ids []string
	addrs := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, nil, fmt.Errorf("%w: -nodes entry %q is not ID=URL", errUsage, part)
		}
		if _, dup := addrs[id]; dup {
			return nil, nil, fmt.Errorf("%w: -nodes lists %q twice", errUsage, id)
		}
		ids = append(ids, id)
		addrs[id] = addr
	}
	return ids, addrs, nil
}

// client runs one put, get or delete against HTTP replicas.
func client(ctx context.Context, cmd string, args []string, stdout io.Writer) error {
	fs := newFlags(cmd)
	nodesFlag := fs.String("nodes", "", "comma-separated ID=URL list")
	n := fs.Int("n", 3, "replicas per key")
	r := fs.Int("r", 2, "read quorum")
	w := fs.Int("w", 2, "write quorum")
	id := fs.String("id", "cli", "coordinator id written into version tags")
	timeout := fs.Duration("timeout", 2*time.Second, "time limit per phase")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	want := 1
	if cmd == "put" {
		want = 2
	}
	if fs.NArg() != want {
		return fmt.Errorf("%w: %s takes %d argument(s), got %d", errUsage, cmd, want, fs.NArg())
	}
	ids, addrs, err := parseNodes(*nodesFlag)
	if err != nil {
		return err
	}
	tr, err := transport.NewHTTP(addrs, *timeout)
	if err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	rg, err := ring.New(ids, 64)
	if err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	cfg := quorum.Config{N: *n, R: *r, W: *w, Timeout: *timeout, Retries: 1, Backoff: 50 * time.Millisecond}
	coord, err := quorum.New(*id, rg, tr, cfg)
	if err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	defer coord.Wait()
	key := fs.Arg(0)
	nodes, err := coord.Replicas(key)
	if err != nil {
		return err
	}
	setting := fmt.Sprintf("N=%d R=%d W=%d, replicas %s", *n, *r, *w, strings.Join(nodes, ","))
	switch cmd {
	case "put", "delete":
		var tag replica.Tag
		if cmd == "put" {
			tag, err = coord.Put(ctx, key, []byte(fs.Arg(1)))
		} else {
			tag, err = coord.Delete(ctx, key)
		}
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s %q ok: tag %s, at least %d acks (%s)\n", cmd, key, tag, *w, setting)
	case "get":
		res, err := coord.Get(ctx, key)
		if err != nil {
			return err
		}
		if !res.Found {
			fmt.Fprintf(stdout, "get %q: not found, %d replies (%s)\n", key, res.Replies, setting)
			return nil
		}
		fmt.Fprintf(stdout, "get %q = %q: tag %s, %d replies (%s)\n", key, res.Value, res.Tag, res.Replies, setting)
	}
	return nil
}

// tradeoffs prints stale reads and fault tolerance for every R and W at one N.
func tradeoffs(ctx context.Context, args []string, stdout io.Writer) error {
	fs := newFlags("tradeoffs")
	n := fs.Int("n", 3, "replicas")
	trials := fs.Int("trials", 2000, "stale-read trials per setting")
	seed := fs.Uint64("seed", 1, "random seed")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%w: unexpected arguments %v", errUsage, fs.Args())
	}
	if *n < 1 || *n > 7 {
		return fmt.Errorf("%w: -n must be 1..7", errUsage)
	}
	if *trials < 1 || *trials > sim.MaxTrials {
		return fmt.Errorf("%w: -trials must be 1..%d", errUsage, sim.MaxTrials)
	}
	rng := rand.New(rand.NewPCG(*seed, *seed))
	fmt.Fprintf(stdout, "N=%d, %d trials per setting, seed %d\n", *n, *trials, *seed)
	fmt.Fprintln(stdout, "| N | R | W | R+W>N | stale reads | measured | expected | writes survive down | reads survive down |")
	fmt.Fprintln(stdout, "|---|---|---|---|---|---|---|---|---|")
	for r := 1; r <= *n; r++ {
		for w := 1; w <= *n; w++ {
			row, err := sim.Measure(ctx, *n, r, w, *trials, rng)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "| %d | %d | %d | %s | %d/%d | %.3f | %.3f | %d | %d |\n",
				row.N, row.R, row.W, yesNo(row.Overlap), row.Stale, row.Trials, row.StaleRate(), row.Expected, row.WriteTolerate, row.ReadTolerate)
		}
	}
	return nil
}

// yesNo formats a bool for the table.
func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

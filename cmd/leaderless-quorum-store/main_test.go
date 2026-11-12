package main

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/EquinoxWN/leaderless-quorum-store/internal/replica"
	"github.com/EquinoxWN/leaderless-quorum-store/internal/transport"
)

func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(context.Background(), args, &out, &errb)
	return code, out.String(), errb.String()
}

// End to end over real HTTP: three replica servers, quorum writes and reads, nodes failing.
func TestClusterOverHTTP(t *testing.T) {
	var servers []*httptest.Server
	var parts []string
	for _, id := range []string{"a", "b", "c"} {
		s := httptest.NewServer(transport.NewHandler(replica.New()))
		t.Cleanup(s.Close)
		servers = append(servers, s)
		parts = append(parts, id+"="+s.URL)
	}
	nodes := strings.Join(parts, ",")
	code, out, errs := runCLI(t, "put", "-nodes", nodes, "user:42", "alice")
	if code != 0 || !strings.Contains(out, `put "user:42" ok: tag 1@cli`) {
		t.Fatalf("put: %d %q %q", code, out, errs)
	}
	code, out, _ = runCLI(t, "get", "-nodes", nodes, "user:42")
	if code != 0 || !strings.Contains(out, `= "alice"`) {
		t.Fatalf("get: %d %q", code, out)
	}
	servers[0].Close() // one node down: N=3, R=2, W=2 still works
	code, out, errs = runCLI(t, "put", "-nodes", nodes, "-timeout", "500ms", "user:42", "bob")
	if code != 0 || !strings.Contains(out, "tag 2@cli") {
		t.Fatalf("put with one node down: %d %q %q", code, out, errs)
	}
	code, out, _ = runCLI(t, "get", "-nodes", nodes, "-timeout", "500ms", "user:42")
	if code != 0 || !strings.Contains(out, `= "bob"`) {
		t.Fatalf("get with one node down: %d %q", code, out)
	}
	servers[1].Close() // two down: W=2 fails, W=1 R=1 succeeds
	code, _, errs = runCLI(t, "put", "-nodes", nodes, "-timeout", "500ms", "user:42", "carol")
	if code != 1 || !strings.Contains(errs, "needs 2 of 3 replicas") {
		t.Fatalf("put with two down: %d %q", code, errs)
	}
	code, out, errs = runCLI(t, "put", "-nodes", nodes, "-timeout", "500ms", "-r", "1", "-w", "1", "user:42", "dave")
	if code != 0 {
		t.Fatalf("W=1 put with two down: %d %q %q", code, out, errs)
	}
	code, out, _ = runCLI(t, "delete", "-nodes", nodes, "-timeout", "500ms", "-r", "1", "-w", "1", "user:42")
	if code != 0 || !strings.Contains(out, "delete") {
		t.Fatalf("delete: %d %q", code, out)
	}
	code, out, _ = runCLI(t, "get", "-nodes", nodes, "-timeout", "500ms", "-r", "1", "user:42")
	if code != 0 || !strings.Contains(out, "not found") {
		t.Fatalf("get after delete: %d %q", code, out)
	}
}

func TestRunRejectsBadArguments(t *testing.T) {
	bad := [][]string{
		{},
		{"nope"},
		{"put", "-nodes", "a=http://127.0.0.1:1", "onlykey"},
		{"get", "-nodes", "a=http://127.0.0.1:1"},
		{"get", "k"},
		{"get", "-nodes", "a", "k"},
		{"get", "-nodes", "a=http://h:1,a=http://h:2", "k"},
		{"get", "-nodes", "a=127.0.0.1:1", "k"},
		{"get", "-nodes", "a=http://h:1", "-n", "2", "k"},
		{"get", "-nodes", "a=http://h:1,b=http://h:2,c=http://h:3", "-r", "4", "k"},
		{"get", "-bogus", "k"},
		{"serve"},
		{"serve", "-id", "a", "extra"},
		{"tradeoffs", "-n", "0"},
		{"tradeoffs", "-n", "8"},
		{"tradeoffs", "-trials", "0"},
		{"tradeoffs", "x"},
	}
	for _, args := range bad {
		if code, _, errs := runCLI(t, args...); code != 2 || !strings.Contains(errs, "usage:") {
			t.Errorf("%v: exit %d, stderr %q", args, code, errs)
		}
	}
	if code, _, errs := runCLI(t, "get", "-nodes", "a=http://127.0.0.1:1", "-n", "1", "-r", "1", "-w", "1", "-timeout", "200ms", "k"); code != 1 || !strings.Contains(errs, "quorum") {
		t.Errorf("unreachable node: exit %d, %q", code, errs)
	}
	if code, _, errs := runCLI(t, "get", "-nodes", "a=http://127.0.0.1:1", "-n", "1", "-r", "1", "-w", "1", ""); code != 1 || !strings.Contains(errs, "key") {
		t.Errorf("empty key: exit %d, %q", code, errs)
	}
}

func TestTradeoffsPrintsEverySetting(t *testing.T) {
	code, out, errs := runCLI(t, "tradeoffs", "-n", "2", "-trials", "50", "-seed", "3")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errs)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3+4 {
		t.Fatalf("want header plus 4 rows, got:\n%s", out)
	}
	if !strings.Contains(out, "| 2 | 1 | 1 | no |") || !strings.Contains(out, "| 2 | 2 | 1 | yes | 0/50 |") {
		t.Fatalf("unexpected table:\n%s", out)
	}
}

func TestServeStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errb bytes.Buffer
	if code := run(ctx, []string{"serve", "-id", "a", "-addr", "127.0.0.1:0"}, &out, &errb); code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if !strings.Contains(out.String(), "node a serving on http://127.0.0.1:") || !strings.Contains(out.String(), "node a stopped") {
		t.Fatalf("output %q", out.String())
	}
	if code := run(context.Background(), []string{"serve", "-id", "a", "-addr", "256.0.0.1:99999"}, &out, &errb); code != 1 {
		t.Fatalf("bad address: exit %d", code)
	}
}

package sim

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"
)

func TestBinomAndMissChance(t *testing.T) {
	cases := []struct {
		n, k int
		want float64
	}{{5, 2, 10}, {3, 0, 1}, {3, 3, 1}, {2, 3, 0}, {3, -1, 0}, {7, 3, 35}}
	for _, c := range cases {
		if got := binom(c.n, c.k); got != c.want {
			t.Errorf("binom(%d,%d) = %v, want %v", c.n, c.k, got, c.want)
		}
	}
	if got := missChance(3, 1, 1); math.Abs(got-2.0/3) > 1e-12 {
		t.Errorf("missChance(3,1,1) = %v", got)
	}
	if got := missChance(5, 2, 2); math.Abs(got-0.3) > 1e-12 {
		t.Errorf("missChance(5,2,2) = %v", got)
	}
	if got := missChance(5, 1, 2); math.Abs(got-0.6) > 1e-12 {
		t.Errorf("missChance(5,1,2) = %v, want 0.6", got)
	}
	if got := missChance(3, 2, 2); got != 0 {
		t.Errorf("missChance(3,2,2) = %v, want 0", got)
	}
}

func TestMeasureMatchesTheory(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 1))
	ctx := context.Background()
	for _, c := range []struct{ n, r, w int }{{3, 1, 1}, {3, 1, 2}, {3, 2, 2}, {5, 2, 2}, {5, 3, 3}, {5, 1, 5}} {
		row, err := Measure(ctx, c.n, c.r, c.w, 600, rng)
		if err != nil {
			t.Fatal(err)
		}
		if row.Overlap != (c.r+c.w > c.n) {
			t.Fatalf("%+v: overlap flag wrong", row)
		}
		if row.Overlap && row.Stale != 0 {
			t.Fatalf("N=%d R=%d W=%d overlaps but %d stale reads", c.n, c.r, c.w, row.Stale)
		}
		// 600 trials: 4 standard deviations of a binomial proportion is at most 0.082.
		if math.Abs(row.StaleRate()-row.Expected) > 0.082 {
			t.Fatalf("N=%d R=%d W=%d: stale rate %.3f, theory %.3f", c.n, c.r, c.w, row.StaleRate(), row.Expected)
		}
		if !row.Overlap && row.Stale == 0 {
			t.Fatalf("N=%d R=%d W=%d: no stale reads seen", c.n, c.r, c.w)
		}
		if row.WriteTolerate != c.n-max(c.r, c.w) || row.ReadTolerate != c.n-c.r {
			t.Fatalf("N=%d R=%d W=%d: tolerates %d down for writes, %d for reads", c.n, c.r, c.w, row.WriteTolerate, row.ReadTolerate)
		}
	}
}

func TestMeasureRejectsBadInput(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 1))
	ctx := context.Background()
	for _, trials := range []int{0, -1, MaxTrials + 1} {
		if _, err := Measure(ctx, 3, 2, 2, trials, rng); err == nil {
			t.Errorf("trials=%d accepted", trials)
		}
	}
	if _, err := Measure(ctx, 3, 4, 2, 10, rng); err == nil {
		t.Error("R > N accepted")
	}
	if (Row{}).StaleRate() != 0 {
		t.Error("empty row rate")
	}
}

func TestMeasureStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Measure(ctx, 3, 2, 2, 10, rand.New(rand.NewPCG(1, 1))); err == nil {
		t.Fatal("cancelled context ignored")
	}
}

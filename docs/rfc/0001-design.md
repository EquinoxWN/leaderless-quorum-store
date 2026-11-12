# RFC 0001: leaderless-quorum-store design

- **Status:** Accepted for M1 (M2 and M3 sections are plans)
- **Author:** EquinoxWN
- **Created:** 2026-11-11

## Problem

A key-value store with a single leader per key stops accepting writes for that key while the
leader is down or unreachable. Dynamo-style stores (Amazon Dynamo, Cassandra, Riak) avoid this:
every key lives on N replicas, any node can coordinate a request, a write needs W
acknowledgements and a read needs R replies. The price is that consistency depends on N, R and W,
and the trade-off is easy to state but rarely shown with running code and measured numbers.

Users: engineers learning or tuning leaderless replication (Cassandra consistency levels, Riak
`r`/`w`, DynamoDB-like designs) who want to see what a given N/R/W does before trusting it.

## Goals (Define: critical-to-quality measures)

| CTQ | Target (M1) | How it is measured |
|---|---|---|
| Placement | every key on exactly N distinct replicas | `TestEachKeyIsStoredOnExactlyNReplicas` (200 keys, 6 nodes, N=3) |
| Quorum rule | write returns after W acks, read after R replies; fails fast when impossible | `TestWriteNeedsWAcksAndReadNeedsRReplies`, `TestFailsFastWhenQuorumIsImpossible` |
| Overlap | 0 stale reads for every R + W > N setting | `tradeoffs` at N=3 and N=5, 2,000 trials per setting; CI gate |
| Trade-off | stale-read rate for R + W <= N within 4 standard deviations of C(N-W,R)/C(N,R) | `TestMeasureMatchesTheory` |
| Robustness | every replica call has a timeout and bounded retries; no error dropped | quality doc, rule 1 |

Backlog M1 items: write this RFC; store each key on N replicas with W-ack writes and R-reply reads;
show that R + W > N makes read and write sets overlap while lower values trade consistency for
availability.

## Non-goals

- Running as a hosted production service; authentication or TLS between nodes.
- Durable storage: M1 replicas keep data in memory and lose it on restart.
- Hinted handoff and read repair (M2), Merkle-tree anti-entropy and network partition tests with
  charts (M3).
- Linearizability. M1 reads do not write back, so two reads may disagree while a write is in flight.
- Membership changes at runtime: the node list is fixed when the coordinator starts.

## Design

```
client ── put/get ──> coordinator (any process; library or CLI)
                         │ ring.Preference(key, N) -> N distinct nodes
                         │ fan out to all N, wait for the first R (read) or W (write) successes
                         ▼
              transport.Transport ── Local (in-process, fault injection)
                                  └─ HTTP (JSON, per-request timeout) ──> replica node: serve
```

- **Placement (`internal/ring`).** A consistent-hash ring with virtual nodes (SHA-256 of
  `node#i`). The preference list for a key is the first N distinct nodes clockwise from the key's
  hash. Adding a node moves only the keys it now owns (about 1/(n+1)).
- **Versions (`internal/replica`).** Each record carries a tag (counter, writer id). Higher counter
  wins; equal counters are ordered by writer id, so every replica picks the same winner. A replica
  stores a record only when its tag is newer, so repeated or late writes are no-ops (idempotent).
  Deletes are tombstones.
- **Writes (`internal/quorum`).** Two phases, as in the multi-writer ABD register: (1) ask the N
  replicas for their newest tag and wait for R replies; (2) send the value with tag
  (max counter seen + 1, coordinator id) to all N and return after W acknowledgements. The other
  replicas keep receiving the write in the background, bounded by the timeout. The version comes
  from the replicas, not from a wall clock (ADR 0002).
- **Reads.** Ask the N replicas, return the newest record among the first R replies, cancel the rest.
- **Failure handling.** Each phase has a timeout; each replica call is retried at most `Retries`
  times with doubling backoff, only for retryable errors (unreachable, 5xx); a 4xx rejection is
  never retried. As soon as fewer than the needed replicas can still answer, the call returns a
  quorum error that keeps every replica error.
- **Transport.** `Local` runs replicas in process with injectable faults (node down, writes lost,
  delay). `HTTP` talks JSON to `serve` nodes: `GET`/`PUT /v1/kv/{key}`, body size limits, unknown
  fields rejected, server read/write/idle timeouts.

Why R + W > N gives overlap: a write set has W of the N replicas and a read set R of them. If
R + W > N, the two sets cannot be disjoint (pigeonhole), so at least one reply carries the latest
acknowledged tag, and the coordinator returns the highest tag. If R + W <= N, a read set can miss
the write set entirely; with random sets the chance is C(N-W, R) / C(N, R).

Availability with this design: reads need R live replicas; writes need max(R, W) live replicas,
because the version query uses the read quorum. Lowering W alone therefore does not make writes
more available unless R is lowered too (measured in `tradeoffs`).

## Alternatives considered

| Option | Why not (for M1) |
|---|---|
| Wall-clock last-write-wins (Cassandra's default) | One round trip per write, but ordering depends on clock sync; a node with a fast clock silently wins over later writes. The portfolio rules forbid ordering events by wall clock. |
| Vector clocks with siblings (Dynamo, Riak) | Keeps concurrent writes instead of choosing one, but needs client-side merge and sibling pruning. Planned as a later option once read repair (M2) exists. |
| Leader per key (Raft or primary-backup) | Linearizable, but writes stop while the leader is down, which is the problem this project studies. |
| Sloppy quorum (write to any N healthy nodes) | Higher write availability, but needs hinted handoff to move data back; that is the M2 item, so M1 uses strict quorums on the preference list. |

## Measurement plan (Measure, Analyze, Control)

- `leaderless-quorum-store tradeoffs -n N -trials T -seed S` runs the real coordinator against
  in-process replicas. Each trial writes `old` to all replicas, writes `new` while writes to all but
  a random W replicas are lost, then reads while all but a random R replicas are down. It reports
  stale reads, the theoretical rate, and the most nodes that can be down while writes and reads
  still succeed.
- Results are in `docs/results/m1.md`. Control: CI runs `make ci`, which includes the overlap gate
  (`scripts/overlap_gate.sh` fails if any R + W > N row has a stale read), the three-process HTTP
  demo and `make audit`.
- M3 replaces the injected faults with network partitions between processes and publishes charts.

## Milestones

- **M1:** This RFC. Each key is stored on N replicas; a write succeeds after W acknowledgements and a
  read after R responses. R + W > N makes read and write sets overlap; lower values trade
  consistency for availability.
- **M2:** Hinted handoff for down replicas; read repair of stale replicas.
- **M3:** Merkle-tree anti-entropy; partition tests measuring stale-read rate and availability for
  several N/R/W settings, with charts.

## Risks and open questions

- Concurrent writes from two coordinators with the same counter are resolved by writer id, so one
  of them is lost without notice. Vector clocks would surface it; M1 documents it.
- A write that fails its quorum may still have reached some replicas and become visible later. This
  is inherent to leaderless writes without rollback; callers must treat a failed write as "unknown".
- Data is in memory, so a restarted replica comes back empty until M2/M3 repair exists.
- The version query adds one round trip per write and couples write availability to R.

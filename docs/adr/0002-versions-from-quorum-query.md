# ADR 0002: Version tags from a quorum query, not from wall clocks

- **Status:** Accepted (M1)

## Context

Replicas must agree on which of two writes to a key is newer. Cassandra uses a client or
coordinator wall-clock timestamp (last write wins); Dynamo and Riak use vector clocks and keep
concurrent versions as siblings. A wall clock orders writes wrongly when clocks drift, and the
portfolio's quality rules do not allow ordering events by wall clock. Vector clocks need client
merges, which M1 does not have.

## Decision

A write first asks the key's N replicas for their newest tag and waits for R replies (the read
quorum). It then writes with tag (highest counter seen + 1, coordinator id). The coordinator also
keeps a logical clock so its own tags never go backwards. Replicas keep the record with the highest
tag; equal counters are ordered by coordinator id. This is the multi-writer ABD register's write
protocol with configurable R and W.

## Consequences

- With R + W > N the query sees the latest acknowledged write, so a later write always gets a
  higher tag, whichever coordinator or process sends it, with no clock synchronisation. Tested by
  `TestLaterWriteFromAnotherCoordinatorWins` and the CLI demo (each CLI call is a new process).
- Each write costs one extra round trip (measured only in-process for M1).
- Writes need max(R, W) live replicas, not W. The `tradeoffs` table shows this: with N=3, R=2,
  W=1, writes survive only 1 node down.
- Two writes that overlap in time can get the same counter; the one with the higher coordinator id
  wins and the other is lost silently. Vector clocks or siblings would expose it; that stays on the
  roadmap.
- With R + W <= N the query can miss the newest tag, so a later write may get a lower tag and be
  ignored. This is the same consistency loss as stale reads and is measured with them.

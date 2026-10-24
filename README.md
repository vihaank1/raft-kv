# raft-kv

An implementation of the **Raft consensus algorithm** (leader election + log replication) with a replicated key-value state machine, plus a **deterministic fault-injection simulator** that checks Raft's safety properties continuously. Written from the Raft paper (Ongaro & Ousterhout, 2014) in pure Python with no dependencies.

The point of the project is less "I wrote Raft" and more **"I can show it is correct under failure"**: every run is reproducible from a seed, safety invariants are asserted throughout, and the test suite includes deliberately broken Raft variants that the checkers must catch.

## What is implemented

- Leader election with randomized timeouts, term handling, and the log up-to-date voting rule
- Log replication with consistency checks, conflict truncation, and fast backtracking hints
- Commit rule (majority replication, current-term entries only) with a leader no-op so earlier-term entries commit safely
- Crash/restart (volatile state lost, persistent state kept), network partitions, message drop, delay/reordering, and duplication
- A replicated KV store (`put`) built on the committed log

## Invariants checked

| Property | How it is checked |
|---|---|
| Election Safety: at most one leader per term | Recorded on every leader election |
| Log Matching: same index + term implies identical prefix | Pairwise check every 10 ticks and at the end of each run |
| State Machine Safety: no two nodes apply different entries at one index | Checked on every apply across all nodes |
| Liveness after healing (a fresh write commits on all nodes) | Separate test after crashes and partitions end |

## Results (reproduce with the commands below)

- **500 chaos seeds x 2,000 ticks, 5 nodes: 0 safety violations** (40,989 committed log indices across runs)
- Also run at **3 and 7 nodes (200 seeds x 1,500 ticks each): 0 violations**, and liveness-after-heal passed 30/30 at every size
- **50/50 liveness-after-heal seeds** commit a fresh write on every node
- **3/3 injected bugs caught**: double voting (breaks Election Safety), a too-small commit quorum, and not truncating conflicting log entries (both break State Machine Safety). Each was detected within the first few seeds.
- 83 tests; the chaos suite runs in about a second because time is simulated, not real.

```bash
python -m pytest -q
PYTHONPATH=src python -m raftkv.fuzz --seeds 500 --ticks 2000
```

## Layout

- `src/raftkv/raft.py` is the protocol, a pure state machine driven by `tick` and `handle`
- `src/raftkv/sim.py` is the simulated network and cluster plus invariant checkers
- `src/raftkv/fuzz.py` is chaos and liveness runners with a CLI
- `tests/buggy.py` holds the intentionally broken variants used to test the tests

## Honest limitations

- **Simulation only.** There are no real sockets, threads, or wall-clock timing, and "stable storage" is in-memory, not written to disk. A real deployment would need fsync-before-reply persistence.
- **Not implemented:** snapshots / log compaction, cluster membership changes, client sessions for exactly-once semantics, and read-only (linearizable) reads. `get` is not implemented; only replicated `put`.
- **Testing is evidence, not proof.** This is randomized testing against invariants, not formal verification (no TLA+ or model checking). Log Matching is sampled every 10 ticks, and the committed test suite fuzzes 5-node clusters (3 and 7 nodes were checked in ad hoc runs).
- The fault model does not include disk corruption or Byzantine behavior.

## Where to take it next

Add disk-backed persistence with crash-at-any-write testing, log compaction with snapshots, linearizable reads, a Jepsen-style linearizability checker over client histories, and a real network transport.

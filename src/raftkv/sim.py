"""Deterministic cluster simulator with fault injection and invariant checking.

Faults: message drop, delay/reordering, duplication, network partitions, node crash/restart.
Every run is reproducible from (seed, config). Safety invariants are checked continuously.
"""
import random
from dataclasses import dataclass

from .raft import LEADER, Node


class InvariantViolation(AssertionError):
    pass


@dataclass
class NetConfig:
    drop: float = 0.05
    dup: float = 0.02
    min_delay: int = 1
    max_delay: int = 6


class Cluster:
    def __init__(self, n: int = 5, seed: int = 0, net: NetConfig | None = None, node_cls=Node):
        self.n, self.now = n, 0
        self.rng = random.Random(seed)
        self.net = net or NetConfig()
        self.leaders_by_term: dict[int, int] = {}
        self.applied: dict[int, object] = {}     # log index -> entry first applied there (by any node)
        self.committed_count = 0
        self.inflight: list[tuple[int, int, dict]] = []   # (deliver_at, seq, msg)
        self._seq = 0
        self.up = set(range(n))
        self.groups: list[set] | None = None
        ids = list(range(n))
        self.nodes = [node_cls(i, ids, random.Random(self.rng.random()),
                               on_apply=self._on_apply, on_leader=self._on_leader) for i in ids]

    # ---------- invariant hooks ----------
    def _on_leader(self, nid: int, term: int):
        prev = self.leaders_by_term.setdefault(term, nid)
        if prev != nid:
            raise InvariantViolation(f"Election Safety: nodes {prev} and {nid} both leaders in term {term}")

    def _on_apply(self, nid: int, index: int, entry):
        seen = self.applied.setdefault(index, entry)
        if seen != entry:
            raise InvariantViolation(
                f"State Machine Safety: index {index} applied {seen} on one node and {entry} on node {nid}")
        self.committed_count = max(self.committed_count, index)

    def check_log_matching(self):
        for a in range(self.n):
            for b in range(a + 1, self.n):
                la, lb = self.nodes[a].log, self.nodes[b].log
                hi = min(len(la), len(lb)) - 1
                match = max((i for i in range(hi, 0, -1) if la[i].term == lb[i].term), default=0)
                if la[:match + 1] != lb[:match + 1]:
                    raise InvariantViolation(f"Log Matching: nodes {a},{b} diverge at/below index {match}")

    # ---------- network ----------
    def _send(self, msgs: list[dict]):
        for m in msgs:
            copies = 2 if self.rng.random() < self.net.dup else 1
            for _ in range(copies):
                if self.rng.random() < self.net.drop:
                    continue
                self._seq += 1
                at = self.now + self.rng.randint(self.net.min_delay, self.net.max_delay)
                self.inflight.append((at, self._seq, m))

    def _reachable(self, a: int, b: int) -> bool:
        if self.groups is None:
            return True
        return any(a in g and b in g for g in self.groups)

    def partition(self, groups: list[set]):
        self.groups = groups

    def heal(self):
        self.groups = None

    def crash(self, nid: int):
        self.up.discard(nid)

    def restart(self, nid: int):
        if nid not in self.up:
            self.up.add(nid)
            self.nodes[nid].crash_restart(self.now)

    # ---------- time ----------
    def step(self):
        self.now += 1
        due = sorted(x for x in self.inflight if x[0] <= self.now)
        self.inflight = [x for x in self.inflight if x[0] > self.now]
        for _, _, m in due:
            if m["dst"] in self.up and m["src"] in self.up and self._reachable(m["src"], m["dst"]):
                self._send(self.nodes[m["dst"]].handle(m, self.now))
        for i in sorted(self.up):
            self._send(self.nodes[i].tick(self.now))

    def run(self, ticks: int, check_every: int = 10):
        for t in range(ticks):
            self.step()
            if t % check_every == 0:
                self.check_log_matching()

    # ---------- client ----------
    def leader(self) -> int | None:
        cands = [i for i in self.up if self.nodes[i].state == LEADER]
        return max(cands, key=lambda i: self.nodes[i].current_term) if cands else None

    def propose(self, command) -> tuple[int, int] | None:
        """Submit to the current leader; returns (leader_id, index) or None."""
        lid = self.leader()
        if lid is None:
            return None
        idx = self.nodes[lid].propose(command)
        return (lid, idx) if idx is not None else None

    def put_and_wait(self, key, value, max_ticks: int = 400) -> bool:
        """Propose a put and run until a majority has applied it. Returns success."""
        for _ in range(max_ticks):
            r = self.propose(("put", key, value))
            if r:
                lid, idx = r
                for _ in range(max_ticks):
                    self.step()
                    n = sum(1 for i in self.up if self.nodes[i].last_applied >= idx)
                    if n * 2 > self.n and self.nodes[lid].log[idx].command == ("put", key, value):
                        return True
                    if self.nodes[lid].state != LEADER:
                        break
                continue
            self.step()
        return False

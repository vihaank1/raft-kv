"""Raft consensus (leader election + log replication), written against the Raft paper (Ongaro & Ousterhout, 2014).

Nodes are pure state machines: `tick(now)` and `handle(msg, now)` return messages to send.
There are no threads, sockets, or wall-clock time; the simulator in `sim.py` drives everything,
which makes every run reproducible from a seed.

Persistent state (current_term, voted_for, log) survives `crash()` in memory to model stable
storage; it is NOT written to disk (see README limitations).
"""
from dataclasses import dataclass

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


@dataclass(frozen=True)
class Entry:
    term: int
    command: object  # None marks the no-op a new leader appends


class Node:
    HEARTBEAT = 5
    ELECTION_MIN, ELECTION_MAX = 25, 50

    def __init__(self, nid: int, peers: list[int], rng, on_apply=None, on_leader=None):
        self.id, self.peers, self.rng = nid, [p for p in peers if p != nid], rng
        self.on_apply, self.on_leader = on_apply, on_leader
        # persistent
        self.current_term = 0
        self.voted_for = None
        self.log = [Entry(0, None)]  # 1-indexed; index 0 is a sentinel
        self._reset_volatile(0)

    # ---------- lifecycle ----------
    def _reset_volatile(self, now: int):
        self.state = FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.kv: dict = {}
        self.next_index: dict = {}
        self.match_index: dict = {}
        self.votes: set = set()
        self.next_heartbeat = 0
        self._reset_election_timer(now)

    def crash_restart(self, now: int):
        """Lose all volatile state; keep persistent state (term, vote, log)."""
        self._reset_volatile(now)

    def _reset_election_timer(self, now: int):
        self.election_deadline = now + self.rng.randint(self.ELECTION_MIN, self.ELECTION_MAX)

    # ---------- helpers ----------
    @property
    def last_index(self) -> int:
        return len(self.log) - 1

    @property
    def last_term(self) -> int:
        return self.log[-1].term

    def _quorum(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def _become_follower(self, term: int):
        if term > self.current_term:
            self.current_term, self.voted_for = term, None
        self.state = FOLLOWER
        self.votes = set()

    def _msg(self, dst: int, type_: str, **kw) -> dict:
        return {"type": type_, "src": self.id, "dst": dst, "term": self.current_term, **kw}

    # ---------- time ----------
    def tick(self, now: int) -> list[dict]:
        if self.state == LEADER:
            if now >= self.next_heartbeat:
                return self._broadcast_append(now)
            return []
        if now >= self.election_deadline:
            return self._start_election(now)
        return []

    def _start_election(self, now: int) -> list[dict]:
        self.current_term += 1
        self.state, self.voted_for = CANDIDATE, self.id
        self.votes = {self.id}
        self._reset_election_timer(now)
        out = [self._msg(p, "RequestVote", last_index=self.last_index, last_term=self.last_term)
               for p in self.peers]
        if len(self.votes) >= self._quorum():  # single-node cluster
            out += self._become_leader(now)
        return out

    def _become_leader(self, now: int) -> list[dict]:
        self.state = LEADER
        self.next_index = {p: self.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self.log.append(Entry(self.current_term, None))  # no-op lets us commit prior-term entries
        if self.on_leader:
            self.on_leader(self.id, self.current_term)
        self._advance_commit()
        return self._broadcast_append(now)

    def _broadcast_append(self, now: int) -> list[dict]:
        self.next_heartbeat = now + self.HEARTBEAT
        return [self._append_for(p) for p in self.peers]

    def _append_for(self, p: int) -> dict:
        prev = self.next_index[p] - 1
        return self._msg(p, "AppendEntries", prev_index=prev, prev_term=self.log[prev].term,
                         entries=self.log[prev + 1:], leader_commit=self.commit_index)

    # ---------- client ----------
    def propose(self, command) -> int | None:
        """Append a command if this node believes it is leader. Returns its log index."""
        if self.state != LEADER:
            return None
        self.log.append(Entry(self.current_term, command))
        self._advance_commit()
        return self.last_index

    # ---------- messages ----------
    def handle(self, m: dict, now: int) -> list[dict]:
        if m["term"] > self.current_term:
            self._become_follower(m["term"])
        handler = {"RequestVote": self._on_request_vote, "RequestVoteReply": self._on_vote_reply,
                   "AppendEntries": self._on_append, "AppendEntriesReply": self._on_append_reply}
        return handler[m["type"]](m, now)

    def _on_request_vote(self, m, now):
        grant = False
        if m["term"] >= self.current_term and self.voted_for in (None, m["src"]):
            up_to_date = (m["last_term"], m["last_index"]) >= (self.last_term, self.last_index)
            if up_to_date:
                grant, self.voted_for = True, m["src"]
                self._reset_election_timer(now)
        return [self._msg(m["src"], "RequestVoteReply", granted=grant)]

    def _on_vote_reply(self, m, now):
        if self.state != CANDIDATE or m["term"] != self.current_term or not m["granted"]:
            return []
        self.votes.add(m["src"])
        if len(self.votes) >= self._quorum():
            return self._become_leader(now)
        return []

    def _on_append(self, m, now):
        if m["term"] < self.current_term:
            return [self._msg(m["src"], "AppendEntriesReply", success=False, match=0,
                              hint=self.last_index)]
        if self.state != FOLLOWER:
            self._become_follower(m["term"])
        self._reset_election_timer(now)
        prev = m["prev_index"]
        if prev > self.last_index or self.log[prev].term != m["prev_term"]:
            return [self._msg(m["src"], "AppendEntriesReply", success=False, match=0,
                              hint=min(self.last_index, max(prev - 1, 0)))]
        for i, e in enumerate(m["entries"], start=prev + 1):
            if i <= self.last_index:
                if self.log[i].term != e.term:
                    del self.log[i:]       # conflict: drop it and everything after
                    self.log.append(e)
                # else identical entry already present: keep (never truncate on a match)
            else:
                self.log.append(e)
        last_new = prev + len(m["entries"])
        if m["leader_commit"] > self.commit_index:
            self.commit_index = min(m["leader_commit"], last_new)
            self._apply()
        return [self._msg(m["src"], "AppendEntriesReply", success=True, match=last_new, hint=0)]

    def _on_append_reply(self, m, now):
        if self.state != LEADER or m["term"] != self.current_term:
            return []
        p = m["src"]
        if m["success"]:
            self.match_index[p] = max(self.match_index[p], m["match"])
            self.next_index[p] = self.match_index[p] + 1
            self._advance_commit()
            return []
        self.next_index[p] = max(1, min(self.next_index[p] - 1, m["hint"] + 1))
        return [self._append_for(p)]

    # ---------- commit / apply ----------
    def _advance_commit(self):
        for n in range(self.last_index, self.commit_index, -1):
            if self.log[n].term != self.current_term:
                break  # only commit entries from the current term by counting replicas
            replicas = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if replicas >= self._quorum():
                self.commit_index = n
                break
        self._apply()

    def _apply(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            cmd = self.log[self.last_applied].command
            if cmd is not None:
                op, key, *rest = cmd
                if op == "put":
                    self.kv[key] = rest[0]
            if self.on_apply:
                self.on_apply(self.id, self.last_applied, self.log[self.last_applied])

"""Deliberately broken Raft variants. The fuzzer must catch each one, which shows the checkers have teeth."""
from raftkv.raft import Entry, Node


class DoubleVoteNode(Node):
    """Bug: grants a vote even if it already voted for someone else this term."""
    def _on_request_vote(self, m, now):
        grant = False
        if m["term"] >= self.current_term:
            if (m["last_term"], m["last_index"]) >= (self.last_term, self.last_index):
                grant, self.voted_for = True, m["src"]
                self._reset_election_timer(now)
        return [self._msg(m["src"], "RequestVoteReply", granted=grant)]


class TinyQuorumNode(Node):
    """Bug: leader commits once just 2 nodes (itself + one follower) have the entry, regardless of cluster size."""
    def _advance_commit(self):
        for n in range(self.last_index, self.commit_index, -1):
            if self.log[n].term != self.current_term:
                break
            replicas = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if replicas >= 2:
                self.commit_index = n
                break
        self._apply()


class NoTruncateNode(Node):
    """Bug: on a log conflict, keeps the stale entries and appends after them instead of truncating."""
    def _on_append(self, m, now):
        if m["term"] < self.current_term:
            return super()._on_append(m, now)
        if self.state != "follower":
            self._become_follower(m["term"])
        self._reset_election_timer(now)
        prev = m["prev_index"]
        if prev > self.last_index or self.log[prev].term != m["prev_term"]:
            return [self._msg(m["src"], "AppendEntriesReply", success=False, match=0,
                              hint=min(self.last_index, max(prev - 1, 0)))]
        for i, e in enumerate(m["entries"], start=prev + 1):
            if i > self.last_index or self.log[i].term != e.term:
                self.log.append(e)  # BUG: never truncates the conflicting suffix
        last_new = prev + len(m["entries"])
        if m["leader_commit"] > self.commit_index:
            self.commit_index = min(m["leader_commit"], last_new)
            self._apply()
        return [self._msg(m["src"], "AppendEntriesReply", success=True, match=last_new, hint=0)]

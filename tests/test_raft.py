import pytest
from buggy import DoubleVoteNode, NoTruncateNode, TinyQuorumNode

from raftkv.fuzz import chaos_run, liveness_after_heal
from raftkv.raft import LEADER
from raftkv.sim import Cluster, InvariantViolation, NetConfig


def test_elects_exactly_one_leader_without_faults():
    c = Cluster(5, seed=1, net=NetConfig(drop=0, dup=0))
    c.run(300)
    leaders = [n for n in c.nodes if n.state == LEADER]
    assert len(leaders) == 1


def test_replicates_writes_to_all_nodes():
    c = Cluster(5, seed=2, net=NetConfig(drop=0, dup=0))
    c.run(200)
    assert c.put_and_wait("a", 1) and c.put_and_wait("b", 2)
    c.run(100)
    assert all(n.kv == {"a": 1, "b": 2} for n in c.nodes)


def test_committed_data_survives_leader_crash():
    c = Cluster(5, seed=3, net=NetConfig(drop=0, dup=0))
    c.run(200)
    assert c.put_and_wait("x", 42)
    old = c.leader()
    c.crash(old)
    c.run(300)
    new = c.leader()
    assert new is not None and new != old
    assert c.put_and_wait("y", 7)
    c.run(100)
    for i in c.up:
        assert c.nodes[i].kv.get("x") == 42 and c.nodes[i].kv.get("y") == 7


def test_minority_partition_cannot_commit_but_majority_can():
    c = Cluster(5, seed=4, net=NetConfig(drop=0, dup=0))
    c.run(200)
    lid = c.leader()
    minority = {lid, (lid + 1) % 5}
    c.partition([minority, set(range(5)) - minority])
    idx = c.nodes[lid].propose(("put", "lost", 1))
    c.run(300)
    assert c.nodes[lid].commit_index < idx or c.nodes[lid].log[idx].command != ("put", "lost", 1) \
        or all(c.nodes[i].last_applied < idx for i in minority)
    assert all(c.nodes[i].kv.get("lost") is None for i in minority)
    # majority side elects its own leader and commits
    maj_leader = max((i for i in range(5) if i not in minority and c.nodes[i].state == LEADER),
                     key=lambda i: c.nodes[i].current_term, default=None)
    assert maj_leader is not None
    assert c.nodes[maj_leader].propose(("put", "kept", 2)) is not None
    c.run(200)
    c.heal()
    c.run(400)
    assert all(n.kv.get("kept") == 2 for n in c.nodes)
    assert all(n.kv.get("lost") is None for n in c.nodes)  # uncommitted minority write was discarded


@pytest.mark.parametrize("seed", range(60))
def test_chaos_safety(seed):
    chaos_run(seed, ticks=1200)  # raises InvariantViolation on any safety failure


@pytest.mark.parametrize("seed", range(15))
def test_liveness_after_faults_heal(seed):
    assert liveness_after_heal(seed)


@pytest.mark.parametrize("cls", [DoubleVoteNode, TinyQuorumNode, NoTruncateNode])
def test_fuzzer_catches_injected_bugs(cls):
    """The checkers must have teeth: each broken variant is caught within a bounded number of seeds."""
    for seed in range(60):
        try:
            chaos_run(seed, ticks=1500, node_cls=cls)
        except InvariantViolation:
            return
    pytest.fail(f"{cls.__name__} was not detected in 60 seeds")


def test_runs_are_deterministic():
    assert chaos_run(11, 800) == chaos_run(11, 800)

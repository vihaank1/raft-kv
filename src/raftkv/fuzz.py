"""Chaos fuzzing: random crashes, restarts, partitions, and client writes, checking safety throughout."""
import argparse
import random

from .sim import Cluster, InvariantViolation, NetConfig


def chaos_run(seed: int, ticks: int = 1500, n: int = 5, node_cls=None, verbose=False) -> dict:
    kwargs = {"node_cls": node_cls} if node_cls else {}
    c = Cluster(n=n, seed=seed, net=NetConfig(), **kwargs)
    r = random.Random(seed * 7919 + 1)
    writes = 0
    for t in range(ticks):
        x = r.random()
        if x < 0.004:
            c.crash(r.randrange(n))
        elif x < 0.012:
            c.restart(r.randrange(n))
        elif x < 0.016:
            ids = list(range(n)); r.shuffle(ids); k = r.randint(1, n - 1)
            c.partition([set(ids[:k]), set(ids[k:])])
        elif x < 0.024:
            c.heal()
        if r.random() < 0.05 and c.propose(("put", f"k{r.randint(0, 9)}", t)):
            writes += 1
        c.step()
        if t % 10 == 0:
            c.check_log_matching()
    c.check_log_matching()
    return {"seed": seed, "terms": len(c.leaders_by_term), "committed": c.committed_count, "writes": writes}


def liveness_after_heal(seed: int, n: int = 5) -> bool:
    """After chaos, heal everything and require the cluster to commit a fresh write on every node."""
    c = Cluster(n=n, seed=seed)
    r = random.Random(seed)
    for t in range(600):
        if r.random() < 0.01:
            c.crash(r.randrange(n))
        if r.random() < 0.02:
            c.restart(r.randrange(n))
        if t % 100 == 50:
            ids = list(range(n)); r.shuffle(ids); c.partition([set(ids[:2]), set(ids[2:])])
        c.step()
    c.heal()
    for i in range(n):
        c.restart(i)
    c.run(300)
    ok = c.put_and_wait("final", seed, max_ticks=300)
    c.run(200)
    return ok and all(node.kv.get("final") == seed for node in c.nodes)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=100)
    ap.add_argument("--ticks", type=int, default=1500)
    a = ap.parse_args()
    ok = bad = 0
    total_commits = 0
    for s in range(a.seeds):
        try:
            res = chaos_run(s, a.ticks)
            total_commits += res["committed"]
            ok += 1
        except InvariantViolation as e:
            bad += 1
            print(f"seed {s}: VIOLATION {e}")
    live = sum(liveness_after_heal(s) for s in range(min(a.seeds, 50)))
    print(f"chaos: {ok}/{a.seeds} seeds clean, {bad} violations, {total_commits} total committed indices")
    print(f"liveness after heal: {live}/{min(a.seeds, 50)} seeds")


if __name__ == "__main__":
    main()

import sys
import pathlib
import collections
from typing import Set, Tuple, Dict, List, Iterator, IO

from cephlib.sensors_rpc_plugin import CephOp


def find_closest_pars(pairs: Set[Tuple[str, str]]) -> Set[Tuple[str, str]]:
    # find pairs, which never breaks by other event (possible direct deps)
    close_pairs = set()  # type: Set[Tuple[str, str]]

    # need all events, that appears on first and second positions, as only such events can be in the middle of any other
    e1s, e2s = zip(*pairs)
    all_events = set(e1s).union(e2s)

    for e1, e2 in pairs:
        # for each pair iterate over all other event and check that pairs with this new env inside
        # are missing in allowed_pairs
        for e3 in all_events:
            if (e1, e3) in pairs and (e3, e2) in pairs:
                break
        else:
            close_pairs.add((e1, e2))

    return close_pairs


ALL_STAGES = ['queued_for_pg', 'reached_pg', 'started', 'op_commit', 'sub_op_commit_rec', 'op_applied',
              'commit_sent', 'done']

prev_op = {'commit_sent': 'sub_op_commit_rec',
           'done': 'commit_sent',
           'op_applied': 'op_commit',
           'op_commit': 'started',
           'reached_pg': 'queued_for_pg',
           'started': 'reached_pg',
           'sub_op_commit_rec': 'started'}


def detect_order(pairs: Set[Tuple[str, str]]) -> Iterator[Tuple[str, List[str]]]:
    next_evts = {e1 for e1, _ in pairs}.difference({e2 for _, e2 in pairs})
    processed: Set[str] = set()
    while next_evts:
        new_next = set()
        for evt in next_evts:
            if evt not in processed:
                next_to_evt = [evt2 for evt1, evt2 in pairs if evt1 == evt]
                yield evt, next_to_evt
                new_next.update(next_to_evt)
                processed.add(evt)
        next_evts = new_next


def get_events_for_op(op_events: List[Dict[str, str]]) -> List[str]:
    name_map = {"waiting for rw locks": "waiting_for_rw_locks"}
    names = []  # type: List[str]
    for step in op_events:
        ename = step['event']
        if ename.startswith("waiting for subops from"):
            id_first, id_second = ename[len("waiting for subops from "):].split(",")
            name_map = {
                "sub_op_applied_rec from {}".format(id_first): "sub_op_applied_rec_first",
                "sub_op_applied_rec from {}".format(id_second): "sub_op_applied_rec_second",
                "sub_op_commit_rec from {}".format(id_first): "sub_op_commit_rec_first",
                "sub_op_commit_rec from {}".format(id_second): "sub_op_commit_rec_second"
            }
            ename = "waiting_for_subops"
        else:
            ename = name_map.get(ename, ename)
        names.append(ename)

    return names


def get_uniq_evt_sets_counts(evts: List[Tuple[str, ...]]) -> Dict[Tuple[str, ...], int]:
    uniq: Dict[Tuple[str, ...], int] = collections.defaultdict(int)
    for names in evts:
        uniq[tuple(sorted(names))] += 1
    return uniq


def find_deps(ops: List[CephOp]):
    # j depend on i,  i always appears before j
    dependents = {(i, j) for i in CephOp.all_stages for j in CephOp.all_stages if i != j and 'initiated' not in (i, j)}
    all_found_stages: Set[str] = set()

    for op in ops:
        prev_evts: List[str] = []
        for curr_evt, _ in op.iter_events():
            for prev_evt in prev_evts:
                try:
                    # prev_evt appears before curr_evt, so prev_evt can't depend on curr_evt
                    dependents.remove((curr_evt, prev_evt))
                except KeyError:
                    pass
            prev_evts.append(curr_evt)
            all_found_stages.add(curr_evt)

    filtered_deps = set()
    for i, j in dependents:
        if i in all_found_stages and j in all_found_stages:
            filtered_deps.add((i, j))

    all_deps: Dict[str, List] = {op: [] for op in all_found_stages}
    for i, j in filtered_deps:
        all_deps[i].append(j)

    closest_pairs = list(find_closest_pars(filtered_deps))
    prev_stage = dict((j, i) for (i, j) in closest_pairs)

    # import pprint
    # pprint.pprint(prev_stage)
    # pprint.pprint(all_found_stages)

    closest_pairs.sort(key=lambda x: -len(all_deps[x[0]]))
    for p1, p2 in closest_pairs:
        print("{} => {}".format(p1, p2))


def iter_ceph_ops(fd: IO):
    data = fd.read()
    offset = 0
    while offset < len(data):
        op, offset = CephOp.unpack(data, offset)
        yield op


def calc_stages_time(op: CephOp) -> Dict[str, int]:
    all_ops = dict(op.iter_events())
    res = {}
    for name, vl in all_ops.items():
        if vl != 0:
            curr = name
            dtime = vl
            while curr in prev_op:
                if prev_op[curr] in all_ops:
                    dtime = vl - all_ops[prev_op[curr]]
                    break
                curr = prev_op[curr]
            assert dtime >= 0, all_ops
            res[name] = dtime
    return res


def main():
    all_ops = []
    for name in pathlib.Path(sys.argv[1]).glob("perf_monitoring/*/ceph.osd*.historic.bin"):
        all_ops.extend(iter_ceph_ops(name.open("rb")))

    # find_deps(all_ops)
    stimes = {}
    for op in all_ops:
        for name, vl in calc_stages_time(op).items():
            stimes.setdefault(name, []).append(vl)


    for name, vals in sorted(stimes.items()):
        print(f"{name:20s} {sum(vals) // len(vals):>10d} {len(vals):>10d}")

    import seaborn
    from matplotlib import pyplot

    names, vals = zip(*stimes.items())
    ax = seaborn.boxplot(data=vals)
    ax.set_xticklabels(names, size=14, rotation=20)
    ax.set_yscale("log")
    pyplot.show()

    return 0


if __name__ == "__main__":
    exit(main())
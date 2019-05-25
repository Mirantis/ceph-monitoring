from __future__ import annotations

import sys
import math
import logging
import argparse
from typing import Iterator, Tuple, Iterable, List, Dict, Any

import numpy
import pandas
import matplotlib
from matplotlib import pyplot

from cephlib import OpType
from koder_utils import seconds_to_str, SimpleTable, renter_to_text as renter_to_text_base, Align

from . import setup_logging, get_file
from .hdf5util import load_hdf

matplotlib.rcParams.update({'font.size': 30, 'lines.linewidth': 5})


logger = logging.getLogger("report")


def renter_to_text(t: SimpleTableWC) -> str:
    text = renter_to_text_base(t)
    return t.caption.center(len(text[0])) + "\n" + text

# def analyze_pgs(pg1_path: str, pg2_path: str):
#     pg_dump1 = json.load(open(pg1_path))
#     pg_dump2 = json.load(open(pg2_path))
#
#     stats1 = {pg["pgid"]: pg for pg in pg_dump1['pg_stats']}
#     wdiff = []
#     rdiff = []
#     wdiff_osd = Counter()
#     rdiff_osd = Counter()
#     for pg2 in pg_dump2['pg_stats']:
#         if pg2['pgid'].split(".")[0] in ("117", "115"):
#             wd = pg2["stat_sum"]["num_write"] - stats1[pg2['pgid']]["stat_sum"]["num_write"]
#             wdiff.append(wd)
#             for osd_id in pg2["up"]:
#                 wdiff_osd[osd_id] += wd
#
#             rd = pg2["stat_sum"]["num_read"] - stats1[pg2['pgid']]["stat_sum"]["num_read"]
#             rdiff.append(rd)
#             rdiff_osd[pg2["up_primary"]] += rd
#
#     wdiff.sort()
#     p5, p50, p95 = numpy.percentile(wdiff, [5, 50, 95])
#     print("Per PG writes:")
#     print(f"  average   {int(numpy.average(wdiff)):>10d}")
#     print(f"      min   {wdiff[0]:>10d}")
#     print(f"    5perc   {int(p5):>10d}")
#     print(f"   50perc   {int(p50):>10d}")
#     print(f"   95perc   {int(p95):>10d}")
#     print(f"      max   {wdiff[-1]:>10d}")
#
#     rdiff.sort()
#     p5, p50, p95 = numpy.percentile(rdiff, [5, 50, 95])
#     print("\nPer PG reads:")
#     print(f"  average   {int(numpy.average(rdiff)):>10d}")
#     print(f"      min   {rdiff[0]:>10d}")
#     print(f"    5perc   {int(p5):>10d}")
#     print(f"   50perc   {int(p50):>10d}")
#     print(f"   95perc   {int(p95):>10d}")
#     print(f"      max   {rdiff[-1]:>10d}")
#
#     wdiff = list(wdiff_osd.values())
#     wdiff.sort()
#     p5, p50, p95 = numpy.percentile(wdiff, [5, 50, 95])
#     print("\nPer OSD writes:")
#     print(f"  average   {int(numpy.average(wdiff)):>10d}")
#     print(f"      min   {wdiff[0]:>10d}")
#     print(f"    5perc   {int(p5):>10d}")
#     print(f"   50perc   {int(p50):>10d}")
#     print(f"   95perc   {int(p95):>10d}")
#     print(f"      max   {wdiff[-1]:>10d}")
#
#     rdiff = list(rdiff_osd.values())
#     rdiff.sort()
#     p5, p50, p95 = numpy.percentile(rdiff, [5, 50, 95])
#     print("\nPer OSD reads:")
#     print(f"  average   {int(numpy.average(rdiff)):>10d}")
#     print(f"      min   {rdiff[0]:>10d}")
#     print(f"    5perc   {int(p5):>10d}")
#     print(f"   50perc   {int(p50):>10d}")
#     print(f"   95perc   {int(p95):>10d}")
#     print(f"      max   {rdiff[-1]:>10d}")
#

# -------  COMMON FUNCTIONS  -------------------------------------------------------------------------------------------


def_ticks = (2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000, 30000)


def get_stages_duration_2d(df: pandas.DataFrame, bins: Iterable[float] = def_ticks) -> Dict[str, List[float]]:

    res = {
        'total_ops': [],
        'total_dur': [],
        'local_io': [],
        'download': [],
        'wait_for_pg': [],
        'min_duration': [],
        'max_duration': []}

    max_val = df['duration'].max()

    fbins: List[float] = [0] + [val for val in bins if val < max_val] + [max_val]

    for min_val, max_val in zip(fbins[:-1], fbins[1:]):
        ops = df[(df['duration'] < max_val) & (df['duration'] >= min_val)]
        res['total_ops'].append(len(ops))
        res['total_dur'].append(ops['duration'].sum())
        res['local_io'].append(ops['local_io'].sum())
        res['wait_for_pg'].append(ops['wait_for_pg'].sum())
        res['download'].append(ops['download'].sum())
        res['min_duration'].append(min_val)
        res['max_duration'].append(max_val)
    return res


# -----------   PLOT FUNCTIONS  ----------------------------------------------------------------------------------------


class SimpleTableWC(SimpleTable):
    def __init__(self, caption: str, *headers: str) -> None:
        SimpleTable.__init__(self, *headers)
        self.caption = caption

    def add_row(self, *vals) -> None:
        super().add_row(*map(str, vals))


def_ticks2 = (100, 200, 300, 500, 700, 1000, 1500, 2000, 3000, 5000, 10000, 20000, 30000)


def plot_stages_part_distribution(df: pandas.DataFrame, min_time: int = 100, max_time: int = 30000,
                                  bins: int = 20, xticks: Tuple[int, ...] = def_ticks2) -> None:

    bins = numpy.logspace(math.log10(min_time), math.log10(max_time), num=bins)
    vals = get_stages_duration_2d(df, bins)

    local_io = numpy.array(vals['local_io'])
    wait_for_pg = numpy.array(vals['wait_for_pg'])
    download = numpy.array(vals['download'])
    total = numpy.array(vals['total_dur'])

    total[total < 1] = 1
    local_io = local_io / total
    download = download / total
    wait_for_pg = wait_for_pg / total

    pyplot.plot(range(len(local_io)), local_io, marker='o', label="disk io")
    pyplot.plot(range(len(download)), download, marker='o', label="net io")
    pyplot.plot(range(len(wait_for_pg)), wait_for_pg, marker='o', label="pg lock")

    pyplot.xlabel('Request latency, logscale')
    pyplot.ylabel('Time part, consumed by different stages')

    xticks_pos = [math.log10(vl / 100) * ((len(local_io) - 1) / math.log10(30000 / 100)) for vl in xticks]
    pyplot.xticks(xticks_pos, list(map(str, xticks)))
    pyplot.legend()
    pyplot.show()


def plot_op_time_distribution(df: pandas.DataFrame, min_time: int = 100, max_time: int = 30000,
                              bins: int = 40, clip: int = 2000) -> None:
    times = df['duration']
    bins = numpy.logspace(math.log10(min_time), math.log10(max_time), num=bins)
    bins = [0] + list(bins)

    if bins[-1] < times.max():
        bins.append(times.max())

    vals, _ = numpy.histogram(times, bins)
    vals[-2] += vals[-1]
    vals = vals[1:-1]
    bins = numpy.array(bins[1:-1])
    bins_centers = (bins[1:] + bins[:-1]) / 2
    vals = numpy.clip(vals, 0, clip)
    pyplot.plot(bins_centers, vals, linestyle='--', marker='o', color='b')
    pyplot.xlabel('Request latency, ms')
    pyplot.ylabel('Request count, clipped on 2000 ')
    pyplot.show()


# ---------  Table-based reports ---------------------------------------------------------------------------------------


def top_slow_pgs(df: pandas.DataFrame, count: int = 20) -> SimpleTableWC:
    pg_ids = df[df['duration'] > 100].groupby(['pool', 'pg']).value_counts()
    pg_ids = pg_ids.sort_values(ascending=False)[:count].items()
    res = SimpleTableWC("Top slow PG:", "Pool.PG", "Slow request count")
    for (pool, pg), cnt in pg_ids.items():
        res.add_row(f"{pool}.{pg:x}", cnt)
    return res


def top_slow_osds(df: pandas.DataFrame, count: int = 10, time_limit: int = 100) -> Iterator[SimpleTableWC]:
    last_n = lambda ddf: ddf.sort_values(ascending=False)[:count].items()

    res = SimpleTableWC("OSDs with largest slow requests count:", "OSD id", "Count")
    for osd_id, cnt in last_n(df['osd_id'][df['duration'] > time_limit].value_counts()):
        res.add_row(osd_id, cnt)
    yield res

    res = SimpleTableWC("OSDs with longest io:", "OSD id", "Spend time")
    for osd_id, total in last_n(df.groupby(['osd_id'])['local_io'].agg('sum')):
        res.add_row(osd_id, seconds_to_str(total))
    yield res

    res = SimpleTableWC("OSDs with longest pg wait:", "OSD id", "Spend time")
    for osd_id, total in last_n(df.groupby(['osd_id'])['wait_for_pg'].agg('sum')):
        res.add_row(osd_id, seconds_to_str(total))
    yield res

    res = SimpleTableWC("OSDs with longest net transfer time:", "OSD id", "Spend time")
    for osd_id, total in last_n(df.groupby(['osd_id'])['download'].agg('sum')):
        res.add_row(osd_id, seconds_to_str(total))
    yield res


def top_slow_nodes(df: pandas.DataFrame, count: int = 10, time_limit: int = 100) -> Iterator[SimpleTableWC]:
    last_n = lambda ddf: ddf.sort_values(ascending=False)[:count].items()

    res = SimpleTableWC("Nodes with most slow requests counts:", "Node", "Count")
    for node, cnt in last_n(df['host'][df['duration'] > time_limit].value_counts()):
        res.add_row(node, cnt)
    yield res

    res = SimpleTableWC("Nodes with longest io:", "Node", "Time")
    for node, total in last_n(df.groupby(['host'])['local_io'].agg('sum')):
        res.add_row(node, seconds_to_str(total))
    yield res

    res = SimpleTableWC("Nodes with longest pg wait:", "Node", "Time")
    for node, total in last_n(df.groupby(['host'])['wait_for_pg'].agg('sum')):
        res.add_row(node, seconds_to_str(total))
    yield res

    res = SimpleTableWC("Nodes with longest net transfer time:", "Node", "Time")
    for node, total in last_n(df.groupby(['host'])['download'].agg('sum')):
        res.add_row(node, seconds_to_str(total))
    yield res


def slowness_cumulative(df: pandas.DataFrame) -> SimpleTableWC:
    net_slowest = (df['download'] > df['wait_for_pg']) & (df['download'] > df['local_io'])
    pg_slowest = (df['wait_for_pg'] > df['download']) & (df['wait_for_pg'] > df['local_io'])
    disk_slowest = (df['local_io'] > df['wait_for_pg']) & (df['local_io'] > df['download'])

    MS2S = 1000
    duration = df['duration']
    res = SimpleTableWC("Slowness source:", "Slowness", "Count", "Time,s", "Avg,ms", "5pc,ms", "50pc,ms", "95pc,ms")

    for key, durations in [('disk', duration[disk_slowest]),
                           ('net', duration[net_slowest]),
                           ('pg', duration[pg_slowest])]:

        if len(durations) > 0:
            p05, p50, p95 = map(int, numpy.percentile(durations, [5, 50, 95]))
            avg = int(numpy.average(durations))
        else:
            p05 = p50 = p95 = ""
            avg = ""
        res.add_row(key, len(durations), int(durations.sum() / MS2S), avg, p05, p50, p95)
    return res


def duration_distribution(df: pandas.DataFrame, bins: Iterable[float]) -> SimpleTableWC:
    max_dura = df['duration'].max()
    hist, edges = numpy.histogram(df['duration'], [i for i in bins if i < max_dura])
    total = len(df) / 100.

    res = SimpleTableWC("Requests distribution by duration", "Max duration", "Count, %", "Cum. total, %", "Count")
    res.align = [Align.right, Align.right, Align.right, Align.right]
    cumulative = 0
    for upper, val in zip(edges[1:], hist):
        cumulative += val
        res.add_row(upper, "{:>.1f}%".format(val / total), "{:>.1f}%".format(cumulative / total), val)
    return res


def slowness_by_duration(df: pandas.DataFrame) -> SimpleTableWC:
    st2dur = get_stages_duration_2d(df)
    res = SimpleTableWC("Slowness source for different duration ranges",
                        "Max request time(ms)", "Total ops", "% total ops",
                        "Total time", "% total time", "Net %", "Disk %", "PG wait %")
    total_ops_count = sum(st2dur['total_ops']) / 100
    total_time = sum(st2dur['total_dur']) / 100
    for total, total_dur, disk, net, pg, max_dur in zip(st2dur['total_ops'], st2dur['total_dur'],
                                                        st2dur['local_io'], st2dur['download'],
                                                        st2dur['wait_for_pg'], st2dur['max_duration']):
        if total == 0:
            continue

        total_dur_c = 0.01 if total_dur < 1 else total_dur / 100

        res.add_row(max_dur,
                    total,
                    "{:.1f}".format(total / total_ops_count),
                    seconds_to_str(total_dur),
                    "{:.1f}".format(total_dur / total_time),
                    int(net / total_dur_c),
                    int(disk / total_dur_c),
                    int(pg / total_dur_c))
    return res


def pool_pg_wait(df: pandas.DataFrame, pool_df: pandas.DataFrame) -> SimpleTableWC:
    bypool = df[['pool_name', 'duration', 'wait_for_pg']].groupby(['pool_name'])
    count = bypool.count()
    count_dct = dict(zip(count.index, count['duration'].values))
    total = bypool['duration'].agg('sum').to_dict()
    wait_pg = bypool['wait_for_pg'].agg('sum').to_dict()
    wait_pg_perc = {name: int(100 * wait / max(1, total[name])) for name, wait in wait_pg.items()}

    t = SimpleTableWC("Slowness by pool", "Name", "Total ops", "Total slow", "Slow %", "Total slow time",
                      "Total pg wait", "pg wait avg", "pg wait %")

    total_ops = 0
    total_slow = 0
    total_slow_time = 0
    total_pg_wait = 0

    for pool_name, wait_perc in sorted(wait_pg_perc.items(), key=lambda x: -x[1]):
        write_ops_arr = pool_df[pool_df['name'] == pool_name]['write_ops']
        read_ops_arr = pool_df[pool_df['name'] == pool_name]['read_ops']
        write_ops = write_ops_arr.values[-1] - write_ops_arr.values[0]
        read_ops = read_ops_arr.values[-1] - read_ops_arr.values[0]

        total_ops += write_ops + read_ops
        total_slow += count_dct[pool_name]
        total_slow_time += total[pool_name]
        total_pg_wait += wait_pg[pool_name]

        t.add_row(pool_name,
                  write_ops + read_ops,
                  count_dct[pool_name],
                  100 * count_dct[pool_name] // max(1, write_ops + read_ops),
                  seconds_to_str(total[pool_name]),
                  seconds_to_str(int(wait_pg[pool_name])),
                  int(wait_pg[pool_name] / max(1, count_dct[pool_name])),
                  wait_perc)

    t.add_row("total",
              total_ops,
              total_slow,
              100 * total_slow // max(1, total_ops),
              seconds_to_str(total_slow_time),
              seconds_to_str(int(total_pg_wait)),
              int(total_pg_wait / max(1, total_slow)),
              "-")

    return t

# ----------------------------------------------------------------------------------------------------------------------


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), help="log level", default='INFO')
    parser.add_argument("--log", help="log file")

    subparsers = parser.add_subparsers(dest='subparser_name')

    report_parser = subparsers.add_parser('report', help="Make a report on data from hdf5 file")
    report_parser.add_argument("--count", default=10, type=int, help="Slowest object(PG/OSD/node) count")
    report_parser.add_argument("--slow-osd", action="store_true", help="Slow osd report")
    report_parser.add_argument("--slow-node", action="store_true", help="Slow osd report")
    report_parser.add_argument("--slowness-source", action="store_true", help="Slow source for slow requests")
    report_parser.add_argument("--duration-distribution", action="store_true", help="Show all reports")
    report_parser.add_argument("--per-pool", action="store_true")

    plot_parser = subparsers.add_parser('plot', help="Make a report on data from hdf5 file")
    plot_parser.add_argument("--slowness-source", action="store_true", help="Slow source for slow requests")
    plot_parser.add_argument("--duration-distribution", action="store_true", help="Show all reports")

    for rparser in (plot_parser, report_parser):
        rparser.add_argument("--op", nargs='+', choices=['read', 'write_primary', 'write_secondary', 'write'],
                             default=['read', 'write'],
                             help="Select op type")
        rparser.add_argument("--min-time", type=int, default=0, help="Select min op time to considered slow")
        rparser.add_argument("--all", action="store_true", help="Slow source for slow requests")
        rparser.add_argument("hdf5_file", help="HDF5 file to read data from")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    setup_logging(get_file('logging.json'), opts.log_level, None, False)

    if opts.subparser_name == 'report' or opts.subparser_name == 'plot':
        logger.info(f"Start loading HDF5 file {opts.hdf5_file}")
        info = load_hdf(opts.hdf5_file)
        logger.info("Done")
        df = info.data
        if opts.min_time != 0:
            df = df[df['duration'] > opts.min_time]

        all_ops = set()
        for op in set(opts.op):
            if op == 'write':
                all_ops.add('write_primary')
                all_ops.add('write_secondary')
            else:
                all_ops.add(op)

        if all_ops != {'read', 'write_primary', 'write_secondary'}:
            selectors = [df['op_type'] == getattr(OpType, name).value for name in all_ops]
            selector = selectors[0]
            for more_sel in selectors[1:]:
                selector |= more_sel
            df = df[selector]

        # primary_writes = df[df['op_type'] == ceph_ho_dumper.OpType.write_primary.value]
        # secondary_writes = df[df['op_type'] == ceph_ho_dumper.OpType.write_secondary.value]
        # writes = df[(df['op_type'] == ceph_ho_dumper.OpType.write_secondary.value) |
        #             (df['op_type'] == ceph_ho_dumper.OpType.write_secondary.value)]
        # reads = df[df['op_type'] == ceph_ho_dumper.OpType.read.value]

        if opts.subparser_name == 'plot':
            if opts.all or opts.slowness_source:
                plot_stages_part_distribution(df)

            if opts.all or opts.duration_distribution:
                plot_op_time_distribution(df, max_time=10000, clip=50000)
        else:
            if opts.all or opts.duration_distribution:

                print(renter_to_text(duration_distribution(df, def_ticks)))
                print()

            if opts.all or opts.slowness_source:
                print(renter_to_text(slowness_by_duration(df)))
                print()
                print(renter_to_text(slowness_cumulative(df)))
                print()

            # if opts.all or opts.per_pool:
            #     print(renter_to_text(pool_pg_wait(df, info.pool_info)))
            #     print()

            if opts.all or opts.slow_osd:
                for table in top_slow_osds(df, opts.count):
                    print(renter_to_text(table))
                    print()

            if (opts.slow_node or opts.all) and 'host' in df:
                for table in top_slow_nodes(df, opts.count):
                    print(renter_to_text(table))
                    print()

        return 0

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

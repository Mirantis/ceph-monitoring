import logging
import contextlib
import collections
from dataclasses import dataclass
from typing import List, Any, Optional, Dict, Callable, Coroutine, AsyncIterable, Tuple, Iterator, AsyncIterator, \
    Iterable

from aiorpc import IAIORPCNode, ConnectionPool, HistoricCollectionStatus, iter_unreachable, unpack_historic_data
from aiorpc.service import (get_config as get_aiorpc_config, get_http_conn_pool_from_cfg, get_inventory_path,
                            AIORPCServiceConfig)
from cephlib import RadosDF, CephRole, CephReport
from koder_utils import CMDResult, async_wait_cycle

from .collect_info import read_inventory, get_inventory, set_nodes_roles, get_report, Inventory

logger = logging.getLogger("collect")


@contextlib.asynccontextmanager
async def get_pool() -> AsyncIterable[ConnectionPool]:
    aiorpc_cfg = get_aiorpc_config(path=None)
    pool = get_http_conn_pool_from_cfg(aiorpc_cfg)
    async with pool:
        yield pool


@contextlib.asynccontextmanager
async def ceph_cmd_runner(ceph_master: Optional[str]) -> AsyncIterable[Callable[..., Coroutine[Any, Any, CMDResult]]]:
    inv = get_inventory(read_inventory(get_inventory_path()), ceph_master)
    async with get_pool() as pool:
        async with pool.connection(inv.ceph_master) as conn:
            yield conn.run


async def pool_iostat(ceph_master: str, timeout: int):
    prev_rdf: Optional[Dict[str, RadosDF.RadosDFPoolInfo]] = None

    template = "{:^30s} {:>15d} {:>15d} {:>15d} {:>15d} {:>15d} {:>15d}"

    header = "{:^30s} {:>15s} {:>15s} {:>15s} {:>15s} {:>15s} {:>15s}"
    header = header.format("Pool name", "Reads", "Writes", "Read MiBps", "Write MiBps", "Read chunk KiB",
                           "Write chunk KiB")

    sep = "-" * len(header)

    KiB = 1024
    MiB = KiB * KiB
    async with ceph_cmd_runner(ceph_master) as run:
        async for _ in async_wait_cycle(timeout):
            out = await run("rados df --format json")
            rdf = RadosDF.from_json(out.stdout)
            first = True
            if prev_rdf:
                for pdf in sorted(rdf.pools, key=lambda x: x.name):
                    if pdf.name not in prev_rdf:
                        continue

                    ppdf = prev_rdf[pdf.name]

                    dread = pdf.read_ops - ppdf.read_ops
                    dwrite = pdf.write_ops - ppdf.write_ops
                    dread_b = pdf.read_bytes - ppdf.read_bytes
                    dwrite_b = pdf.write_bytes - ppdf.write_bytes
                    avg_read_size_kb = 0 if not dread else dread_b // dread
                    avg_write_size_kb = 0 if not dwrite else dwrite_b // dwrite

                    if first:
                        print(f"{sep}\n{header}\n{sep}")
                        first = False

                    print(template.format(pdf.name,
                                          dread // timeout,
                                          dwrite // timeout,
                                          dread_b // timeout // MiB,
                                          dwrite_b // timeout // MiB,
                                          avg_read_size_kb // KiB,
                                          avg_write_size_kb // KiB))

            prev_rdf = {pdf.name: pdf for pdf in rdf.pools}

            if not first:
                print("\n")


@dataclass
class ClusterConnection:
    inv: Inventory
    pool: ConnectionPool
    aiorpc_cfg: AIORPCServiceConfig
    failed_hosts: List[str]
    ceph_report: CephReport
    master_conn: IAIORPCNode


@contextlib.asynccontextmanager
async def connect_to_cluster(ceph_master: str, ceph_extra_args: Optional[List[str]]) \
        -> AsyncIterable[ClusterConnection]:
    inv = get_inventory(read_inventory(get_inventory_path()), ceph_master)
    aiorpc_cfg = get_aiorpc_config(path=None)

    async with get_pool() as pool:
        ceph_report, _ = await get_report(inv.ceph_master, pool, aiorpc_cfg.cmd_timeout, ceph_extra_args)
        set_nodes_roles(inv, ceph_report)
        yield ClusterConnection(
            inv,
            pool,
            aiorpc_cfg,
            failed_hosts=[node async for node in iter_unreachable(inv.sorted, pool)],
            ceph_report=ceph_report,
            master_conn=await pool.rpc_connect(inv.ceph_master)
        )


async def configure_historic(clconn: ClusterConnection,
                             size: int,
                             duration: int,
                             ceph_extra_args: Optional[List[str]],
                             node2osds: Dict[str, List[int]]) -> Dict[str, List[int]]:
    historic_params = {
        'size': size,
        'duration': duration,
        'ceph_extra_args': ceph_extra_args,
        'cmd_timeout': clconn.aiorpc_cfg.cmd_timeout,
        'release_i': clconn.ceph_report.version.release.value
    }

    osd_nodes: List[str] = []
    for node, roles in clconn.inv:
        if CephRole.osd in roles:
            if node in clconn.failed_hosts:
                logger.warning(f"Fail to connect to osd node {node}, ignore it")
            else:
                osd_nodes.append(node)

    async def check_node_historic(node: str, conn: IAIORPCNode) -> Optional[List[int]]:
        status: HistoricCollectionStatus = await conn.proxy.ceph.get_historic_collection_status()
        if status.cfg is not None:
            return None
        else:
            failed, other = await conn.proxy.ceph.configure_historic(node2osds[node], **historic_params)
            return list(set(node2osds[node]).difference(failed))

    initiated_osds: Dict[str, List[int]] = {}
    async for node, osd_ids in clconn.pool.amap(check_node_historic, osd_nodes):
        if osd_ids:
            initiated_osds[node] = osd_ids
        elif osd_ids is None:
            logger.warning(f"Background historic collection is running on node {node}. Exclude node")
        else:
            logger.warning(f"No osd's found for node {node} with osd role")

    return initiated_osds


def get_node2osds(clconn: ClusterConnection) -> Dict[str, List[int]]:
    node2osds: Dict[str, List[int]] = {}
    for meta in clconn.ceph_report.osd_metadata:
        node2osds.setdefault(meta.hostname, []).append(meta.id)
    return node2osds


@contextlib.asynccontextmanager
async def with_hist_settings(clconn: ClusterConnection,
                             size: int,
                             duration: int,
                             ceph_extra_args: Optional[List[str]],
                             def_size: int = 20,
                             def_duration: int = 600) -> AsyncIterator[Dict[str, List[int]]]:
    node2osds = get_node2osds(clconn)

    conf = await configure_historic(clconn, size, duration, ceph_extra_args, node2osds)
    try:
        yield conf
    finally:
        await configure_historic(clconn, def_size, def_duration, ceph_extra_args, node2osds)


@dataclass
class OpsRecord:
    ctime: float
    rados_df: RadosDF
    ops: List[Dict]


async def iter_historic(clconn: ClusterConnection,
                        size: int,
                        duration: int,
                        ceph_extra_args: Optional[List[str]],
                        pool_timeout: int,
                        initiated_osds: Dict[str, List[int]]) -> AsyncIterable[OpsRecord]:

    historic_params = {
        'size': size,
        'duration': duration,
        'ceph_extra_args': ceph_extra_args,
        'cmd_timeout': clconn.aiorpc_cfg.cmd_timeout,
        'release_i': clconn.ceph_report.version.release.value
    }

    async def collect_node(node: str, conn: IAIORPCNode) -> List[Dict]:
        data = await conn.proxy.ceph.get_historic(initiated_osds[node], **historic_params)
        if data:
            return list(unpack_historic_data(data))
        return []

    pool_timeout = pool_timeout if pool_timeout else duration / 2
    async for ctime in async_wait_cycle(pool_timeout):
        fut_rados = clconn.master_conn.run("rados df --format json")

        all_ops: List[Dict[str, Any]] = []
        async for _, ops in clconn.pool.amap(collect_node, initiated_osds):
            all_ops.extend(ops)

        rados_df_res = await fut_rados
        rados_df_res.check_returncode()
        rados_df = RadosDF.from_json(rados_df_res.stdout)
        yield OpsRecord(ctime, rados_df, all_ops)


@dataclass
class HistSummary:
    iops: Optional[float]
    ppc50: Optional[float]
    ppc95: Optional[float]
    ppc99: Optional[float]
    avg: Optional[float]
    total_50ms: Optional[float]
    total_100ms: Optional[float]
    slowest: Optional[float]


def get_hist_summary(timings: List[float], total_ops: int, dtime: float) -> HistSummary:
    timings.sort()
    if total_ops == 0 or len(timings) == 0:
        return HistSummary(None, None, None, None, None, None, None, None)
    else:
        not_in_timings = total_ops - len(timings)
        iops = int(total_ops / dtime)
        if not_in_timings < 0 and abs(not_in_timings) < 0.1 * len(timings):
            not_in_timings = 0

        if not_in_timings > 0:
            ppc50_idx = int(total_ops * 0.5) - not_in_timings
            ppc95_idx = int(total_ops * 0.95) - not_in_timings
            ppc99_idx = int(total_ops * 0.99) - not_in_timings

            ppc50 = timings[ppc50_idx] if ppc50_idx > 0 else None
            ppc95 = timings[ppc95_idx] if ppc95_idx > 0 else None
            ppc99 = timings[ppc99_idx] if ppc99_idx > 0 else None
        else:
            ppc50 = None
            ppc95 = None
            ppc99 = None

        avg = sum(timings) // total_ops
        total_50ms = int(len([x for x in timings if x > 50]) / dtime)
        total_100ms = int(len([x for x in timings if x > 100]) / dtime)
        return HistSummary(iops, ppc50, ppc95, ppc99, avg, total_50ms, total_100ms, timings[-1])


def calc_statistic(history: List[OpsRecord]) -> Iterator[Tuple[int, HistSummary]]:
    if len(history) < 2:
        return

    per_pool_durations: Dict[int, List[float]] = collections.defaultdict(list)
    per_pool_ops: Dict[int, int] = {}

    dtime = history[-1].ctime - history[0].ctime
    for rec in history:
        for op in rec.ops:
            per_pool_durations[op['pack_pool_id']].append(op['duration'])

    last_pools_df = {pool.id: pool.write_ops + pool.read_ops for pool in history[-1].rados_df.pools}
    for first_pool_df in history[0].rados_df.pools:
        if first_pool_df.id in last_pools_df:
            per_pool_ops[first_pool_df.id] = last_pools_df[first_pool_df.id] - first_pool_df.write_ops \
                - first_pool_df.read_ops

    for pool_id, ops in per_pool_ops.items():
        yield pool_id, get_hist_summary(per_pool_durations[pool_id], ops, dtime)


async def historic_iostat(opts: Any):
    async with connect_to_cluster(opts.ceph_master, opts.ceph_extra_args) as clconn:
        async with with_hist_settings(clconn, opts.size, opts.duration, opts.ceph_extra_args) as initiated_osds:
            if len(initiated_osds) == 0:
                logger.warning("No suitable nodes found. Exit")
                return

            pool_id2name_map: Dict[int, str] = {pool.pool: pool.pool_name for pool in clconn.ceph_report.osdmap.pools}
            history: List[OpsRecord] = []

            template = "{:^%ss} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s}" % \
                max(map(len, pool_id2name_map.values()))
            tostr = lambda x: '-' if not x else str(int(x))

            itr = iter_historic(clconn, opts.size, opts.duration, opts.ceph_extra_args,
                                opts.pool_timeout, initiated_osds)

            async for rec in itr:
                history = history[-opts.history_size:] + [rec]
                first = True
                for pool_id, summary in sorted(calc_statistic(history)):
                    name = pool_id2name_map[pool_id]

                    if first:
                        header = template.format("Pool name", "iops", ">50ms", ">100ms", "avg, ms", "p50 ms", "p95 ms",
                                                 "p99 ms", "Slowest")
                        print(f"{header}\n{'-' * len(header)}")
                        first = False

                    print(template.format(name, tostr(summary.iops), tostr(summary.total_50ms),
                                          tostr(summary.total_100ms), tostr(summary.avg),
                                          tostr(summary.ppc50), tostr(summary.ppc95), tostr(summary.ppc99),
                                          tostr(summary.slowest)))

                if not first:
                    print("\n\n")


# for name, count in sorted(ops_per_volume.items(), key=lambda x: x[1])[:10]:
#     print(f"{name:>30} {count}")


def get_rbd_vol_loads(ops: Iterable[Dict]) -> Dict[str, int]:
    ops_per_volume: Dict[str, int] = collections.Counter()
    for op in ops:
        name = op.get('obj_name')
        if name and ":::rbd_data." in name:
            ops_per_volume[name.split(":::rbd_data.")[1].split(".")[0]] += 1
    return ops_per_volume


async def historic_set(opts: Any):
    async with connect_to_cluster(opts.ceph_master, opts.ceph_extra_args) as clconn:
        configured = await configure_historic(clconn, opts.size, opts.duration,
                                              opts.ceph_extra_args, get_node2osds(clconn))
        for node, osds in configured.items():
            logger.info(f"Node {node} - {len(osds)} osds configured")


async def historic_get_settings(opts: Any):

    settings: Dict[str, Tuple[int, float]] = {}
    async with connect_to_cluster(opts.ceph_master, opts.ceph_extra_args) as clconn:
        node2osds = get_node2osds(clconn)
        async def get_node_historic_settings(node: str, conn: IAIORPCNode) -> Dict[int, Tuple[int, float]]:
            return await conn.proxy.ceph.get_historic_settings(node2osds[node],
                                                               opts.ceph_extra_args,
                                                               cmd_timeout=clconn.aiorpc_cfg.cmd_timeout,
                                                               release_i=clconn.ceph_report.version.release.value)

        async for _, node_settings in clconn.pool.amap(get_node_historic_settings, node2osds.keys()):
            common_osds = set(settings) & set(node_settings)
            assert not common_osds, f"Doubled osds {common_osds}"
            settings.update(node_settings)

        print(f"OSD_ID   SIZE  DURATION")
        for osd_id, (size, duration) in sorted(settings.items(), key=lambda x: int(x[0])):
            print(f"{osd_id:>5s}   {size:>5d}  {int(duration):>5d}")

# TODO: per pool slow iops
# per rbd drive slow iops
# per osd/node slowness

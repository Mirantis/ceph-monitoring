import asyncio
import logging
import contextlib
import collections
from dataclasses import dataclass
from typing import List, Any, Optional, Dict, Callable, Coroutine, AsyncIterable, Tuple

from aiorpc import IAIORPCNode, ConnectionPool, HistoricCollectionStatus, iter_unreachable
from aiorpc.plugins.ceph import unpack_historic_simple
from aiorpc.service import get_config as get_aiorpc_config, get_http_conn_pool_from_cfg, get_inventory_path, \
    AIORPCServiceConfig
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


async def pool_iostat(opts: Any):
    prev_rdf: Optional[Dict[str, RadosDF.RadosDFPoolInfo]] = None

    template = "{:^30s} {:>15d} {:>15d} {:>15d} {:>15d} {:>15d} {:>15d}"

    header = "{:^30s} {:>15s} {:>15s} {:>15s} {:>15s} {:>15s} {:>15s}"
    header = header.format("Pool name", "Reads", "Writes", "Read MiBps", "Write MiBps", "Read chunk KiB",
                           "Write chunk KiB")

    sep = "-" * len(header)

    KiB = 1024
    MiB = KiB * KiB
    async with ceph_cmd_runner(opts.ceph_master) as run:
        async for _ in async_wait_cycle(opts.timeout):
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
                        print(sep)
                        print(header)
                        print(sep)
                        first = False

                    print(template.format(pdf.name,
                                          dread // opts.timeout,
                                          dwrite // opts.timeout,
                                          dread_b // opts.timeout // MiB,
                                          dwrite_b // opts.timeout // MiB,
                                          avg_read_size_kb // KiB,
                                          avg_write_size_kb // KiB))

            prev_rdf = {pdf.name: pdf for pdf in rdf.pools}

            if not first:
                print()
                print()


@dataclass
class ClusterConnection:
    inv: Inventory
    pool: ConnectionPool
    aiorpc_cfg: AIORPCServiceConfig
    failed_hosts: List[str]
    ceph_report: CephReport
    master_conn: IAIORPCNode


@contextlib.asynccontextmanager
async def connect_to_cluster(opts: Any) -> AsyncIterable[ClusterConnection]:
    inv = get_inventory(read_inventory(get_inventory_path()), opts.ceph_master)
    aiorpc_cfg = get_aiorpc_config(path=None)

    async with get_pool() as pool:
        ceph_report, _ = await get_report(inv.ceph_master, pool, aiorpc_cfg.cmd_timeout, opts.ceph_extra_args)
        set_nodes_roles(inv, ceph_report)
        yield ClusterConnection(
            inv,
            pool,
            aiorpc_cfg,
            failed_hosts=[node async for node in iter_unreachable(inv.sorted, pool)],
            ceph_report=ceph_report,
            master_conn=await pool.rpc_connect(inv.ceph_master)
        )


async def iter_historic(clconn: ClusterConnection, opts: Any) -> AsyncIterable:
    node2osds: Dict[str, List[int]] = {}
    conns: Dict[str, IAIORPCNode] = {}
    for meta in clconn.ceph_report.osd_metadata:
        node2osds.setdefault(meta.hostname, []).append(meta.id)

    for node, roles in clconn.inv:
        if CephRole.osd in roles:
            if node in clconn.failed_hosts:
                logger.warning(f"Ignoring node {node} as fail to connect to it")
                continue

            conn = await clconn.pool.rpc_connect(node)
            status: HistoricCollectionStatus = await conn.proxy.ceph.get_historic_collection_status()
            if status.cfg is not None:
                logger.error(f"Background historic collection is running. Stop it first")
                return
            else:
                failed, other = await conn.proxy.ceph.configure_historic(node2osds[node],
                                                                         opts.size,
                                                                         opts.duration,
                                                                         clconn.aiorpc_cfg.cmd_timeout,
                                                                         opts.ceph_extra_args,
                                                                         clconn.ceph_report.version.release)
                node2osds[node] = list(set(node2osds[node]).difference(failed))
                if node2osds[node]:
                    conns[node] = conn
                else:
                    del node2osds[node]

    if len(conns) == 0:
        logger.warning("No suitable nodes found. Exit")
        return

    async def collect_node(conn: IAIORPCNode, ops: List) -> None:
        for op_dct in unpack_historic_simple(await conn.proxy.ceph.get_historic()):
            print(op_dct)
            return

            # if '.' in sid:
            #     pool_id, pg_id = map(int, sid.split("."))
            #     per_pg_data[(pool_id, pg_id)].extend(values)
            #     per_pool_data[pool_id].extend(values)
            # else:
            #     osd_id = int(sid)
            #     per_osd_data[osd_id].extend(values)

    timeout = max([2, 0.1 * len(conns)])
    p_pools_total_ops: Dict[str, int] = {}

    async for ctime in async_wait_cycle(opts.duration / 2):
        per_osd_data: Dict[int, List[int]] = collections.defaultdict(list)
        per_pg_data: Dict[Tuple[int, int], List[int]] = collections.defaultdict(list)
        per_pool_data: Dict[int, List[int]] = collections.defaultdict(list)
        all_ops = []

        fut_rados = clconn.master_conn.run("rados df --format json")
        coros = {collect_node(node, all_ops): node for node in conns.values()}
        ready, not_ready = await asyncio.wait(coros, timeout=timeout)
        # TODO: reconnect to not_ready

        for fut in ready:
            fut.result()

        rados_df_res = await fut_rados
        rados_df_res.check_returncode()
        rados_df = RadosDF.from_json(rados_df_res.stdout)

        pools_total_ops: Dict[str, int] = {pinfo.name: (pinfo.read_ops + pinfo.write_ops)
                                           for pinfo in rados_df.pools}
        delta: Dict[str, int] = {}
        for name, total_ops in pools_total_ops.items():
            if name in p_pools_total_ops:
                delta[name] = total_ops - p_pools_total_ops[name]

        yield delta, per_osd_data, per_pg_data, per_pool_data, ctime
        p_pools_total_ops = pools_total_ops


async def historic_iostat(opts: Any):
    template = "{:^20s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s}"
    header = template.format("Pool name", "iops", ">50ms", ">100ms", "avg, ms", "p50 ms", "p95 ms",
                             "p99 ms", "Slowest")
    sep = "-" * len(header)

    async with connect_to_cluster(opts) as clconn:
        ptime = 0
        pool_id2name_map: Dict[int, str] = {pool.pool: pool.pool_name for pool in clconn.ceph_report.osdmap.pools}

        async for delta, per_osd_data, per_pg_data, per_pool_data, ctime in iter_historic(clconn, opts):
            if ptime != 0:
                dtime = ctime - ptime
                first = True
                for pool_id, timings in sorted(per_pool_data.items()):
                    name = pool_id2name_map[pool_id]

                    if name not in delta or len(timings) == 0:
                        continue

                    if first:
                        print(header)
                        print(sep)
                        first = False

                    timings.sort()
                    total = delta[name]
                    if total == 0:
                        iops = '-'
                        ppc50 = '-'
                        ppc95 = '-'
                        ppc99 = '-'
                        avg = '-'
                        total_50ms = '-'
                        total_100ms = '-'
                    else:
                        not_in_timings = total - len(timings)
                        iops = str(int(total / dtime))
                        if not_in_timings < 0 and abs(not_in_timings) < 0.1 * len(timings):
                            not_in_timings = 0

                        if not_in_timings > 0:
                            ppc50_idx = int(total * 0.5) - not_in_timings
                            ppc95_idx = int(total * 0.95) - not_in_timings
                            ppc99_idx = int(total * 0.99) - not_in_timings

                            ppc50 = str(timings[ppc50_idx]) if ppc50_idx > 0 else '-'
                            ppc95 = str(timings[ppc95_idx]) if ppc95_idx > 0 else '-'
                            ppc99 = str(timings[ppc99_idx]) if ppc99_idx > 0 else '-'
                        else:
                            ppc50 = '?'
                            ppc95 = '?'
                            ppc99 = '?'

                        avg = str(sum(timings) // total)
                        total_50ms = int(len([x for x in timings if x > 50]) / dtime)
                        total_100ms = int(len([x for x in timings if x > 100]) / dtime)

                    print(template.format(name, iops, str(total_50ms), str(total_100ms), str(avg),
                                          str(ppc50), str(ppc95), str(ppc99), str(timings[-1])))

                if not first:
                    print()
                    print()

            ptime = ctime


# TODO: per pool slow iops
# per rbd drive slow iops
# per osd/node slowness

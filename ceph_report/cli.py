import asyncio
import collections
import contextlib
import json
import sys
import logging
import argparse
from pathlib import Path
from typing import List, Any, Optional, Dict, Callable, Coroutine, AsyncIterable, Tuple

try:
    import stackprinter
except ImportError:
    stackprinter = None

from aiorpc import IAIORPCNode, ConnectionPool, HistoricCollectionStatus
import aiorpc_service
from aiorpc_service import get_config as get_aiorpc_config, get_http_conn_pool_from_cfg, get_inventory_path
from cephlib import RadosDF
from koder_utils import CMDResult

from . import setup_logging, get_file
from .collect_info import read_inventory, get_inventory


logger = logging.getLogger("ceph_report")


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
        while True:
            out = await run("rados df --format json")
            rdf = RadosDF.from_json(json.loads(out.stdout))
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

            await asyncio.sleep(opts.timeout)


async def historic_iostat(opts: Any):
    inv = get_inventory(read_inventory(get_inventory_path()), opts.ceph_master)
    conns: Dict[str, IAIORPCNode] = {}
    async with get_pool() as pool:
        for node, _ in inv:
            conn = conns[node] = await pool.rpc_connect(node)
            status: HistoricCollectionStatus = await conn.get_historic_collection_status()
            assert status.cfg, f"Historic collection is not running on node {node}. Start it first"

        per_osd_data: Dict[int, List[int]] = collections.defaultdict(list)
        per_pg_data: Dict[Tuple[int, int], List[int]] = collections.defaultdict(list)
        per_pool_data: Dict[int, List[int]] = collections.defaultdict(list)

        async def collect_node(conn: IAIORPCNode) -> None:
            for sid, values in await conn.proxy.pull_collected_historic_summary():
                if '.' in sid:
                    pool_id, pg_id = map(int, sid.split("."))
                    per_pg_data[(pool_id, pg_id)].extend(values)
                    per_pool_data[pool_id].extend(values)
                else:
                    osd_id = int(sid)
                    per_osd_data[osd_id].extend(values)

        timeout = max([2, 0.1 * len(conns)])

        while True:
            per_osd_data.clear()
            per_pg_data.clear()
            per_pool_data.clear()

            coros = {collect_node(node): node for node in conns.values()}
            ready, not_ready = await asyncio.wait(coros, timeout=timeout)
            # TODO: reconnect to not_ready

            for fut in ready:
                fut.result()

            for pool_id, data in per_pool_data.items():
                print(pool_id, len(data), sum(data))

            await asyncio.sleep(status.cfg.duration)



def parse_args(argv: List[str]) -> Any:
    root_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = root_parser.add_subparsers(dest='subparser_name')
    pool_load = subparsers.add_parser('iostat', help='Show pools load')
    pool_load.add_argument('-t', '--timeout', default=5, type=int, help='Show timeout')
    slow_parser = subparsers.add_parser('historic', help='Show slow requests info')

    for parser in (pool_load, slow_parser):
        parser.add_argument("-l", "--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
        parser.add_argument("--extended-tb", action='store_true')
        parser.add_argument("--aiorpc-service-root", default=None, type=Path)
        parser.add_argument("--ceph-master", default=None)

    return root_parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)

    if opts.extended_tb:
        if stackprinter:
            def hook(*args):
                msg = stackprinter.format(args, style='color')
                print(msg)

            sys.excepthook = hook
        else:
            logger.error("Can't set extended tb, as no module 'stackprinter' installed ")

    # dirty hack to simplify development
    if opts.aiorpc_service_root:
        aiorpc_service.INSTALL_PATH = opts.aiorpc_service_root

    setup_logging(get_file("logging.json"), opts.log_level, None, False)

    try:
        if opts.subparser_name == 'iostat':
            asyncio.run(pool_iostat(opts))
        else:
            asyncio.run(historic_iostat(opts))
    except KeyboardInterrupt:
        pass

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
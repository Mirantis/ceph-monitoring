import collections
import hashlib
import os
import sys
import time
import json
import shutil
import struct
import asyncio
import os.path
import argparse
import datetime
import tempfile
import subprocess
import logging.config
from pathlib import Path
from dataclasses import dataclass, field
from typing import Coroutine, AsyncIterable, TypeVar, cast, Set, Iterator, Optional, Tuple, Dict, Any, List

import aiorpc_service
from aiorpc import ConnectionPool, HistoricCollectionConfig, HistoricCollectionStatus, IAOIRPCNode, iter_unreachable
from aiorpc_service import get_config as get_aiorpc_config, get_http_conn_pool_from_cfg, get_inventory_path
from cephlib import parse_ceph_version, CephReport, CephRelease, CephCLI, get_ceph_version, CephRole
from koder_utils import (make_storage, IStorageNNP, IAsyncNode, LocalHost, get_hostname, ignore_all, get_all_ips,
                         b2ssize, rpc_map, read_inventory)

from . import setup_logging, get_file
from .collectors import LUMINOUS_MAX_PG, DEFAULT_MAX_PG, AUTOPG, ALL_COLLECTORS, Role, CephCollector, Collector
from .utils import CLIENT_NAME_RE, CLUSTER_NAME_RE, re_checker
from .prom_query import get_block_devs_loads


logger = logging.getLogger('collect')


CEPH_DETECT_INVENTORY = 'DETECT'


# ------------------------  Collect coordinator functions --------------------------------------------------------------


class ReportFailed(RuntimeError):
    pass


@dataclass
class ExceptionWithNode(Exception):
    exc: Exception
    hostname: str


@dataclass
class Inventory:
    nodes: Set[str]
    roles: Dict[CephRole, Set[str]]
    ceph_master: str
    node_roles: Dict[str, Set[CephRole]] = field(default_factory=dict, init=False)

    def __post_init__(self):
        for role, nodes in self.roles:
            for node in nodes:
                self.node_roles.setdefault(node, set()).add(role)

    def add_node(self, node: str, *roles: CephRole) -> None:
        self.nodes.add(node)
        for role in roles:
            self.roles.setdefault(role, set()).add(node)
            self.node_roles.setdefault(node, set()).add(role)

    @property
    def sorted(self) -> List[str]:
        return sorted(self.nodes)

    def get(self, role: CephRole) -> List[str]:
        return sorted(self.roles.get(role, []))

    def __contains__(self, item: str) -> bool:
        return item in self.nodes

    def __iter__(self) -> Iterator[Tuple[str, Set[CephRole]]]:
        return iter(self.node_roles.items())

    def __len__(self) -> int:
        return len(self.node_roles)

    def remove(self, node: str) -> None:
        self.nodes.remove(node)
        del self.node_roles[node]
        for _, nodes in self.roles.items():
            if node in nodes:
                nodes.remove(node)


@dataclass
class RemoteNodesCfg:
    inventory: Inventory
    conn_pool: ConnectionPool
    failed_hosts: List[str]
    ceph_report: CephReport
    osd_nodes: Dict[str, List[int]]
    opts: Any
    ceph_extra_args: List[str]
    ceph_extra_args_s: str
    cmd_timeout: float


ReportCoro = Coroutine[Any, Any, Optional[Dict[str, Any]]]


async def get_collectors(storage: IStorageNNP,
                         opts: Any,
                         pool: ConnectionPool,
                         inventory: Inventory,
                         report: CephReport) -> AsyncIterable[Tuple[Optional[str], ReportCoro]]:

    for func in ALL_COLLECTORS[Role.base]:
        yield None, func(Collector(storage, None, opts, pool))

    for func in ALL_COLLECTORS[Role.ceph_master]:
        yield inventory.ceph_master, \
            func(CephCollector(storage.sub_storage("master"), inventory.ceph_master, opts, pool, report))

    role_mapping = {
        CephRole.osd: Role.ceph_osd,
        CephRole.mon: Role.ceph_mon
    }

    if not opts.ceph_master_only:
        for node, roles in inventory:
            async with pool.connection(node) as conn:
                hostname = await get_hostname(conn)

            for role in roles:
                for func in ALL_COLLECTORS.get(role_mapping[role], []):
                    assert role in {CephRole.osd, CephRole.mon}
                    stor = storage if role == CephRole.osd else storage.sub_storage(f"mon/{node}")
                    yield hostname, func(CephCollector(stor, node, opts, pool, report))

            for func in ALL_COLLECTORS[Role.node]:
                yield hostname, func(Collector(storage.sub_storage(f"hosts/{node}"), node, opts, pool))

        if opts.historic:
            yield None, collect_historic(pool, inventory, storage.sub_storage("historic"))


def encrypt_and_upload(url: str,
                       report_file: str,
                       key_file: str,
                       web_cert_file: str,
                       http_user_password: str,
                       timeout: int = 360):
    fd, enc_report = tempfile.mkstemp(prefix="ceph_report_", suffix=".enc")
    os.close(fd)
    cmd = ["bash", get_file("upload.sh"), report_file, enc_report, key_file, web_cert_file, url]

    try:
        proc = subprocess.run(cmd,
                              stdin=subprocess.PIPE,
                              stderr=subprocess.STDOUT,
                              stdout=subprocess.PIPE,
                              input=(http_user_password + "\n").encode(),
                              timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.error(f"Fail to upload data: upload timeout")
        raise ReportFailed()

    finally:
        if os.path.exists(enc_report):
            os.unlink(enc_report)

    if proc.returncode != 0:
        logger.error(f"Fail to upload data: {proc.stdout.decode().strip()}")
        raise ReportFailed()

    logger.info("File successfully uploaded")


def pack_output_folder(out_folder: Path, out_file: Path):
    cmd = ['tar', "--create", "--gzip", "--file", str(out_file), *[fl.name for fl in out_folder.iterdir()]]
    tar_res = subprocess.run(cmd, cwd=str(out_folder))
    if tar_res.returncode != 0:
        logger.error(f"Fail to archive results. Please found raw data at {str(out_folder)!r}")
    else:
        md = hashlib.md5()
        with out_file.open("rb") as fd:
            data = fd.read(1024 * 1024)
            if data:
                md.update(data)

        sz = out_file.stat().st_size
        logger.info(f"Result saved into {str(out_file)!r} size={b2ssize(sz)}, md5={md.hexdigest()}")


def get_cluster_name_part(customer: str, cluster: str) -> str:
    return f"{customer}.{cluster}.{datetime.datetime.now():%Y_%h_%d.%H_%M}"


def get_output_folder(output_folder: Path, customer: str, cluster: str, wipe: bool) -> Optional[Path]:
    output_folder = output_folder / f"ceph_report.{get_cluster_name_part(customer, cluster)}"

    if output_folder.exists() and wipe:
        shutil.rmtree(output_folder)

    if not output_folder.exists():
        output_folder.mkdir(parents=True)

    return output_folder


def get_output_arch(output_folder: Path, customer: str, cluster: str) -> Optional[Path]:
    return output_folder / f"ceph_report.{get_cluster_name_part(customer, cluster)}.tar.gz"


async def collect_prom(prom_url: str, inventory: List[str], target: Path, time_range_hours: int):
    data = await get_block_devs_loads(prom_url, inventory, time_range_hours * 60)

    # data is {metric: str => {(host: str, device: str) => [values: float]}}

    for metric, values in data.items():
        for (host, device), measurements in values.items():
            with (target / 'monitoring' / f"{metric}@{device}@{host}.bin").open("wb") as fd:
                if measurements:
                    fd.write(struct.pack('!%sd' % len(measurements), *measurements))


async def check_master(conn: IAsyncNode):
    if (await conn.run('which ceph')).returncode != 0:
        logger.error("No 'ceph' command available on master node.")
        raise ReportFailed()

    version = parse_ceph_version(await conn.run_str('ceph --version'))
    if version.release < CephRelease.jewel:
        logger.error(f"Too old ceph version {version!r}, only jewel and later ceph supported")
        raise ReportFailed()


T = TypeVar('T')


async def raise_with_node(coro: Coroutine[Any, Any, T], hostname: str) -> T:
    try:
        return await coro
    except Exception as local_exc:
        raise ExceptionWithNode(local_exc, hostname) from local_exc


async def get_report(ceph_master: str, pool: ConnectionPool, timeout: float, ceph_extra_args: List[str]) -> CephReport:
    async def _get_report(node_conn: IAsyncNode) -> CephReport:
        await check_master(node_conn)
        version = await get_ceph_version(node_conn, ceph_extra_args)
        return await CephCLI(node_conn, ceph_extra_args, timeout, version.release).discover_report()

    if ceph_master == 'localhost':
        return await _get_report(LocalHost())
    else:
        async with pool.connection(ceph_master) as conn:
            return await _get_report(conn)


def get_inventory(opts: Any, node_list: List[str]) -> Inventory:
    if opts.ceph_master is None:
        ceph_master = node_list[0] if node_list else 'localhost'
    else:
        ceph_master = opts.ceph_master

    return Inventory(nodes=set(node_list), roles={}, ceph_master=ceph_master)


async def do_preconfig(opts: Any) -> RemoteNodesCfg:

    # dirty hack to simplify development
    if opts.aiorpc_service_root:
        aiorpc_service.INSTALL_PATH = opts.aiorpc_service_root

    aiorpc_cfg = get_aiorpc_config(path=None)
    conn_pool = get_http_conn_pool_from_cfg(aiorpc_cfg)
    node_list = read_inventory(get_inventory_path())
    inv = get_inventory(opts, node_list)

    async with conn_pool:
        ceph_report = await get_report(inv.ceph_master, conn_pool, aiorpc_cfg.cmd_timeout, opts.ceph_extra_args)
        for mon in ceph_report.mons:
            if mon.name in inv:
                inv.add_node(mon.name, CephRole.mon)
        for osd in ceph_report.osds:
            if osd.hostname in inv:
                inv.add_node(osd.hostname, CephRole.osd)

        logger.debug("Find nodes: " + ", ".join(inv.sorted))
        failed_hosts = [node async for node in iter_unreachable(inv.sorted, conn_pool)]
        if opts.must_connect_to_all:
            for ip_or_hostname, err in failed_hosts:
                logger.error(f"Can't connect to extra node {ip_or_hostname}: {err}")
            raise ReportFailed()
        for host in failed_hosts:
            inv.remove(host)

    osd_nodes = collections.defaultdict(list)

    for osd_meta in ceph_report.osds:
        if osd_meta.hostname in inv:
            osd_nodes[osd_meta.hostname].append(osd_meta.osd_id)

    return RemoteNodesCfg(inventory=inv,
                          conn_pool=conn_pool,
                          failed_hosts=failed_hosts,
                          ceph_report=ceph_report,
                          osd_nodes=osd_nodes,
                          opts=opts,
                          ceph_extra_args=opts.ceph_extra_args,
                          ceph_extra_args_s=" ".join(f"'{arg}'" for arg in opts.ceph_extra_args),
                          cmd_timeout=aiorpc_cfg.cmd_timeout)


async def run_collection(opts: Any,
                         output_folder: Optional[Path],
                         output_arch: Optional[Path]) -> Optional[Dict[str, Any]]:
    cfg = await do_preconfig(opts)
    if output_folder:
        if opts.dont_pack_result:
            logger.info(f"Store data into {output_folder}")
        else:
            logger.info(f"Will store results into {output_arch}")
            logger.info(f"Temporary folder {output_folder}")
        storage = make_storage(str(output_folder), existing=False, serializer='raw')
    else:
        storage = None

    logger.info(f"Found {len(cfg.inventory.get(CephRole.osd))} nodes with osds")
    logger.info(f"Found {len(cfg.inventory.get(CephRole.mon))} nodes with mons")
    logger.info(f"Run with {len(cfg.inventory)} hosts in total")

    # This variable is updated from main function
    if opts.detect_only:
        logger.info(f"Exiting, as detect-only mode requested")
        return

    async with cfg.conn_pool:
        nodes_info = []
        for node, _ in cfg.inventory:
            async with cfg.conn_pool.connection(node) as conn:
                nodes_info.append({
                    'name': await get_hostname(conn),
                    'ssh_enpoint': node,
                    'all_ips': await get_all_ips(conn)
                })

        storage.put_raw(json.dumps(nodes_info).encode(), "hosts.json")

        collectors_coro = get_collectors(storage=storage,
                                         opts=opts,
                                         pool=cfg.conn_pool,
                                         inventory=cfg.inventory,
                                         report=cfg.ceph_report)
        all_coros = [(hostname, coro) async for hostname, coro in collectors_coro]

        result: Dict[str, Any] = {}
        try:
            wrapped_coros = [raise_with_node(coro, hostname) for hostname, coro in all_coros]
            for res_item in await asyncio.gather(*wrapped_coros, return_exceptions=False):
                if res_item:
                    assert isinstance(res_item, Dict)
                    assert all(isinstance(key, str) for key in res_item)
                    result.update(res_item)

        except ExceptionWithNode as exc:
            logger.error(f"Exception happened during collecting from node " +
                         f"{exc.hostname} (see full tb below): {exc.exc}")
            raise
        except Exception as exc:
            logger.error(f"Exception happened(see full tb below): {exc}")
            raise
        finally:
            logger.info("Collecting logs and teardown RPC servers")
            for node, _ in cfg.inventory:
                with ignore_all:
                    async with cfg.conn_pool.connection(node) as conn:
                        storage.put_raw((await conn.proxy.sys.get_logs()).encode(), f"rpc_logs/{node}.txt")

    logger.info(f"Totally collected data from {len(cfg.inventory)} nodes")

    osd_count = {hostname: len(ids) for hostname, ids in cfg.osd_nodes.items()}

    mon_nodes = cfg.inventory.get(CephRole.mon)
    for node, _ in sorted(cfg.inventory):
        if node in mon_nodes and node in cfg.osd_nodes:
            logger.info(f"Node {node} has mon and {len(cfg.osd_nodes[node])} osds")
        elif node in cfg.osd_nodes:
            logger.info(f"Node {node} has {osd_count[node]} osds")
        elif node in mon_nodes:
            logger.info(f"Node {node} has mon")

    logger.info(f"Totally found {len(mon_nodes)} monitors, {len(osd_count)} " +
                f"OSD nodes with {sum(osd_count.values())} OSD daemons")

    if output_folder and opts.prometheus:
        await collect_prom(opts.prometheus, cfg.inventory.sorted, output_folder, opts.prometheus_interval)

    return result


MiB = 2 ** 20
GiB = 2 ** 30


async def historic_start_coro(conn: IAOIRPCNode, hostname: str, cfg: RemoteNodesCfg, all_names: List[str]) -> None:
    cmds = [f'rados {cfg.ceph_extra_args_s} --format json df',
            f'ceph {cfg.ceph_extra_args_s} --format json df',
            f'ceph {cfg.ceph_extra_args_s} --format json -s']

    first = hostname == all_names[0]
    osd_ids = cfg.osd_nodes[hostname]
    hst = HistoricCollectionConfig(osd_ids=osd_ids,
                                   size=cfg.opts.size,
                                   duration=cfg.opts.duration,
                                   min_duration=cfg.opts.min_duration,
                                   dump_unparsed_headers=False,
                                   pg_dump_timeout=3600 if first else None,
                                   extra_cmd=cmds if first else [],
                                   extra_dump_timeout=600 if first else None,
                                   max_record_file=cfg.opts.max_file_size * MiB,
                                   min_device_free=cfg.opts.min_disk_free * GiB,
                                   collection_end_time=time.time() + cfg.opts.max_collection_time * 60 * 60,
                                   packer_name='compact',
                                   cmd_timeout=cfg.cmd_timeout,
                                   ceph_extra_args=cfg.ceph_extra_args)

    await conn.proxy.ceph.start_historic_collection(hst)


async def start_historic(cfg: RemoteNodesCfg) -> None:
    all_names = cfg.inventory.get(CephRole.osd)

    mp = rpc_map(cfg.conn_pool, historic_start_coro, all_names, cfg=cfg, all_names=all_names)
    async for hostname, res in mp:
        prefix = f"{hostname} {'Primary ' if hostname == all_names[0] else ''}"
        if isinstance(res, Exception):
            logger.error(f"{prefix}Failed to start: {res}")
        else:
            logger.info(f"{prefix}OK")


async def status_historic(conn_pool: ConnectionPool, inventory: Inventory) -> None:
    async def coro(conn: IAOIRPCNode, _: str) -> Optional[HistoricCollectionStatus]:
        return await conn.proxy.ceph.get_historic_collection_status()

    async for hostname, res in rpc_map(conn_pool, coro, inventory.get(CephRole.osd)):
        if isinstance(res, Exception):
            logger.error(f"{hostname} Failed: {res}")
        elif res:
            hst: HistoricCollectionStatus = cast(HistoricCollectionStatus, res)
            status = 'RUNNING' if hst.cfg else 'NOT_RUNNING'
            logger.info(f"{hostname} | {status:>10s} | {b2ssize(hst.file_size):>10s}B")


async def stop_historic(conn_pool: ConnectionPool, inventory: Inventory) -> None:
    async def coro(conn: IAOIRPCNode, _: str) -> None:
        await conn.proxy.ceph.stop_historic_collection()

    async for hostname, res in rpc_map(conn_pool, coro, inventory.get(CephRole.osd)):
        if isinstance(res, Exception):
            logger.error(f"{hostname} Failed to stop: {res}")
        else:
            logger.info(f"{hostname} OK")


async def remove_historic(conn_pool: ConnectionPool, inventory: Inventory) -> None:
    async def coro(conn: IAOIRPCNode, _: str) -> None:
        await conn.proxy.ceph.remove_historic_data()

    async for hostname, res in rpc_map(conn_pool, coro, inventory.get(CephRole.osd)):
        if isinstance(res, Exception):
            logger.error(f"{hostname} Failed to clean: {res}")
        else:
            logger.info(f"{hostname} Wiped")


async def collect_historic(conn_pool: ConnectionPool, inventory: Inventory, storage: IStorageNNP) -> None:
    async def coro(conn: IAOIRPCNode, hostname: str) -> int:
        with storage.get_fd(f"{hostname}.bin", "cb") as fd:
            async for chunk in conn.collect_historic():
                fd.write(chunk)
            return fd.tell()

    async for node, res in rpc_map(conn_pool, coro, inventory.get(CephRole.osd)):
        if isinstance(res, Exception):
            logger.error(f"{node} Failed to collect: {res}")
        else:
            logger.info(f"{node} {b2ssize(res)}B of historic data is collected")


def parse_args(argv: List[str]) -> Any:

    root_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                          usage='%(prog)s [options] [-- ceph_extra_args]')

    subparsers = root_parser.add_subparsers(dest='subparser_name')

    # ------------------------------------------------------------------------------------------------------------------
    collect_config = subparsers.add_parser('collect_config', help='Collect data')
    collect_config.add_argument("--config", default=None, type=Path, help="Config path")

    # ------------------------------------------------------------------------------------------------------------------
    collect_parser = subparsers.add_parser('collect', help='Collect data')
    collect_parser.add_argument("--dont-pack-result", action="store_true", help="Don't create archive")
    collect_parser.add_argument("--no-rbd-info", action='store_true', help="Don't collect info for rbd volumes")
    collect_parser.add_argument("--ceph-master-only", action="store_true",
                                help="Run only ceph master data collection, no info from " +
                                "osd/monitors would be collected")
    collect_parser.add_argument("--detect-only", action="store_true",
                                help="Don't collect any data, only detect cluster nodes")
    collect_parser.add_argument("--no-pretty-json", action="store_true", help="Don't prettify json data")
    collect_parser.add_argument("--collect-txt", action="store_true", help="Collect human-readable outputs(txt)")
    collect_parser.add_argument("--collect-rgw", action="store_true", help="Collect radosgw info")
    collect_parser.add_argument("--collect-maps", action="store_true", help="Collect txt/binary osdmap/crushmap")
    collect_parser.add_argument("--max-pg-dump-count", default=AUTOPG, type=int,
                                help=f"maximum PG count to by dumped with 'pg dump' cmd, by default {LUMINOUS_MAX_PG} "
                                + f"for luminous, {DEFAULT_MAX_PG} for other ceph versions (%(default)s)")
    collect_parser.add_argument("--ceph-log-max-lines", default=10000, type=int,
                                help="Max lines from osd/mon log (%(default)s)")
    collect_parser.add_argument("--prometheus", default=None, help="Prometheus url to collect data")
    collect_parser.add_argument("--prometheus-interval", default=24 * 7, type=int,
                                help="For how many hours to the past grab data from prometheus")
    collect_parser.add_argument("--historic", action='store_true', help="Collect historic data")

    # ------------------------------------------------------------------------------------------------------------------

    historic_start = subparsers.add_parser('historic_start', help='Upload report to server')
    historic_start.add_argument('--size', metavar='OPS_TO_RECORD', default=200, type=int,
                                help='Collect X slowest requests for each given period')
    historic_start.add_argument('--duration', metavar='SECONDS', default=60, type=int, help='Collect cycle')
    historic_start.add_argument('--min-duration', metavar='MS', default=50, type=int,
                                help='Min operation duration to collect')
    historic_start.add_argument('--max-file-size', metavar='SIZE_MiB', default=1024, type=int,
                                help='Max record file size in MiB')
    historic_start.add_argument('--max-collection-time', metavar='HOURS', default=24, type=int,
                                help='Max time to record in hours')
    historic_start.add_argument('--min-disk-free', metavar='SIZE_GiB', default=50, type=int,
                                help='Min disk free size in GiB to left')

    # ------------------------------------------------------------------------------------------------------------------

    historic_status = subparsers.add_parser('historic_status', help='Show status of historic collection')
    historic_stop = subparsers.add_parser('historic_stop', help='Stop historic collection')
    historic_collect = subparsers.add_parser('historic_collect', help='Download collected historic data')
    historic_remove = subparsers.add_parser('historic_remove', help='Wipe all historic records')

    # ------------------------------------------------------------------------------------------------------------------

    upload = subparsers.add_parser('upload', help='Upload report to server')
    upload.add_argument('--url', required=True, help="Url to upload to")
    upload.add_argument('--key', default=get_file("enc_key.pub"), type=Path,
                        help="Server open key for data encryption (%(default)s)")
    upload.add_argument('--cert', default=get_file("mira_report_storage.crt"), type=Path,
                        help="Storage server cert file (%(default)s)")
    upload.add_argument('--upload-script-path', type=Path,
                        default=get_file("upload.sh"), help="upload.sh path (%(default)s)")
    upload.add_argument('--http-creds', required=True,
                        help="Http user:password, as provided by mirantis support")
    upload.add_argument('report', help="path to report archive")

    # ------------------------------------------------------------------------------------------------------------------
    for parser in (collect_parser, historic_start, historic_stop, historic_collect,
                   historic_status, upload, historic_remove):
        parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                            help="Console log level, see logging.json for defaults")
        parser.add_argument("--persistent-log", action="store_true",
                            help="Log to /var/log/ceph_report_collector.log as well")
        parser.add_argument("--aiorpc-service-root", default=None, type=Path,
                            help="DIRTY HACK, USE FOR DEV ONLY - RPC agent installation root (%(default)s)")

    # ------------------------------------------------------------------------------------------------------------------

    for parser in (collect_parser, historic_collect):
        parser.add_argument("--cluster", help=f"Cluster name, should match {CLIENT_NAME_RE}",
                            type=re_checker(CLIENT_NAME_RE), required=True)
        parser.add_argument("--customer", help=f"Customer name, should match {CLUSTER_NAME_RE}",
                            type=re_checker(CLUSTER_NAME_RE), required=True)
        parser.add_argument("--output-folder", default=Path('/tmp'), type=Path,
                            help="Folder to put result to (%(default)s)")
        parser.add_argument("--wipe", action='store_true', help="Wipe results directory before store data")

    # ------------------------------------------------------------------------------------------------------------------

    for parser in (collect_parser, historic_start, historic_stop, historic_collect, historic_status, historic_remove):
        parser.add_argument("--ceph-master", metavar="NODE", default=None,
                            help="Run all ceph cluster commands from NODE, (first inventory node by default)")
        parser.add_argument("--must-connect-to-all", action="store_true",
                            help="Must successfully connect to all ceph nodes")

    # ------------------------------------------------------------------------------------------------------------------

    if '--' in argv:
        ceph_extra_args = argv[argv.index('--'):]
        argv = argv[:argv.index('--') - 1]
    else:
        ceph_extra_args = []

    opts = root_parser.parse_args(argv[1:])
    opts.ceph_extra_args = ceph_extra_args

    return opts


async def historic_main(opts: Any, subparser: str):
    cfg = await do_preconfig(opts)

    async with cfg.conn_pool:
        if subparser == 'historic_start':
            await start_historic(cfg)
        elif subparser == 'historic_status':
            await status_historic(cfg.conn_pool, cfg.inventory)
        elif subparser == 'historic_stop':
            await stop_historic(cfg.conn_pool, cfg.inventory)
        elif subparser == 'historic_collect':
            path = get_output_folder(opts.output_folder, opts.customer, opts.cluster, opts.wipe) / "historic"
            path.mkdir()
            storage = make_storage(str(path), existing=False, serializer='raw')
            logger.info(f"Collecting historic data to {path}")
            await collect_historic(cfg.conn_pool, cfg.inventory, storage)
        elif subparser == 'historic_remove':
            await remove_historic(cfg.conn_pool, cfg.inventory)
        else:
            raise RuntimeError(f"Unknown cmd {subprocess}")


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    log_config = get_file("logging.json")

    if opts.subparser_name == 'collect_config':
        # make opts from config
        opts.subparser_name = 'collect'

    if opts.subparser_name == 'collect':
        try:
            if opts.detect_only:
                output_folder = None
            else:
                output_folder = get_output_folder(opts.output_folder, opts.customer, opts.cluster, opts.wipe)

            if opts.detect_only or opts.dont_pack_result:
                output_arch = None
            else:
                output_arch = get_output_arch(opts.output_folder, opts.customer, opts.cluster)

            setup_logging(log_config, opts.log_level, output_folder, opts.persistent_log)
            logger.info(repr(argv))

            result = asyncio.run(run_collection(opts, output_folder, output_arch))

            if output_folder:
                if opts.dont_pack_result:
                    logger.warning("Unpacked tree is kept as --dont-pack-result option is set, so no archive created")
                    print(f"Result saved into {output_folder}")
                else:
                    assert output_arch is not None
                    pack_output_folder(output_folder, output_arch)
                    print(f"Result saved into {output_arch}")
                    shutil.rmtree(output_folder)

            res_str = "\n".join(f"       {key:>20s}: {val!r}" for key, val in result.items())
            if res_str.strip():
                logger.debug(f"Results:\n{res_str}")

        except ReportFailed:
            return 1
        except Exception:
            logger.exception("During make_storage/collect")
            raise
    else:
        setup_logging(log_config, opts.log_level, None, opts.persistent_log)
        try:
            if opts.subparser_name == 'upload':
                try:
                    encrypt_and_upload(url=opts.url,
                                       report_file=opts.report,
                                       key_file=opts.key,
                                       web_cert_file=opts.cert,
                                       http_user_password=opts.http_creds)
                except subprocess.CalledProcessError:
                    pass
            elif opts.subparser_name in ('historic_start', 'historic_status',
                                         'historic_stop', 'historic_collect', 'historic_remove'):
                asyncio.run(historic_main(opts, opts.subparser_name))
            else:
                print("Not implemented", file=sys.stderr)
                return 1
        except ReportFailed:
            return 1

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

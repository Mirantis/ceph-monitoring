import os
import time
import json
import shutil
import struct
import asyncio
import os.path
import hashlib
import datetime
import tempfile
import subprocess
import collections
import logging.config
from pathlib import Path
from dataclasses import dataclass, field
from typing import Coroutine, AsyncIterable, TypeVar, cast, Set, Iterator, Optional, Tuple, Dict, Any, List

from aiorpc import ConnectionPool, HistoricCollectionConfig, HistoricCollectionStatus, IAIORPCNode, iter_unreachable
import aiorpc.service
from aiorpc.service import get_config as get_aiorpc_config, get_http_conn_pool_from_cfg, get_inventory_path
from cephlib import parse_ceph_version, CephReport, CephRelease, CephCLI, get_ceph_version, CephRole
from koder_utils import (make_storage, IAsyncNode, LocalHost, get_hostname, ignore_all, get_all_ips, IStorage,
                         b2ssize, rpc_map, read_inventory)
from . import get_file
from .collectors import ALL_COLLECTORS, Role, CephCollector, Collector
from .prom_query import get_block_devs_loads


logger = logging.getLogger('collect')


CEPH_DETECT_INVENTORY = 'DETECT'


# ------------------------  Collect coordinator functions --------------------------------------------------------------


class ReportFailed(RuntimeError):
    pass


@dataclass
class ExceptionWithNode(Exception):
    exc: Exception
    hostname: Optional[str]


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
    raw_report: Any
    osd_nodes: Dict[str, List[int]]
    opts: Any
    ceph_extra_args: List[str]
    ceph_extra_args_s: str
    cmd_timeout: float


ReportCoro = Coroutine[Any, Any, Optional[Dict[str, Any]]]


async def get_collectors(storage: IStorage,
                         opts: Any,
                         pool: ConnectionPool,
                         inventory: Inventory,
                         report: CephReport) -> AsyncIterable[Tuple[Optional[str], ReportCoro]]:

    for func in ALL_COLLECTORS[Role.base]:
        yield None, func(Collector(storage, None, opts, pool))  # type: ignore

    for func in ALL_COLLECTORS[Role.ceph_master]:
        collector = CephCollector(storage.sub_storage("master"), inventory.ceph_master, opts, pool, report)
        yield inventory.ceph_master, func(collector)  # type: ignore

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
                    yield hostname, func(CephCollector(stor, node, opts, pool, report))  # type: ignore

            for func in ALL_COLLECTORS[Role.node]:
                yield hostname, func(Collector(storage.sub_storage(f"hosts/{node}"), node, opts, pool))  # type: ignore

        if opts.historic:
            yield None, collect_historic(pool, inventory, storage.sub_storage("historic"))  # type: ignore


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
        proc = subprocess.run(cast(List[str], cmd),
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


async def raise_with_node(coro: Coroutine[Any, Any, T],
                          hostname: Optional[str],
                          done_set: Set[Tuple[Optional[str], Any]]) -> T:
    try:
        res = await coro
        done_set.add((hostname, coro))
        return res
    except (asyncio.CancelledError, KeyboardInterrupt):
        raise
    except Exception as local_exc:
        done_set.add((hostname, coro))
        raise ExceptionWithNode(local_exc, hostname) from local_exc


async def get_report(ceph_master: str, pool: ConnectionPool, timeout: float, ceph_extra_args: List[str]) \
        -> Tuple[CephReport, Any]:

    async def _get_report(node_conn: IAsyncNode) -> Tuple[CephReport, Any]:
        await check_master(node_conn)
        version = await get_ceph_version(node_conn, ceph_extra_args)
        return await CephCLI(node_conn, ceph_extra_args, timeout, version.release).discover_report()

    if ceph_master == 'localhost':
        return await _get_report(LocalHost())
    else:
        async with pool.connection(ceph_master) as conn:
            return await _get_report(conn)


def get_inventory(node_list: List[str], ceph_master: str = None) -> Inventory:
    if ceph_master is None:
        ceph_master = node_list[0] if node_list else 'localhost'
    else:
        ceph_master = ceph_master

    return Inventory(nodes=set(node_list), roles={}, ceph_master=ceph_master)


def set_nodes_roles(inv: Inventory, report: CephReport) -> None:
    for mon in report.monmap.mons:
        if mon.name in inv:
            inv.add_node(mon.name, CephRole.mon)
    for osd in report.osd_metadata:
        if osd.hostname in inv:
            inv.add_node(osd.hostname, CephRole.osd)


async def do_preconfig(opts: Any) -> RemoteNodesCfg:

    # dirty hack to simplify development
    if opts.aiorpc_service_root:
        aiorpc.service.INSTALL_PATH = opts.aiorpc_service_root

    aiorpc_cfg = get_aiorpc_config(path=None)
    conn_pool = get_http_conn_pool_from_cfg(aiorpc_cfg)
    node_list = read_inventory(get_inventory_path())
    inv = get_inventory(node_list, opts.ceph_master)

    async with conn_pool:
        ceph_report, raw = await get_report(inv.ceph_master, conn_pool, aiorpc_cfg.cmd_timeout, opts.ceph_extra_args)
        set_nodes_roles(inv, ceph_report)
        logger.debug("Find nodes: " + ", ".join(inv.sorted))
        failed_hosts = [node async for node in iter_unreachable(inv.sorted, conn_pool)]
        if opts.must_connect_to_all:
            for ip_or_hostname, err in failed_hosts:
                logger.error(f"Can't connect to extra node {ip_or_hostname}: {err}")
            raise ReportFailed()
        for host in failed_hosts:
            inv.remove(host)

    osd_nodes: Dict[str, List[int]] = collections.defaultdict(list)

    for osd_meta in ceph_report.osd_metadata:
        if osd_meta.hostname in inv:
            osd_nodes[osd_meta.hostname].append(osd_meta.id)

    return RemoteNodesCfg(inventory=inv,
                          conn_pool=conn_pool,
                          failed_hosts=failed_hosts,
                          ceph_report=ceph_report,
                          raw_report=raw,
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
        return None

    async with cfg.conn_pool:
        nodes_info = []
        for node, _ in cfg.inventory:
            async with cfg.conn_pool.connection(node) as conn:
                nodes_info.append({
                    'name': await get_hostname(conn),
                    'ssh_enpoint': node,
                    'all_ips': await get_all_ips(conn)
                })

        storage.put_raw(str(cfg.ceph_report.version).encode(), "master/version.txt")
        storage.put_raw(json.dumps(nodes_info).encode(), "hosts.json")
        storage.put_raw(json.dumps(cfg.raw_report).encode(), "master/report.json")

        # release memory, report can be quite a large
        cfg.raw_report = None

        collectors_coro = get_collectors(storage=storage,
                                         opts=opts,
                                         pool=cfg.conn_pool,
                                         inventory=cfg.inventory,
                                         report=cfg.ceph_report)
        all_coros = [(hostname, coro) async for hostname, coro in collectors_coro]

        result: Dict[str, Any] = {}
        done_set: Set[Tuple[Optional[str], Any]] = set()
        try:
            wrapped_coros = [raise_with_node(coro, hostname, done_set) for hostname, coro in all_coros]
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
            for hostname, coro in set(all_coros).difference(done_set):
                logger.warning(f"Still running coro: {hostname}, {coro.cr_code.co_filename}::{coro.cr_code.co_name}")
            logger.info("Collecting rpc logs")
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


async def historic_start_coro(conn: IAIORPCNode, hostname: str, cfg: RemoteNodesCfg, all_names: List[str]) -> None:
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
    async def coro(conn: IAIORPCNode, _: str) -> Optional[HistoricCollectionStatus]:
        return await conn.proxy.ceph.get_historic_collection_status()

    async for hostname, res in rpc_map(conn_pool, coro, inventory.get(CephRole.osd)):
        if isinstance(res, Exception):
            logger.error(f"{hostname} Failed: {res}")
        elif res:
            hst: HistoricCollectionStatus = cast(HistoricCollectionStatus, res)
            status = 'RUNNING' if hst.running else 'NOT_RUNNING'
            logger.info(f"{hostname} | {status:>10s} | {b2ssize(hst.file_size):>10s}B")


async def stop_historic(conn_pool: ConnectionPool, inventory: Inventory) -> None:
    async def coro(conn: IAIORPCNode, _: str) -> None:
        await conn.proxy.ceph.stop_historic_collection()

    async for hostname, res in rpc_map(conn_pool, coro, inventory.get(CephRole.osd)):
        if isinstance(res, Exception):
            logger.error(f"{hostname} Failed to stop: {res}")
        else:
            logger.info(f"{hostname} OK")


async def remove_historic(conn_pool: ConnectionPool, inventory: Inventory) -> None:
    async def coro(conn: IAIORPCNode, _: str) -> None:
        await conn.proxy.ceph.remove_historic_data()

    async for hostname, res in rpc_map(conn_pool, coro, inventory.get(CephRole.osd)):
        if isinstance(res, Exception):
            logger.error(f"{hostname} Failed to clean: {res}")
        else:
            logger.info(f"{hostname} Wiped")


async def collect_historic(conn_pool: ConnectionPool, inventory: Inventory, storage: IStorage) -> None:
    async def coro(conn: IAIORPCNode, hostname: str) -> int:
        with storage.get_fd(f"{hostname}.bin", "cb") as fd:
            async for chunk in conn.collect_historic():
                fd.write(chunk)
            return fd.tell()

    async for node, res in rpc_map(conn_pool, coro, inventory.get(CephRole.osd)):
        if isinstance(res, Exception):
            logger.error(f"{node} Failed to collect: {res}")
        else:
            logger.info(f"{node} {b2ssize(res)}B of historic data is collected")

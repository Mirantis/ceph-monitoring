import collections
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
from dataclasses import dataclass
from typing import Coroutine, AsyncIterable, TypeVar, cast
from typing import Optional, Tuple, Dict, Any, List

from agent import (ConnectionPool, HistoricCollectionConfig, BlockType, HistoricCollectionStatus, IAgentRPCNode,
                   get_connection_pool, check_nodes)
from agent import get_config as get_agent_config
from cephlib import parse_ceph_version, CephReport, CephRelease, CephCLI, get_ceph_version, CephRole
from koder_utils import (make_storage, IStorageNNP, IAsyncNode, LocalHost, get_hostname, ignore_all, get_all_ips,
                         b2ssize, rpc_map)

from . import get_config, setup_logging, get_file
from .collectors import LUMINOUS_MAX_PG, DEFAULT_MAX_PG, AUTOPG, ALL_COLLECTORS, Role, CephCollector, Collector
from .utils import CLIENT_NAME_RE, CLUSTER_NAME_RE, re_checker, read_inventory
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


ReportCoro = Coroutine[Any, Any, Optional[Dict[str, Any]]]


async def get_collectors(storage: IStorageNNP, opts: Any, master_hostname: str, pool: ConnectionPool,
                         inventory: List[str],
                         report: CephReport) -> AsyncIterable[Tuple[Optional[str], ReportCoro]]:

    for func in ALL_COLLECTORS[Role.base]:
        yield None, func(Collector(storage, None, opts, pool))

    mon_nodes = {mon.name for mon in report.mons}
    osd_nodes = {osd.hostname for osd in report.osds}

    for func in ALL_COLLECTORS[Role.ceph_master]:
        yield master_hostname, func(CephCollector(storage.sub_storage("master"), master_hostname, opts, pool, report))

    if not opts.ceph_master_only:
        for node in inventory:
            async with pool.connection(node) as conn:
                hostname = await get_hostname(conn)

            if hostname in mon_nodes:
                for func in ALL_COLLECTORS[Role.ceph_mon]:
                    yield hostname, func(CephCollector(storage.sub_storage(f"mon/{node}"), node, opts, pool, report))

            if hostname in osd_nodes:
                for func in ALL_COLLECTORS[Role.ceph_osd]:
                    yield hostname, func(CephCollector(storage, node, opts, pool, report))

            for func in ALL_COLLECTORS[Role.node]:
                yield hostname, func(Collector(storage.sub_storage(f"hosts/{node}"), node, opts, pool))


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


def pack_output_folder(out_folder: str, out_file: str):
    cmd = ['tar', "--create", "--gzip", "--file", str(out_file), *os.listdir(out_folder)]
    tar_res = subprocess.run(cmd, cwd=out_folder)
    if tar_res.returncode != 0:
        logger.error(f"Fail to archive results. Please found raw data at {out_folder!r}")
    else:
        logger.info(f"Result saved into {out_file!r}")


def get_cluster_name_part(customer: str, cluster: str) -> str:
    return f"{customer}.{cluster}.{datetime.datetime.now():%Y_%h_%d.%H_%M}"


def check_and_prepare_paths(opts: Any) -> Tuple[Optional[str], Optional[str]]:
    part = get_cluster_name_part(opts.customer, opts.cluster)
    folder_name = f"ceph_report.{part}"
    arch_name = f"ceph_report.{part}.tar.gz"

    output_folder: Optional[str] = os.path.join(opts.base_folder, opts.output_folder, folder_name)
    output_arch: Optional[str] = os.path.join(opts.base_folder, opts.output_folder, arch_name)

    if opts.detect_only:
        output_arch = None
        output_folder = None
    elif opts.dont_pack_result:
        output_arch = None

    if output_folder is not None:
        if os.path.exists(output_folder):
            if opts.wipe:
                shutil.rmtree(output_folder)
                os.makedirs(output_folder, exist_ok=True)
        else:
            os.makedirs(output_folder, exist_ok=True)

    return output_folder, output_arch


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


@dataclass
class RemoteNodesCfg:
    inventory: Optional[List[str]]
    conn_pool: ConnectionPool
    good_hosts: List[str]
    failed_hosts: List[str]
    ceph_report: CephReport
    osd_nodes: Dict[str, List[int]]
    mon_nodes: List[str]
    opts: Any
    ceph_extra_args: List[str]
    ceph_extra_args_s: str
    ceph_master: str
    osd_in_order: List[str]

    def get_nodes(self, role: CephRole) -> List[str]:
        if role is CephRole.osd:
            return self.osd_in_order
        elif role is CephRole.mon:
            return sorted(self.mon_nodes)
        else:
            assert False, f"Not supported role {role}"


async def do_preconfig(opts: Any) -> RemoteNodesCfg:
    agent_cfg = get_agent_config(None if opts.agent_cfg is None else Path(opts.agent_cfg))
    conn_pool = get_connection_pool(agent_cfg)

    if opts.inventory == CEPH_DETECT_INVENTORY:
        inventory = None
    else:
        if opts.inventory:
            inv_path = Path(opts.inventory)

            if not inv_path.is_absolute():
                inv_path = Path(opts.base_folder) / inv_path

            inv_path = inv_path.resolve()

            if not inv_path.is_file():
                print(f"--inventory value must be file {inv_path!r}", file=sys.stderr)
                raise ReportFailed()

        else:
            inv_path = agent_cfg.inventory

        inventory = list(read_inventory(inv_path))

    if opts.ceph_master is None:
        if inventory:
            ceph_master = inventory[0]
        else:
            ceph_master = 'localhost'
    else:
        ceph_master = opts.ceph_master

    async with conn_pool:
        ceph_report = await get_report(ceph_master, conn_pool, opts.cmd_timeout, opts.ceph_extra_args)

        if not inventory:
            inventory = list({osd.hostname for osd in ceph_report.osds}) + [mon.name for mon in ceph_report.mons]
            inventory.sort()

        logger.debug("Find nodes: " + ", ".join(inventory))
        good_hosts, failed_hosts = await check_nodes(inventory, conn_pool)

    if failed_hosts and opts.must_connect_to_all:
        for ip_or_hostname, err in failed_hosts:
            logger.error(f"Can't connect to extra node {ip_or_hostname}: {err}")
        raise ReportFailed()

    osd_nodes = collections.defaultdict(list)

    for osd_meta in ceph_report.osds:
        if osd_meta.hostname in inventory:
            osd_nodes[osd_meta.hostname].append(osd_meta.osd_id)

    mon_nodes = [mon.name for mon in ceph_report.mons if mon.name in inventory]

    return RemoteNodesCfg(inventory=inventory, conn_pool=conn_pool, good_hosts=good_hosts, failed_hosts=failed_hosts,
                          ceph_report=ceph_report, osd_nodes=osd_nodes, mon_nodes=mon_nodes, opts=opts,
                          ceph_extra_args=opts.ceph_extra_args,
                          ceph_extra_args_s=" ".join(f"'{arg}'" for arg in opts.ceph_extra_args),
                          ceph_master=ceph_master,
                          osd_in_order=sorted(osd_nodes, key=lambda x: inventory.index(x)))


async def run_collection(opts: Any, output_folder: Optional[str], output_arch: Optional[str]) -> Dict[str, Any]:
    cfg = await do_preconfig(opts)
    if output_folder:
        if opts.dont_pack_result:
            logger.info("Store data into %r", output_folder)
        else:
            logger.info("Will store results into %s", output_arch)
            logger.info("Temporary folder %r", output_folder)
        storage = make_storage(output_folder, existing=False, serializer='raw')
    else:
        storage = None

    logger.info(f"Found {len(cfg.ceph_report.osds)} nodes with osds")
    logger.info(f"Found {len(cfg.ceph_report.mons)} nodes with mons")
    logger.info(f"Run with {len(cfg.good_hosts)} hosts in total")

    # This variable is updated from main function
    if opts.detect_only:
        logger.info(f"Exiting, as detect-only mode requested")
        return

    async with cfg.conn_pool:
        nodes_info = []
        for node in cfg.good_hosts:
            async with cfg.conn_pool.connection(node) as conn:
                nodes_info.append({
                    'name': await get_hostname(conn),
                    'ssh_enpoint': node,
                    'all_ips': await get_all_ips(conn)
                })

        storage.put_raw(json.dumps(nodes_info).encode(), "hosts.json")

        collectors_coro = get_collectors(storage=storage, opts=opts, pool=cfg.conn_pool, inventory=cfg.good_hosts,
                                         report=cfg.ceph_report, master_hostname=cfg.ceph_master)
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
            for node in cfg.good_hosts:
                with ignore_all:
                    async with cfg.conn_pool.connection(node) as conn:
                        storage.put_raw((await conn.conn.sys.get_logs()).encode(), f"rpc_logs/{node}.txt")

    logger.info(f"Totally collected data from {len(cfg.good_hosts)} nodes")

    osd_count = {hostname: len(ids) for hostname, ids in cfg.osd_nodes.items()}

    for node in sorted(cfg.good_hosts):
        if node in cfg.mon_nodes and node in cfg.osd_nodes:
            logger.info(f"Node {node} has mon and {len(cfg.osd_nodes[node])} osds")
        elif node in cfg.osd_nodes:
            logger.info(f"Node {node} has {osd_count[node]} osds")
        elif node in cfg.mon_nodes:
            logger.info(f"Node {node} has mon")

    logger.info(f"Totally found {len(cfg.mon_nodes)} monitors, {len(osd_count)} " +
                f"OSD nodes with {sum(osd_count.values())} OSD daemons")

    if output_folder and opts.prometheus:
        await collect_prom(opts.prometheus, cfg.inventory, Path(output_folder), opts.prometheus_interval)

    return result


MiB = 2 ** 20
GiB = 2 ** 30


async def start_coro(conn: IAgentRPCNode, hostname: str, cfg: RemoteNodesCfg, all_names: List[str]) -> None:
    cmds = [f'rados {cfg.ceph_extra_args_s} --format json df',
            f'ceph {cfg.ceph_extra_args_s} --format json df',
            f'ceph {cfg.ceph_extra_args_s} --format json -s']

    try:
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
                                       collection_end_time=time.time() + 24 * 60 * 60,
                                       packer_name='compact',
                                       cmd_timeout=cfg.opts.cmd_timeout,
                                       ceph_extra_args=cfg.ceph_extra_args)

        await conn.conn.ceph.start_historic_collection(hst)
    except:
        import traceback
        traceback.print_exc()
        raise


async def start_historic(opts: Any) -> None:
    cfg = await do_preconfig(opts)
    all_names = cfg.get_nodes(CephRole.osd)

    async with cfg.conn_pool:
        mp = rpc_map(cfg.conn_pool, start_coro, all_names, cfg=cfg, all_names=all_names)
        async for hostname, res in mp:
            prefix = f"{hostname:>20s} {'Primary ' if hostname == all_names[0] else ''}"
            if isinstance(res, Exception):
                print(f"{prefix}Failed to start: {res}")
            else:
                print(f"{prefix}OK")


async def status_historic(opts: Any) -> None:
    async def coro(conn: IAgentRPCNode, hostname: str) -> Optional[HistoricCollectionStatus]:
        try:
            return await conn.conn.ceph.get_historic_collection_status()
        except Exception:
            import traceback
            traceback.print_exc()

    cfg = await do_preconfig(opts)
    async with cfg.conn_pool:
        async for hostname, res in rpc_map(cfg.conn_pool, coro, cfg.get_nodes(CephRole.osd)):
            if isinstance(res, Exception):
                print(f"{hostname:>20s} Failed: {res}")
            elif res:
                hst: HistoricCollectionStatus = cast(HistoricCollectionStatus, res)
                status = 'RUNNING' if hst.cfg else 'NOT_RUNNING'
                print(f"{hostname:>20s} | {status:>10s} | {b2ssize(hst.file_size):>10s}B")


async def stop_historic(opts: Any) -> None:
    async def coro(conn: IAgentRPCNode, hostname: str) -> None:
        await conn.conn.ceph.stop_historic_collection()

    cfg = await do_preconfig(opts)
    async with cfg.conn_pool:
        async for hostname, res in rpc_map(cfg.conn_pool, coro, cfg.get_nodes(CephRole.osd)):
            if isinstance(res, Exception):
                print(f"{hostname:>20s} Failed to stop: {res}")
            else:
                print(f"{hostname:>20s} OK")


async def remove_historic(opts: Any) -> None:
    async def coro(conn: IAgentRPCNode, hostname: str) -> None:
        await conn.conn.ceph.remove_historic_data()

    cfg = await do_preconfig(opts)
    async with cfg.conn_pool:
        async for hostname, res in rpc_map(cfg.conn_pool, coro, cfg.get_nodes(CephRole.osd)):
            if isinstance(res, Exception):
                print(f"{hostname:>20s} Failed to clean: {res}")
            else:
                print(f"{hostname:>20s} Wiped")


async def collect_historic(opts: Any) -> None:
    async def coro(conn: IAgentRPCNode, hostname: str, path: Path, fname: str) -> int:
        async with conn.conn.streamed.ceph.get_collected_historic_data(0) as data_iter:
            with (path / fname.format(hostname=hostname)).open("wb") as fd:
                async for tp, chunk in data_iter:
                    assert tp == BlockType.binary
                    fd.write(chunk)
                return fd.tell()

    cfg = await do_preconfig(opts)
    path = Path(opts.output_folder)
    if not path.exists():
        path.mkdir(parents=True)
    part = get_cluster_name_part(opts.customer, opts.cluster)
    fname = f"historic_ops.{part}.{{hostname}}.bin"

    async with cfg.conn_pool:
        async for hostname, res in rpc_map(cfg.conn_pool, coro, cfg.get_nodes(CephRole.osd), path=path, fname=fname):
            if isinstance(res, Exception):
                print(f"{hostname:>20s} Failed to collect: {res}")
            else:
                print(f"{hostname:>20s} OK")


def parse_args(argv: List[str]) -> Any:

    root_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                          usage='%(prog)s [options] [-- ceph_extra_args]')

    subparsers = root_parser.add_subparsers(dest='subparser_name')

    # ------------------------------------------------------------------------------------------------------------------
    collect_config = subparsers.add_parser('collect_config', help='Collect data')
    collect_config.add_argument("--config", default=None, help="Config path")

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
    collect_parser.add_argument("--wipe", action='store_true', help="Wipe results directory before store data")
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
    historic_start.add_argument('--min-disk-free', metavar='SIZE_GiB', default=50, type=int,
                                help='Min disk free size in GiB to left')
    historic_start.add_argument('--storage', metavar='FILE', default="/var/lib/mirantis/agent/ceph_historic_log.bin",
                                help='Remote file to store logs into')

    # ------------------------------------------------------------------------------------------------------------------

    historic_status = subparsers.add_parser('historic_status', help='Show status of historic collection')
    historic_stop = subparsers.add_parser('historic_stop', help='Stop historic collection')
    historic_collect = subparsers.add_parser('historic_collect', help='Download collected historic data')
    historic_remove = subparsers.add_parser('historic_remove', help='Wipe all historic records')

    # ------------------------------------------------------------------------------------------------------------------

    upload = subparsers.add_parser('upload', help='Upload report to server')
    upload.add_argument("--base-folder", default=".", help="Base folder for all paths")
    upload.add_argument('--url', required=True, help="Url to upload to")
    upload.add_argument('--key', default=str(get_file("enc_key.pub")),
                        help="Server open key for data encryption (%(default)s)")
    upload.add_argument('--cert', default=str(get_file("mira_report_storage.crt")),
                        help="Storage server cert file (%(default)s)")
    upload.add_argument('--upload-script-path',
                        default=str(get_file("upload.sh")), help="upload.sh path (%(default)s)")
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
        parser.add_argument("--agent-cfg", default=None, help="RPC agent config path")

    # ------------------------------------------------------------------------------------------------------------------

    for parser in (collect_parser, historic_collect):
        parser.add_argument("--cluster", help=f"Cluster name, should match {CLIENT_NAME_RE}",
                            type=re_checker(CLIENT_NAME_RE), required=True)
        parser.add_argument("--customer", help=f"Customer name, should match {CLUSTER_NAME_RE}",
                            type=re_checker(CLUSTER_NAME_RE), required=True)
        parser.add_argument("--output-folder", default="/tmp/last_historic",
                            help="Folder to put result to (%(default)s)")

    # ------------------------------------------------------------------------------------------------------------------

    for parser in (collect_parser, historic_start, historic_stop, historic_collect, historic_status, historic_remove):
        parser.add_argument("--cmd-timeout", default=60, type=int, help="Cmd's run timeout")
        parser.add_argument("--max-conn", default=16, type=int, help="Max connection per node")
        parser.add_argument("--inventory", metavar='FILE',
                            help=f"Path to file with list of ssh ip/names of ceph nodes, " +
                                 f"use {CEPH_DETECT_INVENTORY} to autodetect from ceph. " +
                                 f"If not provided - will use agent inventory")
        parser.add_argument("--base-folder", default=str(Path(".").resolve()),
                            help="Base folder for all paths (%(default)s)")
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


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    log_config = get_file("logging.json")

    if opts.subparser_name == 'collect_config':
        # make opts from config
        opts.subparser_name = 'collect'

    if opts.subparser_name == 'collect':
        try:
            output_folder, output_arch = check_and_prepare_paths(opts)
            setup_logging(log_config, opts.log_level, output_folder, opts.persistent_log)
            logger.info(repr(argv))

            result = asyncio.run(run_collection(opts, output_folder, output_arch))

            if output_folder:
                if opts.dont_pack_result:
                    logger.warning("Unpacked tree is kept as --dont-pack-result option is set, so no archive created")
                    print("Result stored into", output_folder)
                else:
                    assert output_arch is not None
                    pack_output_folder(output_folder, output_arch)
                    print("Result saved into", output_arch)
                    shutil.rmtree(output_folder)

            res_str = "\n".join(f"       {key:>20s}: {val!r}" for key, val in result.items())
            logger.debug(f"Results:\n{res_str}")

        except ReportFailed:
            return 1
        except Exception:
            logger.exception("During make_storage/collect")
            raise
    elif opts.subparser_name == 'upload':
        setup_logging(log_config, opts.log_level, None, opts.persistent_log)
        try:
            encrypt_and_upload(url=opts.url,
                               report_file=opts.report,
                               key_file=opts.key,
                               web_cert_file=opts.cert,
                               http_user_password=opts.http_creds)
        except subprocess.CalledProcessError:
            pass
    else:
        try:
            if opts.subparser_name == 'historic_start':
                coro = start_historic(opts)
            elif opts.subparser_name == 'historic_status':
                coro = status_historic(opts)
            elif opts.subparser_name == 'historic_stop':
                coro = stop_historic(opts)
            elif opts.subparser_name == 'historic_collect':
                coro = collect_historic(opts)
            elif opts.subparser_name == 'historic_remove':
                coro = remove_historic(opts)
            else:
                print("Not implemented", file=sys.stderr)
                return 1
            asyncio.run(coro)
        except ReportFailed:
            return 1

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

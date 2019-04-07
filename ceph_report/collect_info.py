import os
import sys
import json
import shutil
import struct
import asyncio
import os.path
import argparse
import datetime
import tempfile
import traceback
import subprocess
import logging.config
from pathlib import Path
from dataclasses import dataclass
from typing import Callable, Coroutine, NamedTuple
from typing import Optional, Tuple, Dict, Any, List, Union

import agent
from agent.client import AsyncRPCClient, IAgentRPCNode
from cephlib.classes import CephReleases
from cephlib.discover import discover_report, parse_ceph_version, CephReport
from koder_utils.storage.storage import make_storage, IStorageNNP
from koder_utils.rpc_node import IAsyncNode, LocalHost, get_hostname
from koder_utils.utils import ignore_all

from .collectors import (CephOSDCollector, CephMonCollector, CephMasterCollector, NodeCollector,
                         LUMINOUS_MAX_PG, DEFAULT_MAX_PG, AUTOPG)
from .utils import CLIENT_NAME_RE, CLUSTER_NAME_RE, re_checker, read_inventory, get_file, setup_logging
from .prom_query import get_block_devs_loads


logger = logging.getLogger('collect')

# ------------------------  Collect coordinator functions --------------------------------------------------------------


class ReportFailed(RuntimeError):
    pass


@dataclass
class ExceptionWithNode(Exception):
    exc: Exception
    hostname: str


async def get_collectors(storage: IStorageNNP,
                         opts: Any,
                         master: IAsyncNode,
                         nodes: List[IAgentRPCNode],
                         report: CephReport) -> List[Tuple[Callable[[], Coroutine[Any, Any, None]], str]]:

    hostname = await get_hostname(master)
    res = [(CephMasterCollector(storage, opts, master, hostname, report).collect, hostname)]

    mon_nodes = {mon.name for mon in report.mons}
    osd_nodes = {osd.hostname for osd in report.osds}

    if not opts.ceph_master_only:
        for node in nodes:
            hostname = await get_hostname(master)
            if hostname in mon_nodes:
                res.append((CephOSDCollector(storage, opts, node, hostname, report).collect, hostname))

            if hostname in osd_nodes:
                res.append((CephMonCollector(storage, opts, node, hostname, report).collect, hostname))

            res.append((NodeCollector(storage, opts, node, hostname).collect, hostname))
    return res


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
                              input=(http_user_password + "\n").encode("utf8"),
                              timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.error(f"Fail to upload data: upload timeout")
        raise ReportFailed()

    finally:
        if os.path.exists(enc_report):
            os.unlink(enc_report)

    if proc.returncode != 0:
        logger.error(f"Fail to upload data: {proc.stdout.decode('utf8').strip()}")
        raise ReportFailed()

    logger.info("File successfully uploaded")


def pack_output_folder(out_folder: str, out_file: str):
    cmd = ['tar', "--create", "--gzip", "--file", str(out_file), *os.listdir(out_folder)]
    tar_res = subprocess.run(cmd, cwd=out_folder)
    if tar_res.returncode != 0:
        logger.error(f"Fail to archive results. Please found raw data at {out_folder!r}")
    else:
        logger.info(f"Result saved into {out_file!r}")


def check_and_prepare_paths(opts: Any) -> Tuple[str, Optional[str], Optional[str]]:
    inv_path = os.path.join(opts.base_folder, opts.inventory) if opts.inventory else None

    # verify options
    if inv_path and not os.path.isfile(inv_path):
        print(f"--inventory value must be file {opts.inventory!r}", file=sys.stderr)
        raise ReportFailed(f"--inventory value must be file {opts.inventory!r}")

    ctime = f"{datetime.datetime.now():%Y_%h_%d.%H_%M}"
    folder_name = f"ceph_report.{opts.customer}.{opts.cluster}.{ctime}"
    arch_name = f"ceph_report.{opts.customer}.{opts.cluster}.{ctime}.tar.gz"

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

    return inv_path, output_folder, output_arch


def get_api_key(api_key_path: Path) -> str:
    if not api_key_path.is_file():
        logger.critical(f"Can't find API key at {api_key_path}")
        raise ReportFailed(f"Can't find API key at {api_key_path}")
    return api_key_path.open().read()


def get_certificates(certs_folder: Path) -> Dict[str, Path]:
    certificates: Dict[str, Path] = {}

    if not certs_folder.is_dir():
        if certs_folder:
            logger.critical(f"Can't cert folder at {certs_folder}")
            raise ReportFailed(f"Can't cert folder at {certs_folder}")
        else:
            logger.warning(f"Can't cert folder at {certs_folder}")

    for file in certs_folder.glob('agent_server.*.cert'):
        node_name = file.name.split(".", 1)[1].rsplit(".", 1)[0]
        certificates[node_name] = file

    return certificates


async def collect_prom(prom_url: str, inventory: List[str], target: Path, time_range_hours: int):
    data = await get_block_devs_loads(prom_url, inventory, time_range_hours * 60)

    # data is {metric: str => {(host: str, device: str) => [values: float]}}

    for metric, values in data.items():
        for (host, device), measurements in values.items():
            with (target / 'monitoring' / f"{metric}@{device}@{host}.bin").open("wb") as fd:
                if measurements:
                    fd.write(struct.pack('!%sd' % len(measurements), *measurements))


async def get_master_node(self) -> Optional[IAsyncNode]:
    if self.opts.ceph_master:
        if self.opts.ceph_master == '-':
            return self.first_inventory_node
        logger.info(f"Connecting to ceph-master: {self.opts.ceph_master}")
        nodes, err = await self.connect_and_init([self.opts.ceph_master])
        assert len(nodes) + len(err) == 1
        if err:
            logger.error(f"Can't connect to ceph-master {self.opts.ceph_master}: {err[0]}")
            return None
        return nodes[0]
    else:
        return LocalHost()


async def check_master(conn: IAsyncNode):
    if (await conn.run('which ceph')).returncode != 0:
        logger.error("No 'ceph' command available on master node.")
        raise ReportFailed()

    version = parse_ceph_version(await conn.run_str('ceph --version'))
    if version.release < CephReleases.jewel:
        logger.error(f"Too old ceph version {version!r}, only jewel and later ceph supported")
        raise ReportFailed()


async def raise_with_node(func, hostname: str) -> None:
    try:
        await func()
    except Exception as local_exc:
        raise ExceptionWithNode(local_exc, hostname) from local_exc


async def run_collection(opts: Any, inv_path: Optional[str], output_folder: Optional[str], output_arch: Optional[str]):
    if output_folder:
        if opts.dont_pack_result:
            logger.info("Store data into %r", output_folder)
        else:
            logger.info("Will store results into %s", output_arch)
            logger.info("Temporary folder %r", output_folder)
        storage = make_storage(output_folder, existing=False, serializer='raw')
    else:
        storage = None

    api_key_path = Path(opts.api_key)
    api_key = get_api_key(api_key_path)
    certificates_path = Path(opts.certs_folder)
    certificates = get_certificates(certificates_path)

    inventory = list(read_inventory(inv_path))

    conn_pool = ConnectionPool(api_key, certificates, max_conn_per_node=opts.max_conn)

    failed = []
    for node in inventory:
        try:
            conn = await conn_pool.get_conn(node)
        except:
            failed.append(node)

        conn_pool.release_conn(node, conn)

    if failed and opts.must_connect_to_all:
        for ip_or_hostname, err in failed:
            logger.error(f"Can't connect to extra node {ip_or_hostname}: {err}")
        raise ReportFailed()

    # find master
    if opts.ceph_master == 'localhost':
        master_node_l = LocalHost()
    else:
        name = inventory[0] if opts.ceph_master == '-' else opts.ceph_master
        master_node_l = [conn for conn in conns if conn.conn_addr == name]

    if len(master_node_l) != 1:
        logger.error(f"Inventory empty or has duplicated nodes or empty")
        raise ReportFailed()

    master_node = master_node_l[0]

    await check_master(master_node)

    if opts.ceph_master_only:
        return
    else:
        ceph_report = await discover_report(master_node, opts)

        logger.info(f"Found {len(ceph_report.osds)} nodes with osds")
        logger.info(f"Found {len(ceph_report.mons)} nodes with mons")
        logger.info(f"Run with {len(conns)} hosts in total")

    nodes = ...

    # This variable is updated from main function
    if opts.detect_only:
        return

    storage.put_raw(json.dumps([node.dct() for node in nodes]).encode('utf8'), "hosts.json")

    all_coros = await get_collectors(storage, opts, master_node, nodes, ceph_report)

    try:
        await asyncio.gather(*(raise_with_node(func, node) for func, node in all_coros), return_exceptions=False)
    except ExceptionWithNode as exc:
        logger.error(f"Exception happened during collecting from node {exc.hostname} (see full tb below): {exc.exc}")
        raise
    except Exception as exc:
        logger.error(f"Exception happened(see full tb below): {exc}")
        raise
    finally:
        logger.info("Collecting logs and teardown RPC servers")
        for node in nodes:
            with ignore_all:
                storage.put_raw((await node.rpc_conn.sys.get_logs()).encode('utf8'), f"rpc_logs/{node.name}.txt")

    logger.info(f"Totally collected data from {len(nodes)} nodes")

    for node in sorted(nodes, key=lambda x: x.hostname):
        if node.osds and node.mon:
            logger.info(f"Node {node.hostname} has mon and {len(node.osds)} osds")
        elif node.osds:
            logger.info(f"Node {node.hostname} has {len(node.osds)} osds")
        elif node.mon:
            logger.info(f"Node {node.hostname} has mon")

    logger.info("Totally found %s monitors, %s OSD nodes with %s OSD daemons",
                sum(1 for node in nodes if node.mon),
                sum(1 for node in nodes if node.osds),
                sum(len(node.osds) for node in nodes if node.osds))

    if output_folder and opts.prometheus:
        await collect_prom(opts.prometheus, inventory, Path(output_folder), opts.prometheus_interval)


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparsers = parser.add_subparsers(dest='subparser_name')

    collect_parser = subparsers.add_parser('collect', help='Collect data')
    collect_parser.add_argument("--ceph-master", metavar="NODE", default='-',
                                help="Run all ceph cluster commands from NODE, (first inventory node by default)")
    collect_parser.add_argument("--inventory", metavar='FILE',
                                help="Path to file with list of ssh ip/names of ceph nodes")
    collect_parser.add_argument("--dont-pack-result", action="store_true", help="Don't create archive")
    collect_parser.add_argument("--cluster", help=f"Cluster name, should match {CLIENT_NAME_RE}",
                                type=re_checker(CLIENT_NAME_RE), required=True)
    collect_parser.add_argument("--customer", help=f"Customer name, should match {CLUSTER_NAME_RE}",
                                type=re_checker(CLUSTER_NAME_RE), required=True)
    collect_parser.add_argument("--output-folder", default="/tmp", help="Folder to put result to (%(default)s)")
    collect_parser.add_argument("--base-folder", default=str(Path(".").resolve()),
                                help="Base folder for all paths (%(default)s)")

    # collection flags
    collect_parser.add_argument("--max-conn", default=16, type=int, help="Max connection per node")
    collect_parser.add_argument("--no-rbd-info", action='store_true', help="Don't collect info for rbd volumes")
    collect_parser.add_argument("--ceph-master-only", action="store_true",
                                help="Run only ceph master data collection, no info from " +
                                "osd/monitors would be collected")
    collect_parser.add_argument("--ceph-extra", default="", help="Extra opts to pass to 'ceph' command")
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
    collect_parser.add_argument("--must-connect-to-all", action="store_true",
                                help="Must successfully connect to all ceph nodes")

    default_certs_folder = Path(agent.__file__).resolve().parent.parent / 'agent_client_keys'
    collect_parser.add_argument("--wipe", action='store_true', help="Wipe results directory before store data")
    collect_parser.add_argument("--cmd-timeout", default=60, type=int, help="Cmd's run timeout")
    collect_parser.add_argument("--api-key", default=str(default_certs_folder / 'agent_api.key'),
                                help="RPC api key file path (%(default)s)")
    collect_parser.add_argument("--certs-folder", default=str(default_certs_folder),
                                help="Folder with rpc_conn ssl certificates (%(default)s)")

    collect_parser.add_argument("--prometheus", default=None, help="Prometheus url to collect data")
    collect_parser.add_argument("--prometheus-interval", default=24 * 7, type=int,
                                help="For how many hours to the past grab data from prometheus")

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

    for p in (upload, collect_parser):
        p.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                       help="Console log level, see logging.json for defaults")
        p.add_argument("--persistent-log", action="store_true",
                       help="Log to /var/log/ceph_report_collector.log as well")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    log_config = get_file("logging.json")

    if opts.subparser_name == 'collect':
        try:
            inv_path, output_folder, output_arch = check_and_prepare_paths(opts)
            setup_logging(opts.log_level, log_config, output_folder, opts.persistent_log)
            logger.info(repr(argv))

            asyncio.run(run_collection(opts, inv_path, output_folder, output_arch))

            if output_folder:
                if opts.dont_pack_result:
                    logger.warning("Unpacked tree is kept as --dont-pack-result option is set, so no archive created")
                    print("Result stored into", output_folder)
                else:
                    assert output_arch is not None
                    pack_output_folder(output_folder, output_arch)
                    print("Result saved into", output_arch)
                    shutil.rmtree(output_folder)
        except ReportFailed:
            return 1
        except Exception:
            logger.exception("During make_storage/collect")
            raise
    else:
        assert opts.subparser_name == 'upload'
        setup_logging(opts.log_level, log_config, None, opts.persistent_log)
        try:
            encrypt_and_upload(url=opts.url,
                               report_file=opts.report,
                               key_file=opts.key,
                               web_cert_file=opts.cert,
                               http_user_password=opts.http_creds)
        except subprocess.CalledProcessError:
            pass

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

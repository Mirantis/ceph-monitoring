import os
import re
import sys
import json
import shutil
import logging
import os.path
import asyncio
import argparse
import datetime
import tempfile
import subprocess
import logging.config
from pathlib import Path
from typing import Dict, Any, List, Tuple, Set, Callable, Optional, Type, Iterator, Coroutine

from dataclasses import dataclass

from cephlib.storage import make_storage, IStorageNNP

AGENT_PATH = os.environ.get("RPC_AGENT_PATH", "/opt/mirantis/agent")
sys.path.append(AGENT_PATH)

from .collect_node_classes import INode, Node, Local
from .collectors import Collector, CephDataCollector, NodeCollector, LUMINOUS_MAX_PG, DEFAULT_MAX_PG, AUTOPG
from .collect_utils import init_rpc_and_fill_data, CephServices, discover_ceph_services, iter_extra_host


logger = logging.getLogger('collect')


base_files_path = Path(sys.argv[0]).parent / 'files'


# ------------------------  Collect coordinator functions --------------------------------------------------------------


ALL_COLLECTORS: List[Type[Collector]] = [CephDataCollector, NodeCollector]
ALL_COLLECTORS_MAP: Dict[str, Type[Collector]] = {collector.name: collector for collector in ALL_COLLECTORS}

CollectorsGenerator = Iterator[Tuple[Callable[[], Coroutine[Any, Any, None]], INode]]


class ReportFailed(RuntimeError):
    pass


@dataclass
class ExceptionWithNode(Exception):
    exc: Exception
    node: INode


class CollectorCoordinator:
    def __init__(self, storage: IStorageNNP, inventory: Optional[str], opts: Any, api_key: str,
                 certificates: Dict[str, Any]) -> None:
        self.nodes: List[Node] = []
        self.opts = opts
        self.storage = storage
        self.inventory = inventory
        self.api_key = api_key
        self.ceph_master_node: Optional[INode] = None
        self.nodes: List[Node] = []
        self.certificates = certificates

    async def connect_and_init(self, ips_or_hostnames: List[str]) -> Tuple[List[Node], List[Tuple[str, str]]]:
        return await init_rpc_and_fill_data(ips_or_hostnames, access_key=self.api_key,
                                            certificates=self.certificates)

    async def get_master_node(self) -> Optional[INode]:
        if self.opts.ceph_master:
            logger.info(f"Connecting to ceph-master: {self.opts.ceph_master}")
            nodes, err = await self.connect_and_init([self.opts.ceph_master])
            assert len(nodes) + len(err) == 1
            if err:
                logger.error(f"Can't connect to ceph-master {self.opts.ceph_master}: {err[0]}")
                return None
            return nodes[0]
        else:
            return Local()

    @staticmethod
    def fill_ceph_services(nodes: List[Node], ceph_services: CephServices) -> Set[str]:
        all_nodes_ips: Dict[str, Node] = {}
        for node in nodes:
            all_nodes_ips.update({ip: node for ip in node.all_ips})

        missing_ceph_ips = set()
        for ip, mon_name in ceph_services.mons.items():
            if ip not in all_nodes_ips:
                missing_ceph_ips.add(ip)
            else:
                all_nodes_ips[ip].mon = mon_name

        for ip, osds_info in ceph_services.osds.items():
            if ip not in all_nodes_ips:
                missing_ceph_ips.add(ip)
            else:
                all_nodes_ips[ip].osds = osds_info

        return missing_ceph_ips

    async def connect_to_nodes(self) -> List[Node]:

        self.ceph_master_node = await self.get_master_node()
        if self.ceph_master_node is None:
            raise ReportFailed()

        if (await self.ceph_master_node.run('which ceph')).returncode != 0:
            logger.error("No 'ceph' command available on master node.")
            raise ReportFailed()

        ceph_services = None if self.opts.ceph_master_only \
            else await discover_ceph_services(self.ceph_master_node, self.opts)

        nodes, errs = await self.connect_and_init(list(iter_extra_host(self.inventory)))
        if errs:
            for ip_or_hostname, err in errs:
                logging.error(f"Can't connect to extra node {ip_or_hostname}: {err}")
            raise ReportFailed()

        # add information from monmap and osdmap about ceph services and connect to extra nodes
        if ceph_services:

            missing_ceph_ips = self.fill_ceph_services(nodes, ceph_services)

            ceph_nodes, errs = await self.connect_and_init(list(missing_ceph_ips))
            if errs:
                lfunc = logging.error if self.opts.all_ceph else logging.warning
                lfunc("Can't connect to ceph nodes")
                for ip, err in errs:
                    lfunc(f"    {ip} => {err}")

                if self.opts.all_ceph:
                    raise ReportFailed()

            missing_mons = {ip: mon for ip, mon in ceph_services.mons.items() if ip in missing_ceph_ips}
            missing_osds = {ip: osds for ip, osds in ceph_services.osds.items() if ip in missing_ceph_ips}

            missing_ceph_ips2 = self.fill_ceph_services(ceph_nodes, CephServices(missing_mons, missing_osds))
            assert not missing_ceph_ips2

            nodes += ceph_nodes

        logger.info("Found %s nodes with osds", sum(1 for node in nodes if node.osds))
        logger.info("Found %s nodes with mons", sum(1 for node in nodes if node.mon))
        logger.info(f"Run with {len(nodes)} hosts in total")

        for node in nodes:
            logger.debug(f"Node: {node.ceph_info()}")

        return nodes

    def collect_ceph_data(self) -> CollectorsGenerator:
        assert self.ceph_master_node is not None
        yield CephDataCollector(self.storage,
                                self.opts,
                                self.ceph_master_node,
                                pretty_json=not self.opts.no_pretty_json).collect_master, self.ceph_master_node

        if not self.opts.ceph_master_only:
            for node in self.nodes:
                if node.mon:
                    collector = CephDataCollector(self.storage,
                                                  self.opts,
                                                  node,
                                                  pretty_json=not self.opts.no_pretty_json)

                    yield collector.collect_monitor, node

                if node.osds:
                    collector = CephDataCollector(self.storage,
                                                  self.opts,
                                                  node,
                                                  pretty_json=not self.opts.no_pretty_json)
                    yield collector.collect_osd, node

    def collect_node_data(self) -> CollectorsGenerator:
        for node in self.nodes:
            yield NodeCollector(self.storage,
                                self.opts,
                                node,
                                pretty_json=not self.opts.no_pretty_json).collect, node

    async def collect(self):
        # This variable is updated from main function
        self.nodes = await self.connect_to_nodes()
        assert self.ceph_master_node is not None
        if self.opts.detect_only:
            return

        self.storage.put_raw(json.dumps([node.dct() for node in self.nodes]).encode('utf8'), "hosts.json")

        async def raise_with_node(func, node: INode):
            try:
                await func()
            except Exception as local_exc:
                raise ExceptionWithNode(local_exc, node) from local_exc

        all_coros = list(self.collect_ceph_data()) + list(self.collect_node_data())
        try:
            await asyncio.gather(*(raise_with_node(func, node) for func, node in all_coros), return_exceptions=False)
        except ExceptionWithNode as exc:
            logger.error(f"Exception happened during collecting from node {exc.node} (see full tb below): {exc.exc}")
            raise
        except Exception as exc:
            logger.error(f"Exception happened(see full tb below): {exc}")
            raise
        finally:
            logger.info("Collecting logs and teardown RPC servers")
            for node in self.nodes + [self.ceph_master_node]:
                try:
                    self.storage.put_raw((await node.rpc.sys.get_logs()).encode('utf8'),
                                         f"rpc_logs/{node.name}.txt")
                except Exception:
                    pass

        logger.info(f"Totally collected data from {len(self.nodes)} nodes")

        for node in sorted(self.nodes, key=lambda x: x.name):
            if node.osds and node.mon:
                logger.info(f"Node {node.name} has mon and {len(node.osds)} osds")
            elif node.osds:
                logger.info(f"Node {node.name} has {len(node.osds)} osds")
            elif node.mon:
                logger.info(f"Node {node.name} has mon")

        logger.info("Totally found %s monitors, %s OSD nodes with %s OSD daemons",
                    sum(1 for node in self.nodes if node.mon),
                    sum(1 for node in self.nodes if node.osds),
                    sum(len(node.osds) for node in self.nodes if node.osds))


def encrypt_and_upload(url: str,
                       report_file: str,
                       key_file: str,
                       web_cert_file: str,
                       http_user_password: str,
                       timeout: int = 360):
    fd, enc_report = tempfile.mkstemp(prefix="ceph_report_", suffix=".enc")
    os.close(fd)
    cmd = ["bash", str(base_files_path / "upload.sh"), report_file, enc_report, key_file, web_cert_file, url]

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


def setup_logging(log_level: str, log_config_file: Path, out_folder: Optional[str], persistent_log: bool = False):
    log_config = json.load(log_config_file.open())
    handlers = ["console"]

    if out_folder:
        handlers.append("log_file")
        log_file = os.path.join(out_folder, "log.txt")
        log_config["handlers"]["log_file"]["filename"] = log_file
    else:
        del log_config["handlers"]["log_file"]

    if persistent_log:
        handlers.append("persistent_log_file")
    else:
        del log_config["handlers"]["persistent_log_file"]

    if log_level is not None:
        log_config["handlers"]["console"]["level"] = log_level

    for key in list(log_config['loggers']):
        log_config['loggers'][key]["handlers"] = handlers

    logging.config.dictConfig(log_config)


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


def collect(opts: Any, inv_path: Optional[str], output_folder: Optional[str], output_arch: Optional[str]):
    if output_folder:
        if opts.dont_pack_result:
            logger.info("Store data into %r", output_folder)
        else:
            logger.info("Will store results into %s", output_arch)
            logger.info("Temporary folder %r", output_folder)
        storage = make_storage(output_folder, existing=False, serializer='raw')
    else:
        storage = None

    api_key_path = Path(AGENT_PATH) / 'agent_client_keys/agent_api.key' \
        if opts.api_key is None else Path(opts.api_key)

    api_key = get_api_key(api_key_path)

    certificates_path = (Path(AGENT_PATH) / 'agent_client_keys') \
        if opts.certs_folder is None else Path(opts.certs_folder)

    certificates = get_certificates(certificates_path)

    # run collect
    asyncio.run(CollectorCoordinator(storage, opts=opts, inventory=inv_path, api_key=api_key,
                                     certificates=certificates).collect())

    # compress/commit output folder
    if output_folder:
        if opts.dont_pack_result:
            logger.warning("Unpacked tree is kept as --dont-pack-result option is set, so no archive created")
            print("Result stored into", output_folder)
        else:
            assert output_arch is not None
            pack_output_folder(output_folder, output_arch)
            print("Result saved into", output_arch)
            shutil.rmtree(output_folder)


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparsers = parser.add_subparsers(dest='subparser_name')

    collect_parser = subparsers.add_parser('collect', help='Collect data')
    collect_parser.add_argument("--ceph-master", metavar="NODE", help="Run all ceph cluster commands from NODE")
    collect_parser.add_argument("--inventory", metavar='FILE',
                                help="Path to file with list of ssh ip/names of ceph nodes")
    collect_parser.add_argument("--dont-pack-result", action="store_true", help="Don't create archive")
    collect_parser.add_argument("--cluster", help="Cluster name, should match [a-zA-Z_0-9-]+", required=True)
    collect_parser.add_argument("--customer", help="Customer name, should match [a-zA-Z_0-9-]+", required=True)
    collect_parser.add_argument("--output-folder", help="Folder to put result to", default="/tmp")
    collect_parser.add_argument("--base-folder", default=".", help="Base folder for all paths")

    # collection flags
    collect_parser.add_argument("--no-rbd-info", action='store_true', help="Don't collect info for rbd volumes")
    collect_parser.add_argument("--ceph-master-only", action="store_true",
                                help="Run only ceph master data collection, no info from " +
                                "osd/monitors would be collected")
    collect_parser.add_argument("--ceph-extra", default="", help="Extra opts to pass to 'ceph' command")
    collect_parser.add_argument("--detect-only", action="store_true",
                                help="Don't collect any data, only detect cluster nodes")
    collect_parser.add_argument("--no-pretty-json", action="store_true", help="Don't prettify json data")

    collect_parser.add_argument("--max-pg-dump-count", default=AUTOPG, type=int,
                                help=f"maximum PG count to by dumped with 'pg dump' cmd, by default {LUMINOUS_MAX_PG} "
                                + f"for luminous, {DEFAULT_MAX_PG} for other ceph versions")
    collect_parser.add_argument("--ceph-log-max-lines", default=10000, type=int, help="Max lines from osd/mon log")
    collect_parser.add_argument("--must-connect-to-all", action="store_true",
                                help="Must successfully connect to all ceph nodes")

    collect_parser.add_argument("--wipe", action='store_true', help="Wipe results directory before store data")
    collect_parser.add_argument("--cmd-timeout", default=60, type=int, help="Cmd's run timeout")
    collect_parser.add_argument("--api-key", default=None, help="RPC api key file path")
    collect_parser.add_argument("--certs-folder", default=None, help="Folder with rpc ssl certificates")

    upload = subparsers.add_parser('upload', help='Upload report to server')
    upload.add_argument("--base-folder", default=".", help="Base folder for all paths")
    upload.add_argument('--url', required=True, help="Url to upload to")
    upload.add_argument('--key', default=str(base_files_path / "enc_key.pub"),
                        help="Server open key for data encryption")
    upload.add_argument('--cert', default=str(base_files_path / "mira_report_storage.crt"), help="Server cert file")
    upload.add_argument('--upload-script-path', default=str(base_files_path / "upload.sh"), help="upload.sh path")
    upload.add_argument('--http-creds', required=True, help="Http user:password, as provided by mirantis support")
    upload.add_argument('report', help="path to report archive")

    for p in (upload, collect_parser):
        p.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                       help="Console log level, see logging.json for defaults")
        p.add_argument("--persistent-log", action="store_true",
                       help="Log to /var/log/ceph_report_collector.log as well")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    log_config = base_files_path / "logging.json"

    if opts.subparser_name == 'collect':
        try:
            if not re.match("[0-9a-zA-Z_-]+$", opts.cluster):
                logger.error("Cluster name incorrect - %s, should match [0-9a-zA-Z_-]+$", opts.cluster)
                return 1

            if not re.match("[0-9a-zA-Z_-]+$", opts.customer):
                logger.error("Customer name incorrect - %s, should match [0-9a-zA-Z_-]+$", opts.customer)
                return 1

            inv_path, output_folder, output_arch = check_and_prepare_paths(opts)
            setup_logging(opts.log_level, log_config, output_folder, opts.persistent_log)
            logger.info(repr(argv))

            collect(opts, inv_path, output_folder, output_arch)

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

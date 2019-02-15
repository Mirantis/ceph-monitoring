import re
import abc
import sys
import time
import json
import zlib
import array
import socket
import random
import shutil
import logging
import os.path
import tempfile
import argparse
import datetime
import ipaddress
import traceback
import functools
import contextlib
import subprocess
import logging.config
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, Executor
from typing import Dict, Any, List, Tuple, Set, Callable, Optional, Union, Type, Iterator, NamedTuple, cast, Iterable

try:
    import logging_tree
except ImportError:
    logging_tree = None

from agent.agent import ConnectionClosed, SimpleRPCClient
from cephlib.storage import make_storage, IStorageNNP
from cephlib.common import run_locally, tmpnam, setup_logging
from cephlib.rpc import init_node, rpc_run
from cephlib.discover import get_osds_nodes, get_mons_nodes, OSDInfo
from cephlib.sensors_rpc_plugin import unpack_rpc_updates


logger = logging.getLogger('collect')


ExecResult = NamedTuple('ExecResult', [('code', int), ('output_b', bytes), ('output', str)])
AUTOPG = -1


#  -------------------   Node classes and interfaces -------------------------------------------------------------------


class INode(metaclass=abc.ABCMeta):
    name = ""  # type: str

    @abc.abstractmethod
    def run_exc(self, cmd: str, **kwargs) -> bytes:
        pass

    @abc.abstractmethod
    def get_file(self, name: str, compress: bool = False) -> bytes:
        pass

    def run(self, cmd: str, encoding: str = 'utf8', **kwargs) -> ExecResult:
        code = 0
        try:
            raw = self.run_exc(cmd, **kwargs)
        except subprocess.CalledProcessError as exc:
            code = exc.returncode
            raw = (exc.output + (b"" if exc.stderr is None else exc.stderr))

        return ExecResult(code, raw, raw.decode(encoding))

    @abc.abstractmethod
    def exists(self, fname: str) -> bool:
        pass

    @abc.abstractmethod
    def listdir(self, path: str) -> Iterable[str]:
        pass


class Node(INode):
    def __init__(self, ssh_enpoint: str, hostname: str, rpc: SimpleRPCClient) -> None:
        self.ssh_enpoint = ssh_enpoint
        self.hostname = hostname
        self.rpc = rpc
        self.osds = []   # type: List[OSDInfo]
        self.all_ips = {ssh_enpoint}  # type: Set[str]
        self.mon = None  # type: Optional[str]
        self.load_config = {}  # type: Dict[str, Any]

    def __cmp__(self, other: 'Node') -> int:
        x = (self.hostname, self.ssh_enpoint)
        y = (other.hostname, other.ssh_enpoint)
        if x == y:
            return 0
        return 1 if x > y else -1

    def dct(self) -> Dict[str, Any]:
        return {'name': self.hostname, 'ssh_enpoint': self.ssh_enpoint, 'all_ips': list(self.all_ips)}

    @property
    def name(self) -> str:  # type: ignore
        return self.hostname

    def __str__(self) -> str:
        return "Node(name={0.name}, ssh_enpoint={0.ssh_enpoint})".format(self)

    def merge(self, other: 'Node', overwrite_ssh: bool = True):

        if self.mon and not other.mon:
            self.mon = other.mon

        self.osds = list(set(self.osds + other.osds))

        if overwrite_ssh:
            self.ssh_enpoint = other.ssh_enpoint

        if not self.hostname and other.hostname:
            self.hostname = other.hostname

        self.all_ips.update(other.all_ips)

    def run_exc(self, cmd: str, **kwargs) -> bytes:
        assert self.rpc is not None
        assert 'node_name' not in kwargs
        return rpc_run(self.rpc, cmd, node_name=self.name, **kwargs)

    def get_file(self, name: str, compress: bool = False) -> bytes:
        data = self.rpc.fs.get_file(name, compress=compress)
        return zlib.decompress(data) if compress else data

    def ceph_info(self) -> str:
        info = self.name
        if self.mon:
            info += " mon=" + self.mon
        if self.osds:
            info += " osds=[" + ",".join(str(osd.id) for osd in self.osds) + "]"
        return info

    def exists(self, fname: str) -> bool:
        return self.rpc.fs.file_exists(fname)

    def listdir(self, path: str) -> Iterable[str]:
        return self.rpc.fs.listdir(path)


class Local(INode):
    name = 'localhost'

    def run_exc(self, cmd: str, **kwargs) -> bytes:
        assert 'node_name' not in kwargs
        return run_locally(cmd, **kwargs)

    def __str__(self) -> str:
        return "Localhost()"

    def get_file(self, name: str, compress: bool = False) -> bytes:
        return open(name, 'rb').read()

    def exists(self, fname: str) -> bool:
        return os.path.exists(fname)

    def listdir(self, path: str) -> Iterable[str]:
        return [i for i in os.listdir(path) if i not in ('.', '..')]


# ------------ HELPER FUNCTIONS: GENERAL -------------------------------------------------------------------------------


def ip_and_hostname(ip_or_hostname: str) -> Tuple[str, Optional[str]]:
    """returns (ip, maybe_hostname)"""
    try:
        ipaddress.ip_address(ip_or_hostname)
        return ip_or_hostname, None
    except ValueError:
        return socket.gethostbyname(ip_or_hostname), ip_or_hostname


# ------------ HELPER FUNCTIONS: PARSERS -------------------------------------------------------------------------------


def parse_ipa4(data: str) -> Set[str]:
    """
    parse 'ip -o -4 a' output
    """
    res = set()
    # 26: eth0    inet 169.254.207.170/16
    for line in data.split("\n"):
        line = line.strip()
        if line:
            _, dev, _, ip_sz, *_ = line.split()
            ip, sz = ip_sz.split('/')
            ipaddress.IPv4Address(ip)
            res.add(ip)
    return res


def parse_proc_file(fc: str, ignore_err: bool = False) -> Dict[str, str]:
    res = {}  # type: Dict[str, str]
    for ln in fc.split("\n"):
        ln = ln.strip()
        if ln:
            try:
                name, val = ln.split(":")
            except ValueError:
                if not ignore_err:
                    raise
            else:
                res[name.strip()] = val.strip()
    return res


def parse_devices_tree(lsblkdct: Dict[str, Any]) -> Dict[str, str]:
    def fall_down(fnode: Dict[str, Any], root: str, res_dict: Dict[str, str]):
        res_dict['/dev/' + fnode['name']] = root
        for ch_node in fnode.get('children', []):
            fall_down(ch_node, root, res_dict)

    res = {}  # type: Dict[str, str]
    for node in lsblkdct['blockdevices']:
        fall_down(node, '/dev/' + node['name'], res)
        res['/dev/' + node['name']] = '/dev/' + node['name']
    return res


def parse_sockstat_file(fc: str) -> Optional[Dict[str, Dict[str, str]]]:
    res = {}  # type: Dict[str, Dict[str, str]]
    for ln in fc.split("\n"):
        if ln.strip():
            if ':' not in ln:
                return None
            name, params = ln.split(":", 1)
            params_l = params.split()
            if len(params_l) % 2 != 0:
                return None
            res[name] = dict(zip(params_l[:-1:2], params_l[1::2]))
    return res


# ------------ HELPER FUNCTIONS: RPC -----------------------------------------------------------------------------------


def get_host_interfaces(rpc: INode) -> List[Tuple[bool, str]]:
    """Return list of host interfaces, returns pair (is_physical, name)"""
    res = []  # type: List[Tuple[bool, str]]
    content = rpc.run("ls -l /sys/class/net").output

    for line in content.strip().split("\n")[1:]:
        if not line.startswith('l'):
            continue

        params = line.split()
        if len(params) < 11:
            continue

        res.append(('/devices/virtual/' not in params[10], params[8]))
    return res


def get_device_for_file(node: Node, fname: str) -> Tuple[str, str]:
    """Find storage device, on which file is located"""

    dev = node.rpc.fs.get_dev_for_file(fname)
    dev = dev.decode('utf8')
    assert dev.startswith('/dev'), "{!r} is not starts with /dev".format(dev)
    root_dev = dev = dev.strip()
    rr = re.match('^(/dev/[shv]d.*?)\\d+', root_dev)
    if rr:
        root_dev = rr.group(1)
    return root_dev, dev


CephServices = NamedTuple('CephServices', [('mons', Dict[str, str]), ('osds', Dict[str, List[OSDInfo]])])


def discover_ceph_services(master_node: INode, opts: Any, thcount: int = 1) -> CephServices:
    """Find ceph monitors and osds using ceph osd dump and ceph mon_map"""

    exec_func = lambda x: master_node.run_exc(x).decode('utf8')

    mons = {}  # type: Dict[str, str]
    for mon_id, (ip, name) in get_mons_nodes(exec_func, opts.ceph_extra).items():
        assert ip not in mons
        mons[ip] = name

    osds = {}  # type: Dict[str, List[OSDInfo]]
    osd_nodes = get_osds_nodes(exec_func, opts.ceph_extra, get_config=False)
    for ip, osds_info in osd_nodes.items():
        assert ip not in osds
        osds[ip] = osds_info

    return CephServices(mons, osds)


def init_rpc_and_fill_data(ips_or_hostnames: List[str],
                           ssh_opts: str,
                           executor: Executor,
                           with_sudo: bool) -> Tuple[List[Node], List[Tuple[str, str]]]:
    """Connect to nodes and fill Node object with basic node info: ips and hostname"""

    rpc_nodes = []  # type: List[Node]
    failed_nodes = []  # type: List[Tuple[str, str]]

    def init_node_with_code(ips_or_hostname: str) -> Tuple[bool, Union[Node, str]]:
        try:
            rpc, _ = init_node(ips_or_hostname, ssh_opts=ssh_opts, with_sudo=with_sudo)
            hostname = rpc_run(rpc, "hostname", node_name=ips_or_hostname).strip().decode('utf8')
            node = Node(ssh_enpoint=ips_or_hostname, hostname=hostname, rpc=rpc)
            node.all_ips.update(parse_ipa4(node.run("ip -o -4 a").output))
            logger.debug("%s -> %s, %s", node.ssh_enpoint, node.hostname, node.all_ips)
            return True, node
        except Exception as exc:
            return False, str(exc)

    hostnames = set()  # type: Set[str]
    for name_or_ip, (is_ok, node_or_err) in zip(ips_or_hostnames, executor.map(init_node_with_code, ips_or_hostnames)):
        if is_ok:
            assert isinstance(node_or_err, Node)
            rpc_nodes.append(node_or_err)
            assert node_or_err.hostname not in hostnames
            hostnames.add(node_or_err.hostname)
        else:
            assert isinstance(node_or_err, str)
            failed_nodes.append((name_or_ip, node_or_err))

    return rpc_nodes, failed_nodes


def iter_extra_host(opts: Any) -> Iterator[str]:
    """Iterate over all extra hosts from -I & --inventory options, if some"""
    if opts.inventory:
        with open(opts.inventory) as fd:
            for ln in fd:
                ln = ln.strip()
                if ln and not ln.startswith("#"):
                    assert ':' not in ln
                    assert len(ln.split()) == 1
                    yield ln
    yield from opts.node


#  ---------------  COLLECTORS -----------------------------------------------------------------------------------------


CMDRes = namedtuple("CMDRes", ["stdout", "code"])

class Collector:
    """Base class for data collectors. Can collect data for only one node."""
    name = None  # type: str
    collect_roles = []  # type: List[str]

    def __init__(self,
                 storage: IStorageNNP,
                 opts: Any,
                 node: INode,
                 pretty_json: bool = False) -> None:
        self.storage = storage
        self.opts = opts
        self.node = node
        self.pretty_json = pretty_json

    @contextlib.contextmanager
    def chdir(self, path: str):
        """Chdir for point in storage tree, where current results are stored"""
        saved = self.storage
        self.storage = self.storage.sub_storage(path)
        try:
            yield
        finally:
            self.storage = saved

    def save_raw(self, path: str, data: bytes):
        self.storage.put_raw(data, path)

    def save(self, path: str, frmt: str, code: int, data: Union[str, bytes, array.array],
             extra: List[str] = None) -> str:
        """Save results into storage"""
        if code == 0 and frmt == 'json':
            assert isinstance(data, (str, bytes))
            data = json.dumps(json.loads(data), indent=4, sort_keys=True)

        rpath = "{}.{}".format(path, frmt if code == 0 else "err")

        if isinstance(data, str):
            assert extra is None
            self.save_raw(rpath, data.encode('utf8'))
        elif isinstance(data, bytes):
            assert extra is None
            self.save_raw(rpath, data)
        elif isinstance(data, array.array):
            self.storage.put_array(rpath, data, extra if extra else [])
        else:
            raise TypeError("Can't save value of type {!r} (to {!r})".format(type(data), rpath))

        return data

    def save_file(self, path: str, file_path: str, frmt: str = 'txt', compress: bool = True) -> bytes:
        """Download file from node and save it into storage"""
        try:
            content = self.node.get_file(file_path, compress=compress)
            code = 0
        except (IOError, RuntimeError) as exc:
            logger.warning("Can't get file %r from node %s. %s", file_path, self.node, exc)
            content = str(exc)  # type: ignore
            code = 1

        self.save(path, frmt, code, content)
        return content if code == 0 else None  # type: ignore

    def save_output(self, path: str, cmd: str, frmt: str = 'txt') -> CMDRes:
        """Run command on node and store result into storage"""
        code, _, out = self.node.run(cmd)

        if code != 0:
            logger.warning("Cmd %s failed %s with code %s", cmd, self.node, code)
            frmt = 'err'

        self.save(path, frmt, code, out)
        return CMDRes(code=code, stdout=out)

    @abc.abstractmethod
    def collect(self, collect_roles_restriction: List[str]):
        """Do collect data,
        collect_roles_restriction is a list of allowed roles to be collected"""
        pass


DEFAULT_MAX_PG = 2 ** 15
LUMINOUS_MAX_PG = 2 ** 17


class CephDataCollector(Collector):
    name = 'ceph'
    collect_roles = ['osd', 'mon', 'ceph-master']
    master_collected = False
    cluster_name = 'ceph'
    num_pgs = None

    def __init__(self, *args, **kwargs) -> None:
        Collector.__init__(self, *args, **kwargs)
        opt = self.opts.ceph_extra + (" " if self.opts.ceph_extra else "")
        self.radosgw_admin_cmd = "radosgw-admin {}".format(opt)
        self.ceph_cmd = "ceph {}--format json ".format(opt)
        self.ceph_cmd_txt = "ceph {}".format(opt)
        self.rbd_cmd = "rbd {} ".format(opt)
        self.rados_cmd = "rados {}--format json ".format(opt)
        self.rados_cmd_txt = "rados {}".format(opt)

    def collect(self, collect_roles_restriction: List[str]) -> None:
        if 'mon' in collect_roles_restriction:
            self.collect_monitor()

        if 'osd' in collect_roles_restriction:
            self.collect_osd()

        if 'ceph-master' in collect_roles_restriction:
            # TODO: there a race condition in next two lines, but no reason to care about it
            assert not self.master_collected, "ceph-master role have to be collected only once"
            self.__class__.master_collected = True  # type: ignore
            self.collect_master()

    def collect_master(self) -> None:
        # assert self.node is None, "Master data can only be collected from local node"

        with self.chdir("master"):
            curr_data = "{}\n{}\n{}".format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                time.time())

            code, _, out = self.node.run(self.ceph_cmd + "osd versions")
            is_luminous = (code == 0)
            if is_luminous:
                self.save("osd_versions2", 'txt', 0, out)
                self.save_output("mon_versions2", self.ceph_cmd + "mon versions", "txt")
                self.save_output("mgr_versions2", self.ceph_cmd + "mgr versions", "txt")

            self.save_output("osd_versions", self.ceph_cmd + "tell 'osd.*' version", "txt")
            self.save_output("mon_versions", self.ceph_cmd + "tell 'mon.*' version", "txt")

            self.save("collected_at", 'txt', 0, curr_data)
            status = json.loads(self.save_output("status", self.ceph_cmd + "status", 'json').stdout)

            health = status['health']
            status_str = health['status'] if 'status' in health else health['overall_status']
            ceph_health = {'status': status_str}  # type: Dict[str, Union[str, int]]
            avail = status['pgmap']['bytes_avail']
            total = status['pgmap']['bytes_total']
            ceph_health['free_pc'] = int(avail * 100 / total + 0.5)

            active_clean = sum(pg_sum['count']
                               for pg_sum in status['pgmap']['pgs_by_state']
                               if pg_sum['state_name'] == "active+clean")
            total_pg = status['pgmap']['num_pgs']
            ceph_health["ac_perc"] = int(active_clean * 100 / total_pg + 0.5)
            ceph_health["blocked"] = "unknown"

            ceph_health_js = json.dumps(ceph_health)
            self.save("ceph_health_dict", "js", 0, ceph_health_js)

            self.__class__.num_pgs = status['pgmap']['num_pgs']  # type: ignore

            max_pg = (LUMINOUS_MAX_PG if is_luminous else DEFAULT_MAX_PG) \
                     if AUTOPG == self.opts.max_pg_dump_count \
                     else self.opts.max_pg_dump_count

            if self.__class__.num_pgs > max_pg:    # type: ignore
                logger.warning(
                    ("pg dump skipped, as num_pg ({}) > max_pg_dump_count ({})." +
                     " Use --max-pg-dump-count NUM option to change the limit").format(
                         self.__class__.num_pgs, max_pg))    # type: ignore
                cmds = []  # type: List[str]
            else:
                cmds = ['pg dump']

            cmds.extend(['osd tree',
                         'df',
                         'auth list',
                         'health',
                         'mon_status',
                         'osd lspools',
                         'osd perf',
                         'osd df',
                         'health detail',
                         "osd crush dump",
                         "node ls",
                         "features",
                         "report",
                         "time-sync-status"])

            for cmd in cmds:
                self.save_output(cmd.replace(" ", "_").replace("-", '_'), self.ceph_cmd + cmd, 'json')

            osd_dump = self.save_output('osd_dump', self.ceph_cmd + "osd dump", 'json')

            if not self.opts.no_rbd_info:
                rbd_pools = []
                if osd_dump and osd_dump[0] == 0:
                    for pool in json.loads(osd_dump[1])['pools']:
                        if 'application_metadata' in pool:
                            if 'rbd' in pool.get('application_metadata'):
                                rbd_pools.append(pool['pool_name'])
                        elif 'rgw' not in pool['pool_name']:
                            rbd_pools.append(pool['pool_name'])

                for pool in rbd_pools:
                    self.save_output('rbd_du_{}'.format(pool), self.rbd_cmd + "du", 'txt')
                    cmd = 'for image in $(rbd list -p rbd) ; do echo "{}" ; echo "$image" ; {} info "$image" ; done'
                    cmd = cmd.format('-' * 60, self.rbd_cmd)
                    self.save_output("rbd_images_{}".format(pool), cmd, "txt")

            self.save_output("rados_df", self.rados_cmd + "df", 'json')
            self.save_output("rados_df", self.rados_cmd_txt + "df", 'txt')

            self.save_output("realm_list", self.radosgw_admin_cmd + "realm list", "txt")
            self.save_output("zonegroup_list", self.radosgw_admin_cmd + "zonegroup list", "txt")
            self.save_output("zone_list", self.radosgw_admin_cmd + "zone list", "txt")

            self.save_output("default_config", self.ceph_cmd_txt + "--show-config", 'txt')
            self.save_output("ceph_s", self.ceph_cmd_txt + "-s", 'txt')
            self.save_output("ceph_osd_dump", self.ceph_cmd_txt + "osd dump", 'txt')
            self.save_output("osd_utilization", self.ceph_cmd_txt + 'osd utilization', 'txt')
            self.save_output("osd_blocked_by", self.ceph_cmd_txt + "osd blocked-by", "txt")
            self.save_output("osd dump", self.ceph_cmd_txt + "osd dump", "txt")

            temp_fl = "%08X" % random.randint(0, 2 << 64)
            cr_fname = "/tmp/ceph_collect." + temp_fl + ".cr"
            code, _, _ = self.node.run(self.ceph_cmd + "osd getcrushmap -o " + cr_fname)
            if code != 0:
                logger.error("Fail to get crushmap")
            else:
                self.save_file('crushmap', cr_fname, 'bin')
                code, _, _ = self.node.run("crushtool -d {0} -o {0}.txt".format(cr_fname))
                if code != 0:
                    logger.error("Fail to decompile crushmap")
                else:
                    self.save_file('crushmap', cr_fname + ".txt", 'txt')

            osd_fname = "/tmp/ceph_collect." + temp_fl + ".osd"
            code, _, _ = self.node.run(self.ceph_cmd + "osd getmap -o " + osd_fname)
            if code != 0:
                logger.error("Fail to get osdmap")
            else:
                self.save_file('osdmap', osd_fname, 'bin')
                self.save_output('osdmap', "osdmaptool --print " + osd_fname, "txt")

    def collect_osd(self):
        assert isinstance(self.node, Node)

        # check OSD process status
        out = self.node.run("ps aux | grep ceph-osd").output
        osd_re = re.compile(r"ceph-osd[\t ]+.*(-i|--id)[\t ]+(\d+)")
        running_osds = set(int(rr.group(2)) for rr in osd_re.finditer(out))

        ids_from_ceph = set(osd.id for osd in cast(Node, self.node).osds)
        unexpected_osds = running_osds.difference(ids_from_ceph)

        for osd_id in unexpected_osds:
            logger.warning("Unexpected osd-{} in node {}.".format(osd_id, self.node))

        with self.chdir('hosts/' + self.node.name):
            self.save_output("cephdisk", "ceph-disk list")
            cephdisklist_js = self.save_output("cephdisk", "ceph-disk list --format=json", "json").stdout

            code, _, ceph_vol_output = self.node.run("ceph-volume lvm list --format json")
            if not code:
                self.save("cephvolume_lvm", 'json', 0, ceph_vol_output)
            else:
                ceph_vol_output = None

            lsblk_js = self.save_output("lsblk", "lsblk -a --json", "json").stdout
            self.save_output("lsblk", "lsblk -a")

        devs_for_osd = {}  # type: Dict[int, Dict[str, str]]
        dev_tree = parse_devices_tree(json.loads(lsblk_js))

        if ceph_vol_output:
            cephvolume_dct = json.loads(ceph_vol_output)
            for osd_id_s, osd_data in cephvolume_dct.items():
                assert len(osd_data) == 1
                osd_data = osd_data[0]
                assert len(osd_data['devices']) == 1
                dev = osd_data['devices'][0]
                devs_for_osd[int(osd_id_s)] = {"block_dev": dev, "block.db_dev": dev, "block.wal_dev": dev,
                                               'store_type': 'bluestore'}
        else:
            cephdisk_dct = json.loads(cephdisklist_js)

            for dev_info in cephdisk_dct:
                for part_info in dev_info.get('partitions', []):
                    if "cluster" in part_info and part_info.get('type') == 'data':
                        osd_id = int(part_info['whoami'])
                        devs_for_osd[osd_id] = {attr: part_info[attr]
                                                for attr in ("block_dev", "journal_dev", "path",
                                                             "block.db_dev", "block.wal_dev")
                                                if attr in part_info}
                        devs_for_osd[osd_id]['store_type'] = 'filestore' if "journal_dev" in part_info else 'bluestore'

        for osd in self.node.osds:
            with self.chdir('osd/{}'.format(osd.id)):
                cmd = "tail -n {} /var/log/ceph/ceph-osd.{}.log".format(self.opts.ceph_log_max_lines, osd.id)
                self.save_output("log", cmd)

                osd_daemon_cmd = "ceph --admin-daemon /var/run/ceph/ceph-osd.{}.asok".format(osd.id)
                self.save_output("perf_dump", osd_daemon_cmd + " perf dump")
                self.save_output("perf_hist_dump", osd_daemon_cmd + " perf histogram dump".format(osd.id))

                # TODO: much of this can be done even id osd is down for filestore
                if osd.id in running_osds:
                    if not osd.config:
                        config, code = self.save_output("config",
                                                        "ceph daemon osd.{} config show".format(osd.id),
                                                        "json")
                        assert code == 0
                        self.save("config", "json", 0, config)
                    else:
                        self.save("config", "txt", 0, osd.config)
                else:
                    logger.warning("osd-{} in node {} is down. No config available".format(osd.id, self.node.name))

                stor_type = devs_for_osd[osd.id]['store_type']

                if stor_type == 'filestore':
                    cmd = "ls -1 '{0}'".format(os.path.join(osd.storage, 'current'))
                    code, _, res = self.node.run(cmd)

                    if code == 0:
                        pgs = [name.split("_")[0] for name in res.split() if "_head" in name]
                        res = "\n".join(pgs)

                    self.save("pgs", "txt", code, res)

                if stor_type == 'filestore':
                    data_dev = devs_for_osd[osd.id]["path"]
                    j_dev = devs_for_osd[osd.id]["journal_dev"]
                    osd_dev_conf = {'data': data_dev,
                                    'journal': j_dev,
                                    'r_data': dev_tree[data_dev],
                                    'r_journal': dev_tree[j_dev],
                                    'type': stor_type}
                else:
                    assert stor_type == 'bluestore'
                    data_dev = devs_for_osd[osd.id]["block_dev"]
                    db_dev = devs_for_osd[osd.id].get('block.db_dev', data_dev)
                    wal_dev = devs_for_osd[osd.id].get('block.wal_dev', db_dev)
                    osd_dev_conf = {'data': data_dev,
                                    'wal': wal_dev,
                                    'db': db_dev,
                                    'r_data': dev_tree[data_dev],
                                    'r_wal': dev_tree[wal_dev],
                                    'r_db': dev_tree[db_dev],
                                    'type': stor_type}

                self.save('devs_cfg', 'json', 0, json.dumps(osd_dev_conf))

        pids = self.node.rpc.sensors.find_pids_for_cmd('ceph-osd')
        logger.debug("Found next pids for OSD's on node %s: %r", self.node.name, pids)
        if pids:
            for pid in pids:
                self.collect_osd_process_info(pid)

    def collect_osd_process_info(self, pid: int):
        assert isinstance(self.node, Node)
        cmdline = self.node.get_file('/proc/{}/cmdline'.format(pid)).decode('utf8')
        opts = cmdline.split("\x00")
        for op, val in zip(opts[:-1], opts[1:]):
            if op in ('-i', '--id'):
                osd_id = val
                break
        else:
            logger.warning("Can't get osd id for cmd line %r", cmdline.replace("\x00", ' '))
            return

        osd_proc_info = {}  # type: Dict[str, Any]

        with self.chdir('osd/{}'.format(osd_id)):
            pid_dir = "/proc/{}/".format(pid)
            self.save("cmdline", "bin", 0, cmdline)
            osd_proc_info['fd_count'] = len(self.node.rpc.fs.listdir(pid_dir + "fd"))

            fpath = pid_dir + "net/sockstat"
            ssv4 = parse_sockstat_file(self.node.get_file(fpath).decode('utf8'))
            if not ssv4:
                logger.warning("Broken file {!r} on node {}".format(fpath, self.node))
            else:
                osd_proc_info['ipv4'] = ssv4

            fpath = pid_dir + "net/sockstat6"
            ssv6 = parse_sockstat_file(self.node.get_file(fpath).decode('utf8'))
            if not ssv6:
                logger.warning("Broken file {!r} on node {}".format(fpath, self.node))
            else:
                osd_proc_info['ipv6'] = ssv6
            osd_proc_info['sock_count'] = self.node.rpc.sensors.count_sockets_for_process(pid)

            proc_stat = self.node.get_file(pid_dir + "status").decode('utf8')
            self.save("proc_status", "txt", 0, proc_stat)
            osd_proc_info['th_count'] = int(proc_stat.split('Threads:')[1].split()[0])

            # IO stats
            io_stat = self.node.get_file(pid_dir + "io").decode('utf8')
            osd_proc_info['io'] = parse_proc_file(io_stat)

            # Mem stats
            mem_stat = self.node.get_file(pid_dir + "status").decode('utf8')
            osd_proc_info['mem'] = parse_proc_file(mem_stat)

            # memmap
            mem_map = self.node.get_file(pid_dir + "maps", compress=True).decode('utf8')
            osd_proc_info['memmap'] = []
            for ln in mem_map.strip().split("\n"):
                mem_range, access, offset, dev, inode, *pathname = ln.split()
                osd_proc_info['memmap'].append([mem_range, access, " ".join(pathname)])

            # sched
            sched = self.node.get_file(pid_dir + "sched", compress=True).decode('utf8')
            try:
                data = "\n".join(sched.strip().split("\n")[2:])
                osd_proc_info['sched'] = parse_proc_file(data, ignore_err=True)
            except:
                osd_proc_info['sched'] = {}
                osd_proc_info['sched_raw'] = sched

            stat = self.node.get_file(pid_dir + "stat").decode('utf8')
            osd_proc_info['stat'] = stat.split()

            self.save("procinfo", "json", 0, json.dumps(osd_proc_info))

    def collect_monitor(self) -> None:
        assert isinstance(self.node, Node)
        assert self.node.mon is not None
        with self.chdir("mon/{}".format(self.node.mon)):
            self.save_output("mon_daemons", "ps aux | grep ceph-mon")
            tail_ln = "tail -n {} ".format(self.opts.ceph_log_max_lines)
            self.save_output("mon_log", tail_ln + "/var/log/ceph/ceph-mon.{}.log".format(self.node.mon))
            self.save_output("ceph_log", tail_ln + " /var/log/ceph/ceph.log")

            log_issues = self.node.rpc.sensors.find_issues_in_ceph_log(self.opts.ceph_log_max_lines)
            self.save("ceph_log_wrn_err", "txt", 0, log_issues)

            issues_count, regions = self.node.rpc.sensors.analyze_ceph_logs_for_issues()
            self.save("log_issues_count", "json", 0, json.dumps(issues_count))
            self.save("status_regions", "json", 0, json.dumps(regions))

            self.save_output("ceph_audit", tail_ln + " /var/log/ceph/ceph.audit.log")
            self.save_output("config", self.ceph_cmd + "daemon mon.{} config show".format(self.node.mon),
                             frmt='json')

            self.save_output("ceph_var_dirs_size", "du -s /var/lib/ceph/m*")

            log_issues = self.node.rpc.sensors.find_issues_in_ceph_log(self.opts.ceph_log_max_lines)
            self.save("ceph_log_wrn_err", "txt", 0, log_issues)


class NodeCollector(Collector):
    name = 'node'
    collect_roles = ['node']

    node_commands = [
        ("df", 'txt', 'df'),
        ("dmidecode", "txt", "dmidecode"),
        ("dmesg", "txt", "dmesg"),
        ("ipa4", "txt", "ip -o -4 a"),
        ("ipa", "txt", "ip a"),
        ("ifconfig", "txt", "ifconfig"),
        ("ifconfig_short", "txt", "ifconfig -s"),
        # ("journalctl", 'txt', 'journalctl -b'),
        ("lshw", "xml", "lshw -xml"),
        ("lsblk", "txt", "lsblk -O"),
        ("lsblk_short", "txt", "lsblk"),
        ("mount", "txt", "mount"),
        ("netstat", "txt", "netstat -nap"),
        ("netstat_stat", "txt", "netstat -s"),
        ("sysctl", "txt", "sysctl -a"),
        ("uname", "txt", "uname -a"),
    ]

    node_files = [
        "/proc/diskstats", "/proc/meminfo", "/proc/loadavg", "/proc/cpuinfo", "/proc/uptime", "/proc/vmstat"
    ]

    node_renamed_files = [("netdev", "/proc/net/dev"),
                          ("dev_netstat", "/proc/net/netstat"),
                          ("softnet_stat", "/proc/net/softnet_stat"),
                          ("ceph_conf", "/etc/ceph/ceph.conf")]

    def collect(self, collect_roles_restriction: List[str]) -> None:
        if 'node' in collect_roles_restriction:
            with self.chdir('hosts/' + self.node.name):
                self.collect_kernel_modules_info()
                self.collect_common_features()
                self.collect_files()
                self.collect_bonds_info()
                self.collect_interfaces_info()
                self.collect_block_devs()
                self.collect_packages()

    def collect_kernel_modules_info(self):
        try:
            self.save_output("lsmod", "lsmod", frmt="txt")
        except Exception as exc:
            logger.warning("Failed to list kernel modules: %r on node %s: %s", "lsmod", self.node, exc)
            return

        try:
            self.save_output(
                "modinfo_all",
                "for name in $(lsmod | awk '{print $1}') ; do modinfo $name ; echo '-----' ; done",
                "txt")
        except Exception as exc:
            logger.warning("Failed to list kernel modules info: %r on node %s: %s", "modinfo **", self.node, exc)
        return

    def collect_common_features(self):
        for path_offset, frmt, cmd in self.node_commands:
            try:
                self.save_output(path_offset, cmd, frmt=frmt)
            except Exception as exc:
                logger.warning("Failed to run %r on node %s: %s", cmd, self.node, exc)

    def collect_files(self):
        for fpath in self.node_files:
            try:
                self.save_file(os.path.basename(fpath), fpath)
            except Exception as exc:
                logger.warning("Failed to download file %r from node %s: %s", fpath, self.node, exc)

        for name, fpath in self.node_renamed_files:
            try:
                self.save_file(name, fpath)
            except Exception as exc:
                logger.warning("Failed to download file %r from node %s: %s", fpath, self.node, exc)

    def collect_bonds_info(self):
        # collect_bonds_info
        bondmap = {}
        if self.node.exists("/proc/net/bonding"):
            for fname in self.node.listdir("/proc/net/bonding"):
                self.save_file("bond_" + fname, "/proc/net/bonding/" + fname)
                bondmap[fname] = "bond_" + fname
        self.save("bonds", 'json', 0, json.dumps(bondmap))

    def collect_packages(self):
        try:
            if self.node.exists("/etc/debian_version"):
                self.save_output("packages_deb", "dpkg -l", frmt="txt")
            else:
                self.save_output("packages_rpm", "yum list installed", frmt="txt")
        except Exception as exc:
            logger.warning("Failed to download packages information from node %s: %s", self.node, exc)

    def collect_block_devs(self) -> None:
        assert isinstance(self.node, Node)

        bdevs_info = self.node.rpc.sensors.get_block_devs_info()
        bdevs_info = {name.decode('utf8'): data for name, data in bdevs_info.items()}
        for name_prefix in ['loop']:
            for name in bdevs_info:
                if name.startswith(name_prefix):
                    del bdevs_info[name]

        hdparm_exists, smartctl_exists, nvme_exists = self.node.rpc.fs.binarys_exists(['hdparm', 'smartctl', 'nvme'])   

        missing = []
        if not hdparm_exists:
            missing.append('hdparm')

        if not smartctl_exists:
            missing.append('smartctl-tools')

        if not nvme_exists:
            missing.append('nvme-tools')
        else:
            code, _, out = self.node.run('nvme version')
            if code != 0:
                ver = 0  # type: float
            else:
                try:
                    *_, version = out.split()
                    ver = float(version)
                except:
                    ver = 0

            if ver < 1.0:
                logger.warning("Nvme tool too old %s, at least 1.0 version is required", ver)
            else:
                out_t = self.save_output('nvme_list', 'nvme list -o json', frmt='json')
                if out_t is not None:
                    code, out = out_t
                    if code == 0:
                        try:
                            for dev in json.loads(out)['Devices']:
                                name = os.path.basename(dev['DevicePath'])
                                self.save_output('block_devs/{}/nvme_smart_log'.format(name),
                                                 'nvme smart-log {} -o json'.format(dev['DevicePath']), frmt='json')
                        except:
                            logging.warning("Failed to process nvme list output")

        if missing:
            logger.warning("%s is not installed on %s", ",".join(missing), self.node.name)

        out_t = self.save_output("lsblkjs", "lsblk -O -b -J", frmt="json")
        if out_t:
            code, out_js = out_t
            if code == 0:
                for dev_node in json.loads(out_js)['blockdevices']:
                    name = dev_node['name']
                    with self.chdir('block_devs/' + name):
                        if hdparm_exists:
                            self.save_output('hdparm', "sudo hdparm -I /dev/" + name)

                        if smartctl_exists:
                            self.save_output('smartctl', "sudo smartctl -a /dev/" + name)

    def collect_interfaces_info(self) -> None:
        interfaces = {}
        for is_phy, dev in get_host_interfaces(self.node):
            interface = {'dev': dev, 'is_phy': is_phy}
            interfaces[dev] = interface

            if is_phy:
                code, _, eth_out = self.node.run("ethtool " + dev)
                if code == 0:
                    interface['ethtool'] = eth_out

                code, _, iwconfig_out = self.node.run("iwconfig " + dev)
                if code == 0:
                    interface['iwconfig'] = iwconfig_out

        self.save('interfaces', 'json', 0, json.dumps(interfaces))


class LoadCollector(Collector):
    name = 'load'
    collect_roles = ['load']

    def collect(self, collect_roles_restriction: List[str]) -> None:
        raise RuntimeError("collect should not be called for {} class instance".format(self.__class__.__name__))

    def start_performance_monitoring(self) -> None:
        assert isinstance(self.node, Node)

        assert self.node.load_config == {}

        if self.opts.ceph_historic:
            self.node.load_config.setdefault("ceph", {}).setdefault("sources", []).append('historic')

        if self.opts.ceph_perf:
            self.node.load_config.setdefault("ceph", {}).setdefault("sources", []).append('perf_dump')

        if self.opts.ceph_historic or self.opts.ceph_perf:
            self.node.load_config['ceph']['osds'] = 'all'

        if self.node.load_config:
            self.node.rpc.sensors.start(self.node.load_config)

    def collect_performance_data(self) -> None:
        assert isinstance(self.node, Node)
        if self.node.load_config == {}:
            return

        for sensor_path, data, is_unpacked, units in unpack_rpc_updates(self.node.rpc.sensors.get_updates()):
            if '.' in sensor_path:
                sensor, dev, metric = sensor_path.split(".")
            else:
                sensor, dev, metric = sensor_path, "", ""

            if is_unpacked:
                ext = 'csv' if isinstance(data, array.array) else 'json'
                extra = [sensor, dev, metric, units]
                self.save("perf_monitoring/{}/{}".format(self.node.name, sensor_path), ext, 0, data,
                          extra=extra)
            else:
                assert isinstance(data, bytes)
                if metric == 'historic':
                    self.save_raw("perf_monitoring/{}/{}.bin".format(self.node.name, sensor_path), data)
                elif metric == 'perf_dump':
                    self.save_raw("perf_monitoring/{}/{}.json".format(self.node.name, sensor_path), data)


# ------------------------  Collect coordinator functions --------------------------------------------------------------


ALL_COLLECTORS = [CephDataCollector, NodeCollector, LoadCollector]  # type: List[Type[Collector]]

ALL_COLLECTORS_MAP = dict((collector.name, collector)
                          for collector in ALL_COLLECTORS)  # type: Dict[str, Type[Collector]]


class ReportFailed(RuntimeError):
    pass


class CollectorCoordinator:
    def __init__(self, storage: IStorageNNP, opts: Any, executor: Executor) -> None:
        self.nodes = []  # type: List[Node]
        self.opts = opts
        self.executor = executor
        self.storage = storage

        self.ssh_opts = "-o LogLevel=quiet -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null " + \
            "-o ConnectTimeout={} -o ConnectionAttempts=2".format(opts.ssh_conn_timeout)

        if opts.ssh_user:
            self.ssh_opts += " -l {0} ".format(opts.ssh_user)

        if opts.ssh_opts:
            self.ssh_opts += opts.ssh_opts + " "

        self.ceph_master_node = None  # type: Optional[INode]
        self.nodes = []  # type: List[Node]
        self.collect_load_data_at = 0
        self.allowed_collectors = self.opts.collectors.split(',')
        self.load_collectors = []  # type: List[LoadCollector]

    def connect_and_init(self, ips_or_hostnames: List[str]) -> Tuple[List[Node], List[Tuple[str, str]]]:
        return init_rpc_and_fill_data(ips_or_hostnames, ssh_opts=self.ssh_opts,
                                      with_sudo=self.opts.sudo, executor=self.executor)

    def get_master_node(self) -> Optional[INode]:
        if self.opts.ceph_master:
            logger.info("Connecting to ceph-master: {}".format(self.opts.ceph_master))
            nodes, err = self.connect_and_init([self.opts.ceph_master])
            assert len(nodes) + len(err) == 1
            if err:
                logger.error("Can't connect to ceph-master {}: {}".format(self.opts.ceph_master, err[0]))
                return None
            return nodes[0]
        else:
            return Local()

    def fill_ceph_services(self, nodes: List[Node], ceph_services: CephServices) -> Set[str]:
        all_nodes_ips = {}  # type: Dict[str, Node]
        # all_nodes_ips: Dict[str, Node] = {}
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

    def connect_to_nodes(self) -> List[Node]:

        self.ceph_master_node = self.get_master_node()
        if self.ceph_master_node is None:
            raise ReportFailed()

        if self.ceph_master_node.run('which ceph').code != 0:
            logger.error("No 'ceph' command available on master node.")
            raise ReportFailed()

        ceph_services = None if self.opts.ceph_master_only \
                        else discover_ceph_services(self.ceph_master_node, self.opts, thcount=self.opts.pool_size)

        nodes, errs = self.connect_and_init(list(iter_extra_host(self.opts)))
        if errs:
            for ip_or_hostname, err in errs:
                logging.error("Can't connect to extra node %s: %s", ip_or_hostname, err)
            raise ReportFailed()

        # add information from monmap and osdmap about ceph services and connect to extra nodes
        if ceph_services:

            missing_ceph_ips = self.fill_ceph_services(nodes, ceph_services)

            ceph_nodes, errs = self.connect_and_init(list(missing_ceph_ips))
            if errs:
                lfunc = logging.error if self.opts.all_ceph else logging.warning
                lfunc("Can't connect to ceph nodes")
                for ip, err in errs:
                    lfunc("    %s => %s", ip, err)

                if self.opts.all_ceph:
                    raise ReportFailed()

            missing_mons = {ip: mon for ip, mon in ceph_services.mons.items() if ip in missing_ceph_ips}
            missing_osds = {ip: osds for ip, osds in ceph_services.osds.items() if ip in missing_ceph_ips}

            missing_ceph_ips2 = self.fill_ceph_services(ceph_nodes, CephServices(missing_mons, missing_osds))
            assert not missing_ceph_ips2

            nodes += ceph_nodes

        logger.info("Found %s nodes with osds", sum(1 for node in nodes if node.osds))
        logger.info("Found %s nodes with mons", sum(1 for node in nodes if node.mon))
        logger.info("Run with %s hosts in total", len(nodes))

        for node in nodes:
            logger.debug("Node: %s", node.ceph_info())

        return nodes

    def start_load_collectors(self) -> Iterator[Callable]:
        if LoadCollector.name in self.allowed_collectors:
            for node in self.nodes:
                collector = LoadCollector(self.storage,
                                          self.opts,
                                          node,
                                          pretty_json=not self.opts.no_pretty_json)
                yield collector.start_performance_monitoring
                self.load_collectors.append(collector)
            logger.info("Start performance collectors")

            # time when to stop load collection
            self.collect_load_data_at = time.time() + self.opts.load_collect_seconds

    def finish_load_collectors(self) -> Iterator[Callable]:
        if LoadCollector.name in self.allowed_collectors and self.load_collectors:
            stime = self.collect_load_data_at - time.time()
            if stime > 0:
                logger.info("Waiting for %s seconds for performance collectors", int(stime + 0.5))
                time.sleep(stime)

            logger.info("Collecting performance info")
            for collector in self.load_collectors:
                yield collector.collect_performance_data

    def collect_ceph_data(self) -> Iterator[Callable]:
        if CephDataCollector.name in self.allowed_collectors:
            assert self.ceph_master_node is not None
            collector = CephDataCollector(self.storage,
                                          self.opts,
                                          self.ceph_master_node,
                                          pretty_json=not self.opts.no_pretty_json)
            yield functools.partial(collector.collect, ['ceph-master'])

            if not self.opts.ceph_master_only:
                for node in self.nodes:
                    collector = CephDataCollector(self.storage,
                                                  self.opts,
                                                  node,
                                                  pretty_json=not self.opts.no_pretty_json)
                    roles = ["mon"] if node.mon else []
                    if node.osds:
                        roles.append("osd")
                    yield functools.partial(collector.collect, roles)

    def run_other_collectors(self) -> Iterator[Callable]:
        # run all other collectors
        for collector_name in self.allowed_collectors:
            if collector_name not in (LoadCollector.name, CephDataCollector.name):
                for node in self.nodes:
                    collector_cls = ALL_COLLECTORS_MAP[collector_name]
                    collector = collector_cls(self.storage,
                                              self.opts,
                                              node,
                                              pretty_json=not self.opts.no_pretty_json)
                    yield functools.partial(collector.collect, ['node'])

    def collect(self):
        # This variable is updated from main function
        self.nodes = self.connect_to_nodes()
        if self.opts.detect_only:
            return

        self.storage.put_raw(json.dumps([node.dct() for node in self.nodes]).encode('utf8'), "hosts.json")

        for collector in self.allowed_collectors:
            if collector not in ALL_COLLECTORS_MAP:
                logger.error("Can't found collector {}. Only {} are available".format(
                                collector, ','.join(ALL_COLLECTORS_MAP)))
                raise ReportFailed()

        try:
            funcs = [self.start_load_collectors,
                     self.collect_ceph_data,
                     self.run_other_collectors,
                     self.finish_load_collectors]

            for func in funcs:
                futures = []
                for run_func in func():
                    future = self.executor.submit(run_func)
                    future._run__func = run_func
                    futures.append(future)

                for future in futures:
                    try:
                        node = future._run__func.func.__self__.node
                    except AttributeError:
                        node = None

                    try:
                        future.result()
                    except Exception as exc:
                        exc.node = node
                        raise

        except Exception as exc:
            if getattr(exc, 'node') is not None:
                logger.error("Exception happened during collecting from node %s (see full tb below): %s", exc.node, exc)
            else:
                logger.error("Exception happened(see full tb below): %s", exc)
            raise
        finally:
            logger.info("Collecting logs and teardown RPC servers")
            for node in self.nodes + [self.ceph_master_node]:
                if isinstance(node, Node):
                    try:
                        self.storage.put_raw(
                            node.rpc.server.get_logs().encode('utf8'),
                            "rpc_logs/{0}.txt".format(node.name))
                    except Exception:
                        pass
                    try:
                        node.rpc.server.stop()
                    except ConnectionClosed:
                        node.rpc = None

        logger.info("Totally collected data from %s nodes", len(self.nodes))

        for node in sorted(self.nodes, key=lambda x: x.name):
            if node.osds and node.mon:
                logger.info("Node %s has mon and %s osds", node.name, len(node.osds))
            elif node.osds:
                logger.info("Node %s has %s osds", node.name, len(node.osds))
            elif node.mon:
                logger.info("Node %s has mon", node.name)

        logger.info("Totally found %s monitors, %s OSD nodes with %s OSD daemons",
                    sum(1 for node in self.nodes if node.mon),
                    sum(1 for node in self.nodes if node.osds),
                    sum(len(node.osds) for node in self.nodes if node.osds))


def parse_args(argv: List[str]) -> Any:
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("-c", "--collectors", default="ceph,node,load",
                   help="Comma separated list of collectors. Select from : " +
                   ",".join(coll.name for coll in ALL_COLLECTORS))
    p.add_argument("--ceph-master", metavar="NODE", help="Run all ceph cluster commands from NODE")
    p.add_argument("--no-rbd-info", action='store_true', help="Don't collect info for rbd volumes")
    p.add_argument("--ceph-master-only", action="store_true", help="Run only ceph master data collection, " +
                   "no info from osd/monitors would be collected")
    p.add_argument("-C", "--ceph-extra", default="", help="Extra opts to pass to 'ceph' command")
    p.add_argument("-D", "--detect-only", action="store_true", help="Don't collect any data, only detect cluster nodes")
    p.add_argument("-g", "--save-to-git", metavar="DIR", help="Absolute path to git repo, where to commit output")
    p.add_argument("--git-push", action='store_true',
                   help="Run git push after commit. -g/--save-to-git must be provided")
    p.add_argument("-I", "--node", nargs='+', default=[],
                   help="List of ceph nodes sshachable addresses/names")
    p.add_argument("--ceph-historic", action="store_true", help="Collect ceph historic ops")
    p.add_argument("--ceph-perf", action="store_true", help="Collect ceph perf dump data (a LOT of data!)")
    p.add_argument("--inventory", metavar='FILE',
                   help="Absolute path to file with list of sshachable ip/names of ceph nodes")
    p.add_argument("-j", "--no-pretty-json", action="store_true", help="Don't prettify json data")
    p.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                   help="Console log level, see logging.json for defaults")
    p.add_argument("-L", "--log-config", help="Absolute path to json file with logging config")
    p.add_argument("-m", "--max-pg-dump-count", default=AUTOPG, type=int,
                   help="maximum PG count to by dumped with 'pg dump' cmd, by default {} ".format(LUMINOUS_MAX_PG) +
                   "for luminous, {} for other ceph versions".format(DEFAULT_MAX_PG))
    p.add_argument("-M", "--ceph-log-max-lines", default=10000, type=int, help="Max lines from osd/mon log")
    p.add_argument("-n", "--dont-remove-unpacked", action="store_true", help="Keep unpacked data")
    p.add_argument("-N", "--dont-pack-result", action="store_true", help="Don't create archive")
    p.add_argument("-o", "--output", help="Absolute path to result archive (only if -N don't set)")
    p.add_argument("-O", "--output-folder", help="Absolute path to result folder")
    p.add_argument("-p", "--pool-size", default=32, type=int, help="RPC/local worker pool size")
    p.add_argument("-s", "--load-collect-seconds", default=60, type=int, metavar="SEC",
                   help="Collect performance stats for SEC seconds")
    p.add_argument("-S", "--ssh-opts", help="SSH cli options")
    p.add_argument("-t", "--ssh-conn-timeout", default=60, type=int, help="SSH connection timeout")
    p.add_argument("-u", "--ssh-user", help="SSH user. Current user by default")
    p.add_argument("--sudo", action="store_true", help="Run agent with sudo on remote node")
    p.add_argument("-w", "--wipe", action='store_true', help="Wipe results directory before store data")
    p.add_argument("-A", '--all-ceph', action="store_true", help="Must successfully connect to all ceph nodes")

    if logging_tree:
        p.add_argument("-T", "--show-log-tree", action="store_true", help="Show logger tree.")

    return p.parse_args(argv[1:])


def setup_logging2(opts: Any, out_folder: str, tmp_log_file: bool = False) -> Optional[str]:
    default_lconf_path = os.path.join(os.path.dirname(__file__), 'logging.json')
    log_config_fname = default_lconf_path if opts.log_config is None else opts.log_config

    if opts.detect_only:
        log_file = '/dev/null'
    else:
        if tmp_log_file:
            fd, log_file = tempfile.mkstemp()
            os.close(fd)
        else:
            log_file = os.path.join(out_folder, "log.txt")

    setup_logging(log_config_fname, log_file=log_file, log_level=opts.log_level)

    return log_file if log_file != '/dev/null' else None


def git_prepare(path: str) -> bool:
    logger.info("Checking and cleaning git dir %r", path)
    code, _, res = Local().run("cd {} ; git status --porcelain".format(path))

    if code != 0:
        sys.stderr.write("Folder {} doesn't looks like under git control\n".format(path))
        return False

    if len(res.strip()) != 0:
        sys.stderr.write("Uncommited or untracked files in {}. ".format(path) +
                         "Cleanup directory before proceed\n")
        return False

    for name in os.listdir(path):
        if not name.startswith('.'):
            objpath = os.path.join(path, name)
            if os.path.isdir(objpath):
                shutil.rmtree(objpath)
            else:
                os.unlink(objpath)

    return True


def git_commit(path: str, message: str, push: bool = False):
    cmd = "cd {} ; git add -A ; git commit -m '{}'".format(path, message)
    if push:
        cmd += " ; git push"
    Local().run(cmd)


def pack_output_folder(out_folder: str, out_file: Optional[str], log_level: str):
    if out_file is None:
        out_file = tmpnam(remove_after=False) + ".tar.gz"

    code, _, res = Local().run("cd {} ; tar -zcvf {} *".format(out_folder, out_file))
    if code != 0:
        logger.error("Fail to archive results. Please found raw data at %r", out_folder)
    else:
        logger.info("Result saved into %r", out_file)

        if log_level in ('WARNING', 'ERROR', "CRITICAL"):
            print("Result saved into {}".format(out_file))


def commit_into_git(git_dir: str, log_file: str, target_log_file: str, git_push: bool, ceph_status: Dict[str, Any]):
    logger.info("Comiting into git")

    [h_weak_ref().flush() for h_weak_ref in logging._handlerList]  # type: ignore
    shutil.copy(log_file, target_log_file)
    os.unlink(log_file)

    status_templ = "{0:%H:%M %d %b %y}, {1[status]}, {1[free_pc]}% free, {1[blocked]} req blocked, " + \
                   "{1[ac_perc]}% PG active+clean"
    message = status_templ.format(datetime.datetime.now(), ceph_status)
    git_commit(git_dir, message, git_push)


def main(argv: List[str]) -> int:
    out_folder = None
    log_file = None

    try:
        opts = parse_args(argv)

        # verify options
        if opts.inventory and not os.path.isabs(opts.inventory):
            sys.stderr.write("--inventory path mush be absolute\n")
            return 1

        if opts.output_folder and not os.path.isabs(opts.output_folder):
            sys.stderr.write("--output-folder parameter must be absolute\n")
            return 1

        if opts.output and not os.path.isabs(opts.output):
            sys.stderr.write("--output parameter must be absolute\n")
            return 1

        if opts.save_to_git and not os.path.isabs(opts.save_to_git):
            sys.stderr.write("-g/--save-to-git parameter must be absolute\n")
            return 1

        if opts.log_config and not os.path.isabs(opts.log_config):
            sys.stderr.write("-L/--log-config parameter must be absolute\n")
            return 1

        if opts.git_push and not opts.save_to_git:
            sys.stderr.write("-g/--save-to-git must be provided along with --git-push\n")
            return 1

        if opts.save_to_git and opts.output_folder:
            sys.stderr.write("--output-folder can't be used with -g/-G option\n")
            return 1

        if 'load' in opts.collectors and not (opts.ceph_historic or opts.ceph_perf):
            sys.stderr.write("At least one from --ceph-historic and --ceph-perf option must "
                             "be passed for load collector\n")
            return 1

        # prepare output folder
        if not opts.detect_only:
            if opts.save_to_git:
                if not git_prepare(opts.save_to_git):
                    return 1
                out_folder = opts.save_to_git
            elif opts.output_folder:
                out_folder = opts.output_folder
                if os.path.exists(out_folder):
                    if opts.wipe:
                        shutil.rmtree(out_folder)
                        os.mkdir(out_folder)
                else:
                    os.mkdir(out_folder)
            else:
                out_folder = tempfile.mkdtemp()

        # setup logging
        log_file = setup_logging2(opts, out_folder, opts.save_to_git is not None)  # type: ignore

        if logging_tree and opts.show_log_tree:
            logging_tree.printout()
            return 0
    except Exception:
        if log_file:
            with open(log_file, 'wt') as fd:
                fd.write(traceback.format_exc())
        raise

    try:
        logger.info(repr(argv))
        if out_folder:
            if opts.output_folder:
                logger.info("Store data into %r", out_folder)
            else:
                logger.info("Temporary folder %r", out_folder)
            storage = make_storage(out_folder, existing=False, serializer='raw')
        else:
            storage = None

        # run collect
        with ThreadPoolExecutor(opts.pool_size) as executor:
            CollectorCoordinator(storage, opts, executor).collect()

        # compress/commit output folder
        if out_folder:
            if opts.save_to_git:
                [h_weak_ref().flush() for h_weak_ref in logging._handlerList]  # type: ignore
                target_log_file = os.path.join(opts.save_to_git, "log.txt")
                shutil.copy(log_file, target_log_file)  # type: ignore
            else:
                target_log_file = None  # type: ignore

            if not opts.dont_pack_result:
                pack_output_folder(out_folder, opts.output, opts.log_level)

            if opts.save_to_git:
                assert isinstance(target_log_file, str) and isinstance(log_file, str)
                ceph_health = json.loads(storage.get_raw("ceph_health_dict").decode("utf8"))
                commit_into_git(opts.save_to_git,
                                log_file=log_file,
                                target_log_file=target_log_file,
                                git_push=opts.git_push,
                                ceph_status=ceph_health)
            elif not opts.dont_remove_unpacked:
                if opts.dont_pack_result:
                    logger.warning("Unpacked tree is kept as --dont-pack-result option is set, so no archive created")
                else:
                    shutil.rmtree(out_folder)
                    out_folder = None

            if out_folder:
                logger.info("Temporary folder %r", out_folder)
                if opts.log_level in ('WARNING', 'ERROR', "CRITICAL"):
                    print("Temporary folder %r" % (out_folder,))
    except ReportFailed:
        return 1
    except Exception:
        logger.exception("During make_storage/collect")
        raise

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

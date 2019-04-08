import abc
import os
import re
import time
import json
import array
import random
import logging
import datetime
import contextlib
from typing import Any, Union, List, Dict, Optional, Tuple, Iterator

from agent.client import IAgentRPCNode
from dataclasses import dataclass

from cephlib import CephRelease, parse_ceph_volumes_js, parse_ceph_disk_js, CephReport, CephCmd
from koder_utils import (IStorageNNP, CMDResult, IAsyncNode, parse_devices_tree, parse_sockstat_file,
                         parse_info_file_from_proc, get_host_interfaces, ignore_all)

logger = logging.getLogger('collect')


AUTOPG = -1


async def get_sock_count(conn: IAgentRPCNode, pid: int) -> int:
    return await conn.conn.fs.count_sockets_for_process(pid)


async def get_device_for_file(conn: IAgentRPCNode, fname: str) -> Tuple[str, str]:
    """Find storage device, on which file is located"""

    dev = (await conn.conn.fs.get_dev_for_file(fname)).decode()
    assert dev.startswith('/dev'), "{!r} is not starts with /dev".format(dev)
    root_dev = dev = dev.strip()
    rr = re.match('^(/dev/[shv]d.*?)\\d+', root_dev)
    if rr:
        root_dev = rr.group(1)
    return root_dev, dev


@dataclass
class Collector:
    """Base class for data collectors. Can collect data for only one node."""
    name: str = "BaseCollector"

    def __init__(self, storage: IStorageNNP, opts: Any, node: IAsyncNode, hostname: str) -> None:
        self.storage = storage
        self.hostname = hostname
        self.opts = opts
        self.node = node
        self.pretty_json = self.opts.pretty_json

    @abc.abstractmethod
    async def collect(self) -> None:
        pass

    @abc.abstractmethod
    def with_storage(self, storage: IStorageNNP) -> 'Collector':
        pass

    @contextlib.contextmanager
    def chdir(self, path: str) -> Iterator['Collector']:
        """Chdir for point in storage tree, where current results are stored"""
        saved = self.storage
        try:
            yield self.with_storage(self.storage.sub_storage(path))
        finally:
            self.storage = saved

    def save_raw(self, path: str, data: bytes):
        self.storage.put_raw(data, path)

    def save(self, path: str, frmt: str, code: int, data: Union[str, bytes, array.array],
             extra: List[str] = None) -> Union[str, bytes, array.array]:
        """Save results into storage"""
        if code == 0 and frmt == 'json' and self.pretty_json:
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

    async def read_and_save(self, path: str, file_path: str, frmt: str = 'txt', compress: bool = True) -> bytes:
        """Download file from node and save it into storage"""
        try:
            content = await self.node.read(file_path, compress=compress)
            code = 0
        except (IOError, RuntimeError) as exc:
            logger.warning("Can't get file %r from node %s. %s", file_path, self.node, exc)
            content = str(exc)  # type: ignore
            code = 1

        self.save(path, frmt, code, content)
        return content if code == 0 else None  # type: ignore

    async def run_and_save_output(self, path: str, cmd: str, data_format: str = 'txt') -> CMDResult:
        """Run command on node and store result into storage"""
        res = await self.node.run(cmd, merge_err=False)

        if res.returncode != 0:
            logger.warning(f"Cmd {cmd} failed {self.node} with code {res.returncode}")
            data_format = 'err'
            save = res.stdout + res.stderr_b.decode()
        else:
            save = res.stdout

        try:
            self.save(path, data_format, res.returncode, save)
        except json.decoder.JSONDecodeError:
            print(res.stdout[:20])
            raise
        return res


DEFAULT_MAX_PG = 2 ** 15
LUMINOUS_MAX_PG = 2 ** 17


class CephBase:
    def __init__(self, report: CephReport, opts: Any):
        opt = opts.ceph_extra + (" " if opts.ceph_extra else "")
        self.radosgw_admin_cmd = "radosgw-admin {}".format(opt)
        self.ceph_cmd = "ceph {}--format json ".format(opt)
        self.ceph_cmd_txt = "ceph {}".format(opt)
        self.rbd_cmd = "rbd {} ".format(opt)
        self.rados_cmd = "rados {}--format json ".format(opt)
        self.rados_cmd_txt = "rados {}".format(opt)
        self.report = report


class CephMasterCollector(Collector, CephBase):
    name = 'ceph-master'
    collect_roles = ['ceph-master']
    master_collected = False
    cluster_name = 'ceph'

    def __init__(self, storage: IStorageNNP, opts: Any, node: IAsyncNode, hostname: str, report: CephReport) -> None:
        Collector.__init__(self, storage.sub_storage("master"), opts, node, hostname)
        CephBase.__init__(self, opts, report)

    def with_storage(self, storage: IStorageNNP) -> 'CephMasterCollector':
        return self.__class__(storage, self.opts, self.node, self.hostname, self.report)

    async def collect(self) -> None:
        # TODO: this should not be here
        time2 = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        curr_data = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}\n{time2}\n{time.time()}"
        self.save("collected_at", 'txt', 0, curr_data)
        self.save("report", 'json', 0, json.dumps(self.report.raw))

        pre_luminous = self.report.version.release < CephRelease.luminous
        if pre_luminous:
            await self.run_and_save_output("osd_versions_old", self.ceph_cmd_txt + "tell 'osd.*' version", "txt")
            await self.run_and_save_output("mon_versions_old", self.ceph_cmd_txt + "tell 'mon.*' version", "txt")

        await self.run_and_save_output("status", self.ceph_cmd + "status", 'json')

        if AUTOPG == self.opts.max_pg_dump_count:
            max_pg = (DEFAULT_MAX_PG if pre_luminous else LUMINOUS_MAX_PG)
        else:
            max_pg = self.opts.max_pg_dump_count

        cmds = ["df", "auth list", "mon_status", "osd perf", "osd df", "node ls", "features", "time-sync-status"]
        if self.report.raw['num_pg'] > max_pg:
            logger.warning(f"pg dump skipped, as num_pg ({self.report.raw['num_pg']}) > max_pg_dump_count ({max_pg})." +
                           " Use --max-pg-dump-count NUM option to change the limit")
        else:
            cmds.append("pg dump")

        for cmd in cmds:
            await self.run_and_save_output(cmd.replace(" ", "_").replace("-", '_'), self.ceph_cmd + cmd, 'json')

        if not self.opts.no_rbd_info:
            await self.collect_rbd_volumes_info()

        await self.run_and_save_output("rados_df", self.rados_cmd + "df", 'json')

        if self.opts.collect_rgw:
            await self.collect_rgw_info()

        await self.run_and_save_output("default_config", self.ceph_cmd_txt + "--show-config", 'txt')
        await self.run_and_save_output("osd_blocked_by", self.ceph_cmd + "osd blocked-by", "json")

        if self.opts.collect_txt:
            await self.collect_txt()

        if self.opts.collect_maps:
            await self.collect_crushmap()
            await self.collect_osd_map()

    async def collect_rbd_volumes_info(self):
        logger.debug("Collecting RBD volumes stats")
        for pool in self.report.raw["osdmap"]["pools"]:
            if 'application_metadata' in pool:
                if 'rbd' not in pool.get('application_metadata'):
                    continue
            elif 'rgw' in pool['pool_name']:
                continue

            name = pool['pool_name']
            await self.run_and_save_output(f'rbd_du_{name}', self.rbd_cmd + "du", 'txt')
            sep = '-' * 60
            cmd = f'for image in $(rbd list -p rbd) ; do echo "{sep}" ; " + \
                f"echo "$image" ; {self.rbd_cmd} info "$image" ; done'
            await self.run_and_save_output(f"rbd_images_{name}", cmd, "txt")

    async def collect_crushmap(self):
        temp_fl = "%08X" % random.randint(0, 2 << 64)
        cr_fname = f"/tmp/ceph_collect.{temp_fl}.cr"
        crushmap_res = await self.node.run(self.ceph_cmd + f"osd getcrushmap -o {cr_fname}")
        if crushmap_res.returncode != 0:
            logger.error("Fail to get crushmap")
        else:
            await self.read_and_save('crushmap', cr_fname, 'bin')
            crushtool_res = await self.node.run(f"crushtool -d {cr_fname} -o {cr_fname}.txt")
            if crushtool_res.returncode != 0:
                logger.error("Fail to decompile crushmap")
            else:
                await self.read_and_save('crushmap', cr_fname + ".txt", 'txt')

    async def collect_osd_map(self):
        temp_fl = "%08X" % random.randint(0, 2 << 64)
        osd_fname = f"/tmp/ceph_collect.{temp_fl}.osd"
        osdmap_res = await self.node.run(self.ceph_cmd + f"osd getmap -o {osd_fname}")
        if osdmap_res.returncode != 0:
            logger.error("Fail to get osdmap")
        else:
            await self.read_and_save('osdmap', osd_fname, 'bin')
            await self.run_and_save_output('osdmap', f"osdmaptool --print {osd_fname} txt")

    async def collect_rgw_info(self):
        await self.run_and_save_output("realm_list", self.radosgw_admin_cmd + "realm list", "txt")
        await self.run_and_save_output("zonegroup_list", self.radosgw_admin_cmd + "zonegroup list", "txt")
        await self.run_and_save_output("zone_list", self.radosgw_admin_cmd + "zone list", "txt")

    async def collect_txt(self):
        for cmd in ("status", "osd tree", "df", "osd df", "rados df", "osd dump", "osd blocked-by"):
            await self.run_and_save_output(cmd.replace(" ", "_"), self.ceph_cmd_txt + cmd, 'txt')
        await self.run_and_save_output("-s", "ceph_s", 'txt')


async def collect_rbd_volumes_info(report, ceph_report: CephReport):
    logger.debug("Collecting RBD volumes stats")
    for pool in report.raw["osdmap"]["pools"]:
        if 'application_metadata' in pool:
            if 'rbd' not in pool.get('application_metadata'):
                continue
        elif 'rgw' in pool['pool_name']:
            continue

        name = pool['pool_name']

        await collector(f'rbd_du_{name}').rbd("du")

        sep = '-' * 60
        cmd = f'for image in $(rbd list -p rbd) ; do echo "{sep}" ; " + \
            f"echo "$image" ; {self.rbd_cmd} info "$image" ; done'
        await collector(f"rbd_images_{name}").bash(cmd)
        await collector(f"rbd_images_{name}").ceph_js(cmd)
        await collector(f"rbd_images_{name}").ceph(cmd)

        await self.run_and_save_output(, cmd, "txt")



class CephOSDCollector(Collector, CephBase):
    name = 'ceph-ops'

    def __init__(self, storage: IStorageNNP, opts: Any, node: IAgentRPCNode, hostname: str, report: CephReport) -> None:
        Collector.__init__(self, storage, opts, node, hostname)
        CephBase.__init__(self, opts, report)

    def with_storage(self, storage: IStorageNNP) -> 'CephOSDCollector':
        return self.__class__(storage, self.opts, self.node, self.hostname, self.report)

    async def collect(self):
        # check OSD process status
        psaux = await self.node.run_str("ps aux | grep ceph-osd")
        osd_re = re.compile(r".*?\s+(?P<pid>\d+)\s.*?\bceph-osd[\t ]+.*(-i|--id)[\t ]+(?P<osd_id>\d+)")

        running_osds = set()
        pids = set()
        for rr in osd_re.finditer(psaux):
            running_osds.add(int(rr.group('osd_id')))
            pids.add(int(rr.group('pid')))

        ids_from_ceph = [meta["id"] for meta in self.report.raw["osd_metadata"] if meta["hostname"] == self.hostname]
        unexpected_osds = running_osds.difference(ids_from_ceph)

        for osd_id in unexpected_osds:
            logger.warning(f"Unexpected osd-{osd_id} in node {self.hostname}")

        path_pp = f"hosts/{self.hostname}"

        cephdisklist_js = await self.run_and_save_output(f"{path_pp}/cephdisk", "ceph-disk list --format=json", "json")
        cephvollist_js = await self.run_and_save_output(f"{path_pp}/cephvolume",
                                                        "ceph-volume lvm list --format=json", "json")
        lsblk_js = await self.run_and_save_output(f"{path_pp}/lsblk", "lsblk -a --json", "json")
        await self.run_and_save_output(f"{path_pp}/lsblk", "lsblk -a")

        if self.opts.collect_txt:
            await self.run_and_save_output(f"{path_pp}/cephdisk", "ceph-disk list")
            await self.run_and_save_output(f"{path_pp}/cephvolume", "ceph-volume lvm list")

        dev_tree = parse_devices_tree(json.loads(lsblk_js))

        if cephvollist_js.returncode == 0:
            devs_for_osd: Dict[int, Dict[str, str]] = parse_ceph_volumes_js(cephvollist_js.stdout)
        elif cephdisklist_js.returncode == 0:
            devs_for_osd = parse_ceph_disk_js(cephdisklist_js.stdout)
        else:
            devs_for_osd = {}

        for osd in ids_from_ceph:
            with self.chdir(f'osd/{osd.osd_id}') as collector:
                await collector.collect_osd_info(osd.osd_id,
                                                 devs_for_osd.get(osd.osd_id),
                                                 dev_tree,
                                                 running_osds)

        logger.debug(f"Found next pids for OSD's on node {self.hostname}: {pids}")
        for pid in pids:
            with self.chdir(f'osd/{pid}') as collector:
                logger.debug("Collecting info for OSD with pid %s", pid)
                await collector.collect_process_info(pid)

    async def collect_osd_info(self, osd_id: int, devs: Optional[Dict[str, str]],
                               dev_tree: Dict[str, str], running_osds: List[int]) -> None:

        cmd = f"tail -n {self.opts.ceph_log_max_lines} /var/log/ceph/ceph-osd.{osd_id}.log"
        await self.run_and_save_output("log", cmd)

        osd_daemon_cmd = f"ceph --admin-daemon /var/run/ceph/ceph-osd.{osd_id}.asok"
        await self.run_and_save_output("perf_dump", osd_daemon_cmd + " perf dump")
        await self.run_and_save_output("perf_hist_dump", osd_daemon_cmd + " perf histogram dump")

        if osd_id in running_osds:
            ceph_conf_res = await self.run_and_save_output("config",
                                                           f"ceph daemon osd.{osd_id} config show",
                                                           "json")
            assert ceph_conf_res.returncode == 0
            self.save("config", "json", 0, ceph_conf_res.stdout)
        else:
            logger.warning(f"osd-{osd_id} in node {self.hostname} is down. No config available")

        if devs:
            stor_type = devs['store_type']

            if stor_type == 'filestore':
                data_dev = devs["path"]
                j_dev = devs["journal_dev"]
                osd_dev_conf = {'data': data_dev,
                                'journal': j_dev,
                                'r_data': dev_tree[data_dev],
                                'r_journal': dev_tree[j_dev],
                                'type': stor_type}
            else:
                assert stor_type == 'bluestore'
                data_dev = devs["block_dev"]
                db_dev = devs.get('block.db_dev', data_dev)
                wal_dev = devs.get('block.wal_dev', db_dev)
                osd_dev_conf = {'data': data_dev,
                                'wal': wal_dev,
                                'db': db_dev,
                                'r_data': dev_tree[data_dev],
                                'r_wal': dev_tree[wal_dev],
                                'r_db': dev_tree[db_dev],
                                'type': stor_type}

            self.save('devs_cfg', 'json', 0, json.dumps(osd_dev_conf))
        else:
            self.save('devs_cfg', 'json', 0, json.dumps({}))

    async def collect_process_info(self, pid: int):

        cmdline = (await self.node.read(f'/proc/{pid}/cmdline')).decode()
        opts = cmdline.split("\x00")
        for op, val in zip(opts[:-1], opts[1:]):
            if op in ('-i', '--id'):
                break
        else:
            logger.warning(f"Can't get osd id for cmd line {cmdline.replace(chr(0), ' ')}")
            return

        osd_proc_info = {}  # type: Dict[str, Any]

        pid_dir = f"/proc/{pid}/"
        self.save("cmdline", "bin", 0, cmdline)
        osd_proc_info['fd_count'] = len(list(await self.node.iterdir(pid_dir + "fd")))

        fpath = pid_dir + "net/sockstat"
        ssv4 = parse_sockstat_file((await self.node.read(fpath)).decode())
        if not ssv4:
            logger.warning(f"Broken file {fpath!r} on node {self.node}")
        else:
            osd_proc_info['ipv4'] = ssv4

        fpath = pid_dir + "net/sockstat6"
        ssv6 = parse_sockstat_file((await self.node.read(fpath)).decode())
        if not ssv6:
            logger.warning("Broken file {!r} on node {}".format(fpath, self.node))
        else:
            osd_proc_info['ipv6'] = ssv6
        osd_proc_info['sock_count'] = await self.node.conn.fs.count_sockets_for_process(pid)

        proc_stat = (await self.node.read(pid_dir + "status")).decode()
        self.save("proc_status", "txt", 0, proc_stat)
        osd_proc_info['th_count'] = int(proc_stat.split('Threads:')[1].split()[0])

        # IO stats
        io_stat = (await self.node.read(pid_dir + "io")).decode()
        osd_proc_info['io'] = parse_info_file_from_proc(io_stat)

        # Mem stats
        mem_stat = (await self.node.read(pid_dir + "status")).decode()
        osd_proc_info['mem'] = parse_info_file_from_proc(mem_stat)

        # memmap
        mem_map = (await self.node.read(pid_dir + "maps", compress=True)).decode()
        osd_proc_info['memmap'] = []
        for ln in mem_map.strip().split("\n"):
            mem_range, access, offset, dev, inode, *pathname = ln.split()
            osd_proc_info['memmap'].append([mem_range, access, " ".join(pathname)])

        # sched
        sched = (await self.node.read(pid_dir + "sched", compress=True)).decode()
        try:
            data = "\n".join(sched.strip().split("\n")[2:])
            osd_proc_info['sched'] = parse_info_file_from_proc(data, ignore_err=True)
        except:
            osd_proc_info['sched'] = {}
            osd_proc_info['sched_raw'] = sched

        stat = (await self.node.read(pid_dir + "stat")).decode()
        osd_proc_info['stat'] = stat.split()

        self.save("procinfo", "json", 0, json.dumps(osd_proc_info))


class CephMonCollector(Collector, CephBase):
    name = 'ceph-mon'

    def __init__(self, storage: IStorageNNP, opts: Any, node: IAgentRPCNode, hostname: str, report: CephReport) -> None:
        Collector.__init__(self, storage.sub_storage(f"mon/{hostname}"), opts, node, hostname)
        CephBase.__init__(self, opts, report)
        self.ceph_node = node

    def with_storage(self, storage: IStorageNNP) -> 'CephMonCollector':
        return self.__class__(storage, self.opts, self.ceph_node, self.hostname, self.report)

    async def collect(self):
        await self.run_and_save_output("mon_daemons", "ps aux | grep ceph-mon")
        tail_ln = f"tail -n {self.opts.ceph_log_max_lines} "
        await self.run_and_save_output("mon_log", tail_ln + f"/var/log/ceph/ceph-mon.{self.hostname}.log")
        await self.run_and_save_output("ceph_log", tail_ln + " /var/log/ceph/ceph.log")

        log_issues = await self.node.conn.ceph.find_issues_in_ceph_log(self.opts.ceph_log_max_lines)
        self.save("ceph_log_wrn_err", "txt", 0, log_issues)

        with ignore_all:
            issues_count, regions = await self.node.conn.ceph.analyze_ceph_logs_for_issues()
            self.save("log_issues_count", "json", 0, json.dumps(issues_count))
            self.save("status_regions", "json", 0, json.dumps(regions))

        await self.run_and_save_output("ceph_audit", tail_ln + " /var/log/ceph/ceph.audit.log")
        await self.run_and_save_output("config", self.ceph_cmd + f"daemon mon.{self.hostname} config show",
                                       data_format='json')

        await self.run_and_save_output("ceph_var_dirs_size", "du -s /var/lib/ceph/m*")


class NodeCollector(Collector):
    name = 'node'

    node_commands = [
        ("df", 'txt', 'df'),
        ("dmidecode", "txt", "dmidecode"),
        ("dmesg", "txt", "dmesg"),
        ("ipa4", "txt", "ip -o -4 a"),
        ("ipa", "txt", "ip a"),
        ("ifconfig", "txt", "ifconfig"),
        ("ifconfig_short", "txt", "ifconfig -s"),
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

    def __init__(self, storage: IStorageNNP, opts: Any, node: IAsyncNode, hostname: str) -> None:
        Collector.__init__(self, storage.sub_storage(f'hosts/{self.hostname}'), opts, node, hostname)

    def with_storage(self, storage: IStorageNNP) -> 'NodeCollector':
        return self.__class__(storage, self.opts, self.node, self.hostname)

    async def collect(self) -> None:
        await self.collect_kernel_modules_info()
        await self.collect_common_features()
        await self.collect_files()
        await self.collect_bonds_info()
        await self.collect_interfaces_info()
        await self.collect_block_devs()
        await self.collect_packages()

    async def collect_kernel_modules_info(self):
        try:
            await self.run_and_save_output("lsmod", "lsmod", data_format="txt")
        except Exception as exc:
            logger.warning(f"Failed to list kernel modules on node {self.hostname}: {exc}")
            return

        try:
            await self.run_and_save_output(
                "modinfo_all",
                "for name in $(lsmod | awk '{print $1}') ; do modinfo $name ; echo '-----' ; done",
                "txt")
        except Exception as exc:
            logger.warning(f"Failed to list kernel modules info on {self.hostname}: {exc}")
        return

    async def collect_common_features(self):
        for path_offset, frmt, cmd in self.node_commands:
            await self.run_and_save_output(path_offset, cmd, data_format=frmt)

    async def collect_files(self):
        for fpath in self.node_files:
            await self.read_and_save(os.path.basename(fpath), fpath)

        for name, fpath in self.node_renamed_files:
            await self.read_and_save(name, fpath)

    async def collect_bonds_info(self):
        # collect_bonds_info
        bondmap = {}
        if await self.node.exists("/proc/net/bonding"):
            for fname in (await self.node.iterdir("/proc/net/bonding")):
                await self.read_and_save(f"bond_{fname}", f"/proc/net/bonding/{fname}")
                bondmap[fname] = f"bond_{fname}"
        self.save("bonds", 'json', 0, json.dumps(bondmap))

    async def collect_packages(self):
        try:
            if await self.node.exists("/etc/debian_version"):
                await self.run_and_save_output("packages_deb", "dpkg -l", data_format="txt")
            else:
                await self.run_and_save_output("packages_rpm", "yum list installed", data_format="txt")
        except Exception as exc:
            logger.warning(f"Failed to download packages information from node {self.node}: {exc}")

    async def collect_block_devs(self) -> None:
        bdevs_info = await self.node.conn.fs.get_block_devs_info()
        bdevs_info = {name: data for name, data in bdevs_info.items()}
        for name_prefix in ['loop']:
            for name in bdevs_info:
                if name.startswith(name_prefix):
                    del bdevs_info[name]

        tools = ['hdparm', 'smartctl', 'nvme']
        missing = [name for exists, name in zip((await self.node.conn.fs.binarys_exists(tools)), tools) if not exists]

        if 'nvme' not in missing:
            nvme_res = await self.node.run('nvme version')
            if nvme_res.returncode != 0:
                ver: float = 0
            else:
                try:
                    *_, version = nvme_res.stdout.split()
                    ver = float(version)
                except:
                    ver = 0

            if ver < 1.0:
                logger.warning(f"Nvme tool too old {ver}, at least 1.0 version is required")
            else:
                nvme_list_js = await self.run_and_save_output('nvme_list', 'nvme list -o json', data_format='json')
                if nvme_list_js.returncode == 0:
                    try:
                        for dev in json.loads(nvme_list_js.stdout)['Devices']:
                            name = os.path.basename(dev['DevicePath'])
                            await self.run_and_save_output(f'block_devs/{name}/nvme_smart_log',
                                                           f"nvme smart-log {dev['DevicePath']} -o json",
                                                           data_format='json')
                    except:
                        logging.warning("Failed to process nvme list output")

        if missing:
            logger.warning(f"{','.join(missing)} is not installed on {self.hostname}")

        lsblk_res = await self.run_and_save_output("lsblkjs", "lsblk -O -b -J", data_format="json")

        if lsblk_res.returncode == 0:
            for dev_node in json.loads(lsblk_res.stdout)['blockdevices']:
                name = dev_node['name']

                if 'hdparm' not in missing:
                    await self.run_and_save_output(f'block_devs/{name}/hdparm', f"sudo hdparm -I /dev/{name}")

                if 'smartctl' not in missing:
                    await self.run_and_save_output(f'block_devs/{name}/smartctl', f"sudo smartctl -a /dev/{name}")

    async def collect_interfaces_info(self):
        interfaces = {}
        for is_phy, dev in await get_host_interfaces(self.node):
            interface = {'dev': dev, 'is_phy': is_phy}
            interfaces[dev] = interface

            if is_phy:
                ethtool_res = await self.node.run("ethtool " + dev)
                if ethtool_res.returncode == 0:
                    interface['ethtool'] = ethtool_res.stdout

                iwconfig_res = await self.node.run("iwconfig " + dev)
                if iwconfig_res.returncode == 0:
                    interface['iwconfig'] = iwconfig_res.stdout

        self.save('interfaces', 'json', 0, json.dumps(interfaces))

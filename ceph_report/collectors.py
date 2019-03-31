import os
import re
import time
import json
import array
import random
import logging
import datetime
import contextlib
from typing import Any, Union, List, Dict, cast

from ceph_report.collect_utils import parse_devices_tree, parse_sockstat_file, parse_proc_file, get_host_interfaces
from cephlib.istorage_nnp import IStorageNNP
from agent.utils import CMDResult

from .collect_node_classes import INode, Node

logger = logging.getLogger('collect')


AUTOPG = -1


class Collector:
    """Base class for data collectors. Can collect data for only one node."""
    name: str = "BaseCollector"
    collect_roles: List[str] = []

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

    async def download_file(self, path: str, file_path: str, frmt: str = 'txt', compress: bool = True) -> bytes:
        """Download file from node and save it into storage"""
        try:
            content = await self.node.get_file(file_path, compress=compress)
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
            save = res.stdout + res.stderr_b.decode("utf8")
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

    async def collect_master(self) -> None:
        with self.chdir("master"):
            time2 = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            curr_data = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}\n{time2}\n{time.time()}"

            # TODO: detect ceph version from monitor versions
            osd_ver_res = await self.node.run(self.ceph_cmd + "osd versions")
            is_luminous = (osd_ver_res.returncode == 0)
            if is_luminous:
                self.save("osd_versions2", 'txt', 0, osd_ver_res.stdout)
                await self.run_and_save_output("mon_versions2", self.ceph_cmd + "mon versions", "txt")
                await self.run_and_save_output("mgr_versions2", self.ceph_cmd + "mgr versions", "txt")

            await self.run_and_save_output("osd_versions", self.ceph_cmd + "tell 'osd.*' version", "txt")
            await self.run_and_save_output("mon_versions", self.ceph_cmd + "tell 'mon.*' version", "txt")

            self.save("collected_at", 'txt', 0, curr_data)
            status = await self.run_and_save_output("status", self.ceph_cmd + "status", 'json')
            status = json.loads(status.stdout)

            health = status['health']
            status_str = health['status'] if 'status' in health else health['overall_status']
            ceph_health:  Dict[str, Union[str, int]] = {'status': status_str}
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
                    f"pg dump skipped, as num_pg ({self.__class__.num_pgs}) > max_pg_dump_count ({max_pg})." +
                    " Use --max-pg-dump-count NUM option to change the limit")
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
                await self.run_and_save_output(cmd.replace(" ", "_").replace("-", '_'), self.ceph_cmd + cmd, 'json')

            osd_dump = await self.run_and_save_output('osd_dump', self.ceph_cmd + "osd dump", 'json')

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
                    await self.run_and_save_output(f'rbd_du_{pool}', self.rbd_cmd + "du", 'txt')
                    sep = '-' * 60
                    cmd = f'for image in $(rbd list -p rbd) ; do echo "{sep}" ; " + \
                        f"echo "$image" ; {self.rbd_cmd} info "$image" ; done'
                    await self.run_and_save_output(f"rbd_images_{pool}", cmd, "txt")

            await self.run_and_save_output("rados_df", self.rados_cmd + "df", 'json')
            await self.run_and_save_output("rados_df", self.rados_cmd_txt + "df", 'txt')

            await self.run_and_save_output("realm_list", self.radosgw_admin_cmd + "realm list", "txt")
            await self.run_and_save_output("zonegroup_list", self.radosgw_admin_cmd + "zonegroup list", "txt")
            await self.run_and_save_output("zone_list", self.radosgw_admin_cmd + "zone list", "txt")

            await self.run_and_save_output("default_config", self.ceph_cmd_txt + "--show-config", 'txt')
            await self.run_and_save_output("ceph_s", self.ceph_cmd_txt + "-s", 'txt')
            await self.run_and_save_output("ceph_osd_dump", self.ceph_cmd_txt + "osd dump", 'txt')
            await self.run_and_save_output("osd_utilization", self.ceph_cmd_txt + 'osd utilization', 'txt')
            await self.run_and_save_output("osd_blocked_by", self.ceph_cmd_txt + "osd blocked-by", "txt")
            await self.run_and_save_output("osd dump", self.ceph_cmd_txt + "osd dump", "txt")

            temp_fl = "%08X" % random.randint(0, 2 << 64)
            cr_fname = f"/tmp/ceph_collect.{temp_fl}.cr"
            crushmap_res = await self.node.run(self.ceph_cmd + f"osd getcrushmap -o {cr_fname}")
            if crushmap_res.returncode != 0:
                logger.error("Fail to get crushmap")
            else:
                await self.download_file('crushmap', cr_fname, 'bin')
                crushtool_res = await self.node.run(f"crushtool -d {cr_fname} -o {cr_fname}.txt")
                if crushtool_res.returncode != 0:
                    logger.error("Fail to decompile crushmap")
                else:
                    await self.download_file('crushmap', cr_fname + ".txt", 'txt')

            osd_fname = f"/tmp/ceph_collect.{temp_fl}.osd"
            osdmap_res = await self.node.run(self.ceph_cmd + f"osd getmap -o {osd_fname}")
            if osdmap_res.returncode != 0:
                logger.error("Fail to get osdmap")
            else:
                await self.download_file('osdmap', osd_fname, 'bin')
                await self.run_and_save_output('osdmap', f"osdmaptool --print {osd_fname} txt")

    async def collect_osd(self):
        assert isinstance(self.node, Node)
        # check OSD process status
        psaux = (await self.node.run("ps aux | grep ceph-osd"))
        assert psaux.returncode == 0
        osd_re = re.compile(r"ceph-osd[\t ]+.*(-i|--id)[\t ]+(\d+)")
        running_osds = set(int(rr.group(2)) for rr in osd_re.finditer(psaux.stdout))

        ids_from_ceph = set(osd.osd_id for osd in cast(Node, self.node).osds)
        unexpected_osds = running_osds.difference(ids_from_ceph)

        for osd_id in unexpected_osds:
            logger.warning("Unexpected osd-{} in node {}.".format(osd_id, self.node))

        with self.chdir('hosts/' + self.node.name):
            await self.run_and_save_output("cephdisk", "ceph-disk list")
            await self.run_and_save_output("cephvolume", "ceph-volume lvm list")
            cephdisklist_js = await self.run_and_save_output("cephdisk", "ceph-disk list --format=json", "json")
            cephvollist_js = await self.run_and_save_output("cephvolume", "ceph-volume lvm list --format json", "json")
            lsblk_js = (await self.run_and_save_output("lsblk", "lsblk -a --json", "json")).stdout
            await self.run_and_save_output("lsblk", "lsblk -a")

        devs_for_osd = {}  # type: Dict[int, Dict[str, str]]
        dev_tree = parse_devices_tree(json.loads(lsblk_js))

        if cephvollist_js.returncode == 0:
            cephvolume_dct = json.loads(cephvollist_js.stdout)
            for osd_id_s, osd_data in cephvolume_dct.items():
                assert len(osd_data) == 1
                osd_data = osd_data[0]
                assert len(osd_data['devices']) == 1
                dev = osd_data['devices'][0]
                devs_for_osd[int(osd_id_s)] = {"block_dev": dev,
                                               "block.db_dev": dev,
                                               "block.wal_dev": dev,
                                               "store_type": "bluestore"}

        if cephdisklist_js.returncode == 0:
            cephdisk_dct = json.loads(cephdisklist_js.stdout)
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
            with self.chdir(f'osd/{osd.osd_id}'):
                cmd = f"tail -n {self.opts.ceph_log_max_lines} /var/log/ceph/ceph-osd.{osd.osd_id}.log"
                await self.run_and_save_output("log", cmd)

                osd_daemon_cmd = f"ceph --admin-daemon /var/run/ceph/ceph-osd.{osd.osd_id}.asok"
                await self.run_and_save_output("perf_dump", osd_daemon_cmd + " perf dump")
                await self.run_and_save_output("perf_hist_dump", osd_daemon_cmd + " perf histogram dump")

                # TODO: much of this can be done even id osd is down for filestore
                if osd.osd_id in running_osds:
                    if not osd.config:
                        ceph_conf_res = await self.run_and_save_output("config",
                                                                       f"ceph daemon osd.{osd.osd_id} config show",
                                                                       "json")
                        assert ceph_conf_res.returncode == 0
                        self.save("config", "json", 0, ceph_conf_res.stdout)
                    else:
                        self.save("config", "txt", 0, osd.config)
                else:
                    logger.warning(f"osd-{osd.osd_id} in node {self.node.name} is down. No config available")

                if osd.osd_id in devs_for_osd:
                    stor_type = devs_for_osd[osd.osd_id]['store_type']

                    if stor_type == 'filestore':
                        data_dev = devs_for_osd[osd.osd_id]["path"]
                        j_dev = devs_for_osd[osd.osd_id]["journal_dev"]
                        osd_dev_conf = {'data': data_dev,
                                        'journal': j_dev,
                                        'r_data': dev_tree[data_dev],
                                        'r_journal': dev_tree[j_dev],
                                        'type': stor_type}
                    else:
                        assert stor_type == 'bluestore'
                        data_dev = devs_for_osd[osd.osd_id]["block_dev"]
                        db_dev = devs_for_osd[osd.osd_id].get('block.db_dev', data_dev)
                        wal_dev = devs_for_osd[osd.osd_id].get('block.wal_dev', db_dev)
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

        pids = await self.node.rpc.fs.find_pids_for_cmd('ceph-osd')
        logger.debug("Found next pids for OSD's on node %s: %r", self.node.name, pids)
        if pids:
            for pid in pids:
                await self.collect_osd_process_info(pid)

    async def collect_osd_process_info(self, pid: int):
        logger.debug("Collecting info for OSD with pid %s", pid)
        assert isinstance(self.node, Node)

        cmdline = (await self.node.get_file(f'/proc/{pid}/cmdline')).decode('utf8')
        opts = cmdline.split("\x00")
        for op, val in zip(opts[:-1], opts[1:]):
            if op in ('-i', '--id'):
                osd_id = val
                break
        else:
            logger.warning(f"Can't get osd id for cmd line {cmdline.replace(chr(0), ' ')}")
            return

        osd_proc_info = {}  # type: Dict[str, Any]

        with self.chdir(f'osd/{osd_id}'):
            pid_dir = f"/proc/{pid}/"
            self.save("cmdline", "bin", 0, cmdline)
            osd_proc_info['fd_count'] = len(await self.node.rpc.fs.listdir(pid_dir + "fd"))

            fpath = pid_dir + "net/sockstat"
            ssv4 = parse_sockstat_file((await self.node.get_file(fpath)).decode('utf8'))
            if not ssv4:
                logger.warning(f"Broken file {fpath!r} on node {self.node}")
            else:
                osd_proc_info['ipv4'] = ssv4

            fpath = pid_dir + "net/sockstat6"
            ssv6 = parse_sockstat_file((await self.node.get_file(fpath)).decode('utf8'))
            if not ssv6:
                logger.warning("Broken file {!r} on node {}".format(fpath, self.node))
            else:
                osd_proc_info['ipv6'] = ssv6
            osd_proc_info['sock_count'] = await self.node.rpc.fs.count_sockets_for_process(pid)

            proc_stat = (await self.node.get_file(pid_dir + "status")).decode('utf8')
            self.save("proc_status", "txt", 0, proc_stat)
            osd_proc_info['th_count'] = int(proc_stat.split('Threads:')[1].split()[0])

            # IO stats
            io_stat = (await self.node.get_file(pid_dir + "io")).decode('utf8')
            osd_proc_info['io'] = parse_proc_file(io_stat)

            # Mem stats
            mem_stat = (await self.node.get_file(pid_dir + "status")).decode('utf8')
            osd_proc_info['mem'] = parse_proc_file(mem_stat)

            # memmap
            mem_map = (await self.node.get_file(pid_dir + "maps", compress=True)).decode('utf8')
            osd_proc_info['memmap'] = []
            for ln in mem_map.strip().split("\n"):
                mem_range, access, offset, dev, inode, *pathname = ln.split()
                osd_proc_info['memmap'].append([mem_range, access, " ".join(pathname)])

            # sched
            sched = (await self.node.get_file(pid_dir + "sched", compress=True)).decode('utf8')
            try:
                data = "\n".join(sched.strip().split("\n")[2:])
                osd_proc_info['sched'] = parse_proc_file(data, ignore_err=True)
            except:
                osd_proc_info['sched'] = {}
                osd_proc_info['sched_raw'] = sched

            stat = (await self.node.get_file(pid_dir + "stat")).decode('utf8')
            osd_proc_info['stat'] = stat.split()

            self.save("procinfo", "json", 0, json.dumps(osd_proc_info))

    async def collect_monitor(self):
        assert isinstance(self.node, Node)
        assert self.node.mon is not None

        with self.chdir(f"mon/{self.node.mon}"):
            await self.run_and_save_output("mon_daemons", "ps aux | grep ceph-mon")
            tail_ln = f"tail -n {self.opts.ceph_log_max_lines} "
            await self.run_and_save_output("mon_log", tail_ln + f"/var/log/ceph/ceph-mon.{self.node.mon}.log")
            await self.run_and_save_output("ceph_log", tail_ln + " /var/log/ceph/ceph.log")

            log_issues = await self.node.rpc.ceph.find_issues_in_ceph_log(self.opts.ceph_log_max_lines)
            self.save("ceph_log_wrn_err", "txt", 0, log_issues)

            # TODO: fix me
            try:
                # sometime unknown exc happened
                issues_count, regions = await self.node.rpc.ceph.analyze_ceph_logs_for_issues()
                self.save("log_issues_count", "json", 0, json.dumps(issues_count))
                self.save("status_regions", "json", 0, json.dumps(regions))
            except Exception:
                pass

            await self.run_and_save_output("ceph_audit", tail_ln + " /var/log/ceph/ceph.audit.log")
            await self.run_and_save_output("config", self.ceph_cmd + f"daemon mon.{self.node.mon} config show",
                                           data_format='json')

            await self.run_and_save_output("ceph_var_dirs_size", "du -s /var/lib/ceph/m*")

            log_issues = await self.node.rpc.ceph.find_issues_in_ceph_log(self.opts.ceph_log_max_lines)
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

    async def collect(self) -> None:
        with self.chdir('hosts/' + self.node.name):
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
            logger.warning("Failed to list kernel modules: %r on node %s: %s", "lsmod", self.node, exc)
            return

        try:
            await self.run_and_save_output(
                "modinfo_all",
                "for name in $(lsmod | awk '{print $1}') ; do modinfo $name ; echo '-----' ; done",
                "txt")
        except Exception as exc:
            logger.warning("Failed to list kernel modules info: %r on node %s: %s", "modinfo **", self.node, exc)
        return

    async def collect_common_features(self):
        for path_offset, frmt, cmd in self.node_commands:
            try:
                await self.run_and_save_output(path_offset, cmd, data_format=frmt)
            except Exception as exc:
                logger.warning("Failed to run %r on node %s: %s", cmd, self.node, exc)

    async def collect_files(self):
        for fpath in self.node_files:
            try:
                await self.download_file(os.path.basename(fpath), fpath)
            except Exception as exc:
                logger.warning("Failed to download file %r from node %s: %s", fpath, self.node, exc)

        for name, fpath in self.node_renamed_files:
            try:
                await self.download_file(name, fpath)
            except Exception as exc:
                logger.warning("Failed to download file %r from node %s: %s", fpath, self.node, exc)

    async def collect_bonds_info(self):
        # collect_bonds_info
        bondmap = {}
        if await self.node.exists("/proc/net/bonding"):
            for fname in (await self.node.listdir("/proc/net/bonding")):
                await self.download_file(f"bond_{fname}", f"/proc/net/bonding/{fname}")
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
        assert isinstance(self.node, Node)

        bdevs_info = await self.node.rpc.fs.get_block_devs_info()
        bdevs_info = {name: data for name, data in bdevs_info.items()}
        for name_prefix in ['loop']:
            for name in bdevs_info:
                if name.startswith(name_prefix):
                    del bdevs_info[name]

        hdparm_exists, smartctl_exists, nvme_exists = \
            await self.node.rpc.fs.binarys_exists(['hdparm', 'smartctl', 'nvme'])

        missing = []
        if not hdparm_exists:
            missing.append('hdparm')

        if not smartctl_exists:
            missing.append('smartctl-tools')

        if not nvme_exists:
            missing.append('nvme-tools')
        else:
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
            logger.warning(f"{','.join(missing)} is not installed on {self.node.name}")

        lsblk_res = await self.run_and_save_output("lsblkjs", "lsblk -O -b -J", data_format="json")
        if lsblk_res.returncode == 0:
            for dev_node in json.loads(lsblk_res.stdout)['blockdevices']:
                name = dev_node['name']
                with self.chdir('block_devs/' + name):
                    if hdparm_exists:
                        await self.run_and_save_output('hdparm', f"sudo hdparm -I /dev/{name}")

                    if smartctl_exists:
                        await self.run_and_save_output('smartctl', f"sudo smartctl -a /dev/{name}")

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


from __future__ import annotations

import os
import re
import time
import json
import array
import random
import asyncio
import logging
import datetime
import contextlib
import collections
from enum import IntEnum
from dataclasses import dataclass, field
from typing import Any, List, Dict, Optional, Tuple, TypeVar, Callable, Coroutine, Union, cast

from aiorpc import IAIORPCNode, ConnectionPool
from cephlib import CephRelease, parse_ceph_volumes_js, parse_ceph_disk_js, CephReport, OSDDevInfo, OSDBSDevices, \
    OSDFSDevices
from koder_utils import (IStorage, CMDResult, parse_devices_tree, collect_process_info, get_host_interfaces,
                         ignore_all, IAsyncNode)


logger = logging.getLogger('collect')


T = TypeVar('T')
VT = TypeVar('VT', str, bytes, array.array)


class StorFormat(IntEnum):
    txt = 0
    json = 1
    err = 2
    bin = 3
    xml = 4


@dataclass
class Collector:
    """Base class for data collectors. Can collect data for only one node."""
    storage: IStorage
    hostname: Optional[str]
    opts: Any
    pool: ConnectionPool
    pretty_json: bool = field(init=False, default=False)
    cmds: Dict[str, Tuple[str, StorFormat]] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.pretty_json = not self.opts.no_pretty_json
        self.cmds['bash'] = "", StorFormat.txt

    def with_storage(self: T, storage: IStorage) -> T:
        return self.__class__(storage, self.hostname, self.opts, self.pool)  # type: ignore

    @contextlib.asynccontextmanager
    async def connection(self) -> IAIORPCNode:
        assert self.hostname is not None
        async with self.pool.connection(self.hostname) as conn:
            yield conn

    def chdir(self: T, path: str) -> T:
        """Chdir for point in storage tree, where current results are stored"""
        return self.with_storage(self.storage.sub_storage(path))  # type: ignore

    def save_raw(self, path: str, data: bytes):
        self.storage.put_raw(data, path)

    def save(self, path: str, fmt: StorFormat, code: int, data: VT, extra: List[str] = None) -> VT:
        """Save results into storage"""
        rpath = f"{path}.{fmt.name if code == 0 else 'err'}"
        if isinstance(data, array.array):
            self.storage.put_array(rpath, data, extra if extra else [])
        elif isinstance(data, (str, bytes)):
            pretty_data: Union[str, bytes] = data
            if code == 0 and fmt == StorFormat.json and self.pretty_json:
                assert isinstance(data, (str, bytes))
                try:
                    dt = json.loads(data)
                except json.JSONDecodeError as exc:
                    logger.error(f"Failed to prettify json data for path {path}. Err {exc}. Saving as is")
                else:
                    pretty_data = json.dumps(dt, indent=4, sort_keys=True)

            data_b: bytes = pretty_data.encode() if isinstance(pretty_data, str) else cast(bytes, pretty_data)
            assert extra is None
            self.save_raw(rpath, data_b)
        else:
            raise TypeError(f"Can't save value of type {type(data)!r} (to {rpath!r})")

        return data

    async def run(self, *args, **kwargs) -> CMDResult:
        async with self.connection() as conn:
            return await conn.run(*args, **kwargs)

    async def read_and_save(self, path: str, file_path: str, fmt: StorFormat = StorFormat.txt,
                            compress: bool = True) -> bytes:
        """Download file from node and save it into storage"""
        async with self.connection() as conn:
            try:
                content = await conn.read(file_path, compress=compress)
                code = 0
            except (IOError, RuntimeError) as exc:
                logger.warning(f"Can't get file {file_path!r} from node {self.hostname}. {exc}")
                content = str(exc)  # type: ignore
                code = 1

        self.save(path, fmt, code, content)
        return content if code == 0 else None  # type: ignore

    async def run_and_save_output(self, path: str, cmd: str, fmt: StorFormat = StorFormat.txt) -> CMDResult:
        """Run command on node and store result into storage"""
        async with self.connection() as conn:
            logger.debug(f"{self.hostname} - {cmd}")
            res = await conn.run(cmd, merge_err=False)

        if res.returncode != 0:
            logger.warning(f"Cmd {cmd} failed on {self.hostname} with code {res.returncode}")
            fmt = StorFormat.err
            save = res.stdout + res.stderr_b.decode()
        else:
            save = res.stdout

        self.save(path, fmt, res.returncode, save)
        return res

    def __call__(self, path: str = None) -> CollectorProxy:
        return CollectorProxy(self, path)

    async def run_cmd_and_save_output(self, path: Optional[str], exe: str, args: str,
                                      fmt: Optional[StorFormat] = None) -> CMDResult:
        cmd, def_format = self.cmds[exe]
        if fmt is None:
            fmt = def_format

        if path is None:
            path = args.replace(" ", "_").replace("-", '_')

        extra_space = "" if (cmd.endswith(" ") or cmd == "") else " "
        return await self.run_and_save_output(path, cmd + extra_space + args, fmt)


class CollectorProxy:
    def __init__(self, collector_obj: Collector, path: str = None) -> None:
        self.collector_obj = collector_obj
        self.path = path

    # mypy does not support functions with default arguments
    def __getattr__(self, name: str) -> Callable[[str], Coroutine[Any, Any, CMDResult]]:
        assert name in self.collector_obj.cmds

        async def closure(args: str, fmt: Optional[StorFormat] = None) -> CMDResult:
            return await self.collector_obj.run_cmd_and_save_output(self.path, exe=name, args=args, fmt=fmt)

        return closure


@dataclass
class CephCollector(Collector):
    report: CephReport = None  # type: ignore

    def __post_init__(self) -> None:
        assert self.report is not None, f"Report must be provided for {self.__class__.__name__}"
        super().__post_init__()
        opt = " ".join(f"'{arg}'" for arg in self.opts.ceph_extra_args)
        self.cmds['radosgw'] = f"radosgw-admin {opt}", StorFormat.txt
        self.cmds['ceph_js'] = f"ceph {opt} --format json", StorFormat.json
        self.cmds['ceph'] = f"ceph {opt}", StorFormat.txt
        self.cmds['rbd'] = f"rbd {opt}", StorFormat.txt
        self.cmds['rados_js'] = f"rados {opt} --format json", StorFormat.json
        self.cmds['rados'] = f"rados {opt}", StorFormat.txt

    def with_storage(self, storage: IStorage) -> CephCollector:
        return self.__class__(storage, self.hostname, self.opts, self.pool, self.report)


class Role(IntEnum):
    base = 0
    ceph_master = 1
    ceph_osd = 2
    ceph_mon = 3
    node = 4


CollectFunc = Callable[[Collector], Coroutine[Any, Any, None]]
CephCollectFunc = Callable[[CephCollector], Coroutine[Any, Any, None]]


ALL_COLLECTORS: Dict[Role, List[Union[CephCollectFunc, CollectFunc]]] = collections.defaultdict(list)


def collector(role: Role) -> Callable[[CollectFunc], CollectFunc]:
    def closure(func: CollectFunc) -> CollectFunc:
        ALL_COLLECTORS[role].append(func)
        return func
    return closure


def ceph_collector(role: Role) -> Callable[[CephCollectFunc], CephCollectFunc]:
    def closure(func: CephCollectFunc) -> CephCollectFunc:
        ALL_COLLECTORS[role].append(func)
        return func
    return closure


@collector(Role.base)
async def collect_base(c: Collector) -> None:
    time2 = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    curr_data = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}\n{time2}\n{time.time()}"
    c.save("collected_at", StorFormat.txt, 0, curr_data)


@ceph_collector(Role.ceph_master)
async def save_versions(c: CephCollector) -> None:
    pre_luminous = c.report.version.release < CephRelease.luminous

    coros = []
    if pre_luminous:
        coros.append(c("osd_versions_old").ceph("tell 'osd.*' version"))
        coros.append(c("mon_versions_old").ceph("tell 'mon.*' version"))
    else:
        coros.append(c("versions").ceph_js("versions"))
        coros.append(c("mon_metadata").ceph_js("mon metadata"))

    coros.append(c("status").ceph_js("status"))

    cmds = ["df", "auth list", "mon_status", "osd perf", "osd df", "node ls", "features", "time-sync-status", "pg dump"]

    for cmd in cmds:
        coros.append(c().ceph_js(cmd))

    coros.append(c("rados_df").rados_js("df"))
    coros.append(c("default_config").ceph("--show-config"))
    coros.append(c("osd_blocked_by").ceph_js("osd blocked-by"))

    await asyncio.wait(coros)


@ceph_collector(Role.ceph_master)
async def collect_rbd_volumes_info(c: CephCollector) -> None:
    if c.opts.no_rbd_info:
        logger.debug("Collecting RBD volumes stats")
        rbd_cmd, _ = c.cmds['rbd']
        sep = '-' * 60
        for pool in c.report.osdmap.pools:
            if 'application_metadata' in pool:
                if 'rbd' not in pool.application_metadata:
                    continue
            elif 'rgw' in pool.pool_name:
                continue

            name = pool.pool_name
            await c(f'rbd_du_{name}').rbd(f"du -p {name}")

            cmd = f'for image in $(rbd list -p rbd) ; do echo "{sep}" ; " + \
                f"echo "$image" ; {rbd_cmd} info "$image" ; done'
            await c(f"rbd_images_{name}").bash(cmd)


@ceph_collector(Role.ceph_master)
async def collect_crushmap(c: CephCollector) -> None:
    if c.opts.collect_maps:
        cr_fname = f"/tmp/ceph_collect.{'%08X' % random.randint(0, 2 << 64)}.cr"
        ceph_cmd, _ = c.cmds['ceph']
        crushmap_res = await c.run(f"{ceph_cmd} osd getcrushmap -o {cr_fname}")

        if crushmap_res.returncode != 0:
            logger.error("Fail to get crushmap")
        else:
            await c.read_and_save('crushmap', cr_fname, StorFormat.bin)
            crushtool_res = await c.run(f"crushtool -d {cr_fname} -o {cr_fname}.txt")
            if crushtool_res.returncode != 0:
                logger.error("Fail to decompile crushmap")
            else:
                await c.read_and_save('crushmap', cr_fname + ".txt", StorFormat.txt)


@ceph_collector(Role.ceph_master)
async def collect_osd_map(c: CephCollector) -> None:
    if c.opts.collect_maps:
        osd_fname = f"/tmp/ceph_collect.{'%08X' % random.randint(0, 2 << 64)}.osd"
        ceph_cmd, _ = c.cmds['ceph']
        osdmap_res = await c.run(f"{ceph_cmd} osd getmap -o {osd_fname}")
        if osdmap_res.returncode != 0:
            logger.error("Fail to get osdmap")
        else:
            await c.read_and_save('osdmap', osd_fname, StorFormat.bin)
            await c.run_and_save_output('osdmap', f"osdmaptool --print {osd_fname}", StorFormat.txt)


@ceph_collector(Role.ceph_master)
async def collect_rgw_info(c: CephCollector) -> None:
    if not c.opts.collect_rgw:
        await c().radosgw("realm list")
        await c().radosgw("zonegroup list")
        await c().radosgw("zone list")


@ceph_collector(Role.ceph_master)
async def collect_ceph_txt(c: CephCollector) -> None:
    if c.opts.collect_txt:
        for cmd in ("status", "osd tree", "df", "osd df", "rados df", "osd dump", "osd blocked-by"):
            await c().ceph(cmd)
        await c("ceph_s").ceph("-s")


@ceph_collector(Role.ceph_osd)
async def collect_osd(c: CephCollector) -> None:
    # check OSD process status
    async with c.connection() as conn:
        psaux = await conn.run_str("ps aux | grep ceph-osd")
        osd_re = re.compile(r".*?\s+(?P<pid>\d+)\s.*?\bceph-osd[\t ]+.*(-i|--id)[\t ]+(?P<osd_id>\d+)")

        running_osds: Dict[int, int] = {}
        for rr in osd_re.finditer(psaux):
            osd_id = int(rr.group('osd_id'))
            running_osds[osd_id] = int(rr.group('pid'))

    ids_from_ceph = [meta.id for meta in c.report.osd_metadata if meta.hostname == c.hostname]
    unexpected_osds = set(running_osds).difference(ids_from_ceph)

    logger.info(f"Found next running osd's on node {c.hostname}: {list(running_osds.keys())}")
    logger.info(f"Expecting next osd's for {c.hostname}: {ids_from_ceph}")

    for osd_id in unexpected_osds:
        logger.warning(f"Unexpected osd-{osd_id} in node {c.hostname}")

    not_runnig = set(ids_from_ceph).difference(running_osds)
    if not_runnig:
        logger.warning(f"Next osd's not running on node {c.hostname}: {list(not_runnig)}")

    c_host = c.chdir(f"hosts/{c.hostname}")
    cephdisklist_js, cephvollist_js, lsblk_js = await asyncio.gather(
        c_host("cephdisk").bash("ceph-disk list --format=json", fmt=StorFormat.json),  # type: ignore
        c_host("cephvolume").bash("ceph-volume lvm list --format=json", fmt=StorFormat.json),  # type: ignore
        c_host("lsblk").bash("lsblk -a --json", fmt=StorFormat.json))  # type: ignore

    dev_tree = parse_devices_tree(json.loads(lsblk_js.stdout))

    if cephvollist_js.returncode == 0:
        devs_for_osd: Dict[int, OSDDevInfo] = parse_ceph_volumes_js(cephvollist_js.stdout)
    else:
        devs_for_osd = {}

    if not devs_for_osd and cephdisklist_js.returncode == 0:
        devs_for_osd = parse_ceph_disk_js(cephdisklist_js.stdout)

    logger.debug(f"Found next pids for OSD's on node {c.hostname}: {sorted(running_osds.values())}")

    coros: List[Coroutine[Any, Any, Any]] = [c_host("lsblk").bash("lsblk -a")]

    if c_host.opts.collect_txt:
        coros.append(c_host("cephdisk").bash("ceph-disk list"))
        coros.append(c_host("cephvolume").bash("ceph-volume lvm list"))

    for osd_id in ids_from_ceph:
        coros.append(collect_single_osd(c.chdir(f'osd/{osd_id}'),
                                        osd_id,
                                        pid=running_osds.get(osd_id),
                                        dev_tree=dev_tree,
                                        devs=devs_for_osd.get(osd_id)))
    await asyncio.wait(coros)


async def collect_single_osd(c: CephCollector,
                             osd_id: int,
                             pid: Optional[int],
                             dev_tree: Dict[str, str],
                             devs: Optional[OSDDevInfo]) -> None:

    await c("log").bash(f"tail -n {c.opts.ceph_log_max_lines} /var/log/ceph/ceph-osd.{osd_id}.log")
    await c("perf_dump").ceph(f"daemon osd.{osd_id} perf dump")
    await c("perf_hist_dump").ceph(f"daemon osd.{osd_id} perf histogram dump")

    if pid is not None:
        await c("config").ceph(f"daemon osd.{osd_id} config show", fmt=StorFormat.json)  # type: ignore
    else:
        logger.warning(f"osd-{osd_id} in node {c.hostname} is down. No config available")

    if devs:
        if isinstance(devs, OSDFSDevices):
            data_dev = str(devs.data)
            j_dev = str(devs.journal)
            osd_dev_conf = {'data': data_dev,
                            'journal': str(j_dev),
                            'r_data': dev_tree[data_dev],
                            'r_journal': dev_tree[j_dev],
                            'type': 'filestore'}
        else:
            assert isinstance(devs, OSDBSDevices)
            data_dev = str(devs.block)
            db_dev = str(devs.db)
            wal_dev = str(devs.wal)
            osd_dev_conf = {'data': data_dev,
                            'wal': wal_dev,
                            'db': db_dev,
                            'r_data': dev_tree[data_dev],
                            'r_wal': dev_tree[wal_dev],
                            'r_db': dev_tree[db_dev],
                            'type': 'bluestore'}

    else:
        osd_dev_conf = {}

    c.save('devs_cfg', StorFormat.json, 0, json.dumps(osd_dev_conf))

    logger.debug(f"Collecting info for osd.{osd_id} with pid {pid}")
    async with c.connection() as conn:
        info = await collect_process_info(conn, pid)
    c.save("proc_info", StorFormat.json, 0, json.dumps(info.__dict__))


AVERAGE_BYTES_PER_CEPH_LOG_LINE = 143


@ceph_collector(Role.ceph_mon)
async def collect_mon_info(c: CephCollector) -> None:
    await c("mon_daemons").bash("ps aux | grep ceph-mon")

    # tail = f"tail -n {c.opts.ceph_log_max_lines}"
    # await c("mon_log").bash(f"{tail} /var/log/ceph/ceph-mon.{c.hostname}.log")
    # await c("ceph_log").bash(f"{tail} /var/log/ceph/ceph.log")
    # await c("ceph_audit").bash(f"{tail} /var/log/ceph/ceph.audit.log")

    async with c.connection() as conn:
        read_size = AVERAGE_BYTES_PER_CEPH_LOG_LINE * c.opts.ceph_log_max_lines

        dt = [chunk async for chunk in conn.tail_file(f"/var/log/ceph/ceph-mon.{c.hostname}.log", read_size)]
        c.save("mon_log", StorFormat.txt, 0, b"".join(dt).decode())

        dt = [chunk async for chunk in conn.tail_file("/var/log/ceph/ceph.log", read_size)]
        c.save("ceph_log", StorFormat.txt, 0, b"".join(dt).decode())

        dt = [chunk async for chunk in conn.tail_file(f"/var/log/ceph/ceph.audit.log", read_size)]
        c.save("ceph_audit", StorFormat.txt, 0, b"".join(dt).decode())

        log_issues = await conn.proxy.ceph.find_issues_in_ceph_log(c.opts.ceph_log_max_lines)
        c.save("ceph_log_wrn_err", StorFormat.txt, 0, log_issues)

        with ignore_all:
            issues_count, regions = await conn.proxy.ceph.analyze_ceph_logs_for_issues()
            c.save("log_issues_count", StorFormat.json, 0, json.dumps(issues_count))
            c.save("status_regions", StorFormat.json, 0, json.dumps(regions))

    await c("config").ceph(f"daemon mon.{c.hostname} config show", fmt=StorFormat.json)  # type: ignore
    await c("ceph_var_dirs_size").bash("du -s /var/lib/ceph/m*")


@collector(Role.node)
async def collect_kernel_modules_info(c: Collector) -> None:
    try:
        await c().bash("lsmod")
    except Exception as exc:
        logger.warning(f"Failed to list kernel modules on node {c.hostname}: {exc}")
        return

    try:
        await c("modinfo_all").bash("for name in $(lsmod | awk '{print $1}') ; do modinfo $name ; echo '-----' ; done")
    except Exception as exc:
        logger.warning(f"Failed to list kernel modules info on {c.hostname}: {exc}")


@collector(Role.node)
async def collect_common_features(c: Collector) -> None:
    node_commands = [
        (None, "df"),
        (None, "dmidecode"),
        (None, "dmesg"),
        ("ipa4", "ip -o -4 a"),
        ("ipa", "ip a"),
        (None, "ifconfig"),
        ("ifconfig_short", "ifconfig -s"),
        ("lsblk", "lsblk -O"),
        ("lsblk_short", "lsblk"),
        (None, "mount"),
        ("netstat", "netstat -nap"),
        ("netstat_stat", "netstat -s"),
        ("sysctl", "sysctl -a"),
        ("uname", "uname -a"),
    ]

    await asyncio.wait(
        [c(path_offset).bash(cmd) for path_offset, cmd in node_commands] +
        [c("lshw").bash("lshw -xml", fmt=StorFormat.xml)])  # type: ignore


@collector(Role.node)
async def collect_files(c: Collector) -> None:
    node_files = ["/proc/diskstats", "/proc/meminfo", "/proc/loadavg", "/proc/cpuinfo", "/proc/uptime", "/proc/vmstat"]
    for fpath in node_files:
        await c.read_and_save(os.path.basename(fpath), fpath)

    node_renamed_files = [("netdev", "/proc/net/dev"),
                          ("dev_netstat", "/proc/net/netstat"),
                          ("softnet_stat", "/proc/net/softnet_stat"),
                          ("ceph_conf", "/etc/ceph/ceph.conf")]

    await asyncio.wait([c.read_and_save(name, fpath) for name, fpath in node_renamed_files])


@collector(Role.node)
async def collect_bonds_info(c: Collector) -> None:
    # collect_bonds_info
    bondmap = {}

    async with c.connection() as conn:
        if await conn.exists("/proc/net/bonding"):
            for fname in (await conn.iterdir("/proc/net/bonding")):
                await c.read_and_save(f"bond_{fname}", fname)
                bondmap[str(fname)] = f"bond_{fname}"

    c.save("bonds", StorFormat.json, 0, json.dumps(bondmap))


@collector(Role.node)
async def collect_packages(c: Collector) -> None:
    async with c.connection() as conn:
        try:
            if await conn.exists("/etc/debian_version"):
                await c("packages_deb").bash("dpkg -l")
            else:
                await c("packages_rpm").bash("yum list installed")
        except Exception as exc:
            logger.warning(f"Failed to download packages information from node {c.hostname}: {exc}")


@collector(Role.node)
async def collect_block_devs(c: Collector) -> None:
    async with c.connection() as conn:
        bdevs_info_rpc = await conn.proxy.fs.get_block_devs_info()

        bdevs_info = {name: data for name, data in bdevs_info_rpc.items()}

        for name_prefix in ['loop']:
            for name in bdevs_info:
                if name.startswith(name_prefix):
                    del bdevs_info[name]

        tools = ['hdparm', 'smartctl', 'nvme']
        missing = [name for exists, name in zip((await conn.proxy.fs.binarys_exists(tools)), tools) if not exists]

    if missing:
        logger.warning(f"{','.join(missing)} is not installed on {c.hostname}")

    if 'nvme' not in missing:
        nvme_res = await c.run('nvme version')
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
            nvme_list_js = await c('nvme_list').bash('nvme list -o json', fmt=StorFormat.json)  # type: ignore
            if nvme_list_js.returncode == 0:
                try:
                    for dev in json.loads(nvme_list_js.stdout)['Devices']:
                        name = os.path.basename(dev['DevicePath'])
                        cmd = f"nvme smart-log {dev['DevicePath']} -o json"
                        c(f'block_devs/{name}/nvme_smart_log').bash(cmd, fmt=StorFormat.json)  # type: ignore
                except:
                    logging.warning("Failed to process nvme list output")

    lsblk_res = await c("lsblkjs").bash("lsblk -O -b -J", fmt=StorFormat.json)  # type: ignore

    if lsblk_res.returncode == 0:
        coros = []
        for dev_node in json.loads(lsblk_res.stdout)['blockdevices']:
            name = dev_node['name']

            if 'hdparm' not in missing:
                coros.append(c(f'block_devs/{name}/hdparm').bash(f"sudo hdparm -I /dev/{name}"))

            if 'smartctl' not in missing:
                coros.append(c(f'block_devs/{name}/smartctl').bash(f"sudo smartctl -a /dev/{name}"))

        await asyncio.wait(coros)


async def collect_dev(conn: IAsyncNode, is_phy: bool, dev: str) -> Dict[str, Dict[str, Any]]:
    interface = {'dev': dev, 'is_phy': is_phy}
    interfaces: Dict[str, Dict[str, Any]] = {dev: interface}

    if is_phy:
        ethtool_res = await conn.run("ethtool " + dev)
        if ethtool_res.returncode == 0:
            interface['ethtool'] = ethtool_res.stdout

        iwconfig_res = await conn.run("iwconfig " + dev)
        if iwconfig_res.returncode == 0:
            interface['iwconfig'] = iwconfig_res.stdout
    return interfaces


@collector(Role.node)
async def collect_interfaces_info(c: Collector) -> None:
    async with c.connection() as conn:
        info = await get_host_interfaces(conn)
        interfaces: Any = {}
        for res in await asyncio.gather(*[collect_dev(conn, is_phy, dev) for is_phy, dev in info]):
            interfaces.update(res)

    c.save('interfaces', StorFormat.json, 0, json.dumps(interfaces))

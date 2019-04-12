import re
import json
import logging
import weakref
import datetime
from ipaddress import IPv4Network, IPv4Address
from typing import Iterable, Iterator, Dict, Any, List, Union, Tuple, Optional

import numpy
import dataclasses

from koder_utils import ssize2b
from koder_utils.storage import AttredStorage, TypedStorage

from .hw_info import parse_hw_info
from .cluster_classes import (IPANetDevInfo, NetStats, Disk, DiskType, HWModel, LogicBlockDev, BlockDevType, DevPath,
                              NetworkBond, NetworkAdapter, NetAdapterAddr, Host, Cluster, CephInfo, BlockUsage,
                              OSDPGStats, AggNetStat, PoolDF)
from .ceph_loader import CephLoader

logger = logging.getLogger("cephlib.parse")

netstat_fields = "recv_bytes recv_packets rerrs rdrop rfifo rframe rcompressed" + \
                 " rmulticast send_bytes send_packets serrs sdrop sfifo scolls" + \
                 " scarrier scompressed"


# -----  parse functions -----------------------------------------------------------------------------------------------


def parse_ipa(data: str) -> Dict[str, IPANetDevInfo]:
    """
    parse 'ip a' output
    """
    res = {}
    for line in data.split("\n"):
        if line.strip() != '' and not line[0].isspace():
            rr = re.match(r"\d+:\s+(?P<name>[^:]*):.*?mtu\s+(?P<mtu>\d+)", line)
            if rr:
                name = rr.group('name')
                res[name] = IPANetDevInfo(name, int(rr.group('mtu')))
    return res


def parse_netdev(netdev: str) -> Dict[str, NetStats]:
    info: Dict[str, NetStats] = {}
    for line in netdev.strip().split("\n")[2:]:
        adapter, data = line.split(":")
        adapter = adapter.strip()
        assert adapter not in info
        info[adapter] = NetStats(**dict(zip(netstat_fields.split(), map(int, data.split()))))
    return info


def parse_meminfo(meminfo: str) -> Dict[str, int]:
    info = {}
    for line in meminfo.split("\n"):
        line = line.strip()
        if line == '':
            continue
        name, data = line.split(":", 1)
        data = data.strip()
        if " " in data:
            data = data.replace(" ", "")
            assert data[-1] == 'B'
            val = ssize2b(data[:-1])
        else:
            val = int(data)
        info[name] = val
    return info


def is_routable(ip: IPv4Address, net: IPv4Network) -> bool:
    return not (ip.is_loopback or net.prefixlen == 32)


def parse_diskstats(data: str) -> Dict[str, BlockUsage]:
    #  1 - major number
    #  2 - minor mumber
    #  3 - device name
    #  4 - reads completed successfully
    #  5 - reads merged
    #  6 - sectors read
    #  7 - time spent reading (ms)
    #  8 - writes completed
    #  9 - writes merged
    # 10 - sectors written
    # 11 - time spent writing (ms)
    # 12 - I/Os currently in progress
    # 13 - time spent doing I/Os (ms)
    # 14 - weighted time spent doing I/Os (ms)

    SECTOR_SIZE = 512
    reads_completed = 3
    sectors_read = 5
    rtime = 6
    writes_completed = 7
    sectors_written = 9
    wtime = 10
    io_queue = 11
    io_time = 12
    weighted_io_time = 13

    res = {}

    for line in data.split("\n"):
        line = line.strip()
        if line:
            vals = line.split()
            name = vals[2]

            read_bytes = int(vals[sectors_read]) * SECTOR_SIZE
            write_bytes = int(vals[sectors_written]) * SECTOR_SIZE
            read_iops = int(vals[reads_completed])
            write_iops = int(vals[writes_completed])
            io_time_v = int(vals[io_time])
            w_io_time = int(vals[weighted_io_time])
            iops = read_iops + write_iops
            qd = w_io_time / io_time if io_time > 1000 else None
            lat = w_io_time / iops if iops > 100 else None

            res[name] = BlockUsage(read_bytes=read_bytes,
                                   write_bytes=write_bytes,
                                   total_bytes=read_bytes + write_bytes,
                                   read_iops=read_iops,
                                   write_iops=write_iops,
                                   iops=iops,
                                   io_time=io_time_v,
                                   w_io_time=w_io_time,
                                   lat=lat,
                                   queue_depth=qd)

    return res


def parse_lsblkjs(data: List[Dict[str, Any]],
                  hostname: str,
                  diskstats: Dict[str, BlockUsage]) -> Iterable[Disk]:
    for disk_js in data:
        name = disk_js['name']

        if re.match(r"sr\d+|loop\d+", name):
            continue

        if disk_js['type'] != 'disk':
            logger.warning("lsblk for node %r return device %r, which is %r not 'disk'. Ignoring it",
                           hostname, name, disk_js['type'])
            continue

        tp = disk_js["tran"]
        if tp is None:
            if disk_js['subsystems'] == 'block:scsi:pci':
                tp = 'sas'
            elif disk_js['subsystems'] == 'block:nvme:pci':
                tp = 'nvme'
            elif disk_js['subsystems'] == 'block:virtio:pci':
                tp = 'virtio'

        # raid controllers often don't report ssd drives correctly
        # only SSD has 4k phy sec size
        phy_sec = int(disk_js["phy-sec"])
        if phy_sec == 4096:
            disk_js["rota"] = '0'

        stor_tp = {
            ('sata', '1'): DiskType.sata_hdd,
            ('sata', '0'): DiskType.sata_ssd,
            ('nvme', '0'): DiskType.nvme,
            ('nvme', '1'): DiskType.unknown,
            ('sas',  '1'): DiskType.sas_hdd,
            ('sas',  '0'): DiskType.sas_ssd,
            ('virtio', '0'): DiskType.virtio,
            ('virtio', '1'): DiskType.virtio,
        }.get((tp, disk_js["rota"]))

        if stor_tp is None:
            logger.warning("Can't detect disk type for %r in node %r. tran=%r, rota=%r, subsystem=%r." +
                           " Treating it as " + DiskType.sata_hdd.name,
                           name, hostname, disk_js['tran'], disk_js['rota'], disk_js['subsystems'])
            stor_tp = DiskType.sata_hdd

        lbd = LogicBlockDev(name=name,
                            dev_path=DevPath('/dev/' + name),
                            size=int(disk_js['size']),
                            mountpoint=disk_js['mountpoint'],
                            fs=disk_js["fstype"],
                            tp=BlockDevType.hwdisk,
                            hostname=hostname,
                            usage=diskstats[name])

        dsk = Disk(logic_dev=lbd,
                   rota=disk_js['rota'] == '1',
                   scheduler=disk_js['sched'],
                   extra=disk_js,
                   hw_model=HWModel(vendor=disk_js['vendor'], model=disk_js['model'], serial=disk_js['serial']),
                   rq_size=int(disk_js["rq-size"]),
                   phy_sec=phy_sec,
                   min_io=int(disk_js["min-io"]),
                   tp=stor_tp)

        def fill_mountable(root: Dict[str, Any], parent: Union[Disk, LogicBlockDev]):
            if 'children' in root:
                for ch_js in root['children']:
                    assert '/' not in ch_js['name']
                    part = LogicBlockDev(name=ch_js['name'],
                                         tp=BlockDevType.unknown,
                                         dev_path=DevPath('/dev/' + ch_js['name']),
                                         size=int(ch_js['size']),
                                         mountpoint=ch_js['mountpoint'],
                                         fs=ch_js["fstype"],
                                         parent=weakref.ref(dsk),
                                         label=ch_js.get("partlabel"),
                                         hostname=hostname,
                                         usage=diskstats[name])
                    parent.children[part.name] = part
                    fill_mountable(ch_js, part)

        fill_mountable(disk_js, dsk)

        yield dsk


@dataclasses.dataclass
class DFInfo:
    path: DevPath
    name: str
    size: int
    free: int
    mountpoint: str


def parse_df(data: str) -> Iterator[DFInfo]:
    lines = data.strip().split("\n")
    assert lines[0].split() == ["Filesystem", "1K-blocks", "Used", "Available", "Use%", "Mounted", "on"]
    for ln in lines[1:]:
        name, size, _, free, _, mountpoint = ln.split()
        yield DFInfo(path=DevPath(name), name=name.split("/")[-1], size=int(size),
                     free=int(free), mountpoint=mountpoint)


# --- load functions ---------------------------------------------------------------------------------------------------

def parse_adapter_info(interface: Dict[str, Any]) -> Tuple[Optional[int], Optional[str], Optional[bool]]:
    duplex = None
    speed_s = None
    speed = None

    if 'ethtool' in interface:
        for line in interface['ethtool'].split("\n"):
            if 'Speed:' in line:
                speed_s = line.split(":")[1].strip()
            if 'Duplex:' in line:
                duplex = line.split(":")[1].strip() == 'Full'

    if 'iwconfig' in interface:
        if 'Bit Rate=' in interface['iwconfig']:
            br1 = interface['iwconfig'].split('Bit Rate=')[1]
            if 'Tx-Power=' in br1:
                speed_s = br1.split('Tx-Power=')[0]

    if speed_s is not None:
        for name, mult in {'Kb/s': 125, 'Mb/s': 125000, 'Gb/s': 125000000}.items():
            if name in speed_s:
                speed = int(float(speed_s.replace(name, '')) * mult)
                break

    return speed, speed_s, duplex


def load_bonds(stor_node: AttredStorage, jstor_node: AttredStorage) -> Dict[str, NetworkBond]:
    if 'bonds.json' not in jstor_node:
        return {}

    res = {}
    slave_rr = re.compile(r"Slave Interface: (?P<name>[^\s]+)")
    for name, file in jstor_node.bonds.items():
        slaves = [slave_info.group('name') for slave_info in slave_rr.finditer(stor_node[file])]
        res[name] = NetworkBond(name, slaves)
    return res


def load_interfaces(hostname: str,
                    jstor_node: AttredStorage,
                    stor_node: AttredStorage) -> Dict[str, NetworkAdapter]:

    usage = parse_netdev(stor_node.netdev)
    ifs_info = parse_ipa(stor_node.ipa)
    net_adapters = {}

    for name, vl in list(ifs_info.items()):
        if '@' in name:
            bond, _ = name.split('@')
            if bond not in ifs_info:
                ifs_info[bond] = vl

    for name, adapter_dct in jstor_node.interfaces.items():
        speed, speed_s, duplex = parse_adapter_info(adapter_dct)
        adapter = NetworkAdapter(dev=adapter_dct['dev'],
                                 is_phy=adapter_dct['is_phy'],
                                 mtu=None,
                                 speed=speed,
                                 speed_s=speed_s,
                                 duplex=duplex,
                                 usage=usage[adapter_dct['dev']],
                                 d_usage=None)

        try:
            adapter.mtu = ifs_info[adapter.dev].mtu
        except KeyError:
            logger.warning("Can't get mtu for interface %s on node %s", adapter.dev, hostname)

        net_adapters[adapter.dev] = adapter

    # find ip addresses
    ip_rr_s = r"\d+:\s+(?P<adapter>.*?)\s+inet\s+(?P<ip>\d+\.\d+\.\d+\.\d+)/(?P<size>\d+)"
    for line in stor_node.ipa4.split("\n"):
        match = re.match(ip_rr_s, line)
        if match is not None:
            ip_addr = IPv4Address(match.group('ip'))
            net_addr = IPv4Network("{}/{}".format(ip_addr, int(match.group('size'))), strict=False)
            dev_name = match.group('adapter')
            try:
                adapter = net_adapters[dev_name]
                adapter.ips.append(NetAdapterAddr(ip_addr, net_addr, is_routable=is_routable(ip_addr, net_addr)))
            except KeyError:
                logger.warning("Can't find adapter %r for ip %r", dev_name, ip_addr)

    return net_adapters


def load_hdds(hostname: str,
              jstor_node: AttredStorage,
              stor_node: AttredStorage) -> Tuple[Dict[str, Disk], Dict[str, LogicBlockDev]]:

    disks: Dict[str, Disk] = {}
    storage_devs: Dict[str, LogicBlockDev] = {}
    df_info = {info.name: info for info in parse_df(stor_node.df)}

    def walk_all(dsk: LogicBlockDev):
        if dsk.name in df_info:
            dsk.free_space = df_info[dsk.name].free
        storage_devs[dsk.name] = dsk
        for ch in dsk.children.values():
            walk_all(ch)

    diskstats = parse_diskstats(stor_node.diskstats)

    for dsk in parse_lsblkjs(jstor_node.lsblkjs['blockdevices'], hostname, diskstats):
        disks[dsk.name] = dsk
        walk_all(dsk.logic_dev)

    return disks, storage_devs


def load_perf_monitoring(storage: TypedStorage) -> Tuple[Optional[Dict[int, List[Dict]]], Optional[Dict[int, str]]]:

    if 'perf_monitoring' not in storage.txt:
        return None, None

    osd_perf_dump: Dict[int, List[Dict]] = {}
    osd_historic_ops_paths: Dict[int, str] = {}
    osd_rr = re.compile(r"osd(\d+)$")

    for is_file, host_id in storage.txt.perf_monitoring:
        if is_file:
            logger.warning("Unexpected file %r in perf_monitoring folder", host_id)

        for is_file, fname in storage.txt.perf_monitoring[host_id]:
            if is_file and fname == 'collected_at.csv':
                continue

            if is_file and fname.count('.') == 3:
                sensor, dev_or_osdid, metric, ext = fname.split(".")
                if ext == 'json' and sensor == 'ceph' and metric == 'perf_dump':
                    osd_id = osd_rr.match(dev_or_osdid)
                    assert osd_id, "{0!r} don't match osdXXX name".format(dev_or_osdid)
                    assert osd_id.group(1) not in osd_perf_dump, f"Two set of perf_dump data for osd {osd_id.group(1)}"
                    path = "perf_monitoring/{0}/{1}".format(host_id, fname)
                    osd_perf_dump[int(osd_id.group(1))] = json.loads(storage.raw.get_raw(path).decode())
                    continue
                elif ext == 'bin' and sensor == 'ceph' and metric == 'historic':
                    osd_id = osd_rr.match(dev_or_osdid)
                    assert osd_id, "{0!r} don't match osdXXX name".format(dev_or_osdid)
                    assert osd_id.group(1) not in osd_perf_dump, \
                        "Two set of osd_historic_ops_paths data for osd {0}".format(osd_id.group(1))
                    osd_historic_ops_paths[int(osd_id.group(1))] = "perf_monitoring/{0}/{1}".format(host_id, fname)
                    continue

            logger.warning("Unexpected %s %r in %r host performance_data folder",
                           'file' if is_file else 'folder', fname, host_id)

    return osd_perf_dump, osd_historic_ops_paths


class Loader:
    def __init__(self, storage: TypedStorage) -> None:
        self.storage = storage
        self.ip2host: Dict[str, Host] = {}
        self.hosts: Dict[str, Host] = {}
        self.osd_historic_ops_paths: Any = None
        self.osd_perf_counters_dump: Any = None

    def load_perf_monitoring(self):
        self.osd_perf_counters_dump, self.osd_historic_ops_paths = load_perf_monitoring(self.storage)

    def load_cluster(self) -> Cluster:
        coll_time = self.storage.txt['master/collected_at'].strip()
        report_collected_at_local, report_collected_at_gmt, _ = coll_time.split("\n")
        coll_s = datetime.datetime.strptime(report_collected_at_gmt, '%Y-%m-%d %H:%M:%S')

        return Cluster(hosts=self.hosts,
                       ip2host=self.ip2host,
                       report_collected_at_local=report_collected_at_local,
                       report_collected_at_gmt=report_collected_at_gmt,
                       report_collected_at_gmt_s=coll_s.timestamp())

    def load_hosts(self) -> Tuple[Dict[str, Host], Dict[str, Host]]:
        tcp_sock_re = re.compile('(?im)^tcp6?\\b')
        udp_sock_re = re.compile('(?im)^udp6?\\b')

        for is_file, hostname in self.storage.txt.hosts:
            assert not is_file

            stor_node = self.storage.txt.hosts[hostname]
            jstor_node = self.storage.json.hosts[hostname]

            lshw_xml = stor_node.get('lshw', ext='xml')
            hw_info = None
            if lshw_xml is not None:
                try:
                    hw_info = parse_hw_info(lshw_xml)
                except Exception as exc:
                    logger.warning(f"Failed to parse lshw info: {exc}")

            loadavg = stor_node.get('loadavg')
            disks, logic_block_devs = load_hdds(hostname, jstor_node, stor_node)

            net_adapters = load_interfaces(hostname, jstor_node, stor_node)
            bonds = load_bonds(stor_node, jstor_node)

            info = parse_meminfo(stor_node.meminfo)
            host = Host(name=hostname,
                        stor_id=hostname,
                        net_adapters=net_adapters,
                        disks=disks,
                        bonds=bonds,
                        logic_block_devs=logic_block_devs,
                        uptime=float(stor_node.uptime.split()[0]),
                        open_tcp_sock=len(tcp_sock_re.findall(stor_node.netstat)),
                        open_udp_sock=len(udp_sock_re.findall(stor_node.netstat)),
                        mem_total=info['MemTotal'],
                        mem_free=info['MemFree'],
                        swap_total=info['SwapTotal'],
                        swap_free=info['SwapFree'],
                        load_5m=None if loadavg is None else float(loadavg.strip().split()[1]),
                        hw_info=hw_info,
                        netstat=parse_netstats(stor_node),
                        d_netstat=None)

            self.hosts[host.name] = host

            for adapter in net_adapters.values():
                for ip in adapter.ips:
                    if ip.is_routable:
                        ip_s = str(ip.ip)
                        if ip_s in self.ip2host:
                            logger.error("Ip %s belong to both %s and %s. Skipping new host",
                                         ip_s, hostname, self.ip2host[ip_s].name)
                            continue
                        self.ip2host[ip_s] = host

        return self.hosts, self.ip2host


def load_all(storage: TypedStorage) -> Tuple[Cluster, CephInfo]:
    l = Loader(storage)
    logger.debug("Loading perf data")
    l.load_perf_monitoring()
    logger.debug("Loading hosts")
    l.load_hosts()
    logger.debug("Loading cluster")
    cluster = l.load_cluster()
    ceph_l = CephLoader(storage, l.ip2host, l.hosts, l.osd_perf_counters_dump, l.osd_historic_ops_paths)
    logger.debug("Loading ceph")
    return cluster, ceph_l.load_ceph()


def fill_usage(cluster: Cluster, cluster_old: Cluster, ceph: CephInfo, ceph_old: CephInfo):
    # fill io devices d_usage

    dtime = cluster.report_collected_at_gmt_s - cluster_old.report_collected_at_gmt_s
    assert dtime > 1, f"Dtime == {int(dtime)} is less then 1. Check that you provide correct folders for delta"
    cluster.dtime = dtime

    for host_name, host in cluster.hosts.items():
        host_old = cluster_old.hosts[host_name]
        for dev_name, dev in host.logic_block_devs.items():
            dev_old = host_old.logic_block_devs[dev_name]
            assert not dev.d_usage

            dio_time = dev.usage.io_time - dev_old.usage.io_time
            dwio_time = dev.usage.w_io_time - dev_old.usage.w_io_time
            d_iops = dev.usage.iops - dev_old.usage.iops

            dev.d_usage = BlockUsage(
                read_bytes=(dev.usage.read_bytes - dev_old.usage.read_bytes) / dtime,
                write_bytes=(dev.usage.write_bytes - dev_old.usage.write_bytes) / dtime,
                total_bytes=(dev.usage.total_bytes - dev_old.usage.total_bytes) / dtime,
                read_iops=(dev.usage.read_iops - dev_old.usage.read_iops) / dtime,
                write_iops=(dev.usage.write_iops - dev_old.usage.write_iops) / dtime,
                io_time=dio_time / dtime,
                w_io_time=dwio_time / dtime,
                iops=d_iops / dtime,
                queue_depth=dwio_time / dio_time if dio_time > 1000 else None,
                lat=dio_time / d_iops if d_iops > 100 else None)

        for dev_name, netdev in host.net_adapters.items():
            netdev_old = host_old.net_adapters[dev_name]
            assert not netdev.d_usage
            netdev.d_usage = NetStats(
                    recv_bytes=int((netdev.usage.recv_bytes - netdev_old.usage.recv_bytes) / dtime + 0.499),
                    recv_packets=int((netdev.usage.recv_packets - netdev_old.usage.recv_packets) / dtime + 0.499),
                    rerrs=int((netdev.usage.rerrs - netdev_old.usage.rerrs) / dtime + 0.499),
                    rdrop=int((netdev.usage.rdrop - netdev_old.usage.rdrop) / dtime + 0.499),
                    rfifo=int((netdev.usage.rfifo - netdev_old.usage.rfifo) / dtime + 0.499),
                    rframe=int((netdev.usage.rframe - netdev_old.usage.rframe) / dtime + 0.499),
                    rcompressed=int((netdev.usage.rcompressed - netdev_old.usage.rcompressed) / dtime + 0.499),
                    rmulticast=int((netdev.usage.rmulticast - netdev_old.usage.rmulticast) / dtime + 0.499),
                    send_bytes=int((netdev.usage.send_bytes - netdev_old.usage.send_bytes) / dtime + 0.499),
                    send_packets=int((netdev.usage.send_packets - netdev_old.usage.send_packets) / dtime + 0.499),
                    serrs=int((netdev.usage.serrs - netdev_old.usage.serrs) / dtime + 0.499),
                    sdrop=int((netdev.usage.sdrop - netdev_old.usage.sdrop) / dtime + 0.499),
                    sfifo=int((netdev.usage.sfifo - netdev_old.usage.sfifo) / dtime + 0.499),
                    scolls=int((netdev.usage.scolls - netdev_old.usage.scolls) / dtime + 0.499),
                    scarrier=int((netdev.usage.scarrier - netdev_old.usage.scarrier) / dtime + 0.499),
                    scompressed=int((netdev.usage.scompressed - netdev_old.usage.scompressed) / dtime + 0.499)
                )

        assert not host.d_netstat
        if host.netstat and host_old.netstat:
            host.d_netstat = host.netstat - host_old.netstat

    # pg stats
    for osd in ceph.osds.values():
        assert not osd.d_pg_stats, str(osd.d_pg_stats)
        assert osd.pg_stats is not None
        osd_old = ceph_old.osds[osd.id]

        assert osd_old.pg_stats
        osd.d_pg_stats = OSDPGStats(
            bytes=int((osd.pg_stats.bytes - osd_old.pg_stats.bytes) / dtime),
            reads=int((osd.pg_stats.reads - osd_old.pg_stats.reads) / dtime),
            read_b=int((osd.pg_stats.read_b - osd_old.pg_stats.read_b) / dtime),
            writes=int((osd.pg_stats.writes - osd_old.pg_stats.writes) / dtime),
            write_b=int((osd.pg_stats.write_b - osd_old.pg_stats.write_b) / dtime),
            scrub_errors=int((osd.pg_stats.scrub_errors - osd_old.pg_stats.scrub_errors) / dtime),
            deep_scrub_errors=int((osd.pg_stats.deep_scrub_errors - osd_old.pg_stats.deep_scrub_errors) / dtime),
            shallow_scrub_errors=int((osd.pg_stats.shallow_scrub_errors -
                                      osd_old.pg_stats.shallow_scrub_errors) / dtime)
        )

    # pg stats
    for pool in ceph.pools.values():
        assert not pool.d_df
        old_pool = ceph_old.pools[pool.name]
        pool.d_df = PoolDF(
           size_bytes=int((pool.df.size_bytes - old_pool.df.size_bytes) / dtime),
           size_kb=int((pool.df.size_kb - old_pool.df.size_kb) / dtime),
           num_objects=int((pool.df.num_objects - old_pool.df.num_objects) / dtime),
           num_object_clones=int((pool.df.num_object_clones - old_pool.df.num_object_clones) / dtime),
           num_object_copies=int((pool.df.num_object_copies - old_pool.df.num_object_copies) / dtime),
           num_objects_missing_on_primary=int((pool.df.num_objects_missing_on_primary -
                                               old_pool.df.num_objects_missing_on_primary) / dtime),
           num_objects_unfound=int((pool.df.num_objects_unfound - old_pool.df.num_objects_unfound) / dtime),
           num_objects_degraded=int((pool.df.num_objects_degraded - old_pool.df.num_objects_degraded) / dtime),
           read_ops=int((pool.df.read_ops - old_pool.df.read_ops) / dtime),
           read_bytes=int((pool.df.read_bytes - old_pool.df.read_bytes) / dtime),
           write_ops=int((pool.df.write_ops - old_pool.df.write_ops) / dtime),
           write_bytes=int((pool.df.write_bytes - old_pool.df.write_bytes) / dtime))


def parse_netstats(storage: AttredStorage) -> Optional[AggNetStat]:
    if 'softnet_stat' not in storage:
        return None
    lines = storage.softnet_stat.strip().split("\n")
    return AggNetStat(numpy.array([[int(vl_s, 16) for vl_s in line.split()] for line in lines], dtype=numpy.int64))


def fill_cluster_nets_roles(cluster: Cluster, ceph: CephInfo):
    for net_id, cluster_net_info in cluster.net_data.items():
        if net_id == str(ceph.cluster_net):
            cluster_net_info.roles.add('ceph-cluster')

        if net_id == str(ceph.public_net):
            cluster_net_info.roles.add('ceph-public')

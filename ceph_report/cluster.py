import re
import json
import logging
import datetime
from dataclasses import dataclass, field
from ipaddress import IPv4Network, IPv4Address
from typing import Dict, Any, List, Tuple, Optional, Iterable

import numpy

from koder_utils import (AttredStorage, TypedStorage, parse_lshw_info, Host, parse_netdev, parse_ipa, NetStats, Disk,
                         LogicBlockDev, is_routable, NetworkBond, NetworkAdapter, NetAdapterAddr,
                         AggNetStat, ClusterNetData, parse_df, parse_diskstats, parse_lsblkjs)

from cephlib import CephInfo
from koder_utils.linux import parse_meminfo

from .ceph_loader import CephLoader

logger = logging.getLogger("report")


@dataclass
class Cluster:
    hosts: Dict[str, Host]
    ip2host: Dict[IPv4Address, Host]
    report_collected_at_local: str
    report_collected_at_gmt: str
    report_collected_at_gmt_s: float
    has_second_report: bool = False
    dtime: Optional[float] = field(default=None, init=False)  # type: ignore
    _net_data_cache: Optional[Dict[str, ClusterNetData]] = field(default=None, init=False)  # type: ignore

    @property
    def sorted_hosts(self) -> Iterable[Host]:
        return [host for _, host in sorted(self.hosts.items())]

    @property
    def net_data(self) -> Dict[str, ClusterNetData]:
        if self._net_data_cache is None:
            self._net_data_cache = {}
            for host in self.hosts.values():
                for addr, hw_adapters in host.iter_net_addrs():
                    if addr.is_routable:
                        net_id = str(addr.net)

                        if net_id not in self._net_data_cache:
                            nd = ClusterNetData(net_id, NetStats.empty())
                        else:
                            nd = self._net_data_cache[net_id]

                        for adapter in hw_adapters:

                            if adapter.mtu:
                                nd.mtus.add(adapter.mtu)

                            if adapter.speed:
                                nd.speeds.add(adapter.speed)

                            nd.usage += adapter.usage
                            if adapter.d_usage:
                                if not nd.d_usage:
                                    nd.d_usage = NetStats.empty()

                                nd.d_usage += adapter.d_usage

                        if net_id not in self._net_data_cache:
                            self._net_data_cache[net_id] = nd

        return self._net_data_cache


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

        for is_file2, fname in storage.txt.perf_monitoring[host_id]:
            if is_file2 and fname == 'collected_at.csv':
                continue

            if is_file2 and fname.count('.') == 3:
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
                           'file' if is_file2 else 'folder', fname, host_id)

    return osd_perf_dump, osd_historic_ops_paths


class Loader:
    def __init__(self, storage: TypedStorage) -> None:
        self.storage = storage
        self.ip2host: Dict[IPv4Address, Host] = {}
        self.hosts: Dict[str, Host] = {}
        self.osd_historic_ops_paths: Any = None
        self.osd_perf_counters_dump: Any = None

    def load_perf_monitoring(self):
        self.osd_perf_counters_dump, self.osd_historic_ops_paths = load_perf_monitoring(self.storage)

    def load_cluster(self) -> Cluster:
        coll_time = self.storage.txt.collected_at.strip()
        report_collected_at_local, report_collected_at_gmt, _ = coll_time.split("\n")
        coll_s = datetime.datetime.strptime(report_collected_at_gmt, '%Y-%m-%d %H:%M:%S')

        return Cluster(hosts=self.hosts,
                       ip2host=self.ip2host,
                       report_collected_at_local=report_collected_at_local,
                       report_collected_at_gmt=report_collected_at_gmt,
                       report_collected_at_gmt_s=coll_s.timestamp())

    def load_hosts(self) -> Tuple[Dict[str, Host], Dict[IPv4Address, Host]]:
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
                    hw_info = parse_lshw_info(lshw_xml)
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
                        if ip.ip in self.ip2host:
                            logger.error(f"Ip {ip.ip} belong to both {hostname} and " +
                                         f"{self.ip2host[ip.ip].name}. Skipping new host")
                            continue
                        self.ip2host[ip.ip] = host

        return self.hosts, self.ip2host


def load_all(storage: TypedStorage) -> Tuple[Cluster, CephInfo]:
    loader = Loader(storage)
    logger.debug("Loading perf data")
    loader.load_perf_monitoring()
    logger.debug("Loading hosts")
    loader.load_hosts()
    logger.debug("Loading cluster")
    cluster = loader.load_cluster()
    ceph_l = CephLoader(storage, loader.ip2host, loader.hosts, loader.osd_perf_counters_dump,
                        loader.osd_historic_ops_paths)
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

            dev.d_usage = (dev.usage - dev_old.usage) / dtime
            dev.queue_depth = dwio_time / dio_time if dio_time > 1000 else None,
            dev.lat = dio_time / d_iops if d_iops > 100 else None

        for dev_name, netdev in host.net_adapters.items():
            netdev_old = host_old.net_adapters[dev_name]
            assert not netdev.d_usage
            netdev.d_usage = (netdev.usage.recv_bytes - netdev_old.usage.recv_bytes) / dtime

        assert not host.d_netstat
        if host.netstat and host_old.netstat:
            host.d_netstat = host.netstat - host_old.netstat

    # pg stats
    for osd in ceph.osds.values():
        assert not osd.d_pg_stats, str(osd.d_pg_stats)
        assert osd.pg_stats is not None
        osd_old = ceph_old.osds[osd.id]

        assert osd_old.pg_stats
        osd.d_pg_stats = (osd.pg_stats - osd_old.pg_stats) / dtime

    # pg stats
    for pool in ceph.pools.values():
        assert not pool.d_df
        old_pool = ceph_old.pools[pool.name]
        pool.d_df = (pool.df - old_pool.df) / dtime


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

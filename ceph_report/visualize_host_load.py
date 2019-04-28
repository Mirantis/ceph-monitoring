import collections
from typing import Callable, Union, Optional, List, Tuple, Dict, Set, Sequence

from koder_utils import (b2ssize_10, b2ssize, Host, LogicBlockDev, DiskType, Disk, XMLDocument, table_to_html,
                         SimpleTable, doc_to_string, XMLNode)
from .ceph_loader import CephInfo
from .cluster import Cluster
from .visualize_utils import val_to_color, perf_info_required, tab, table_to_doc
from .obj_links import host_link


def get_io_attr_average(host: Host, dev: LogicBlockDev, attr: str, uptime: bool) -> Optional[float]:
    if uptime:
        if attr in ('queue_depth', 'lat'):
            return getattr(dev.d_usage, attr)
        return getattr(dev.usage, attr) / host.uptime
    else:
        return getattr(dev.d_usage, attr)


def make_storage_devs_load_table(hosts: Sequence[Host],
                                 attr: str,
                                 conversion: Callable[[Union[float, int]], str],
                                 uptime: bool,
                                 max_val: Optional[float],
                                 min_max_val: Dict[DiskType, float]) -> XMLDocument:

    tp_remapper: Dict[DiskType, DiskType] = {
        DiskType.nvme: DiskType.nvme,
        DiskType.sas_ssd: DiskType.sata_ssd,
        DiskType.sata_ssd: DiskType.sata_ssd,
        DiskType.sas_hdd: DiskType.sata_hdd,
        DiskType.sata_hdd: DiskType.sata_hdd,
        DiskType.virtio: DiskType.sata_hdd,
        DiskType.unknown: DiskType.sata_hdd,
    }

    tp_remapper_vls = set(tp_remapper.values())

    all_devs_by_type: Dict[DiskType, Set[str]] = collections.defaultdict(set)
    for host in hosts:
        for name, dev in sorted(host.disks.items()):
            all_devs_by_type[tp_remapper[dev.tp]].add(name)

    vals: Dict[str, Dict[str, float]] = {}
    vals_per_type: Dict[DiskType, List[float]] = collections.defaultdict(list)
    disk_counts_per_host: List[Dict[DiskType, int]] = []
    for host in hosts:
        hvals: Dict[str, float] = {}
        disk_counts_per_host.append(collections.Counter())
        for name, dev in host.disks.items():
            val = get_io_attr_average(host, dev.logic_dev, attr, uptime)
            remapped_tp = tp_remapper[dev.tp]
            vals_per_type[remapped_tp].append(val)
            hvals[name] = val
            disk_counts_per_host[-1][remapped_tp] += 1
        vals[host.name] = hvals

    max_disks_per_tp: Dict[DiskType, int] = {tp: max(per_host.get(tp, 0)
                                                     for per_host in disk_counts_per_host)
                                             for tp in tp_remapper_vls}

    max_per_tp: Dict[DiskType, float] = {}
    for tp, tp_vals in vals_per_type.items():
        if max_val is not None:
            max_per_tp[tp] = max_val
        else:
            max_per_tp[tp] = max(min_max_val[tp], max(i for i in tp_vals if i is not None))

    header_names = sum([[f"{tp.short_name}{cnt}" for cnt in range(mcount)]
                       for tp, mcount in max_disks_per_tp.items()], [])

    table = SimpleTable('host', *header_names)
    for host in hosts:
        table.add_cell(host_link(host.name).link)
        host_vals = vals[host.name]

        curr_host_devs: Dict[DiskType, List[Disk]] = collections.defaultdict(list)

        for dsk in host.disks.values():
            curr_host_devs[tp_remapper[dsk.tp]].append(dsk)

        for itm in curr_host_devs.values():
            itm.sort(key=lambda device: device.name)

        for tp in [DiskType.nvme, DiskType.sata_ssd, DiskType.sata_hdd]:
            for dev in curr_host_devs[tp]:
                tp = tp_remapper[host.disks[dev.name].tp]
                val = host_vals[dev.name]
                color = "#FFFFFF" if val is None or max_val == 0 else val_to_color(float(val) / max_per_tp[tp])

                if val is None:
                    s_val = '?'
                else:
                    s_val = 0 if val < 1 else conversion(val)

                cell_data = doc_to_string(XMLNode('div', text=dev.name, _class="left")) + \
                    doc_to_string(XMLNode('div', text=s_val, _class="right"))
                if val is not None:
                    table.add_cell(cell_data, bgcolor=color, sorttable_customkey=str(val))
                else:
                    table.add_cell(cell_data, bgcolor=color)

            missing_drives = max_disks_per_tp.get(tp, 0) - len(curr_host_devs.get(tp, []))
            for idx in range(missing_drives):
                table.add_cell('-', sorttable_customkey='0')

        table.next_row()

    return table_to_doc(table, zebra='false', extra_cls="io_load_in_color")


@tab("Storage devs load")
@perf_info_required
def show_host_io_load_in_color(cluster: Cluster, ceph: CephInfo, uptime: bool) -> str:
    osd_hosts = {osd.host.name for osd in ceph.osds.values()}
    hosts = [host for _, host in sorted(cluster.hosts.items()) if host.name in osd_hosts]

    minmax_bps_per_type = {
        DiskType.sata_hdd: 100 * 2 ** 20,
        DiskType.nvme: 500 * 2 ** 20,
        DiskType.sata_ssd: 200 * 2 ** 20
    }

    minmax_iops_per_type = {
        DiskType.sata_hdd: 50,
        DiskType.nvme: 500,
        DiskType.sata_ssd: 100
    }

    loads = [
        ('iops', lambda x: b2ssize_10(int(x)), 'IOPS', minmax_iops_per_type),
        ('read_iops', lambda x: b2ssize_10(int(x)), 'Read IOPS', minmax_iops_per_type),
        ('write_iops', lambda x: b2ssize_10(int(x)), 'Write IOPS', minmax_iops_per_type),

        ('total_bytes', lambda x: str(int(x / 2 ** 20)), 'MiBps', minmax_bps_per_type),
        ('read_bytes', lambda x: str(int(x / 2 ** 20)), 'Read MiBps', minmax_bps_per_type),
        ('write_bytes', lambda x: str(int(x / 2 ** 20)), 'Write MiBps', minmax_bps_per_type),

        ('lat', lambda x: '-' if x is None else str(int(x)), 'Latency, ms', {tp: 20 for tp in DiskType}),
        ('queue_depth', lambda x: f"{x:.1f}", 'Average QD', {tp: 3 for tp in DiskType}),
        ('io_time', lambda x: f"{x / 10:.1f}", 'Active time %', {tp: 100 for tp in DiskType}),
    ]

    blocks = []
    for pos, (attr, conversion, tp, min_max_val) in enumerate(loads, 1):
        max_val = 300.0 if attr == 'lat' else None
        table = make_storage_devs_load_table(hosts, attr, conversion, uptime, max_val, min_max_val)
        blocks.append(f"<br><H4>{tp}</H4><br><br>{table}")

    return "<br>".join(blocks)


@tab("Per host net load")
@perf_info_required
def show_host_network_load_in_color(cluster: Cluster, ceph: CephInfo) -> str:
    send_net_io: Dict[str, List[Tuple]] = collections.defaultdict(list)
    recv_net_io: Dict[str, List[Tuple]] = collections.defaultdict(list)

    max_net_count = 0
    for host in cluster.hosts.values():
        cl_if = host.find_interface(ceph.cluster_net)
        pb_if = host.find_interface(ceph.public_net)
        nets = [('cluster', cl_if), ('public', pb_if)]
        nets += sorted((net.dev, net) for net in host.net_adapters.values()  # type: ignore
                       if net is not None and net.is_phy and net != pb_if and net != cl_if )

        uptime = host.uptime
        max_net_count = max(max_net_count, len(nets))
        for name, net in nets:
            if net is None or net.usage is None:
                if name in {'cluster', 'public'}:
                    send_net_io[host.name].append((name, None, None, None))
                    recv_net_io[host.name].append((name, None, None, None))
            else:
                send_net_io[host.name].append((name, net.usage.send_bytes / uptime,
                                              net.usage.send_packets / uptime,
                                              net.speed))
                recv_net_io[host.name].append((name, net.usage.recv_bytes / uptime,
                                              net.usage.recv_packets / uptime,
                                              net.speed))

    std_nets = {'public', 'cluster'}
    ceph_net_count = len(std_nets)
    tables = []

    for io, name in [(send_net_io, "Send (Bps/Pps)"), (recv_net_io, "Receive (Bps/Pps)")]:

        table = SimpleTable("host", "cluster", "public", *(["hw adapter"] * (max_net_count - ceph_net_count)))

        for host_name, net_loads in sorted(io.items()):
            table.add_cell(host_link(host_name).link)
            for net_name, load_bps, load_pps, speed in net_loads:
                if load_bps is None:
                    table.add_cell('-', sorttable_customkey="")
                else:
                    load_bps = int(load_bps)
                    load_pps = int(load_pps)
                    color = "#FFFFFF" if speed is None else val_to_color(float(load_bps) / speed)
                    load_text = f"{b2ssize(load_bps)} / {b2ssize_10(load_pps)}"

                    if net_name in std_nets:
                        text = load_text
                    else:
                        text = doc_to_string(XMLNode('div', text=net_name, _class="left")) + \
                            doc_to_string(XMLNode('div', text=load_text, _class="right"))

                    table.add_cell(text, bgcolor=color, sorttable_customkey=str(load_bps))

            for i in range(max_net_count - len(net_loads)):
                table.add_cell("-", sorttable_customkey='0')

            table.next_row()

        tables.append(f"<h4>{name}</h4><br>{table}")
        tables.append(table_to_doc(table, zebra='false', extra_cls="table-net-load"))
    return "<br><br><br>".join(tables)


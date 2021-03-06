import collections
from typing import Dict, List, Union, Optional, Tuple, Set

from koder_utils import (b2ssize, b2ssize_10, flatten, Table, Column, Disk, LogicBlockDev, group_by, XMLBuilder,
                         SimpleTable, AnyXML, partition_by_len)
from cephlib import CephInfo, CephOSD, Host, FileStoreInfo, BlueStoreInfo, CephIOStats

from .cluster import Cluster
from .visualize_utils import tab, table_to_xml_doc
from .obj_links import host_link, osd_link, mon_link


@tab("Hosts configs")
def show_hosts_config(cluster: Cluster, ceph: CephInfo) -> AnyXML:
    mon_hosts = {mon.host.name for mon in ceph.mons.values()}

    host2osds: Dict[str, Set[int]] = {}
    for osd in ceph.osds.values():
        host2osds.setdefault(osd.host.name, set()).add(osd.id)

    rule2host2osds: Dict[str, Dict[str, List[CephOSD]]] = {}
    for rule in ceph.crush.crushmap.rules:
        curr = rule2host2osds[rule.rule_name] = {}
        for osd_id, _ in ceph.crush.iter_osds_for_rule(rule):
            osd = ceph.osds[osd_id]
            curr.setdefault(osd.host.name, []).append(osd)

    root_names = sorted(rule2host2osds)

    hosts_configs = []
    for host in cluster.sorted_hosts:
        by_speed: Dict[int, int] = collections.Counter()
        for adapter in host.net_adapters.values():
            if adapter.is_phy:
                by_speed[adapter.speed if adapter.speed else 0] += 1

        nets = "<br>".join((f"{speed * 8 / 10 ** 9:.1f} * {if_count}" if speed != 0 else f"'Unknown' * {if_count}")
                           for speed, if_count in sorted(by_speed.items()))

        cl = host.find_interface(ceph.cluster_net)
        pb = host.find_interface(ceph.public_net)
        cluster_bw = None
        client_bw = None

        for adapter2 in [cl, pb]:
            if adapter2:
                adapter_name = adapter2.dev.split(".")[0] if '.' in adapter2.dev else adapter2.dev
                sources = host.bonds[adapter_name].sources if adapter_name in host.bonds else [adapter_name]

                bw = 0
                has_unknown = False
                for src in sources:
                    if host.net_adapters[src].speed:
                        bw += host.net_adapters[src].speed  # type: ignore
                    else:
                        has_unknown = True

                if adapter2 is cl:
                    cluster_bw = f"{bw / 10 ** 9:.1f}" + (" + unknown" if has_unknown else "")
                else:
                    client_bw = f"{bw / 10 ** 9:.1f}" + (" + unknown" if has_unknown else "")

        disks: Dict[str, Dict[int, int]] = collections.defaultdict(collections.Counter)
        for disk in host.disks.values():
            disks[disk.tp.short_name][disk.size] += 1

        res = []
        for name in ('nvme', 'ssd', 'hdd'):
            if name in disks:
                for sz, Column.d in sorted(disks[name].items()):
                    res.append(f"{sz / 2 ** 40:.1f}" + ("" if Column.d == 1 else f" * {Column.d}"))
        storage = "<br>".join(res)

        root2osd = [rule2host2osds.get(root_name, {}).get(host.name, []) for root_name in root_names]
        all_osds_in_roots: Set[int] = set()
        for rosds in root2osd:
            all_osds_in_roots.update(osd.id for osd in rosds)

        all_osds_no_root = host2osds.get(host.name, set()).difference(all_osds_in_roots)

        hosts_configs.append({
            "name": host.name,
            "osds_count": (len(all_osds_no_root),) + tuple(len(rosds) for rosds in root2osd),
            'has_mon': host.name in mon_hosts,
            'cores': host.cpu_cores,
            'ram': int(host.mem_total / 2 ** 30 + 0.5),
            'net': nets,
            'cluster_bw': cluster_bw,
            'client_bw': client_bw,
            'storage': storage
        })

    assert '_no_root' not in root_names

    class HostsConfigTable(Table):
        __html_classes__ = "table_cr"

        count = Column.ed()
        names = Column.list(chars_per_line=40)
        for root_name in root_names:
            locals()[root_name] = Column.ed(f"osd count for<br>{root_name}")  #

        no_root = Column.ed("osd with<br>no root")
        has_mon = Column.s()
        cores = Column.ed("CPU<br>Cores+HT")
        ram = Column.sz("RAM<br>total, GiB")
        storage_devices = Column.s("Storage<br>devices")
        network = Column.s("Network<br>devices<br>Gb")
        ceph_cluster_bw = Column.s("Ceph<br>cluster net<br>GB")
        ceph_client_bw = Column.s("Ceph<br>client net<br>GB")

    configs = HostsConfigTable()

    for grouped_idx in group_by(hosts_configs, mutable_keys="name"):
        items = [hosts_configs[idx] for idx in grouped_idx]
        first_item = items[0]
        row = configs.next_row()
        row.count = len(items)
        row.names = [host_link(itm['name']).link for itm in items]  # type: ignore

        row.no_root = first_item["osds_count"][0]  # type: ignore
        for name, vl in zip(root_names, first_item["osds_count"][1:]):  # type: ignore
            row[name] = vl

        row.has_mon = 'yes' if first_item["has_mon"] else 'no'
        row.cores = first_item["cores"]
        row.ram = first_item["ram"]
        row.network = first_item["net"]
        row.ceph_cluster_bw = first_item["cluster_bw"]
        row.ceph_client_bw = first_item["client_bw"]
        row.storage_devices = first_item["storage"]

    return table_to_xml_doc(configs, id="table-hosts-info", sortable=True, zebra=True)


class HostRunInfo(Table):
    __html_classes__ = "table_cr"

    name = Column.s()
    services = Column.s(dont_sort=True)
    ram_total = Column.d("RAM total, GiB")
    ram_free = Column.d("RAM free, GiB")
    swap = Column.d("Swap used, GiB")
    cores = Column.d("CPU cores")
    load = Column.s("Load avg 5m")
    ip_conn = Column.s("Conn tcp/udp")
    ips = Column.list("IP's")
    scrub_err = Column.d("Total scrub errors")
    new_scrub_err = Column.d("New scrub errors")
    net_err = Column.d("New network drop+serr")
    net_err_no_buff = Column.d("New Dropped no space")
    net_budget_over = Column.d("New net budget running out")


@tab("Hosts status")
def show_hosts_status(cluster: Cluster, ceph: CephInfo) -> AnyXML:
    run_info = HostRunInfo()
    mon_hosts = {mon.host.name for mon in ceph.mons.values()}

    host2osds: Dict[str, List[int]] = {}
    for osd in ceph.osds.values():
        host2osds.setdefault(osd.host.name, []).append(osd.id)

    for host in cluster.sorted_hosts:

        # TODO: add storage devices
        # TODO: add net info

        row = run_info.next_row()
        row.name = host_link(host.name).link, host.name

        if host.name in mon_hosts:
            mon_str: Optional[str] = f'<font color="#8080FF">Mon</font>: ' + mon_link(host.name).link
        else:
            mon_str = None

        srv_strs: List[Tuple[str, int]] = []
        if host.name in host2osds:
            for osd_id in host2osds[host.name]:
                srv_strs.append((osd_link(osd_id).link, len(str(osd_id))))

        row.services = (mon_str + "<br>" if mon_str else "") + '''<font color="#c77405">OSD's</font>: ''' + \
            "<br>".join(", ".join(chunk) for chunk in partition_by_len(srv_strs, 40, 1))  # type: ignore

        row.ram_total = host.mem_total // 2 ** 30
        row.ram_free = host.mem_free // 2 ** 30
        row.swap = (host.swap_total - host.swap_free) // 2 ** 30
        row.cores = host.cpu_cores
        row.load = f"{host.load_5m:.1f}"
        row.ip_conn = f"{host.open_tcp_sock}/{host.open_udp_sock}", host.open_tcp_sock + host.open_udp_sock

        all_ip = flatten(adapter.ips for adapter in host.net_adapters.values())
        row.ips = [str(addr.ip) for addr in all_ip if not addr.ip.is_loopback]

        if ceph.nodes_pg_info and host.name in ceph.nodes_pg_info:
            pgs_info = ceph.nodes_pg_info[host.name]
            row.scrub_err = pgs_info.pg_stats.num_scrub_errors + pgs_info.pg_stats.num_deep_scrub_errors + \
                pgs_info.pg_stats.num_shallow_scrub_errors

            if pgs_info.d_pg_stats is not None:
                row.new_scrub_err = pgs_info.d_pg_stats.scrub_errors + pgs_info.d_pg_stats.deep_scrub_errors + \
                    pgs_info.d_pg_stats.shallow_scrub_errors

        total_net_err = None
        for adapter in host.net_adapters.values():
            if adapter.d_usage is not None:
                total_net_err = (0 if not total_net_err else total_net_err) + adapter.d_usage.total_err

        if total_net_err is not None:
            row.net_err = total_net_err

        if host.d_netstat:
            row.net_err_no_buff = int(host.d_netstat.dropped_no_space_in_q)
            row.net_budget_over = int(host.d_netstat.no_budget)

    return table_to_xml_doc(run_info, id="table-hosts-run-info")


class HostInfoNet(Table):
    __html_classes__ = "table_lc"

    name = Column.s()
    type = Column.s()
    duplex = Column.yes_or_no()
    mtu = Column.ed('MTU')
    speed = Column.d()
    ips = Column.s("IP's")
    roles = Column.s()


def host_net_table(host: Host, ceph: CephInfo) -> AnyXML:
    cluster_networks = [(ceph.public_net, 'ceph-public'), (ceph.cluster_net, 'ceph-cluster')]

    table = HostInfoNet()

    def add_adapter_line(net_adapter, d_name):
        tp = "phy" if net_adapter.is_phy else ('bond' if net_adapter.dev in host.bonds else 'virt')
        roles = [role for net, role in cluster_networks if any((ip.ip in net) for ip in net_adapter.ips)]
        row = table.next_row()
        row.name = d_name, net_adapter.dev
        row.type = tp
        row.duplex = net_adapter.duplex
        row.mtu = net_adapter.mtu
        row.speed = net_adapter.speed if net_adapter.speed else 0
        row.ips = "<br>".join(f"{ip.ip} / {ip.net.prefixlen}" for ip in net_adapter.ips)
        row.roles = "<br>".join(roles)

    all_ifaces = set(host.net_adapters)

    # first show bonds
    for _, bond in sorted(host.bonds.items()):
        dev_name = bond.name.split('/')[-1]
        assert dev_name in all_ifaces, f"{dev_name!r} not in {all_ifaces} for host {host.name}"
        all_ifaces.remove(dev_name)
        adapter = host.net_adapters[dev_name]
        add_adapter_line(adapter, adapter.dev)

        for src in bond.sources:
            assert src in all_ifaces
            all_ifaces.remove(src)
            add_adapter_line(host.net_adapters[src],
                             f'<div class="disk-children-1">+{src}</dev>')

        for adapter_name in list(all_ifaces):
            if adapter_name.startswith(dev_name + '.'):
                all_ifaces.remove(adapter_name)
                add_adapter_line(host.net_adapters[adapter_name],
                                 f'<div class="disk-children-2">->{adapter_name}</dev>')

        table.add_separator()

    for name in sorted(all_ifaces):
        if name != 'lo':
            add_adapter_line(host.net_adapters[name], name)

    return table_to_xml_doc(table, zebra=True, sortable=False, classes="hostinfo-net")


def mib_and_mb(x: int) -> str:
    return f"{b2ssize(x)}B / {b2ssize_10(x)}B"


def find_stor_roles(host: Host, ceph: CephInfo) -> Tuple[Dict[str, Dict[str, Set[int]]], Dict[str, Set[str]]]:
    stor_roles: Dict[str, Dict[str, Set[int]]] = collections.defaultdict(lambda: collections.defaultdict(set))
    stor_classes: Dict[str, Set[str]] = collections.defaultdict(set)

    for osd in ceph.osds.values():
        if osd.host is host:
            if not osd.storage_info:
                continue
            stor_roles[osd.storage_info.data.partition_name]['data'].add(osd.id)
            stor_roles[osd.storage_info.data.name]['data'].add(osd.id)

            if osd.class_name:
                stor_classes[osd.storage_info.data.partition_name].add(osd.class_name)
                stor_classes[osd.storage_info.data.name].add(osd.class_name)

            if isinstance(osd.storage_info, FileStoreInfo):
                stor_roles[osd.storage_info.journal.partition_name]['journal'].add(osd.id)
                stor_roles[osd.storage_info.journal.name]['journal'].add(osd.id)

                if osd.class_name:
                    stor_classes[osd.storage_info.journal.name].add(osd.class_name)
                    stor_classes[osd.storage_info.journal.partition_name].add(osd.class_name)
            else:
                assert isinstance(osd.storage_info, BlueStoreInfo)
                stor_roles[osd.storage_info.db.partition_name]['db'].add(osd.id)
                stor_roles[osd.storage_info.db.name]['db'].add(osd.id)
                stor_roles[osd.storage_info.wal.partition_name]['wal'].add(osd.id)
                stor_roles[osd.storage_info.wal.name]['wal'].add(osd.id)

                if osd.class_name:
                    stor_classes[osd.storage_info.db.partition_name].add(osd.class_name)
                    stor_classes[osd.storage_info.db.name].add(osd.class_name)
                    stor_classes[osd.storage_info.wal.partition_name].add(osd.class_name)
                    stor_classes[osd.storage_info.wal.name].add(osd.class_name)

    return stor_roles, stor_classes


def html_roles(name: str, stor_roles: Dict[str, Dict[str, Set[int]]]) -> str:
    order = ['data', 'journal', 'db', 'wal']
    return "<br>".join(name + ": " + ", ".join(map(str, sorted(ids)))
                       for name, ids in sorted(stor_roles[name].items(), key=lambda x: order.index(x[0])))


class HostInfoDisks(Table):
    name = Column.s()
    type = Column.s()
    size = Column.s()
    roles = Column.s(dont_sort=True)
    classes = Column.s(dont_sort=True)
    scheduler = Column.s()
    model_info = Column.s()
    rq_size = Column.ed()
    phy_sec = Column.sz()
    min_io = Column.sz()


def host_disks_table(host: Host, ceph: CephInfo,
                     stor_roles: Dict[str, Dict[str, Set[int]]],
                     stor_classes: Dict[str, Set[str]]) -> AnyXML:

    table = HostInfoDisks()

    for _, disk in sorted(host.disks.items()):
        row = table.next_row()

        if disk.hw_model.model and disk.hw_model.vendor:
            model = f"Model: {disk.hw_model.vendor.strip()} / {disk.hw_model.model}"
        elif disk.hw_model.model:
            model = f"Model: {disk.hw_model.model}"
        elif disk.hw_model.vendor:
            model = f"Vendor: {disk.hw_model.vendor.strip()}"
        else:
            model = ""

        serial = "" if disk.hw_model.serial is None else f"Serial: {disk.hw_model.serial}"

        if model and serial:
            row.model_info = f"{model}<br>{serial}"
        elif model or serial:
            row.model_info = model if model else serial
        else:
            row.model_info = ""

        row.name = disk.name
        row.type = disk.tp.name
        row.size = mib_and_mb(disk.size), disk.size
        row.roles = html_roles(disk.name, stor_roles)
        if ceph.is_luminous:
            row.classes = "<br>".join(stor_classes.get(disk.name, []))
        row.scheduler = disk.scheduler
        row.rq_size = disk.rq_size
        row.phy_sec = disk.phy_sec
        row.min_io = disk.min_io

    return table_to_xml_doc(table, classes="hostinfo-disks", sortable=True, zebra=True)


class HostInfoMountable(Table):
    name = Column.i()
    type = Column.s()
    size = Column.s()
    mountpoint = Column.s(dont_sort=True)
    ceph_roles = Column.s()
    fs = Column.s()
    free_space = Column.s()
    label = Column.s()


def host_mountable_table(host: Host, stor_roles: Dict[str, Dict[str, Set[int]]]) -> AnyXML:

    table = HostInfoMountable()

    def run_over_children(obj: Union[Disk, LogicBlockDev], level: int):
        row = table.next_row()

        row.name = f'<div class="disk-children-{level}">{obj.name}</div>', obj.name
        row.size = mib_and_mb(obj.size), obj.size
        row.type = obj.tp.name if level == 0 else ''
        row.mountpoint = obj.mountpoint
        row.ceph_roles = "" if level == 0 else html_roles(obj.name, stor_roles)
        row.fs = obj.fs
        row.free_space = (mib_and_mb(obj.free_space), obj.free_space) if obj.free_space is not None else ('', 0)
        row.label = obj.label

        for _, ch in sorted(obj.children.items(), key=lambda x: x[1].partition_num):
            run_over_children(ch, level + 1)

    for _, disk in sorted(host.disks.items()):
        run_over_children(disk, 0)

    return table_to_xml_doc(table, classes="hostinfo-mountable", sortable=False, zebra=True)


def host_info(host: Host, ceph: CephInfo) -> XMLBuilder:
    stor_roles, stor_classes = find_stor_roles(host, ceph)
    doc = XMLBuilder()
    doc.center.H3(host.name)
    doc.br()
    doc.center.H4("Interfaces:")
    doc.br()

    with doc.center:
        doc << host_net_table(host, ceph)

    doc.br()
    doc.br()
    doc.center.H4("HW disks:")
    doc.br()

    with doc.center:
        doc << host_disks_table(host, ceph, stor_roles, stor_classes)

    doc.br()
    doc.br()
    doc.center.H4("Mountable:")
    doc.br()

    with doc.center:
        doc << host_mountable_table(host, stor_roles)

    return doc


@tab("Hosts PG's info")
def show_hosts_pg_info(cluster: Cluster, ceph: CephInfo) -> AnyXML:
    header_row = ["Name",
                  "PGs",
                  "User data TiB",
                  "Reads Miop",
                  "Read TiB",
                  "Writes Miop",
                  "Write TibB"]

    if cluster.has_second_report:
        header_row.extend(["User data<br>store rate", "Read<br>iops", "Read Bps", "Write<br>iops", "Write Bps"])

    table = SimpleTable(*header_row)

    def add_pg_stats(stats: CephIOStats):
        table.add_cell(str(stats.num_bytes // 2 ** 40))
        table.add_cell(str(stats.num_read // 10 ** 6))
        table.add_cell(str(stats.num_read_kb // 2 ** 30))
        table.add_cell(str(stats.num_write // 10 ** 6))
        table.add_cell(str(stats.num_write_kb // 2 ** 20))

    for host in sorted(cluster.hosts.values(), key=lambda x: x.name):
        if host.name in ceph.nodes_pg_info:
            table.add_cell(host_link(host.name).link)
            pgs_info = ceph.nodes_pg_info[host.name]
            table.add_cell(str(len(pgs_info.pgs)))

            add_pg_stats(pgs_info.pg_stats)
            if cluster.has_second_report:
                assert pgs_info.d_pg_stats
                add_pg_stats(pgs_info.d_pg_stats)
            table.next_row()

    tid = "table-hosts-pg-info-long" if cluster.has_second_report else "table-hosts-pg-info"
    return table_to_xml_doc(table, id=tid, classes="table_cr", zebra=True, sortable=True)

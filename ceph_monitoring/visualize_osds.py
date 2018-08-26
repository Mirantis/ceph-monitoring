import logging
import collections
from typing import Dict, List, Optional, Union, Tuple, Any, Callable

import numpy
from dataclasses import dataclass

from cephlib.units import b2ssize, b2ssize_10

from . import html
from .cluster_classes import CephInfo, OSDStatus, DiskType, CephVersion, BlueStoreInfo, FileStoreInfo, LogicBlockDev, \
                             Disk, CephOSD
from .visualize_utils import tab, partition_by_len, to_html_histo, plot, perf_info_required, seconds_to_str_simple
from .obj_links import osd_link, host_link
from .checks import expected_wr_speed
from .plot_data import get_histo_img
from .table import Table, count, bytes_sz, ident, seconds, exact_count, ok_or_fail, idents_list, \
                   yes_or_no, extra_columns, to_str
from .groupby import group_by


logger = logging.getLogger('report')


@dataclass
class OSDInfo:
    id: int
    status: bool
    version: CephVersion
    daemon_runs: bool
    dev_class: str
    reweight: float
    storage_type: str
    storage_total: int
    storage_dev_type: DiskType
    journal_or_wal_collocated: bool
    journal_or_wal_type: DiskType
    journal_or_wal_size: int
    journal_on_file: bool
    db_collocated: Optional[bool]
    db_type: Optional[DiskType]
    db_size: Optional[int]


def group_osds(ceph: CephInfo) -> List[List[OSDInfo]]:
    objs = []

    for osd in ceph.sorted_osds:
        assert osd.storage_info
        data_dev_path = osd.storage_info.data.path
        jinfo = osd.storage_info.journal if isinstance(osd.storage_info, FileStoreInfo) else osd.storage_info.wal

        if isinstance(osd.storage_info, BlueStoreInfo):
            info = osd.storage_info.db.dev_info
            db_collocated: Optional[bool] = info.dev_path != data_dev_path
            db_type: Optional[DiskType] = info.tp
            db_size: Optional[int] = osd.storage_info.db.partition_info.size
        else:
            db_collocated = None
            db_type = None
            db_size = None

        osd_info = OSDInfo(
            id=osd.id,
            status=osd.status == OSDStatus.up,
            version=osd.version,
            daemon_runs=osd.daemon_runs,
            dev_class=osd.class_name if osd.class_name else "",
            reweight=osd.reweight,
            storage_type="bluestore" if isinstance(osd.storage_info, BlueStoreInfo) else "filestore",
            storage_total=osd.total_space,
            storage_dev_type=osd.storage_info.data.dev_info.tp,  # type: ignore
            journal_or_wal_collocated=jinfo.dev_info.dev_path == data_dev_path,
            journal_or_wal_type=jinfo.dev_info.tp,
            journal_or_wal_size=jinfo.partition_info.size,
            journal_on_file=(osd.storage_info.journal.partition_name == osd.storage_info.data.partition_name
                             if isinstance(osd.storage_info, FileStoreInfo) else False),
            db_collocated=db_collocated,
            db_type=db_type,
            db_size=db_size)
        objs.append(osd_info)

    return [[objs[idx] for idx in group] for group in group_by((obj.__dict__ for obj in objs), mutable_keys='id')]


@tab("OSD's state")
def show_osd_state(ceph: CephInfo) -> html.HTMLTable:
    statuses: Dict[OSDStatus, List[str]] = collections.defaultdict(list)

    for osd in ceph.osds.values():
        statuses[osd.status].append(str(osd.id))

    table = html.HTMLTable("table-osds-state", ["Status", "Count", "ID's"])

    for status, osds in sorted(statuses.items()):
        table.add_row([html.ok(status.name) if status == OSDStatus.up else html.fail(status.name),
                       str(len(osds)),
                      "<br>".join(", ".join(grp) for grp in partition_by_len(osds, 120, 1))])

    return table


class OSDLoadTableAgg(Table):
    ids = ident("OSD id's", dont_sort=True)
    node = ident()
    class_and_rules = ident("Class<br>rules")
    pgs = ident("PG's", dont_sort=True)
    open_files = ident("open<br>files")
    ip_conn = ident("ip<br>conn")
    threads = ident("Thread<br>count")
    rss = ident("RSS")
    vmm = ident("VMM")
    cpu_used = ident("CPU<br>Used, s")
    data = ident("Total<br>data")
    read_ops = ident("Read<br>ops<br>uptime")
    read = ident("Read<br>uptime")
    write_ops = ident("Write<br>ops<br>uptime")
    write = ident("Write<br>uptime")


@tab("OSD process info aggregated")
def show_osd_proc_info_agg(ceph: CephInfo) -> html.HTMLTable:

    records: Dict[Tuple[str, str, Tuple[str, ...]], List[CephOSD]] = collections.defaultdict(list)

    for osd in ceph.osds.values():
        rules = tuple(ceph.crush.rules[rule_id].name for rule_id in osd.crush_rules_weights)
        records[(osd.host.name, osd.class_name if osd.class_name else '', rules)].append(osd)

    table = OSDLoadTableAgg()

    for (hostname, classname, rules), osds in sorted(records.items()):
        row = table.next_row()
        ids = [(osd_link(osd.id).link, len(str(osd.id))) for osd in sorted(osds, key=lambda x: x.id)]
        row.ids = "<br>".join(", ".join(part) for part in partition_by_len(ids, 20, 1))  # type: ignore
        row.node = host_link(hostname).link, hostname
        row.pgs = to_html_histo([osd.pg_count for osd in osds], short=True)  # type: ignore
        rule_names = '<br>'.join("rule: " + rule for rule in rules)
        row.class_and_rules = f"cls: {classname}<br>{rule_names}", classname

        #  RUN INFO - FD COUNT, TCP CONN, THREADS
        if all(osd.run_info for osd in osds):
            row.open_files = to_html_histo([osd.run_info.fd_count for osd in osds], short=True)  # type: ignore
            row.ip_conn = to_html_histo([osd.run_info.opened_socks for osd in osds], short=True)  # type: ignore
            row.threads = to_html_histo([osd.run_info.th_count for osd in osds], short=True)  # type: ignore
            row.rss = to_html_histo([osd.run_info.vm_rss for osd in osds], short=True, tostr=b2ssize)  # type: ignore
            row.vmm = to_html_histo([osd.run_info.vm_size for osd in osds], short=True, tostr=b2ssize)  # type: ignore
            row.cpu_used = to_html_histo([int(osd.run_info.cpu_usage) for osd in osds], short=True)  # type: ignore

        row.data = to_html_histo([osd.pg_stats.bytes for osd in osds], short=True, tostr=b2ssize)  # type: ignore
        row.read_ops = to_html_histo([osd.pg_stats.reads for osd in osds], short=True, tostr=b2ssize_10)  # type: ignore
        row.read = to_html_histo([osd.pg_stats.read_b for osd in osds], short=True, tostr=b2ssize)  # type: ignore
        row.write_ops = to_html_histo([osd.pg_stats.writes for osd in osds],  # type: ignore
                                      short=True, tostr=b2ssize_10)  # type: ignore
        row.write = to_html_histo([osd.pg_stats.write_b for osd in osds], short=True, tostr=b2ssize)  # type: ignore


    return table.html(id="table-osd-process-info-agg", align=html.TableAlign.left_right)


class OSDLoadTable(Table):
    id = ident()
    node = ident()
    class_ = ident("Class")
    rules = ident("rules")
    pgs = exact_count("PG's")
    open_files = exact_count("open<br>files")
    ip_conn = ident("ip<br>conn")
    threads = exact_count("thr")
    rss = ident("RSS<br>GiB")
    vmm = ident("VMM<br>GiB")
    cpu_used = ident("CPU Used<br>per 1h uptime")
    data = ident("Total<br>data, GiB")
    write_ops = ident("Write<br>Mops<br>total")
    write = ident("Write<br>total, TiB")
    read_ops = ident("Read<br>Mops<br>total")
    read = ident("Read<br>total, TiB")


@tab("OSD process info")
def show_osd_proc_info(ceph: CephInfo) -> html.HTMLTable:
    table = OSDLoadTable()
    for osd in ceph.sorted_osds:
        row = table.next_row()
        row.id = osd_link(osd.id).link, osd.id
        row.node = host_link(osd.host.name).link, osd.host.name
        row.pgs = osd.pg_count
        row.class_ = osd.class_name
        row.rules = ', '.join(ceph.crush.rules[rule_id].name for rule_id in osd.crush_rules_weights)

        def tostr(v: float) -> str:
            if v > 10:
                return f"{int(v)}"
            if v < 0.1:
                return "0"
            return f"{v:.1f}"

        #  RUN INFO - FD COUNT, TCP CONN, THREADS
        if osd.run_info:
            row.open_files = osd.run_info.fd_count
            row.ip_conn = str(osd.run_info.opened_socks)
            row.threads = osd.run_info.th_count
            row.rss = tostr(osd.run_info.vm_rss / 2 ** 30)
            row.vmm = tostr(osd.run_info.vm_size / 2 ** 30)
            row.cpu_used = seconds_to_str_simple(osd.run_info.cpu_usage * 3600 // osd.host.uptime)

        assert osd.pg_stats

        row.data = tostr(osd.pg_stats.bytes / 2 ** 30)
        row.read_ops = tostr(osd.pg_stats.reads / 10 ** 6)
        row.read = tostr(osd.pg_stats.read_b / 2 ** 40)
        row.write_ops = tostr(osd.pg_stats.writes / 10 ** 6)
        row.write = tostr(osd.pg_stats.write_b / 2 ** 40)
    return table.html(id="table-osd-process-info", align=html.TableAlign.left_right)


@tab("OSD info")
def show_osd_info(ceph: CephInfo) -> html.HTMLTable:
    class OSDInfoTable(Table):
        # id = html.ident()
        count = exact_count()
        ids = idents_list(chars_per_line=50)
        node = ident()
        status = ok_or_fail()
        version = ident("version [hash]")
        daemon_runs = yes_or_no("daemon<br>run")
        dev_class = ident("Storage<br>class")

        weights = extra_columns(ident(), **{f"w_{rule.name}": f"Weight for<br>{rule.name}"
                                            for rule in ceph.crush.rules.values()})

        reweight = ident()
        pg = ident("PG")
        scrub_err = exact_count("Scrub<br>ERR")
        storage_type = ident("Type")
        storage_dev = ident("Storage<br>dev type")
        storage_total = bytes_sz("Storage<br>total")
        journal_or_wal_collocated = ident("Journal<br>or wal<br>colocated")
        journal_or_wal_type = ident("Journal<br>or wal<br>dev type")
        journal_or_wal_size = ident("Journal<br>or wal<br>size")
        journal_on_file = yes_or_no("Journal<br>on file", true_fine=False)
        db_collocated = ident("DB<br>colocated")
        db_type = ident("DB<br>dev type")
        db_size = ident("DB<br>size")

    all_versions = [osd.version for osd in ceph.osds.values()]
    all_versions += [mon.version for mon in ceph.mons.values()]
    all_versions_set = set(all_versions)
    largest_ver = max(all_versions_set)

    fast_drives = {DiskType.nvme, DiskType.sata_ssd, DiskType.sas_ssd}

    table = OSDInfoTable()

    for osd_infos in sorted(group_osds(ceph), key=lambda x: x[0].dev_class):
        osds = [ceph.osds[osd_info.id] for osd_info in osd_infos]
        osd = osds[0]
        osd_info = osd_infos[0]

        row = table.next_row()

        row.count = len(osd_infos)
        row.ids = [(host_link(str(osd_info.id)).link, str(osd_info.id)) for osd_info in osd_infos]
        row.status = osd_info.status

        pgs = [ceph.osds[osd_info.id].pg_count for osd_info in osd_infos]
        assert None not in pgs
        row.pg = to_html_histo(pgs)  # type: ignore

        for rule in ceph.crush.rules.values():
            weights = [tosd.crush_rules_weights[rule.id] for tosd in osds if rule.id in tosd.crush_rules_weights]
            if weights:
                row.weights[f"w_{rule.name}"] = to_html_histo(weights, show_int=False)

        if osd.version is None:
            row.version = html.fail("Unknown"), ""
        else:
            if len(all_versions_set) != 1:
                color: Callable[[str], Union[str, html.TagProxy]] = html.ok if osd.version == largest_ver else html.fail
            else:
                color = lambda x: x
            row.version = color(str(osd.version)), osd.version

        row.daemon_runs = osd.daemon_runs
        row.dev_class = osd.class_name
        rew = f"{osd.reweight:.2f}"
        row.reweight = html.fail(rew) if abs(osd.reweight - 1.0) > .01 else rew
        row.storage_type = osd_info.storage_type

        assert osd.storage_info

        data_drive_color_fn = (html.ok if osd.storage_info.data.dev_info.tp in fast_drives  # type: ignore
                               else lambda x: x)  # type: ignore
        row.storage_dev = data_drive_color_fn(osd.storage_info.data.dev_info.tp.name), \
            osd.storage_info.data.dev_info.tp.name

        # color = "red" if osd.free_perc < 20 else ( "yellow" if osd.free_perc < 40 else "green")
        # avail_perc_str = H.font(osd.free_perc, color=color)

        row.storage_total = osd.total_space

        data_dev_path = osd.storage_info.data.path

        # JOURNAL/WAL/DB info
        if isinstance(osd.storage_info, FileStoreInfo):
            jinfo = osd.storage_info.journal
            if osd.run_info:
                osd_sync = float(osd.config['filestore_max_sync_interval'])
                min_size = osd_sync * expected_wr_speed[osd.storage_info.data.dev_info.tp] * (1024 ** 2)
            else:
                min_size = 0
        else:
            jinfo = osd.storage_info.wal
            min_size = 512 * 1024 * 1024

        if jinfo.dev_info.dev_path == data_dev_path:
            if jinfo.dev_info.tp in (DiskType.nvme, DiskType.sata_ssd, DiskType.sas_ssd):
                vl = "yes"
            else:
                vl = html.fail("yes"), "yes"
        else:
            vl = "no"
        row.journal_or_wal_collocated = vl

        color = html.ok if jinfo.dev_info.tp in fast_drives else html.fail
        row.journal_or_wal_type = color(jinfo.dev_info.tp.name), jinfo.dev_info.tp.name
        size_s = b2ssize(osd_info.journal_or_wal_size)
        row.journal_or_wal_size = (size_s if osd_info.journal_or_wal_size >= min_size else html.fail(size_s)), \
            osd_info.journal_or_wal_size

        if isinstance(osd.storage_info, FileStoreInfo):
            jonfile = osd.storage_info.journal.partition_name == osd.storage_info.data.partition_name
            row.journal_on_file = (html.fail("yes"), "yes") if jonfile else "no"

        if isinstance(osd.storage_info, BlueStoreInfo):
            if osd.storage_info.db.dev_info.size is not None:
                min_db_size = osd.storage_info.data.dev_info.size * 0.05
            else:
                min_db_size = 0

            info = osd.storage_info.db.dev_info

            if info.dev_path == data_dev_path:
                if info.tp in (DiskType.nvme, DiskType.sata_ssd, DiskType.sas_ssd):
                    vl = "yes"
                else:
                    vl = html.fail("yes"), "yes"
            else:
                vl = "no"

            row.db_collocated = vl
            color = html.ok if info.tp in fast_drives else html.fail
            row.db_type = color(info.tp.name), info.tp.name
            size_s = b2ssize(osd_info.db_size)
            assert osd_info.db_size is not None
            row.db_size = size_s if osd_info.db_size >= min_db_size else html.fail(size_s), osd_info.db_size

    return table.html(id="table-osd-info", align=html.TableAlign.left_right)


@tab("OSD's dev uptime average load")
def show_osd_perf_info(ceph: CephInfo) -> html.HTMLTable:
    class Tbl(Table):
        osd = ident()
        cls = ident("Class")
        node = ident()
        apply_lat = ident("apply<br>lat")
        commit_lat = ident("commit<br>lat")
        journal_lat = ident("journal<br>lat")
        xx = ident("ms<br>avg/osd perf")
        data_dev = ident("Data<br>dev")
        data_read = ident("Data read<br>MiBps/iops")
        data_write = ident("Data write<br>MiBps/iops")
        data_lat = ident("Data lat<br>ms")
        data_io_time = to_str("Data<br>IO time<br>ms per s")
        j_dev = ident("J dev")
        j_write = ident("J write<br>MiBps/iops")
        j_lat = ident("J lat<br>ms")
        j_io_time = to_str("J<br>IO time<br>ms per s")
        wal_dev = ident("WAL dev")
        wal_write = ident("WAL write<br>MiBps/iops")
        wal_lat = ident("WAL lat<br>ms")
        wal_io_time = to_str("WAL<br>IO time<br>ms per s")
        db_dev = ident("DB dev")
        db_read = ident("DB read<br>MiBps/iops")
        db_write = ident("DB write<br>MiBps/iops")
        db_lat = ident("DB lat<br>ms")
        db_io_time = to_str("DB IO<br>time<br>ms per s")

    def add_dev_info(row: Any, dev: Union[Disk, LogicBlockDev], attr: str, uptime: float, with_read: bool = True):

        setattr(row, attr + '_dev', dev.name if dev.tp == DiskType.nvme else f"{dev.name} ({dev.tp.short_name})")
        MBupt = 2 ** 20 * int(uptime)
        if with_read:
            setattr(row, attr + '_read', (f"{dev.usage.read_bytes // MBupt} / {int(dev.usage.read_iops / uptime)}",
                                          dev.usage.read_iops / uptime))

        setattr(row, attr + '_write', (f"{dev.usage.write_bytes // MBupt} / {int(dev.usage.write_iops / uptime)}",
                                       dev.usage.write_iops / uptime))

        if dev.usage.lat is None:
            setattr(row, attr + '_lat', ('-', 0))
        else:
            setattr(row, attr + '_lat',
                ((f"{dev.usage.lat / 1000:.1f}s"
                  if dev.usage.lat > 1000
                  else f"{int(dev.usage.lat)}ms"), dev.usage.lat))

        setattr(row, attr + '_io_time', int(dev.usage.io_time // uptime))

    def get_lat_val(osd: CephOSD, lat_name: str) -> Optional[str]:
        if lat_name in osd.osd_perf_dump:
            ms = osd.osd_perf_dump[lat_name] * 1000
        elif lat_name + '_s' in osd.osd_perf_dump:
            ms = osd.osd_perf_dump[lat_name + "_s"] * 1000
        elif lat_name + '_ms' in osd.osd_perf_dump:
            ms = osd.osd_perf_dump[lat_name + "_ms"]
        else:
            return None
        return f"{ms / 1000:.1f}s" if ms > 1000 else f"{int(ms)}ms"

    table = Tbl()
    for osd in ceph.sorted_osds:
        assert osd.storage_info is not None
        row = table.next_row()

        row.osd = osd_link(osd.id).link, osd.id
        row.cls = osd.class_name
        row.node = host_link(osd.host.name).link, osd.host.name
        row.apply_lat = get_lat_val(osd, "apply_latency")

        clat = get_lat_val(osd, "commitcycle_latency")
        if clat is None:
            clat = get_lat_val(osd, "commit_latency")

        row.commit_lat = clat

        jlat = get_lat_val(osd, "journal_latency")
        if jlat is not None:
            row.journal_lat = jlat

        add_dev_info(row, osd.storage_info.data.dev_info, "data", osd.host.uptime)
        if isinstance(osd.storage_info, FileStoreInfo):
            add_dev_info(row, osd.storage_info.journal.dev_info, "j", osd.host.uptime, with_read=False)
        else:
            add_dev_info(row, osd.storage_info.wal.dev_info, "wal", osd.host.uptime, with_read=False)
            add_dev_info(row, osd.storage_info.db.dev_info, "db", osd.host.uptime)

    return table.html("table-osd-dev-uptime-load")


@tab("PG copy per OSD")
def show_osd_pool_pg_distribution(ceph: CephInfo) -> Optional[html.HTMLTable]:
    if ceph.sum_per_osd is None:
        logger.warning("PG copy per OSD: No pg dump data. Probably too many PG")
        return None

    pools_order_t = sorted((-count, name) for name, count in ceph.sum_per_pool.items())
    # pools = sorted(cluster.sum_per_pool)
    pools = [name for _, name in pools_order_t]

    name_headers = []
    for name in pools:
        if len(name) > 10:
            parts = name.split(".")
            best_idx = 0
            best_delta = len(name) + 1
            for idx in range(len(parts)):
                sz1 = len(".".join(parts[:idx]))
                sz2 = len(".".join(parts[idx:]))
                if abs(sz2 - sz1) < best_delta:
                    best_idx = idx
                    best_delta = abs(sz2 - sz1)
            if best_idx != 0 and best_idx != len(parts):
                name = ".".join(parts[:best_idx]) + '.' + '<br>' + ".".join(parts[best_idx:])
        name_headers.append(name)

    table = html.HTMLTable("table-pg-per-osd", ["OSD/pool"] + name_headers + ['sum'])

    for osd_id, row in sorted(ceph.osd_pool_pg_2d.items()):
        data = [osd_link(osd_id).link] + [row.get(pool_name, 0) for pool_name in pools]  # type: ignore
        data.append(ceph.sum_per_osd[osd_id])  # type: ignore
        table.add_row(map(str, data))

    table.add_cell("Total cluster PG", sorttable_customkey=str(max(ceph.osds) + 1))

    list(map(table.add_cell, (ceph.sum_per_pool[pool_name] for pool_name in pools)))  # type: ignore
    table.add_cell(str(sum(ceph.sum_per_pool.values())))
    return table


@tab("PG copy per OSD")
def show_osd_pool_agg_pg_distribution(ceph: CephInfo) -> Optional[html.HTMLTable]:
    if ceph.sum_per_osd is None:
        logger.warning("PG copy per OSD: No pg dump data. Probably too many PG")
        return None

    pool2osd_count: Dict[str, List[Tuple[int, int]]] = {}

    for osd_id, pool_row in ceph.osd_pool_pg_2d.items():
        for pool_name, count in pool_row.items():
            pool2osd_count.setdefault(pool_name, []).append((count, osd_id))

    class PGAggTable(Table):
        name = ident()
        pg = exact_count("PG copies")
        osds = exact_count("OSDS")
        min = ident(dont_sort=True)
        p10 = exact_count("10%")
        p30 = exact_count("30%")
        p50 = exact_count("50%")
        p70 = exact_count("70%")
        p90 = exact_count("90%")
        max = ident(dont_sort=True)

    table = PGAggTable()
    for pool_name, counts in pool2osd_count.items():
        pool = ceph.pools[pool_name]
        counts.sort()
        row = table.next_row()
        row.name = pool_name
        row.pg = pool.pg * pool.size
        row.osds = len(ceph.osds4rule[pool.crush_rule])
        row.min = "<br>".join(osd_link(osd_id).link + f": {cnt}" for cnt, osd_id in counts[:5])
        row.max = "<br>".join(osd_link(osd_id).link + f": {cnt}" for cnt, osd_id in counts[-5:])
        row.p10 = counts[int(len(counts) * 0.1)]
        row.p30 = counts[int(len(counts) * 0.3)]
        row.p50 = counts[int(len(counts) * 0.5)]
        row.p70 = counts[int(len(counts) * 0.7)]
        row.p90 = counts[int(len(counts) * 0.9)]
    return table.html(id="table-pg-per-osd-agg")


@plot
@tab("PG per OSD")
def show_osd_pg_histo(ceph: CephInfo) -> Optional[str]:
    vals = [osd.pg_count for osd in ceph.osds.values() if osd.pg_count is not None]
    return get_histo_img(numpy.array(vals)) if vals else None

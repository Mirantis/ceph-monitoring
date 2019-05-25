import time
import logging
import datetime
import collections
from typing import Callable, Dict, List, Optional

import yaml

from koder_utils import (b2ssize_10, b2ssize, Table, Column, seconds_to_str, table_to_html, XMLBuilder, fail, ok,
                         SimpleTable, AnyXML, htag, RawContent, partition_by_len)
from cephlib import CephInfo, FileStoreInfo, BlueStoreInfo, Pool, get_rule_replication_level, get_rule_osd_class

from . import get_file
from .cluster import Cluster
from .visualize_utils import tab, table_to_xml_doc
from .checks import run_all_checks, CheckMessage
from .report import Report
from .obj_links import err_link, mon_link, pool_link


logger = logging.getLogger("report")


@tab("Status")
def show_cluster_summary(cluster: Cluster, ceph: CephInfo) -> AnyXML:
    """
    Cluster short summary
    """
    class SummaryTable(Table):
        setting = Column.s()
        value = Column.to_str()

    t = SummaryTable()
    t.add_row("Collected at", cluster.report_collected_at_local)
    t.add_row("Collected at GMT", cluster.report_collected_at_gmt)
    t.add_row("Status", ceph.status.health.overall_status.name.upper())
    t.add_row("PG count", ceph.status.pgmap.num_pgs)
    t.add_row("Pool count", len(ceph.pools))
    t.add_row("Used", b2ssize(ceph.status.pgmap.bytes_used))
    t.add_row("Free space", b2ssize(ceph.status.pgmap.bytes_avail))
    t.add_row("Data size", b2ssize(ceph.status.pgmap.data_bytes))

    avail_perc = ceph.status.pgmap.bytes_avail * 100 / ceph.status.pgmap.bytes_total
    t.add_row("Free %", int(avail_perc))
    t.add_row("Monitors count", len(ceph.mons))
    t.add_row("OSD's count", len(ceph.osds))

    mon_vers = {mon.version for mon in ceph.mons.values()}
    t.add_row("Monitor versions", ", ".join(map(str, sorted(mon_vers))))

    osd_vers = {osd.version for osd in ceph.osds.values()}
    vers = [str(ver) for ver in sorted(ver for ver in osd_vers if ver)]
    if None in osd_vers:
        vers.append("Unknown")
    t.add_row("OSD's versions", ", ".join(vers))

    if ceph.has_fs and ceph.has_bs:
        stor_types = "filestore & bluestore"
    elif ceph.has_fs:
        stor_types = "filestore"
    elif ceph.has_bs:
        stor_types = "bluestore"
    else:
        assert False

    t.add_row("OSD storage types",  stor_types)

    t.add_row("Monmap version", ceph.status.monmap.epoch)
    mon_tm = time.mktime(ceph.status.monmap.modified.timetuple())
    collect_tm = time.mktime(time.strptime(cluster.report_collected_at_local, "%Y-%m-%d %H:%M:%S"))
    t.add_row("Monmap modified in", seconds_to_str(int(collect_tm - mon_tm)))
    return table_to_xml_doc(t, id="table-summary")


def show_issues_table(cluster: Cluster, ceph: CephInfo, report: Report) -> None:
    """
    Cluster issues
    """
    config = yaml.safe_load(get_file('check_conf.yaml').open())
    check_results = run_all_checks(config, cluster, ceph)

    t = SimpleTable("Check", "Result", "Comment", c_width=[40, 5, 55])

    failed = fail("Failed")
    passed = ok("Passed")

    err_per_test: Dict[str, List[CheckMessage]] = collections.defaultdict(list)

    for result in check_results:
        t.add_row(result.check_description, passed if result.passed else failed,
                  err_link(result.reporter_id, result.message).link)
        for err in result.fails:
            err_per_test[err.reporter_id].append(err)

    report.add_block('issues', "Issues:", table_to_xml_doc(t, id="table-issues", sortable=False, zebra=True))

    for reporter_id, errs in err_per_test.items():
        table = SimpleTable("Services", "Serv.<br>count", "Error")

        # group errors by services
        err_map: Dict[str, List[str]] = {}
        for err in errs:
            err_map.setdefault(err.message, []).append(str(err.affected_service))

        all_services = set()
        for message, services in err_map.items():
            table.add_row("<br>".join(", ".join(items) for items in partition_by_len(services, 80, 1)),
                          str(len(services)), message)
            all_services.update(services)

        if len(err_map) > 1:
            table.add_row("Total affected", str(len(all_services)), "")

        report.add_block(err_link(reporter_id).id, None, table_to_html(table))
    report.issues.update(err_per_test)


@tab("Current IO Activity")
def show_io_status(ceph: CephInfo) -> AnyXML:
    """
    Current cluster IO load
    """
    class IOTable(Table):
        io_type = Column.s("IO type")
        val = Column.s()

    t = IOTable()
    t.add_row("Client IO Write MiBps", b2ssize(ceph.status.pgmap.write_bytes_sec // 2 ** 20))
    t.add_row("Client IO Write OPS", b2ssize_10(ceph.status.pgmap.write_op_per_sec))
    t.add_row("Client IO Read MiBps", b2ssize(ceph.status.pgmap.read_bytes_per_sec // 2 ** 20))
    t.add_row("Client IO Read OPS", b2ssize_10(ceph.status.pgmap.read_op_per_sec))
    t.add_row("Recovery IO MiBps", b2ssize(ceph.status.pgmap.recovering_bytes_per_sec // 2 ** 20))
    t.add_row("Recovery obj per second", b2ssize_10(ceph.status.pgmap.recovering_objects_per_sec))

    return table_to_xml_doc(t, id="table-io-summary", sortable=False, zebra=True)


class MonitorInfoTable(Table):
    name = Column.s(help="Monitor/host name")
    health = Column.s(help="Monitor health")
    role = Column.s(help="Monitor role")
    free = Column.s("Disk free<br>B (%)", help="Free space on used disk")
    db_size = Column.d("DB size", help="Monitor database size")


@tab("Monitors info")
def show_mons_info(ceph: CephInfo) -> AnyXML:
    """
    Monitors info
    {MonitorInfoTable.help()}
    """

    table = MonitorInfoTable()

    for _, mon in sorted(ceph.mons.items()):
        role = "Unknown"
        health = fail("HEALTH_FAIL")

        # if mon.role is MonRole.unknown:
        #     for mon_info in ceph.status.monmap_stat['mons']:
        #         if mon_info['name'] == mon.name:
        #             if mon_info.get('rank') in [0, 1, 2, 3, 4]:
        #                 health = ok("HEALTH_OK")
        #                 role = "leader" if mon_info.get('rank') == 0 else "follower"
        #                 break
        # else:
        #     health = ok("HEALTH_OK") if mon.name in ceph.status.quorum_names else fail(mon.status)
        #     role = "leader" if mon.role == MonRole.master else \
        #         ("follower" if mon.role == MonRole.master else "Unknown")

        if mon.kb_avail is None:
            perc = "Unknown"
            sort_by = "0"
        else:
            perc = f"{b2ssize(mon.kb_avail * 1024)} ({mon.avail_percent}%)"
            sort_by = str(mon.kb_avail)

        row = table.next_row()
        row.name = mon_link(mon.name).link, mon.name
        row.health = health, mon.status
        row.role = role
        row.free = perc, sort_by
        row.db_size = mon.database_size

    return table_to_xml_doc(table, id="table-mon-info", zebra=True, sortable=True)


@tab("Settings")
def show_primary_settings(ceph: CephInfo) -> AnyXML:
    """
    Most important cluster settings
    """
    table = SimpleTable("Name", "Value")

    table.add_cell(~htag.b("Common"), colspan=2)
    table.add_row("Cluster net", str(ceph.cluster_net))
    table.add_row("Public net", str(ceph.public_net))
    table.add_row("Near full ratio", f"{float(ceph.settings.mon_osd_nearfull_ratio):.3f}")

    if 'mon_osd_backfillfull_ratio' in ceph.settings:
        bfratio = f"{float(ceph.settings.mon_osd_backfillfull_ratio):.3f}"
    elif 'osd_backfill_full_ratio' in ceph.settings:
        bfratio = f"{float(ceph.settings.osd_backfill_full_ratio):.3f}"
    else:
        bfratio = '?'

    table.add_row("Backfill full ratio", bfratio)
    table.add_row("Full ratio", f"{float(ceph.settings.mon_osd_full_ratio):.3f}")
    table.add_row("Filesafe full ratio", f"{float(ceph.settings.osd_failsafe_full_ratio):.3f}")

    def show_opt(name: str, tr_func: Callable[[str], str] = None):
        name_under = name.replace(" ", "_")
        if name_under in ceph.settings:
            vl = ceph.settings[name_under]
            if tr_func is not None:
                vl = tr_func(vl)
            table.add_row(name.capitalize(), vl)

    table.add_cell(~htag.b("Fail detection"), colspan=2)

    show_opt("mon osd down out interval", lambda x: seconds_to_str(int(x)))
    show_opt("mon osd adjust down out interval")
    show_opt("mon osd down out subtree limit")
    show_opt("mon osd report timeout", lambda x: seconds_to_str(int(x)))
    show_opt("mon osd min down reporters")
    show_opt("mon osd reporter subtree level")
    show_opt("osd heartbeat grace", lambda x: seconds_to_str(int(x)))

    table.add_cell("<b>Other</b>", colspan=2)

    show_opt("osd max object size", lambda x: b2ssize(int(x)) + "B")
    show_opt("osd mount options xfs")

    table.add_cell("<b>Scrub</b>", colspan=2)

    show_opt("osd max scrubs")
    show_opt("osd scrub begin hour")
    show_opt("osd scrub end hour")
    show_opt("osd scrub during recovery")
    show_opt("osd scrub thread timeout", lambda x: seconds_to_str(int(x)))
    show_opt("osd scrub min interval", lambda x: seconds_to_str(int(float(x))))
    show_opt("osd scrub chunk max", lambda x: b2ssize(int(x)))
    show_opt("osd scrub sleep", lambda x: seconds_to_str(int(float(x))))
    show_opt("osd deep scrub interval", lambda x: seconds_to_str(int(float(x))))
    show_opt("osd deep scrub stride", lambda x: b2ssize(int(x)) + "B")

    table.add_cell(~htag.b("OSD io"), colspan=2)

    show_opt("osd op queue")
    show_opt("osd client op priority")
    show_opt("osd recovery op priority")
    show_opt("osd scrub priority")
    show_opt("osd op thread timeout", lambda x: seconds_to_str(float(x)))
    show_opt("osd op complaint time", lambda x: seconds_to_str(float(x)))
    show_opt("osd disk threads")
    show_opt("osd disk thread ioprio class")
    show_opt("osd disk thread ioprio priority")
    show_opt("osd op history size")
    show_opt("osd op history duration")
    show_opt("osd recovery max chunk", lambda x: b2ssize(int(x)) + "B")
    show_opt("osd max backfills")
    show_opt("osd backfill scan min")
    show_opt("osd backfill scan max")
    show_opt("osd map cache size")
    show_opt("osd map message max")
    show_opt("osd recovery max active")
    show_opt("osd recovery thread timeout")

    if ceph.has_bs:
        table.add_cell(~htag.b("Bluestore"), colspan=2)

        show_opt("bluestore cache size hdd", lambda x: b2ssize(int(x)) + "B")
        show_opt("bluestore cache size ssd", lambda x: b2ssize(int(x)) + "B")
        show_opt("bluestore cache meta ratio", lambda x: f"{float(x):.3f}")
        show_opt("bluestore cache kv ratio", lambda x: f"{float(x):.3f}")
        show_opt("bluestore cache kv max", lambda x: b2ssize(int(x)) + "B")
        show_opt("bluestore csum type")

    if ceph.has_fs:
        table.add_cell(~htag.b("Filestore"), colspan=2)
        table.add_row("Journal aio", ceph.settings.journal_aio)
        table.add_row("Journal dio", ceph.settings.journal_dio)
        table.add_row("Filestorage sync", str(int(float(ceph.settings.filestore_max_sync_interval))) + 's')

    return table_to_xml_doc(table, id="table-settings", zebra=True)


class RulesetsTable(Table):
    rule = Column.s(help="Rule name")
    id = Column.ed(help="Rule id")
    pools = Column.list(help="Pools, this rule is used for", width=20)
    osd_class = Column.s(help='OSD class used int his rule')
    replication_level = Column.s("Replication<br>level", help='Level on which this rule doing replication')
    pg = Column.s("PG", help='Total PG count, managed by this rule')
    pg_per_osd = Column.ed("PG copy/OSD", help='Average PG copy per OSD for this rule')
    num_osd = Column.ed("# OSD", help="OSD count, this rule put data to")
    total_size = Column.sz(help="Total space osd all OSD's used by this rule")
    free_size = Column.s(help="Free space on OSD's, managed by this rule (not counting replication)")
    data = Column.s("Data size<br>TiB",
                    help="User data size, managed by this rule (without replication)")
    objs = Column.s("Total<br>Kobjects", help="Object count managed by this rule")
    data_disk_sizes = Column.s(help="Disk sizes, used to store data for this rule on OSD's")
    disk_types = Column.list(delim='<br>', help="Disk types, used to store data for this rule on OSD's")
    data_disk_models = Column.list(help="Disk models, used to store data for this rule on OSD's")


@tab("Crush rulesets")
def show_ruleset_info(ceph: CephInfo) -> AnyXML:
    f"""
    Crush ruleset info
    """

    pools: Dict[int, List[Pool]] = {}
    for pool in ceph.pools.values():
        pools.setdefault(pool.crush_rule, []).append(pool)

    table = RulesetsTable()

    cluster_pg = sum(pool.pg for pool in ceph.pools.values())
    cluster_bytes = sum(pool.df.size_bytes for pool in ceph.pools.values())
    cluster_objects = sum(pool.df.num_objects for pool in ceph.pools.values())

    for rule in ceph.crush.crushmap.rules:
        if rule.rule_id not in ceph.osds4rule:
            logger.warning("Skipping visualization of rule %s, as it hs no osd in it", rule.rule_name)
            continue

        row = table.next_row()
        row.rule = rule.rule_name
        row.id = rule.rule_id
        row.pools = [pool_link(pool.name).link for pool in pools.get(rule.rule_id, [])]

        if ceph.is_luminous:
            class_name = get_rule_osd_class(rule)
            row.osd_class = '*' if class_name is None else class_name

        row.replication_level = get_rule_replication_level(rule)
        osds = ceph.osds4rule[rule.rule_id]
        row.num_osd = len(osds)
        total_sz = sum(osd.space.free + osd.space.used for osd in osds)
        row.total_size = total_sz
        total_free = sum(osd.space.free for osd in osds)
        row.free_size = f"{b2ssize(total_free)} ({total_free * 100 // total_sz}%)", total_free

        if cluster_bytes:
            total_data = sum(pool.df.size_bytes for pool in pools.get(rule.rule_id, []))
            row.data = f"{total_data // 2 ** 40} ({total_data * 100 // cluster_bytes}%)"
        else:
            row.data = '-'

        if cluster_objects:
            total_objs = sum(pool.df.num_objects for pool in pools.get(rule.rule_id, []))
            row.objs = f"{total_objs // 10 ** 3} ({total_objs * 100 // cluster_objects}%)"
        else:
            row.objs = '-'

        total_pg = sum(pool.pg for pool in pools.get(rule.rule_id, []))
        row.pg = f"{total_pg} ({total_pg * 100 // cluster_pg}%)", total_pg

        total_pg_copy = sum(pool.pg * pool.size for pool in pools.get(rule.rule_id, []))
        row.pg_per_osd = total_pg_copy // len(osds)

        storage_disks_types = set()
        journal_disks_types = set()
        disks_sizes = set()
        disks_info = set()

        for osd in osds:
            if osd.storage_info:
                dsk = osd.storage_info.data.dev_info
                storage_disks_types.add(dsk.tp.name)
                if isinstance(osd.storage_info, FileStoreInfo):
                    journal_disks_types.add(osd.storage_info.journal.dev_info.tp.name)
                else:
                    assert isinstance(osd.storage_info, BlueStoreInfo)
                    journal_disks_types.add(osd.storage_info.wal.dev_info.tp.name)
                    journal_disks_types.add(osd.storage_info.db.dev_info.tp.name)

                disks_sizes.add(dsk.logic_dev.size)
                disks_info.add(f"{dsk.hw_model.vendor}::{dsk.hw_model.model}")

        row.data_disk_sizes = ", ".join(map(b2ssize, sorted(disks_sizes)))
        row.disk_types = ["data: " + ", ".join(storage_disks_types), "wal/db/j: " + ", ".join(journal_disks_types)]
        row.data_disk_models = sorted(disks_info)

    return table_to_xml_doc(table, id="table-rules", zebra=True, sortable=True)


@tab("Cluster err/warn")
def show_cluster_err_warn(ceph: CephInfo) -> XMLBuilder:
    doc = XMLBuilder()
    with doc.div:
        for err in ceph.log_err_warn:
            doc(err)
            doc.br()
    return doc


class NetsTable(Table):
    mask = Column.s(help="Network mask")
    roles = Column.s("Ceph roles", help="Ceph roles, this network is used for (client/cluster)")
    mtus = Column.s("MTU's", help="MTU's set on adaters of nodes")
    bw = Column.s("Interfaces<br>speeds<br>bits per second",
                  help="Interfaces speed settings in Gbps (10**9 bits per second)")
    data_transferred = Column.sz("Bytes<br>transferred",
                                 help="Total bytes transferred (send+recv) on all adapters between reports")
    pps_transferred = Column.d("Packets<br>transferred",
                               help="Total packets transferred (send+recv) on all adapters between reports")
    multicast = Column.d("Multicast<br>packets",
                         help="Total count of multicast packages received on all adapters between reports")
    total_err = Column.d("Wire<br>errors",
                         help="Total count if transmit errors between reports during all nodes uptime")
    data_transferred_uptime = Column.sz("Bytes<br>transferred<br>uptime",
                                        help="Bytes transferred during all nodes uptime")
    pps_transferred_uptime = Column.d("Packets<br>transferred<br>uptime",
                                      help="Total packets transferred (send+recv) on all " +
                                           "adapters  during all nodes uptime")
    multicast_uptime = Column.d("Multicast<br>packets<br>uptime",
                                help="Total multicast packages received on all adapters during all nodes uptime")
    total_err_uptime = Column.d("Errors<br>uptime", help="Total error on all adapters during all nodes uptime")


@tab("Whole cluster net")
def show_whole_cluster_nets(cluster: Cluster) -> AnyXML:
    f"""
    Cluster networks summary
    """

    table = NetsTable()
    any_d_usage = False

    for _, info in sorted(cluster.net_data.items()):
        row = table.next_row()
        row.mask = info.mask
        row.roles = "<br>".join(info.roles)
        row.mtus = "<br>".join(map(str, info.mtus))
        row.bw = "<br>".join([b2ssize_10(speed * 8) + 'bps' for speed in info.speeds])

        if info.d_usage:
            usage = info.d_usage
            row.data_transferred = usage.recv_bytes + usage.send_bytes
            row.pps_transferred = usage.recv_packets + usage.send_packets
            row.multicast = usage.rmulticast
            row.total_err = usage.total_err
            any_d_usage = True

        row.data_transferred_uptime = info.usage.recv_bytes + info.usage.send_bytes
        row.pps_transferred_uptime = info.usage.recv_packets + info.usage.send_packets
        row.multicast_uptime = info.usage.rmulticast
        row.total_err_uptime = info.usage.total_err

    tid = "table-cluster-nets-wide" if any_d_usage else "table-cluster-nets"
    return table_to_xml_doc(table, id=tid, zebra=True, sortable=True)


class IssuesTable(Table):
    type = Column.s(help="Error short name")
    count = Column.ed(help="Error count of this type")


@tab("Errors summary")
def show_cluster_err_warn_summary(ceph: CephInfo) -> Optional[AnyXML]:
    f"""
    Cluster logs errors/warnings summary and cluster health timeline
    """

    if ceph.errors_count:
        table = IssuesTable()

        for name, cnt in sorted(ceph.errors_count.items(), key=lambda x: x[1]):
            table.add_row(name, cnt)

        res = table_to_xml_doc(table, id="table-errors-summary")
        health_svg = plot_healthnes(ceph)
        if health_svg:
            res.br()
            res.br()
            res.br()
            res << RawContent(health_svg)
        return res


def plot_healthnes(ceph: CephInfo, width: int = 1400, height: int = 30) -> Optional[AnyXML]:
    if not ceph.status_regions:
        return None

    begin = ceph.status_regions[0].begin
    end = ceph.status_regions[-1].end

    if end - begin < 100:
        return None

    total_len = end - begin

    begin_s = datetime.datetime.utcfromtimestamp(begin).strftime('%Y-%m-%d %H:%M:%S')
    end_s = datetime.datetime.utcfromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S')

    doc = XMLBuilder()
    doc.center.H4(f"Health status over time. Begin at {begin_s}, ends at {end_s}")
    doc.br()
    doc.br()
    with doc.svg(width=str(width), height=str(height)):
        curr_x: float = 0
        for region in ceph.status_regions:
            color = "#6282EA" if region.healty else "#DD5F4B"
            end_x = width * (region.end - begin) / total_len
            doc.rect(x=str(curr_x), y="0", width=str(end_x - curr_x), height=str(height), style=f"fill:{color}")
            curr_x = end_x

    return doc

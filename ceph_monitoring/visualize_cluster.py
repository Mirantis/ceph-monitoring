import time
import collections
from pathlib import Path
from typing import Callable, Dict, List

import yaml

from cephlib.units import b2ssize_10, b2ssize

from . import table
from . import html
from .cluster_classes import CephInfo, Cluster, FileStoreInfo, BlueStoreInfo
from .visualize_utils import tab, seconds_to_str, get_all_versions, partition_by_len
from .checks import run_all_checks, CheckMessage
from .report import Report
from .obj_links import err_link, mon_link, pool_link
from .table import Table, count, bytes_sz, ident, idents_list, exact_count, to_str


@tab("Status")
def show_cluster_summary(cluster: Cluster, ceph: CephInfo) -> html.HTMLTable:
    class SummaryTable(table.Table):
        setting = table.ident()
        value = table.to_str()

    t = SummaryTable()
    t.add_row("Collected at", cluster.report_collected_at_local)
    t.add_row("Collected at GMT", cluster.report_collected_at_gmt)
    t.add_row("Status", ceph.status.status.name.upper())
    t.add_row("PG count", ceph.status.num_pgs)
    t.add_row("Pool count", len(ceph.pools))
    t.add_row("Used", b2ssize(ceph.status.bytes_used))
    t.add_row("Avail", b2ssize(ceph.status.bytes_avail))
    t.add_row("Data", b2ssize(ceph.status.data_bytes))

    avail_perc = ceph.status.bytes_avail * 100 / ceph.status.bytes_total
    t.add_row("Free %", int(avail_perc))
    t.add_row("Mon count", len(ceph.mons))
    t.add_row("OSD count", len(ceph.osds))

    mon_vers = get_all_versions(ceph.mons.values())
    t.add_row("Mon version", ", ".join(map(str, sorted(mon_vers))))

    osd_vers = get_all_versions(ceph.osds.values())
    t.add_row("OSD version", ", ".join(map(str, sorted(osd_vers))))

    if ceph.has_fs and ceph.has_bs:
        stor_types = "filestore & bluestore"
    elif ceph.has_fs:
        stor_types = "filestore"
    elif ceph.has_bs:
        stor_types = "bluestore"
    else:
        assert False

    t.add_row("Storage types",  stor_types)

    t.add_row("Monmap version", ceph.status.monmap_stat['epoch'])
    mon_tm = time.mktime(time.strptime(ceph.status.monmap_stat['modified'], "%Y-%m-%d %H:%M:%S.%f"))
    collect_tm = time.mktime(time.strptime(cluster.report_collected_at_local, "%Y-%m-%d %H:%M:%S"))
    t.add_row("Monmap modified in", seconds_to_str(int(collect_tm - mon_tm)))
    return t.html(id="table-summary", sortable=False)


def show_issues_table(cluster: Cluster, ceph: CephInfo, report: Report):
    config = yaml.load((Path(__file__).parent / 'check_conf.yaml').open())
    check_results = run_all_checks(config, cluster, ceph)

    t = html.HTMLTable("table-issues", ["Check", "Result", "Comment"], sortable=False)

    failed = html.fail("Failed")
    passed = html.ok("Passed")

    err_per_test: Dict[str, List[CheckMessage]] = collections.defaultdict(list)

    for result in check_results:
        t.add_cells(result.check_description, passed if result.passed else failed,
                    err_link(result.reporter_id, result.message).link)
        for err in result.fails:
            err_per_test[err.reporter_id].append(err)

    report.add_block('issues', "Issues:", str(t))

    for reporter_id, errs in err_per_test.items():
        table = html.HTMLTable(headers=["Services", "Error"])

        # group errors by services
        err_map: Dict[str, List[str]] = {}
        for err in errs:
            err_map.setdefault(err.message, []).append(str(err.affected_service))

        result_messages = []
        for message, services in err_map.items():
            table.add_cells("<br>".join(", ".join(items) for items in partition_by_len(services, 80, 1)), message)

        report.add_block(err_link(reporter_id).id, None, str(table))


@tab("Current IO Activity")
def show_io_status(ceph: CephInfo) -> html.HTMLTable:
    t = html.HTMLTable("table-io-summary", ["IO type", "Value"], sortable=False)
    t.add_cells("Client IO Write MiBps", b2ssize(ceph.status.write_bytes_sec // 2 ** 20))
    t.add_cells("Client IO Write OPS", b2ssize(ceph.status.write_op_per_sec))
    t.add_cells("Client IO Read MiBps", b2ssize(ceph.status.read_bytes_sec // 2 ** 20))
    t.add_cells("Client IO Read OPS", b2ssize(ceph.status.read_op_per_sec))
    if "recovering_bytes_per_sec" in ceph.status.pgmap_stat:
        t.add_cells("Recovery IO MiBps", b2ssize(ceph.status.pgmap_stat["recovering_bytes_per_sec"]))
    if "recovering_objects_per_sec" in ceph.status.pgmap_stat:
        t.add_cells("Recovery obj per second", b2ssize_10(ceph.status.pgmap_stat["recovering_objects_per_sec"]))
    return t


@tab("Monitors info")
def show_mons_info(ceph: CephInfo) -> html.HTMLTable:
    table = html.HTMLTable("table-mon-info", ["Name", "Health", "Role", "Disk free<br>B (%)"])

    for _, mon in sorted(ceph.mons.items()):
        role = "Unknown"
        health = html.fail("HEALTH_FAIL")
        if mon.status is None:
            for mon_info in ceph.status.monmap_stat['mons']:
                if mon_info['name'] == mon.name:
                    if mon_info.get('rank') in [0, 1, 2, 3, 4]:
                        health = html.ok("HEALTH_OK")
                        role = "leader" if mon_info.get('rank') == 0 else "follower"
                        break
        else:
            health = html.ok("HEALTH_OK") if mon.status == "HEALTH_OK" else html.fail(mon.status)
            role = mon.role

        if mon.kb_avail is None:
            perc = "Unknown"
            sort_by = "0"
        else:
            perc = f"{b2ssize(mon.kb_avail * 1024)} ({mon.avail_percent})"
            sort_by = str(mon.kb_avail)

        table.add_cell(mon_link(mon.name).link)
        table.add_cell(health)
        table.add_cell(role)
        table.add_cell(perc, sorttable_customkey=sort_by)
        table.next_row()

    return table


@tab("Settings")
def show_primary_settings(ceph: CephInfo) -> html.HTMLTable:
    table = html.HTMLTable("table-settings", ["Name", "Value"])

    table.add_cell("<b>Common</b>", colspan="2")
    table.next_row()

    table.add_cells("Cluster net", str(ceph.cluster_net))
    table.add_cells("Public net", str(ceph.public_net))
    table.add_cells("Near full ratio", "%0.3f" % (float(ceph.settings.mon_osd_nearfull_ratio,)))

    if 'mon_osd_backfillfull_ratio' in ceph.settings:
        bfratio = f"{float(ceph.settings['mon_osd_backfillfull_ratio']):.3f}"
    elif 'osd_backfill_full_ratio' in ceph.settings:
        bfratio = f"{float(ceph.settings['osd_backfill_full_ratio']):.3f}"
    else:
        bfratio = '?'

    table.add_cells("Backfill full ratio", bfratio)
    table.add_cells("Full ratio", "%0.3f" % (float(ceph.settings.mon_osd_full_ratio,)))
    table.add_cells("Filesafe full ratio", "%0.3f" % (float(ceph.settings.osd_failsafe_full_ratio,)))

    def show_opt(name: str, tr_func: Callable[[str], str] = None):
        name_under = name.replace(" ", "_")
        if name_under in ceph.settings:
            vl = ceph.settings[name_under]
            if tr_func is not None:
                vl = tr_func(vl)
            table.add_cells(name.capitalize(), vl)

    table.add_cell("<b>Fail detection</b>", colspan="2")
    table.next_row()

    show_opt("mon osd down out interval", lambda x: seconds_to_str(int(x)))
    show_opt("mon osd adjust down out interval")
    show_opt("mon osd down out subtree limit")
    show_opt("mon osd report timeout", lambda x: seconds_to_str(int(x)))
    show_opt("mon osd min down reporters")
    show_opt("mon osd reporter subtree level")
    show_opt("osd heartbeat grace", lambda x: seconds_to_str(int(x)))

    table.add_cell("<b>Other</b>", colspan="2")
    table.next_row()

    show_opt("osd max object size", lambda x: b2ssize(int(x)) + "B")
    show_opt("osd mount options xfs")

    table.add_cell("<b>Scrub</b>", colspan="2")
    table.next_row()

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

    table.add_cell("<b>OSD io</b>", colspan="2")
    table.next_row()

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
        table.add_cell("<b>Bluestore</b>", colspan="2")
        table.next_row()

        show_opt("bluestore cache size hdd", lambda x: b2ssize(int(x)) + "B")
        show_opt("bluestore cache size ssd", lambda x: b2ssize(int(x)) + "B")
        show_opt("bluestore cache meta ratio", lambda x: f"{float(x):.3f}")
        show_opt("bluestore cache kv ratio", lambda x: f"{float(x):.3f}")
        show_opt("bluestore cache kv max", lambda x: b2ssize(int(x)) + "B")
        show_opt("bluestore csum type")

    if ceph.has_fs:
        table.add_cell("<b>Filestore</b>", colspan="2")
        table.next_row()
        table.add_cells("Journal aio", ceph.settings.journal_aio)
        table.add_cells("Journal dio", ceph.settings.journal_dio)
        table.add_cells("Filestorage sync", str(int(float(ceph.settings.filestore_max_sync_interval))) + 's')

    return table


@tab("Crush rulesets")
def show_ruleset_info(ceph: CephInfo) -> html.HTMLTable:

    class RulesetsTable(Table):
        rule = ident()
        id = exact_count()
        pools = idents_list()
        osd_class = ident()
        replication_level = ident("Replication<br>level")
        pg = exact_count("PG")
        pg_per_osd = exact_count("PG/OSD")
        num_osd = exact_count("# OSD")
        size = bytes_sz()
        free_size = to_str()
        data = bytes_sz()
        objs = count()
        data_disk_sizes = ident()
        disk_types = idents_list(delim='<br>')
        data_disk_models = idents_list()

    pools = {}
    for pool in ceph.pools.values():
        pools.setdefault(pool.crush_rule, []).append(pool)

    table = RulesetsTable()

    for rule in ceph.crush.rules.values():
        row = table.next_row()
        row.rule = rule.name
        row.id = rule.id
        row.pools = [pool_link(pool.name).link for pool in pools.get(rule.id, [])]

        if ceph.is_luminous:
            row.osd_class = '*' if rule.class_name is None else rule.class_name

        row.replication_level = rule.replicated_on
        osds = [ceph.osds[osd_node.id] for osd_node in ceph.crush.iter_osds_for_rule(rule.id)]
        row.num_osd = len(osds)
        total_sz = sum(osd.free_space + osd.used_space for osd in osds)
        row.size = total_sz
        total_free = sum(osd.free_space for osd in osds)
        row.free_size = f"{b2ssize(total_free)} ({total_free * 100 // total_sz}%)", total_free
        row.data = sum(pool.df.size_bytes for pool in pools.get(rule.id, []))
        row.objs = sum(pool.df.num_objects for pool in pools.get(rule.id, []))
        total_pg = sum(pool.pg for pool in pools.get(rule.id, []))
        row.pg = total_pg
        row.pg_per_osd = total_pg // len(osds)

        storage_disks_types = set()
        journal_disks_types = set()
        disks_sizes = set()
        disks_info = set()

        for osd in osds:
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

    return table.html(id="table-rules")


@tab("Cluster err/warn")
def show_cluster_err_warn(ceph: CephInfo) -> str:
    return "<br>".join(ceph.log_err_warn)

import collections
from typing import Dict, Optional, Union

import numpy

from koder_utils import b2ssize_10, b2ssize, Table, Column, fail, XMLNode, SimpleTable, RawContent, AnyXML
from cephlib import CephInfo

from .visualize_utils import tab, plot, perf_info_required, table_to_xml_doc
from .obj_links import pool_link, rule_link
from .plot_data import get_histo_img


def get_pools_load_table(ceph: CephInfo, uptime: bool) -> Table:
    class PoolLoadTable(Table):
        __html_classes__ = "table_lr"

        pool = Column.s("Name")
        data = Column.ed("Data TiB", help="Pool total data in Tebibytes")
        objs = Column.ed("MObj", help="Millions of objects in pool")
        total_data_per_pg = Column.ed("Data per PG<br>GiB", help="Average data per PG in Gibibytes")
        kobj_per_pg = Column.ed("Kobj per PG", help="Average object per PG in thousands")

        if not uptime:
            new_data = Column.ed("New data<br>MiBps", help="Customer new data rate in Mibibytes per second")
            new_objs = Column.ed("New objs<br>ps", help="Customer new objects per second")

        write_ops = Column.ed("Write<br>Mops" if uptime else "Write<br>IOPS",
                              help="Average data write speed in Mibibytes per second")
        write_b = Column.ed("Write TiB" if uptime else "Write<br>MiBps")
        write_per_pg = Column.ed("Write Kops<br>per PG" if uptime else "Writes<br>per PG")
        avg_write_size = Column.ed("Avg. write<br>size, KiB")

        read_ops = Column.ed("Read Mops" if uptime else "Read<br>IOPS")
        read_b = Column.ed("Read TiB" if uptime else "Read<br>MiBps")
        read_per_pg = Column.ed("Read Kops<br>per PG" if uptime else "Reads<br>per PG")
        avg_read_size = Column.ed("Avg. read<br>size, KiB")

    table = PoolLoadTable()

    for pool in ceph.pools.values():
        df = pool.df if uptime else pool.d_df

        row = table.next_row()
        row.pool = pool.name
        row.data = pool.df.size_bytes // 2 ** 40
        row.objs = pool.df.num_objects // 10 ** 6

        if not uptime:
            row.new_objs = pool.d_df.num_objects
            row.new_data = pool.d_df.size_bytes // 2 ** 20

        row.total_data_per_pg = pool.df.size_bytes // pool.pg // 2 ** 30
        row.kobj_per_pg = pool.df.num_objects // pool.pg // 1000

        sz_coef = 2 ** 40 if uptime else 2 ** 20
        ops_coef = 10 ** 6 if uptime else 1
        per_pg_coef = 1000 if uptime else 1

        row.write_ops = df.write_ops // ops_coef
        row.write_b = df.write_bytes // sz_coef
        row.write_per_pg = df.write_ops // pool.pg // per_pg_coef
        if df.write_ops > 10:
            row.avg_write_size = df.write_bytes // df.write_ops // 2 ** 10

        row.read_ops = df.read_ops // ops_coef
        row.read_b = df.read_bytes // sz_coef
        row.read_per_pg = df.read_ops // pool.pg // per_pg_coef
        if df.read_ops > 10:
            row.avg_read_size = df.read_bytes // df.read_ops // 2 ** 10

    return table


@tab("Pool's lifetime load")
def show_pools_lifetime_load(ceph: CephInfo) -> AnyXML:
    return table_to_xml_doc(get_pools_load_table(ceph, True), id="table-pools-io-uptime", zebra=True, sortable=True)


@tab("Pool's curr load")
@perf_info_required
def show_pools_curr_load(ceph: CephInfo) -> AnyXML:
    return table_to_xml_doc(get_pools_load_table(ceph, False), id="table-pools-io", zebra=True, sortable=True)


@tab("Pool's stats")
def show_pools_info(ceph: CephInfo) -> AnyXML:
    class PoolsTable(Table):
        __html_classes__ = "table_lr"
        pool = Column.s()
        id = Column.d()
        size = Column.s()
        obj = Column.s()
        bytes = Column.s()
        ruleset = Column.s()
        apps = Column.list(delim=", ")
        pg = Column.s()
        osds = Column.ed("OSD's")
        space = Column.sz("Total<br>OSD Space")
        avg_obj_size = Column.sz("Obj size")
        pg_recommended = Column.ed("PG<br>recc.")
        byte_per_pg = Column.sz("Bytes/PG")
        obj_per_pg = Column.d("Objs/PG")
        pg_count_deviation = Column.s("PG/OSD<br>Deviation")

    table = PoolsTable()

    total_objs = sum(pool.df.num_objects for pool in ceph.pools.values())

    osds_for_rule: Dict[int, int] = {}
    total_size_for_rule: Dict[int, int] = {}
    rules_names = [rule.rule_name for rule in ceph.crush.crushmap.rules]

    for _, pool in sorted(ceph.pools.items()):
        row = table.next_row()
        row.pool = pool_link(pool.name).link, pool.name
        row.id = pool.id

        vl: Union[RawContent, str] = f"{pool.size} / {pool.min_size}"
        if pool.name == 'gnocchi':
            if pool.size != 2 or pool.min_size != 1:
                vl = fail(str(vl))
        else:
            if pool.size != 3 or pool.min_size != 2:
                vl = fail(str(vl))
        row.size = vl, pool.size

        if total_objs:
            obj_perc = pool.df.num_objects * 100 // total_objs
            row.obj = f"{b2ssize_10(pool.df.num_objects)} ({obj_perc}%)", pool.df.num_objects
        else:
            row.obj = "-"

        if ceph.status.pgmap.data_bytes:
            bytes_perc = pool.df.size_bytes * 100 // ceph.status.pgmap.data_bytes
            row.bytes = f"{b2ssize(pool.df.size_bytes)} ({bytes_perc}%)", pool.df.size_bytes
        else:
            row.bytes = '-'

        rule_name = ceph.crush.rule_by_id(pool.crush_rule).rule_name
        cls_name = f"rule{rules_names.index(rule_name)}"
        row.ruleset = f'<div class="{cls_name}">{rule_link(rule_name, pool.crush_rule).link}</div>', pool.crush_rule

        if ceph.is_luminous:
            row.apps = pool.apps

        pg_perc = (pool.pg * 100) // ceph.status.pgmap.num_pgs

        if pool.pgp != pool.pg:
            pg_message = f"{pool.pg}, {XMLNode('font', text=str(pool.pgp), color='red')}"
        else:
            pg_message = str(pool.pg)

        row.pg = pg_message + f" ({pg_perc}%)", pool.pg

        if pool.crush_rule not in osds_for_rule:
            rule = ceph.crush.rule_by_id(pool.crush_rule)
            all_osd = list(ceph.crush.iter_osds_for_rule(rule))
            osds_for_rule[pool.crush_rule] = len(all_osd)
            total_size_for_rule[pool.crush_rule] = \
                sum(ceph.osds[osd_id].space.total for osd_id, _ in all_osd if ceph.osds[osd_id].space is not None)

        row.osds = osds_for_rule[pool.crush_rule]
        row.space = total_size_for_rule[pool.crush_rule]
        row.byte_per_pg = pool.df.size_bytes // pool.pg
        row.avg_obj_size = 0 if pool.df.num_objects == 0 else pool.df.size_bytes // pool.df.num_objects
        row.obj_per_pg = pool.df.num_objects // pool.pg

        if ceph.sum_per_osd is not None:
            osd_ids = [osd_id for osd_id, _ in ceph.crush.iter_osds_for_rule(ceph.crush.rule_by_id(pool.crush_rule))]
            osds_pgs = []

            for osd_id in osd_ids:
                osds_pgs.append(ceph.osd_pool_pg_2d.get(osd_id, {}).get(pool.name, 0))

            avg = float(sum(osds_pgs)) / len(osds_pgs)
            if len(osds_pgs) < 2:
                dev = None
            else:
                dev = (sum((i - avg) ** 2.0 for i in osds_pgs) / (len(osds_pgs) - 1)) ** 0.5

            if avg >= 1E-5:
                if dev is None:
                    dev_perc = "--"
                else:
                    dev_perc = str(int(dev * 100. / avg))

                row.pg_count_deviation = f"{avg:.1f} ~ {dev_perc}%", int(avg * 1000)

    return table_to_xml_doc(table, id="table-pools", sortable=True, zebra=True)


@tab("PG's status")
def show_pg_state(ceph: CephInfo) -> AnyXML:
    statuses: Dict[str, int] = collections.Counter()

    for pg_group in ceph.status.pgmap.pgs_by_state:
        for state_name in pg_group['state_name'].split('+'):
            statuses[state_name] += pg_group["count"]

    table = SimpleTable("Status", "Count", "%")
    table.add_row("any", str(ceph.status.pgmap.num_pgs), "100.00")
    for status, count in sorted(statuses.items()):
        table.add_row(status, str(count), f"{100.0 * count / ceph.status.pgmap.num_pgs:.2f}")

    return table_to_xml_doc(table, id="table-pgs", sortable=True, zebra=True)


@plot
@tab("PG's sizes histo")
def show_pg_size_kde(ceph: CephInfo) -> Optional[RawContent]:
    if ceph.pgs:
        vals = [pg.stat_sum.num_bytes / 2 ** 30 for pg in ceph.pgs.pg_stats]
        return RawContent(get_histo_img(numpy.array(vals), xlabel="PG size GiB", y_ticks=True, ylogscale=True))
    return None

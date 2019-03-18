import collections
from typing import Dict, Optional, Union

import numpy

from cephlib.units import b2ssize_10, b2ssize

from . import html
from .visualize_utils import tab, plot, table_id, perf_info_required
from .cluster_classes import CephInfo
from .obj_links import pool_link, rule_link
from .plot_data import get_histo_img
from .table import Table, count, bytes_sz, ident, idents_list, exact_count


def get_pools_load_table(ceph: CephInfo, uptime: bool) -> Table:
    class PoolLoadTable(Table):
        class html_params:
            align = html.TableAlign.left_right

        pool = ident("Name")
        data = exact_count("Data TiB", help="Pool total data in Tebibytes")
        objs = exact_count("MObj", help="Millions of objects in pool")
        total_data_per_pg = exact_count("Data per PG<br>GiB", help="Average data per PG in Gibibytes")
        kobj_per_pg = exact_count("Kobj per PG", help="Average object per PG in thousands")

        if not uptime:
             new_data = exact_count("New data<br>MiBps", help="Customer new data rate in Mibibytes per second")
             new_objs = exact_count("New objs<br>ps", help="Customer new objects per second")

        write_ops = exact_count("Write<br>Mops" if uptime else "Write<br>IOPS",
                                help="Average data write speed in Mibibytes per second")
        write_b = exact_count("Write TiB" if uptime else "Write<br>MiBps")
        write_per_pg = exact_count("Write Kops<br>per PG" if uptime else "Writes<br>per PG")
        avg_write_size = exact_count("Avg. write<br>size, KiB")

        read_ops = exact_count("Read Mops" if uptime else "Read<br>IOPS")
        read_b = exact_count("Read TiB" if uptime else "Read<br>MiBps")
        read_per_pg = exact_count("Read Kops<br>per PG" if uptime else "Reads<br>per PG")
        avg_read_size = exact_count("Avg. read<br>size, KiB")

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
@table_id("table-pools-io-uptime")
def show_pools_lifetime_load(ceph: CephInfo) -> Table:
    return get_pools_load_table(ceph, True)


@tab("Pool's curr load")
@table_id("table-pools-io")
@perf_info_required
def show_pools_curr_load(ceph: CephInfo) -> Table:
    return get_pools_load_table(ceph, False)


@tab("Pool's stats")
@table_id("table-pools")
def show_pools_info(ceph: CephInfo) -> Table:
    class PoolsTable(Table):
        class html_params:
            align = html.TableAlign.left_right
        pool = ident()
        id = count()
        size = ident()
        obj = ident()
        bytes = ident()
        ruleset = ident()
        apps = idents_list(delim=", ")
        pg = ident()
        osds = exact_count("OSD's")
        space = bytes_sz("Total<br>OSD Space")
        avg_obj_size = bytes_sz("Obj size")
        pg_recommended = exact_count("PG<br>recc.")
        byte_per_pg = bytes_sz("Bytes/PG")
        obj_per_pg = count("Objs/PG")
        pg_count_deviation = ident("PG/OSD<br>Deviation")

    table = PoolsTable()

    total_objs = sum(pool.df.num_objects for pool in ceph.pools.values())

    osds_for_rule: Dict[int, int] = {}
    total_size_for_rule: Dict[int, int] = {}
    rules_names = [rule.name for rule in ceph.crush.rules.values()]

    for _, pool in sorted(ceph.pools.items()):
        row = table.next_row()
        row.pool = pool_link(pool.name).link, pool.name
        row.id = pool.id

        vl: Union[html.TagProxy, str] = f"{pool.size} / {pool.min_size}"
        if pool.name == 'gnocchi':
            if pool.size != 2 or pool.min_size != 1:
                vl = html.fail(str(vl))
        else:
            if pool.size != 3 or pool.min_size != 2:
                vl = html.fail(str(vl))
        row.size = vl, pool.size

        if total_objs:
            obj_perc = pool.df.num_objects * 100 // total_objs
            row.obj = f"{b2ssize_10(pool.df.num_objects)} ({obj_perc}%)", pool.df.num_objects
        else:
            row.obj = "-"

        if ceph.status.data_bytes:
            bytes_perc = pool.df.size_bytes * 100 // ceph.status.data_bytes
            row.bytes = f"{b2ssize(pool.df.size_bytes)} ({bytes_perc}%)", pool.df.size_bytes
        else:
            row.bytes = '-'

        rule_name = ceph.crush.rules[pool.crush_rule].name
        cls_name = f"rule{rules_names.index(rule_name)}"
        row.ruleset = f'<div class="{cls_name}">{rule_link(rule_name, pool.crush_rule).link}</div>', pool.crush_rule

        if ceph.is_luminous:
            row.apps = pool.apps

        pg_perc = (pool.pg * 100) // ceph.status.num_pgs

        if pool.pgp != pool.pg:
            pg_message = f"{pool.pg}, {html.H.font(str(pool.pgp), color='red')}"
        else:
            pg_message = str(pool.pg)

        row.pg = pg_message + f" ({pg_perc}%)", pool.pg

        if pool.crush_rule not in osds_for_rule:
            rule = ceph.crush.rules[pool.crush_rule]
            all_osd = list(ceph.crush.iter_osds_for_rule(rule.id))
            osds_for_rule[pool.crush_rule] = len(all_osd)
            total_size_for_rule[pool.crush_rule] = \
                sum(ceph.osds[osd.id].total_space for osd in all_osd if ceph.osds[osd.id].total_space is not None)

        row.osds = osds_for_rule[pool.crush_rule]
        row.space = total_size_for_rule[pool.crush_rule]
        row.byte_per_pg = pool.df.size_bytes // pool.pg
        row.avg_obj_size = 0 if pool.df.num_objects == 0 else pool.df.size_bytes // pool.df.num_objects
        row.obj_per_pg = pool.df.num_objects // pool.pg

        if ceph.sum_per_osd is not None:
            osds = list(ceph.crush.iter_osds_for_rule(pool.crush_rule))
            osds_pgs = []

            for osd in osds:
                osds_pgs.append(ceph.osd_pool_pg_2d.get(osd.id, {}).get(pool.name, 0))

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

    return table


@tab("PG's status")
def show_pg_state(ceph: CephInfo) -> html.HTMLTable:
    statuses: Dict[str, int] = collections.Counter()

    for pg_group in ceph.status.pgmap_stat['pgs_by_state']:
        for state_name in pg_group['state_name'].split('+'):
            statuses[state_name] += pg_group["count"]

    table = html.HTMLTable("table-pgs", ["Status", "Count", "%"])
    table.add_row(["any", str(ceph.status.num_pgs), "100.00"])
    for status, count in sorted(statuses.items()):
        table.add_row([status, str(count), f"{100.0 * count / ceph.status.num_pgs:.2f}"])

    return table


@plot
@tab("PG's sizes histo")
def show_pg_size_kde(ceph: CephInfo) -> Optional[str]:
    if ceph.pgs:
        vals = [pg.stat_sum.num_bytes / 2 ** 30 for pg in ceph.pgs.pgs.values()]
        return get_histo_img(numpy.array(vals), xlabel="PG size GiB", y_ticks=True)
    return None
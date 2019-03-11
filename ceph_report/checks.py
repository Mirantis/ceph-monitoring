import collections
from enum import Enum
from typing import List, Dict, Callable, Any, Optional, Tuple
from dataclasses import dataclass

from .cluster_classes import Cluster, CephInfo, FileStoreInfo, BlueStoreInfo, DiskType
from cephlib.units import b2ssize

# Check
# mount options
# network speed
# mtu
# ceph settings
# rgw settings
# osd/mon/cluster log errors
# rgw/monitor collocation
# pools pg/load/data
# total cluster pg
# services cpu/ram consumption
# BS cache settings
# kernel settings
# thread count/net connection count
# deep scrub time
# pg io bottleneck
# radosgw performance metrics!
# cpu/disk/net per user request

# osd_class_update_on_start = true
# osd_crush_update_on_start = true
# osd_crush_update_weight_set = true
# add check that osd get to at least one root
# add check that osd located in correct host in crush
# for bluestore check that cache ram works correctly
# check for unused devices on osd node, the same as used for ceph
# check that all osd of same crush node has the same ip in osdmap
# check gaps in osd map
# network/storage devices with no load


class Severity(Enum):
    ok = 0
    notice = 1
    warning = 2
    error = 3
    critical = 4


class ServiceType(Enum):
    osd = 0
    host = 1
    pool = 2
    cluster = 3


@dataclass
class ErrTarget:
    name: str
    type: ServiceType

    def __str__(self) -> str:
        if self.type == ServiceType.cluster:
            return "Cluster"
        else:
            return {ServiceType.host: "Host {}",
                    ServiceType.osd: "osd.{}",
                    ServiceType.pool: "Pool {}"}[self.type].format(self.name)


@dataclass
class CheckMessage:
    reporter_id: str
    severity: Severity
    message: str
    affected_service: ErrTarget


@dataclass
class CheckResult:
    reporter_id: str
    severity: Severity
    check_description: str
    passed: bool
    message: Optional[str]
    readmeurls: List[str]
    fails: List[CheckMessage]


def osd_t(osd_id: int) -> ErrTarget:
    return ErrTarget(str(osd_id), ServiceType.osd)


def host_t(hostname: str) -> ErrTarget:
    return ErrTarget(hostname, ServiceType.host)


def pool_t(name: str) -> ErrTarget:
    return ErrTarget(name, ServiceType.pool)


cluster_t = ErrTarget('', ServiceType.cluster)


class CheckReport:
    def __init__(self) -> None:
        self.results: List[CheckResult] = []
        self.curr_reporter: Any = None
        self.curr_extra: List[CheckMessage] = []

    def set_curr(self, reporter: Any):
        self.curr_reporter = reporter

    def add_result(self, passed: bool, message: str = None):
        self.results.append(CheckResult(self.curr_reporter.__name__,
                                        self.curr_reporter.severity,
                                        self.curr_reporter.description,
                                        passed,
                                        message,
                                        self.curr_reporter.readmeurls,
                                        self.curr_extra))
        self.curr_extra = []

    def add_extra_message(self, sev: Severity, message: str, affected_service: ErrTarget = cluster_t):
        self.curr_extra.append(CheckMessage(self.curr_reporter.__name__, sev, message, affected_service))


CheckConfig = Dict[str, Any]
CheckerFunc = Callable[[CheckConfig, Cluster, CephInfo, CheckReport], None]

ALL_CHECKS = []


def checker(severity: Severity, description: str, *readmeurls: str) -> Callable[[CheckerFunc], CheckerFunc]:
    def closure(func: CheckerFunc) -> CheckerFunc:
        func.severity = severity  # type: ignore
        func.description = description  # type: ignore
        func.readmeurls = readmeurls  # type: ignore
        ALL_CHECKS.append(func)
        return func
    return closure


@checker(Severity.warning, "Scrub errors count")
def no_scrub_errors(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    total_scrub_err = 0
    err_per_osd: Dict[int, int] = {}
    for osd in ceph.sorted_osds:
        assert osd.pg_stats
        err_per_osd[osd.id] = osd.pg_stats.scrub_errors + osd.pg_stats.deep_scrub_errors + \
            osd.pg_stats.shallow_scrub_errors

    for osd_id, err_count in sorted(err_per_osd.items()):
        if err_count > 0:
            report.add_extra_message(Severity.warning, f"{err_count} scrub errors", osd_t(osd_id))

    passed = total_scrub_err <= config.get('max_scrub_errors', 0)
    report.add_result(passed, "" if passed else f"Error count {total_scrub_err}")


@checker(Severity.warning, "SSD/NVME used for journals/wal/db")
def ssd_nvme_journals(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    count = 0
    for osd in ceph.sorted_osds:
        if isinstance(osd.storage_info, FileStoreInfo):
            j_classes = config.get('classes_for_j', ["sata_ssd", "sas-ssd", "nvme"])
            j_disk_type = osd.storage_info.journal.dev_info.tp.name
            if j_disk_type not in j_classes:
                report.add_extra_message(Severity.warning,
                    f"uses wrong disk type {j_disk_type} for journal. One of {j_classes} required",
                    osd_t(osd.id))
                count += 1
        else:
            assert isinstance(osd.storage_info, BlueStoreInfo)

            wal_classes = config.get('classes_for_wal', ["sata_ssd", "sas-ssd", "nvme"])
            wal_disk_type = osd.storage_info.wal.dev_info.tp.name
            if wal_disk_type not in wal_classes:
                report.add_extra_message(Severity.warning,
                    f"uses wrong disk type {wal_disk_type} for wal. One of {wal_classes} required",
                    osd_t(osd.id))

            db_classes = config.get('classes_for_db', ["sata_ssd", "sas-ssd", "nvme"])
            db_disk_type = osd.storage_info.db.dev_info.tp.name
            if db_disk_type not in db_classes:
                report.add_extra_message(Severity.warning,
                    f"uses wrong disk type {db_disk_type} for db. One of {db_classes} required",
                    osd_t(osd.id))

            if db_disk_type not in db_classes or wal_disk_type not in wal_classes:
                count += 1

    report.add_result(count == 0, "" if count == 0 else f"{count} OSD's uses slow disks for journal/wal/db")


@checker(Severity.warning, "Separated client/cluster ceph networks")
def separated_ceph_nets(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    if config.get('separated_nets', True):
        if ceph.cluster_net == ceph.public_net:
            msg = f"Same net {ceph.public_net} is used for both ceph cluster and public"
            report.add_extra_message(Severity.warning, msg)
            report.add_result(False, msg)
        else:
            report.add_result(True, "")


@checker(Severity.warning, "Max journal per device")
def max_journal_per_device(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    host_dev_j_count: Dict[str, int] = collections.Counter()

    duuid = lambda node, dev_name: f"{node.name}::{dev_name}"

    for osd in ceph.sorted_osds:
        if isinstance(osd.storage_info, FileStoreInfo):
            host_dev_j_count[duuid(osd.host, osd.storage_info.journal.name)] += 1
        else:
            assert isinstance(osd.storage_info, BlueStoreInfo)
            host_dev_j_count[duuid(osd.host, osd.storage_info.wal.name)] += 1
            if osd.storage_info.db.name != osd.storage_info.wal.name:
                host_dev_j_count[duuid(osd.host, osd.storage_info.db.name)] += 1

    fcount = 0

    defaults = {
        "virtio": 1,
        "unknown": 1,
        "sata_hdd": 1,
        "sas_hdd": 1,
        "sata_ssd": 6,
        "sas_ssd": 6,
        "nvme": 8
    }

    for host in cluster.hosts.values():
        for disk in host.disks.values():
            count = host_dev_j_count[duuid(host, disk.name)]
            max_journals = config.get('max_journals_per_class', {}).get(disk.tp.name, defaults[disk.tp.name])

            if count > max_journals:
                fcount += 1
                report.add_extra_message(Severity.warning,
                    f"Device {disk.name} has {count} journals, max {max_journals} recommended",
                    host_t(host.name))

    report.add_result(fcount == 0, "" if fcount == 0 else f"{fcount} devices have too may journals")


@checker(Severity.warning, "PG for OSD mast be in configured range")
def pg_per_osd_count(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    min_pg = config.get("min_pg_per_osd", 100)
    max_pg = config.get("max_pg_per_osd", 400)

    assert min_pg < max_pg

    wrong_pg_count = 0
    for osd in ceph.sorted_osds:
        if osd.pg_count < min_pg or osd.pg_count > max_pg:
            wrong_pg_count += 1
            if osd.pg_count > max_pg:
                range_lower = (osd.pg_count - max_pg) // 50 * 50 + max_pg
                range_upper = range_lower + 50
            else:
                range_upper = min_pg - (min_pg - osd.pg_count) // 20 * 20
                range_lower = max(range_upper - 20, 0)

            report.add_extra_message(Severity.warning,
                f"wrong PG count in range [{range_lower}, {range_upper}). " + \
                f"Recommended to be in range [{min_pg}, {max_pg}]",
                osd_t(osd.id))

    report.add_result(wrong_pg_count == 0, "" if wrong_pg_count == 0 else f"{wrong_pg_count} osd's has wrong PG count")


@checker(Severity.warning, "Pool size/min_size")
def pool_size_minsize(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    pool_size = tuple(map(int, config.get('pool_size', "3/2").split("/")))
    gnocci_pool_size = tuple(map(int, config.get('gnocci_pool_size', "2/1").split("/")))

    wrong_size = 0
    for name, pool in sorted(ceph.pools.items()):
        expected_size = gnocci_pool_size if name == 'gnocci' else pool_size
        if (pool.size, pool.min_size) != expected_size:
            wrong_size += 1
            report.add_extra_message(Severity.warning,
                f"Pool {pool.name} size={pool.size}, min_size={pool.min_size}, " +
                f"must be {expected_size[0]}/{expected_size[1]} respectivelly")

    report.add_result(wrong_size == 0, "" if wrong_size == 0 else f"{wrong_size} pools has wrong sizes")


@checker(Severity.warning, "Pool pg must be equals to pgp")
def pg_eq_pgp(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    if not config.get('pg_eq_pgp', True):
        return

    pg_ne_pgp = 0
    for _, pool in sorted(ceph.pools.items()):
        if pool.pg != pool.pgp:
            pg_ne_pgp += 1
            report.add_extra_message(Severity.warning, f"Pool {pool.name} pg({pool.pg}) != pgp(pool.pgp)")

    report.add_result(pg_ne_pgp == 0, "" if pg_ne_pgp == 0 else f"{pg_ne_pgp} pools has pg != pgp")


@checker(Severity.warning, "Small pools should not have lots PG")
def pg_in_empty_pools(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    sett = config.get("almost_empty_pool", {})
    min_cluster_data = sett.get("min_total_GiB", 100) * 1024 ** 3
    if ceph.status.data_bytes < min_cluster_data:
        return

    data_part = sett.get("data_part", 0.0001)
    max_bytes = ceph.status.data_bytes * data_part
    max_pg = sett.get("max_pg", 128)
    max_pg_per_osd = sett.get("max_pg_per_osd", 1)
    max_pg_for_empty_pool = max([max_pg, len(ceph.osds) * max_pg_per_osd])
    max_objs = sum(pool.df.num_object_copies for pool in ceph.pools.values()) * data_part

    failed_pools = []
    too_many_pg = False
    for name, pool in sorted(ceph.pools.items()):
        if pool.pg > max_pg_for_empty_pool:
            if pool.df.size_bytes * pool.size < max_bytes and pool.df.num_object_copies < max_objs:
                too_many_pg = True
                report.add_extra_message(Severity.warning, f"very small " +
                    f"(total_data={pool.df.size_bytes * pool.size}, total_obj_copies={pool.df.num_object_copies}, " +
                    f"less then {data_part} from total) but has to many PG:" +
                    f" {pool.pg} > {max_pg_for_empty_pool}",
                    pool_t(pool.name))
                failed_pools.append(name)

    report.add_result(not too_many_pg, f"Pools {', '.join(failed_pools)} has too many pg" if too_many_pg else "")


@checker(Severity.warning, "Large pools should have PG corresponding to their size")
def large_pools_pg(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    sett = config.get("large_pools", {})
    min_cluster_data = sett.get("min_total_GiB", 100) * 1024 ** 3

    if ceph.status.data_bytes < min_cluster_data:
        return

    min_large_pool_data = sett.get("large_pool_data_part", 0.05) * ceph.status.data_bytes
    min_size_diff = sett.get("min_size_diff", 1.5)

    pgs_and_data = []
    for name, pool in sorted(ceph.pools.items()):
        if pool.df.size_bytes * pool.size > min_large_pool_data:
            pgs_and_data.append((pool.name, pool.df.size_bytes, pool.pg))

    smaller_pools = []
    pg_count_messed = False
    if len(pgs_and_data) >= 2:
        pg_seq = sorted(pgs_and_data)
        for (sm_name, sm_data, sm_pg), (lg_name, lg_data, lg_pg) in zip(pg_seq[1:], pg_seq[:-1]):
            if sm_data * min_size_diff < lg_data and sm_pg > lg_pg:
                pg_count_messed = True
                report.add_extra_message(Severity.warning,
                    f"Has subsantially less data then {lg_name} ({min_size_diff:.1f})" +
                    f" but more PG {sm_pg} > {lg_pg}", pool_t(sm_name))
                smaller_pools.append(sm_name)
    report.add_result(not pg_count_messed,
                      f"Smaller pools({', '.join(smaller_pools)}) has more PG" if pg_count_messed else "")


@checker(Severity.warning, "All crush roots on replication level has >= 3 nodes, and same size in case if 3")
def replication_level_same_size(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    if not config.get('replication_level_same_size', True):
        return

    passed = True
    for root in ceph.crush.roots:
        childs = root.childs
        if len(childs) == 3:
            if abs(childs[0].weight - childs[1].weight) > 1 or abs(childs[2].weight - childs[1].weight) > 1:
                report.add_extra_message(Severity.warning, f"root {root.name} has 3 top childs with different weights")
                passed = False
        if len(childs) < 3:
            report.add_extra_message(Severity.warning, f"root {root.name} has less then 3 top childs")
            passed = False
    report.add_result(passed, "" if passed else "Incorrect tol level childs count of weights")


@checker(Severity.warning, "All OSD's under the same root has same storage types and almost the same size")
def all_osds_in_root_the_same(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):

    sett = config.get("osd_under_same_root", {})
    max_size_diff = sett.get("max_size_diff", 0.01)
    all_eq_types = sett.get("all_eq_types", True)

    failed_size = False
    failed_type = False

    for rule in ceph.crush.rules.values():
        osds = list(ceph.crush.iter_osds_for_rule(rule.id))

        sizes: Dict[int, int] = collections.Counter()
        types: Dict[DiskType, int] = collections.Counter()

        for osd_node in osds:
            osd = ceph.osds[osd_node.id]
            assert osd.storage_info
            sizes[osd.storage_info.data.partition_info.size] += 1
            types[osd.storage_info.data.dev_info.tp] += 1

        most_often_size = sorted((count, sz) for sz, count in sizes.items())[0][1]
        most_often_type = sorted((count, tp) for tp, count in types.items())[0][1]

        for osd_node in osds:
            osd = ceph.osds[osd_node.id]
            assert osd.storage_info


            sz = osd.storage_info.data.partition_info.size

            if abs(sz - most_often_size) / max([sz, most_often_size, 1]) > max_size_diff:
                report.add_extra_message(Severity.warning,
                    f"size {b2ssize(sz)} different " +
                    f"from most often one {b2ssize(most_often_size)} for root {rule.name}",
                    osd_t(osd.id))
                failed_size = True

            if all_eq_types:
                tp = osd.storage_info.data.dev_info.tp
                if tp != most_often_type:
                    report.add_extra_message(Severity.warning,
                        f"disk type {tp.name} different " +
                        f"from most often one {most_often_type.name} for root {rule.name}",
                        osd_t(osd.id))
                    failed_type = True

    if all_eq_types:
        report.add_result(not failed_type, "Not all OSD's for same root has same drive types" if failed_type else "")

    report.add_result(not failed_size, "Not all OSD's for same root has same drive size" if failed_size else "")


@checker(Severity.warning, "OSD's with same size have close usage")
def osd_of_same_size_has_close_data_size(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    data_per_osd: Dict[int, List[float]] = collections.defaultdict(list)
    for osd in ceph.osds.values():
        total_space = osd.used_space + osd.free_space
        if total_space != 0:
            data_per_osd[total_space].append(osd.free_space / total_space)

    failed = False
    max_used_space_diff = config.get("max_used_space_diff", 0.2)
    for sz, free in data_per_osd.items():
        if abs(max(free) - min(free)) > max_used_space_diff:
            report.add_extra_message(Severity.warning,
                f"Min usage for osd's of size {b2ssize(sz)} is {min(free):.2f}, max is {min(free):.2f}, " +
                f"which is greater then allowed {max_used_space_diff}")
            failed = True

    report.add_result(not failed, "Too large dispersion in usage of osd's with same sizes" if failed else "")


@checker(Severity.warning, "OSD of the same type load evenly distributed")
def osd_of_same_size_has_close_load(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    io_per_osd: Dict[str, Dict[int, List[Tuple[int, int]]]] = \
        collections.defaultdict(lambda: collections.defaultdict(list))
    for osd in ceph.osds.values():
        for attr in ("reads", "writes", "read_b", "write_b"):
            io_per_osd[attr][osd.total_space].append((getattr(osd.pg_stats, attr), osd.id))

    sett = config.get("max_load_diff", {})
    min_ops = sett.get("min_mops", 1) * 1000000
    min_bytes = sett.get("min_GiB", 100) * 1024 ** 3
    max_diff = sett.get("max_diff", 0.2)
    failed = False
    for name, dct in io_per_osd.items():
        for sz, ops in dct.items():
            max_osd_ops, osd_max_id = max(ops)
            min_osd_ops, osd_min_id = min(ops)
            if name in ('reads', 'writes') and max_osd_ops < min_ops:
                continue
            if name in ('read_b', 'write_b') and min_osd_ops < min_bytes:
                continue

            diff = abs((max_osd_ops - min_osd_ops) / max_osd_ops)

            if diff > max_diff:
                report.add_extra_message(Severity.warning,
                    f"OSD's of size {b2ssize(sz)} has too large deviation in {name} load - " +
                    f"{diff:.2f} between most({osd_max_id}) and least({osd_min_id}) loaded OSD's, " +
                    f"which is greter then max allowed {max_diff}")
                failed = True

    report.add_result(not failed, "Too large dispersion in load of osd's with same sizes" if failed else "")


expected_wr_speed = {
    DiskType.sata_hdd: 50,
    DiskType.sas_hdd: 75,
    DiskType.nvme: 200,
    DiskType.sata_ssd: 100,
    DiskType.sas_ssd: 100,
    DiskType.virtio: 50,
    DiskType.unknown: 50
}


@checker(Severity.warning, "OSD has enought journal/db/wal")
def osd_has_enought_journal_db(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    bad_size_count = 0

    wal_part_min = config.get("wal_part_min", 0.005)
    db_part_min = config.get("db_part_min", 0.05)

    for osd in ceph.sorted_osds:
        sinfo = osd.storage_info
        if isinstance(sinfo, FileStoreInfo):
            sync = float(osd.config['filestore_max_sync_interval']) if osd.run_info else 5
            min_j_size = expected_wr_speed[sinfo.data.dev_info.tp] * 2 * sync

            if sinfo.journal.partition_info.size / 2 ** 20 < min_j_size:
                report.add_extra_message(Severity.warning,
                    f"too small journal - " +
                    f"{b2ssize(sinfo.journal.partition_info.size)}, min required is {min_j_size}",
                    osd_t(osd.id))
                bad_size_count += 1
        else:
            assert isinstance(sinfo, BlueStoreInfo)
            data_part_size = sinfo.data.partition_info.size
            wal_part_size = sinfo.wal.partition_info.size

            min_wal_size =  data_part_size * wal_part_min
            min_db_size = data_part_size * db_part_min

            this_failed = False

            if sinfo.wal.partition_name == sinfo.db.partition_name:
                if wal_part_size < min_wal_size + min_db_size:
                    this_failed = True
                    report.add_extra_message(Severity.warning,
                        f"too small wal+db partition - {b2ssize(wal_part_size)}, " +
                        f"min required is {b2ssize(min_wal_size + min_db_size)}",
                        osd_t(osd.id))
            else:
                if wal_part_size < min_wal_size:
                    this_failed = True
                    report.add_extra_message(Severity.warning,
                        f"too small wal partition - {b2ssize(wal_part_size)}, " +
                        f"min required is {b2ssize(min_wal_size)}",
                        osd_t(osd.id))

                db_part_size = sinfo.db.partition_info.size
                if db_part_size < min_db_size:
                    this_failed = True
                    report.add_extra_message(Severity.warning,
                        f"too small db partition - {b2ssize(db_part_size)}, " +
                        f"min required is {b2ssize(min_db_size)}",
                        osd_t(osd.id))

            if this_failed:
                bad_size_count += 1

    report.add_result(bad_size_count == 0, f"Journal/DB/wal has not enough space on {bad_size_count} OSD's"
                                           if bad_size_count != 0 else "")


@checker(Severity.warning, "Disks schedulers")
def storage_devs_schedulers(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    allowed_schedulers_dct = config.get("allowed_schedulers", {})
    err_count = 0
    for node in cluster.hosts.values():
        for name, disk in node.disks.items():
            allowed_schedulers = allowed_schedulers_dct.get(disk.tp.name, ['cfg', 'noop', 'deadline', None])
            if disk.scheduler not in allowed_schedulers:
                err_count += 1
                report.add_extra_message(Severity.warning,
                    f"Disk {disk.name} has type {disk.tp.name} and scheduler {disk.scheduler}. " +
                    f"Only {', '.join(allowed_schedulers)} schedulers recommended for this class",
                    host_t(node.name))

    report.add_result(err_count == 0,
                      "" if err_count == 0 else f"{err_count} disks has non-optimal schedulers")


@checker(Severity.warning, "Ceph networks should have jumbo frames enabled")
def network_mtu(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    if not config.get("jumbo_frames_required", True):
        return

    min_j_size = config.get("min_jumbo_size", 9000)
    fails = 0

    for host in cluster.hosts.values():
        cl = host.find_interface(ceph.cluster_net)
        pb = host.find_interface(ceph.public_net)

        all = []
        if cl:
            all.append(cl)
        if pb and pb is not cl:
            all.append(pb)

        for adapter in all:
            tp = 'cluster' if adapter is cl else 'public'
            parent_name = None
            if adapter.mtu < min_j_size:
                report.add_extra_message(Severity.warning,
                    f"Network device {adapter.dev} responcible for {tp} ceph net " +
                    f"has MTU {adapter.mtu} < required {min_j_size}", host_t(host.name))
                fails += 1
            elif '.' in adapter.dev:
                parent_name = adapter.dev.split(".")[0]
                mtu = host.net_adapters[parent_name].mtu
                if mtu < min_j_size:
                    report.add_extra_message(Severity.warning,
                        f"Network device {parent_name}, responcible for {tp} ceph net, " +
                        f"which is parent for {adapter.dev} " +
                        f"has MTU {mtu} < required {min_j_size}", host_t(host.name))
                    fails += 1

            if parent_name in host.bonds:
                sources = host.bonds[parent_name].sources
            elif adapter.dev in host.bonds:
                sources = host.bonds[adapter.dev].sources
            else:
                sources = []

            for src in sources:
                mtu = host.net_adapters[src].mtu
                if mtu < min_j_size:
                    report.add_extra_message(Severity.warning,
                        f"Network device {src}, which is source for ceph bond, " +
                        f"responcible for {tp} ceph net " +
                        f"has MTU {mtu} < required {min_j_size}",
                        host_t(host.name))
                    fails += 1

    report.add_result(fails == 0,
                      "" if fails == 0 else f"{fails} net adapters has too small MTU")


@checker(Severity.warning, "Filestore sync interval")
def check_filestore_sync_interval(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    if ceph.has_fs:
        cfg = config.get('fs_sync_interval', {})
        max_i = cfg.get("max", 30)
        min_i = cfg.get("min", 10)
        real_i = float(ceph.settings.filestore_max_sync_interval)
        if real_i < min_i or real_i > max_i:
            msg = f"Filestore sync interval == {real_i} is not in recommended range [{min_i}, {max_i}]"
            report.add_extra_message(Severity.warning, msg)
            report.add_result(False, msg)
        else:
            report.add_result(True, "")


@checker(Severity.warning, "Network errors")
def check_ceph_net_errors(config: CheckConfig, cluster: Cluster, ceph: CephInfo, report: CheckReport):
    cfg = config.get('net_issues', {})
    lost_max = cfg.get("lost_max_prob", 0.0001)
    no_budget_max_per_hour = cfg.get("no_budget_max_per_hour", 10)
    max_packets_per_core_diff = cfg.get("max_packets_per_core_diff", 2)
    failed_hosts = set()
    for host in cluster.hosts.values():
        failed = set()
        for adapter in host.net_adapters.values():
            usage = adapter.d_usage if adapter.d_usage else adapter.usage

            if usage.rerrs + usage.rdrop > usage.recv_packets * lost_max:
                failed.add(adapter.dev)
                continue

            if usage.serrs + usage.sdrop > usage.send_packets * lost_max:
                failed.add(adapter.dev)
                continue

        if failed:
            report.add_extra_message(Severity.warning,
                                     f"interfaces {', '.join(sorted(failed))} has to hight error rate",
                                     host_t(host.name))
            failed_hosts.add(host.name)

        netstat = host.d_netstat if host.d_netstat else host.netstat

        if netstat and netstat.processed_v.max() / (netstat.processed_v.min() + 1) > max_packets_per_core_diff:
            report.add_extra_message(Severity.warning,
                                     f"Too high difference (> {max_packets_per_core_diff}) for packets processed " +
                                     f"on different cores",
                                     host_t(host.name))
            failed_hosts.add(host.name)

        dtime = cluster.dtime if host.d_netstat else host.uptime
        assert dtime is not None and dtime > 1
        if netstat and netstat.no_budget / dtime / 3600 > no_budget_max_per_hour:
            report.add_extra_message(Severity.warning,
                                     f"Too high out-of-net-budget case >{no_budget_max_per_hour} per hour",
                                     host_t(host.name))
            failed_hosts.add(host.name)

    if failed_hosts:
        report.add_result(False, f"Some network errors were found on hosts {', '.join(failed_hosts)}")
    else:
        report.add_result(True, "")


def run_all_checks(config: CheckConfig, cluster: Cluster, ceph: CephInfo) -> List[CheckResult]:

    report = CheckReport()

    for check in ALL_CHECKS:
        report.set_curr(check)
        check(config, cluster, ceph, report)

    return report.results


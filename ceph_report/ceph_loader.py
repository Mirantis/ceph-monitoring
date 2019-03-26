import re
import json
import logging
import datetime
import collections
from enum import Enum
from ipaddress import IPv4Network
from typing import Dict, Any, List, Union, Tuple, Optional, Set

import numpy
import dataclasses

from cephlib.crush import load_crushmap_js, Crush
from cephlib.storage import TypedStorage
from cephlib.common import AttredDict

from .visualize_utils import StopError
from .cluster_classes import (PGDump, PG, PGStatSum, PGId, PGState, StatusRegion,
                              Pool, PoolDF, CephVersion, CephMonitor, MonRole, CephStatus, CephStatusCode,
                              Host, CephInfo, CephOSD, OSDStatus, CephVersions, FileStoreInfo, BlueStoreInfo,
                              OSDProcessInfo, CephDevInfo, OSDPGStats)
from .collect_info import parse_proc_file


logger = logging.getLogger("cephlib.parse")


NO_VALUE = -1


def mem2bytes(vl: str) -> int:
    vl_sz, units = vl.split()
    assert units == 'kB'
    return int(vl_sz) * 1024


def parse_txt_ceph_config(data: str) -> Dict[str, str]:
    config = {}
    for line in data.strip().split("\n"):
        name, val = line.split("=", 1)
        config[name.strip()] = val.strip()
    return config


def parse_pg_dump(data: Dict[str, Any]) -> PGDump:
    pgs: List[PG] = []

    def pgid_conv(vl_any):
        pool_s, pg_s = vl_any.split(".")
        return PGId(pool=int(pool_s), num=int(pg_s, 16), id=vl_any)

    def datetime_conv(vl_any):
        # datetime.datetime.strptime is too slow
        vl_s, mks = vl_any.split(".")
        ymd, hms = vl_s.split()
        return datetime.datetime(*map(int, ymd.split("-")+ hms.split(":")), int(mks))

    def process_status(status: str) -> Set[PGState]:
        try:
            return {getattr(PGState, status.replace("-", "_")) for status in status.split("+")}
        except AttributeError:
            raise ValueError(f"Unknown status {status}")

    name_map = {
        "stat_sum": lambda x: PGStatSum(**x),
        "state": process_status,
        "pgid": pgid_conv
    }

    for field in dataclasses.fields(PG):
        if field.name not in name_map:
            if field.type is datetime.datetime:
                name_map[field.name] = datetime_conv

    class TabulaRasa:
        pass

    for pg_info in data['pg_stats']:
        # hack to optimize load speed
        dt = {k: (name_map[k](v) if k in name_map else v) for k, v in pg_info.items()}
        tr = TabulaRasa()
        tr.__dict__ = dt
        tr.__class__ = PG
        pgs.append(tr)  # type: ignore

    datetm, mks = data['stamp'].split(".")
    collected_at = datetime.datetime.strptime(datetm, '%Y-%m-%d %H:%M:%S').replace(microsecond=int(mks))
    return PGDump(collected_at=collected_at,
                  pgs={pg.pgid.id: pg for pg in pgs},
                  version=data['version'])


def avg_counters(counts: List[int], values: List[float]) -> numpy.ndarray:
    counts_a = numpy.array(counts, dtype=numpy.float32)
    values_a = numpy.array(values, dtype=numpy.float32)

    with numpy.errstate(divide='ignore', invalid='ignore'):  # type: ignore
        avg_vals = (values_a[1:] - values_a[:-1]) / (counts_a[1:] - counts_a[:-1])

    avg_vals[avg_vals == numpy.inf] = NO_VALUE
    avg_vals[numpy.isnan(avg_vals)] = NO_VALUE  # type: ignore

    return avg_vals  # type: ignore


def load_PG_distribution(pools: Dict[str, Pool], pg_dump: Dict[str, Any]) -> Tuple[Dict[int, Dict[str, int]],
                                                                                   Dict[str, int],
                                                                                   Dict[int, int]]:

    pool_id2name = {pool.id: pool.name for pool in pools.values()}
    osd_pool_pg_2d: Dict[int, Dict[str, int]] = collections.defaultdict(lambda: collections.Counter())
    sum_per_pool: Dict[str, int] = collections.Counter()
    sum_per_osd: Dict[int, int] = collections.Counter()

    if pg_dump:
        for pg in pg_dump['pg_stats']:
            pool_id = int(pg['pgid'].split('.', 1)[0])
            for osd_id in pg['acting']:
                pool_name = pool_id2name[pool_id]
                osd_pool_pg_2d[osd_id][pool_name] += 1
                sum_per_pool[pool_name] += 1
                sum_per_osd[osd_id] += 1

    return {osd_id: dict(per_pool.items()) for osd_id, per_pool in osd_pool_pg_2d.items()}, \
            dict(sum_per_pool.items()), dict(sum_per_osd.items())


version_rr = re.compile(r'ceph version\s+(?P<version>\d+\.\d+\.\d+)(?P<extra>[^ ]*)\s+' +
                        r'[([](?P<hash>[^)\]]*?)[)\]]')


def parse_ceph_versions(data: str) -> Dict[str, CephVersion]:
    osd_ver_rr = re.compile(r"(osd\.\d+|mon\.[a-z0-9A-Z-]+):\s+{")
    vers: Dict[str, CephVersion] = {}
    for line in data.split("\n"):
        line = line.strip()
        if osd_ver_rr.match(line):
            name, data_js = line.split(":", 1)
            rr = version_rr.match(json.loads(data_js)["version"])
            if not rr:
                logger.error("Can't parse version %r from %r", json.loads(data_js)["version"], line)
                raise StopError()
            major, minor, bugfix = map(int, rr.group("version").split("."))
            vers[name.strip()] = CephVersion(major, minor, bugfix,
                                             extra=rr.group("extra"),
                                             commit_hash=rr.group("hash"))
    return vers


def load_monitors(storage: TypedStorage, ver: CephVersions, hosts: Dict[str, Host]) -> Dict[str, CephMonitor]:
    if ver >= CephVersions.luminous:
        mons = storage.json.master.status['monmap']['mons']
    else:
        srv_health = storage.json.master.status['health']['health']['health_services']
        assert len(srv_health) == 1
        mons = srv_health[0]['mons']

    vers = parse_ceph_versions(storage.txt.master.mon_versions)
    result: Dict[str, CephMonitor] = {}
    for srv in mons:
        mon = CephMonitor(name=srv["name"],
                          status=None if ver >= CephVersions.luminous else srv["health"],
                          host=hosts[srv["name"]],
                          role=MonRole.unknown,
                          version=vers.get("mon." + srv["name"]))

        if ver < CephVersions.luminous:
            mon.kb_avail = srv["kb_avail"]
            mon.avail_percent = srv["avail_percent"]
        else:
            mon_db_root = "/var/lib/ceph/mon"
            root = ""
            for info in mon.host.logic_block_devs.values():
                if info.mountpoint is not None:
                    if mon_db_root.startswith(info.mountpoint) and len(root) < len(info.mountpoint):
                        assert info.free_space is not None
                        mon.kb_avail = info.free_space
                        mon.avail_percent = info.free_space * 1024 * 100 // info.size
                        root = info.mountpoint

            try:
                ceph_var_dirs_size = storage.txt.mon[f'{srv["name"]}/ceph_var_dirs_size']
            except KeyError:
                pass
            else:
                for ln in ceph_var_dirs_size.strip().split("\n"):
                    sz, name = ln.split()
                    if '/var/lib/ceph/mon' == name:
                        mon.database_size = int(sz)

        result[mon.name] = mon
    return result


def load_ceph_status(storage: TypedStorage, ver: CephVersions) -> CephStatus:
    mstorage = storage.json.master
    pgmap = mstorage.status['pgmap']
    health = mstorage.status['health']
    status = health['status'] if 'status' in health else health['overall_status']

    return CephStatus(status=CephStatusCode.from_str(status),
                      health_summary=health['checks' if ver >= CephVersions.luminous else 'summary'],
                      num_pgs=pgmap['num_pgs'],
                      bytes_used=pgmap["bytes_used"],
                      bytes_total=pgmap["bytes_total"],
                      bytes_avail=pgmap["bytes_avail"],
                      data_bytes=pgmap["data_bytes"],
                      pgmap_stat=pgmap,
                      monmap_stat=mstorage.status['monmap'],
                      write_bytes_sec=pgmap.get("write_bytes_sec", 0),
                      write_op_per_sec=pgmap.get("write_op_per_sec", 0),
                      read_bytes_sec=pgmap.get("read_bytes_sec", 0),
                      read_op_per_sec=pgmap.get("read_op_per_sec", 0))


class OSDStoreType(Enum):
    filestore = 0
    bluestore = 1
    unknown = 2


# def load_osd_perf_data(osd_id: int,
#                        storage_type: OSDStoreType,
#                        osd_perf_dump: Dict[int, List[Dict]]) -> Dict[str, numpy.ndarray]:
#     osd_perf_dump = osd_perf_dump[osd_id]
#     osd_perf: Dict[str, numpy.ndarray] = {}
#
#     if storage_type == OSDStoreType.filestore:
#         fstor = [obj["filestore"] for obj in osd_perf_dump]
#         for field in ("apply_latency", "commitcycle_latency", "journal_latency"):
#             count = [obj[field]["avgcount"] for obj in fstor]
#             values = [obj[field]["sum"] for obj in fstor]
#             osd_perf[field] = avg_counters(count, values)
#
#         arr = numpy.array([obj['journal_wr_bytes']["avgcount"] for obj in fstor], dtype=numpy.float32)
#         osd_perf["journal_ops"] = arr[1:] - arr[:-1]  # type: ignore
#         arr = numpy.array([obj['journal_wr_bytes']["sum"] for obj in fstor], dtype=numpy.float32)
#         osd_perf["journal_bytes"] = arr[1:] - arr[:-1]  # type: ignore
#     else:
#         bstor = [obj["bluestore"] for obj in osd_perf_dump]
#         for field in ("commit_lat",):
#             count = [obj[field]["avgcount"] for obj in bstor]
#             values = [obj[field]["sum"] for obj in bstor]
#             osd_perf[field] = avg_counters(count, values)
#
#     return osd_perf


def load_pools(storage: TypedStorage, ver: CephVersions) -> Dict[int, Pool]:
    df_info: Dict[int, PoolDF] = {}

    for df_dict in storage.json.master.rados_df['pools']:
        if 'categories' in df_dict:
            assert len(df_dict['categories']) == 1
            df_dict = df_dict['categories'][0]

        del df_dict['name']

        # need 'ints' for older ceph version
        id = int(df_dict.pop('id'))
        df_info[id] = PoolDF(size_bytes=int(df_dict['size_bytes']),
                             size_kb=int(df_dict['size_kb']),
                             num_objects=int(df_dict['num_objects']),
                             num_object_clones=int(df_dict['num_object_clones']),
                             num_object_copies=int(df_dict['num_object_copies']),
                             num_objects_missing_on_primary=int(df_dict['num_objects_missing_on_primary']),
                             num_objects_unfound=int(df_dict['num_objects_unfound']),
                             num_objects_degraded=int(df_dict['num_objects_degraded']),
                             read_ops=int(df_dict['read_ops']),
                             read_bytes=int(df_dict['read_bytes']),
                             write_ops=int(df_dict['write_ops']),
                             write_bytes=int(df_dict['write_bytes']))

    pools: Dict[int, Pool] = {}
    for info in storage.json.master.osd_dump['pools']:
        pool_id = int(info['pool'])
        pools[pool_id] = Pool(id=pool_id,
                              name=info['pool_name'],
                              size=int(info['size']),
                              min_size=int(info['min_size']),
                              pg=int(info['pg_num']),
                              pgp=int(info['pg_placement_num']),
                              crush_rule=int(info['crush_rule'] if ver >= CephVersions.luminous
                                             else info['crush_ruleset']),
                              extra=info,
                              df=df_info[pool_id],
                              apps=list(info.get('application_metadata', {}).keys()),
                              d_df=None)
    return pools


ERR_MAP = {
    0: "HEALTH_OK",
    1: "SCRUB_MISSMATCH",
    2: "CLOCK_SKEW",
    3: "OSD_DOWN",
    4: "REDUCED_AVAIL",
    5: "DEGRADED",
    6: "NO_ACTIVE_MGR",
    7: "SLOW_REQUESTS",
    8: "MON_ELECTION"
}


class CephLoader:
    def __init__(self,
                 storage: TypedStorage,
                 ip2host: Dict[str, Host],
                 hosts: Dict[str, Host],
                 osd_perf_counters_dump: Any = None,
                 osd_historic_ops_paths: Any = None) -> None:
        self.storage = storage
        self.ip2host = ip2host
        self.hosts = hosts
        self.osd_perf_counters_dump = osd_perf_counters_dump
        self.osd_historic_ops_paths = osd_historic_ops_paths

    def load_ceph(self) -> CephInfo:
        settings = AttredDict(**parse_txt_ceph_config(self.storage.txt.master.default_config))

        errors_count = None
        status_regions = None

        for is_file, mon in self.storage.txt.mon:
            if not is_file:
                err_wrn: List[str] = self.storage.txt.mon[f"{mon}/ceph_log_wrn_err"].split("\n")
                try:
                    log_count_dct = self.storage.json.mon[f"{mon}/log_issues_count"]
                except KeyError:
                    log_count_dct = None

                if log_count_dct:
                    errors_count = {ERR_MAP[int(err_id)]: cnt for err_id, cnt in log_count_dct.items()}

                try:
                    status_regions = [StatusRegion(*dt) for dt in self.storage.json.mon[f"{mon}/status_regions"]]
                except KeyError:
                    pass

                break

        mon_vers = parse_ceph_versions(self.storage.txt.master.mon_versions)
        ceph_master_version = max(mon_vers.values()).version
        try:
            pg_dump = self.storage.json.master.pg_dump
        except AttributeError:
            pg_dump = None

        logger.debug("Load pools")
        pools = load_pools(self.storage, ceph_master_version)
        name2pool = {pool.name: pool for pool in pools.values()}

        logger.debug("Load PG distribution")
        osd_pool_pg_2d, sum_per_pool, sum_per_osd = load_PG_distribution(name2pool, pg_dump)

        logger.debug("Load crushmap")
        crush = load_crushmap_js(crush=self.storage.json.master.osd_crush_dump)

        if pg_dump:
            logger.debug("Load pgdump")
            pgs: Optional[PGDump] = parse_pg_dump(pg_dump)
        else:
            pgs = None

        logger.debug("Preparing PG/osd caches")

        osdid2rule: Dict[int, List[Tuple[int, float]]] = {}
        for rule in crush.rules.values():
            for osd_node in crush.iter_osds_for_rule(rule.id):
                osdid2rule.setdefault(osd_node.id, []).append((rule.id, osd_node.weight))

        osds = self.load_osds(sum_per_osd, crush, pgs, osdid2rule)
        osds4rule: Dict[int, List[CephOSD]] = {}

        for osd_id, rule2weights in osdid2rule.items():
            for rule_id, weight in rule2weights:
                osds4rule.setdefault(rule_id, []).append(osds[osd_id])

        logger.debug("Loading monitors/status")

        return CephInfo(osds=osds,
                        mons=load_monitors(self.storage, ceph_master_version, self.hosts),
                        status=load_ceph_status(self.storage, ceph_master_version),
                        version=max(mon_vers.values()),
                        pools=name2pool,
                        osd_pool_pg_2d=osd_pool_pg_2d,
                        sum_per_pool=sum_per_pool,
                        sum_per_osd=sum_per_osd,
                        cluster_net=IPv4Network(settings['cluster_network'], strict=False),
                        public_net=IPv4Network(settings['public_network'], strict=False),
                        settings=settings,
                        pgs=pgs,
                        pgs_second=None,
                        mgrs=None,
                        radosgw=None,
                        crush=crush,
                        log_err_warn=err_wrn,
                        osds4rule=osds4rule,
                        errors_count=errors_count,
                        status_regions=status_regions)

    def load_osd_procinfo(self, osd_id: int) -> OSDProcessInfo:
        cmdln = [i.decode("utf8")
                 for i in self.storage.raw.get_raw('osd/{0}/cmdline.bin'.format(osd_id)).split(b'\x00')]

        pinfo = self.storage.json.osd[str(osd_id)].procinfo

        sched = pinfo['sched']
        if not sched:
            data = "\n".join(pinfo['sched_raw'].strip().split("\n")[2:])
            sched = parse_proc_file(data, ignore_err=True)

        return OSDProcessInfo(cmdline=cmdln,
                              procinfo=pinfo,
                              fd_count=pinfo["fd_count"],
                              opened_socks=pinfo.get('sock_count', 'Unknown'),
                              th_count=pinfo["th_count"],
                              cpu_usage=float(sched["se.sum_exec_runtime"]),
                              vm_rss=mem2bytes(pinfo["mem"]["VmRSS"]),
                              vm_size=mem2bytes(pinfo["mem"]["VmSize"]))

    def load_osd_devices(self, osd_id: int, host: Host) -> Union[None, FileStoreInfo, BlueStoreInfo]:
        osd_stor_node = self.storage.json.osd[str(osd_id)]
        osd_disks_info = osd_stor_node.devs_cfg

        if osd_disks_info == {}:
            return None

        def get_ceph_dev_info(devpath: str, partpath: str) -> CephDevInfo:
            dev_name = devpath.split("/")[-1]
            partition_name = partpath.split("/")[-1]
            return CephDevInfo(hostname=host.name,
                               dev_info=host.disks[dev_name],
                               partition_info=host.logic_block_devs[partition_name])

        data_dev = get_ceph_dev_info(osd_disks_info['r_data'], osd_disks_info['data'])

        if osd_disks_info['type'] == 'filestore':
            j_dev = get_ceph_dev_info(osd_disks_info['r_journal'], osd_disks_info['journal'])
            return FileStoreInfo(data_dev, j_dev)
        else:
            assert osd_disks_info['type']  == 'bluestore'
            db_dev = get_ceph_dev_info(osd_disks_info['r_db'], osd_disks_info['db'])
            wal_dev = get_ceph_dev_info(osd_disks_info['r_wal'], osd_disks_info['wal'])
            return BlueStoreInfo(data_dev, db_dev, wal_dev)

    def load_osds(self,
                  sum_per_osd: Optional[Dict[int, int]],
                  crush: Crush,
                  pgs: Optional[PGDump],
                  osdid2rule: Dict[int, List[Tuple[int, float]]]) -> Dict[int, CephOSD]:
        try:
            fc = self.storage.txt.master.osd_versions
        except:
            fc = self.storage.raw.get_raw('master/osd_versions.err').decode("utf8")

        osd_versions: Dict[int, CephVersion] = {}

        for name, ver in parse_ceph_versions(fc).items():
            nm, id = name.split(".")
            assert nm == 'osd'
            osd_versions[int(id)] = ver

        osd_rw_dict = dict((node['id'], node['reweight'])
                           for node in self.storage.json.master.osd_tree['nodes']
                           if node['id'] >= 0)

        osd_perf_scalar = {}
        for node in self.storage.json.master.osd_perf['osd_perf_infos']:
            osd_perf_scalar[node['id']] = {"apply_latency_s": node["perf_stats"]["apply_latency_ms"],
                                           "commitcycle_latency_s": node["perf_stats"]["commit_latency_ms"]}

        osd_df_map = {node['id']: node for node in self.storage.json.master.osd_df['nodes']}
        osds: Dict[int, CephOSD] = {}

        osd2pg: Optional[Dict[int, List[PG]]] = None

        if pgs:
            osd2pg = collections.defaultdict(list)
            for pg in pgs.pgs.values():
                for osd_id in pg.acting:
                    osd2pg[osd_id].append(pg)

        osd_perf_dump = {int(vals['id']): vals for vals in self.storage.json.master.osd_perf['osd_perf_infos']}

        for osd_data in self.storage.json.master.osd_dump['osds']:
            osd_id = osd_data['osd']

            if osd_id not in osd_df_map:
                logger.warning("Osd %s has no disk space information - ignoring it", osd_id)
                continue

            used_space = osd_df_map[osd_id]['kb_used'] * 1024
            free_space = osd_df_map[osd_id]['kb_avail'] * 1024

            cluster_ip = osd_data['cluster_addr'].split(":", 1)[0]

            try:
                cfg_txt = self.storage.txt.osd[str(osd_id)].config
            except AttributeError:
                try:
                    cfg = self.storage.json.osd[str(osd_id)].config
                except AttributeError:
                    cfg = None
            else:
                cfg = parse_txt_ceph_config(cfg_txt)

            config = AttredDict(**cfg) if cfg is not None else None

            try:
                host = self.ip2host[cluster_ip]
            except KeyError:
                logger.exception("Can't found host for osd %s, as no host own %r ip addr", osd_id, cluster_ip)
                raise

            status = OSDStatus.up if osd_data['up'] else OSDStatus.down
            storage_info = self.load_osd_devices(osd_id, host)

            #  CRUSH RULES/WEIGHTS
            crush_rules_weights: Dict[int, float] = {}
            for rule_id, weight in osdid2rule.get(osd_id, []):
                crush_rules_weights[rule_id] = weight

            pg_stats: Optional[OSDPGStats] = None
            osd_pgs: Optional[List[PG]] = None

            if osd2pg:
                osd_pgs = [pg for pg in osd2pg[osd_id]]
                bytes = sum(pg.stat_sum.num_bytes for pg in osd_pgs)
                reads = sum(pg.stat_sum.num_read for pg in osd_pgs)
                read_b = sum(pg.stat_sum.num_read_kb * 1024 for pg in osd_pgs)
                writes = sum(pg.stat_sum.num_write for pg in osd_pgs)
                write_b = sum(pg.stat_sum.num_write_kb * 1024 for pg in osd_pgs)
                scrub_err = sum(pg.stat_sum.num_scrub_errors for pg in osd_pgs)
                deep_scrub_err = sum(pg.stat_sum.num_deep_scrub_errors for pg in osd_pgs)
                shallow_scrub_err = sum(pg.stat_sum.num_shallow_scrub_errors for pg in osd_pgs)
                pg_stats = OSDPGStats(bytes, reads, read_b, writes, write_b, scrub_err, deep_scrub_err,
                                      shallow_scrub_err)

            historic_ops_storage_path = None if self.osd_historic_ops_paths is None \
                                        else self.osd_historic_ops_paths.get(osd_id)
            perf_cntrs = None if self.osd_perf_counters_dump is None else self.osd_perf_counters_dump.get(osd_id)

            if free_space + used_space < 1:
                free_perc = None
            else:
                free_perc = int((free_space * 100.0) / (free_space + used_space) + 0.5)

            expected_weights = None if storage_info is None else (storage_info.data.dev_info.size / (1024 ** 4))
            total_space = None if storage_info is None else storage_info.data.dev_info.size

            osds[osd_id] = CephOSD(id=osd_id,
                                   status=status,
                                   config=config,
                                   cluster_ip=cluster_ip,
                                   public_ip=osd_data['public_addr'].split(":", 1)[0],
                                   reweight=osd_rw_dict[osd_id],
                                   version=osd_versions.get(osd_id),
                                   pg_count=None if sum_per_osd is None else sum_per_osd.get(osd_id, 0),
                                   used_space=used_space,
                                   free_space=free_space,
                                   free_perc=free_perc,
                                   host=host,
                                   storage_info=storage_info,
                                   run_info=self.load_osd_procinfo(osd_id) if status != OSDStatus.down else None,
                                   expected_weights=expected_weights,
                                   crush_rules_weights=crush_rules_weights,
                                   total_space=total_space,
                                   pgs=osd_pgs,
                                   pg_stats=pg_stats,
                                   d_pg_stats=None,
                                   historic_ops_storage_path=historic_ops_storage_path,
                                   osd_perf_counters=perf_cntrs,
                                   osd_perf_dump=osd_perf_dump[osd_id]['perf_stats'],
                                   class_name=crush.nodes_map[f'osd.{osd_id}'].class_name)

        return osds

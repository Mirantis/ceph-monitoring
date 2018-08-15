import re
import copy
import json
import logging
import datetime
import collections
from enum import Enum
from ipaddress import IPv4Network
from typing import Dict, Any, List, Union, Tuple, Optional

import numpy
import dataclasses

from cephlib.crush import load_crushmap, Crush
from cephlib.storage import TypedStorage
from cephlib.common import AttredDict

from .cluster_classes import (PGDump, PG, PGStatSum, PGId, PGState,
                              Pool, PoolDF, CephVersion, CephMonitor, MonRole, CephStatus, CephStatusCode,
                              Host, CephInfo, CephOSD, OSDStatus, CephVersions, FileStoreInfo, BlueStoreInfo,
                              OSDProcessInfo, CephDevInfo, OSDPGStats, NodePGStats)
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
    for pg_info in data['pg_stats']:
        dt = {}
        for field in dataclasses.fields(PG):
            name = field.name
            tp = field.type
            vl_any = pg_info[name]
            if tp is datetime.datetime:
                vl_s, mks = vl_any.split(".")
                vl = datetime.datetime.strptime(vl_s, '%Y-%m-%d %H:%M:%S').replace(microsecond=int(mks))
            elif name in ("reported_epoch", "reported_seq"):
                vl = int(vl_any)
            elif tp in (int, str, bool):
                assert isinstance(vl_any, tp), f"Wrong type for field {name}. Expected {tp} got {type(vl_any)}"
                vl = vl_any
            elif name == 'stat_sum':
                vl = PGStatSum(**pg_info['stat_sum'])
            elif name in ('acting', 'blocked_by', 'up'):
                assert isinstance(vl_any, list)
                assert all(isinstance(vl_elem, int) for vl_elem in vl_any)
                vl = vl_any
            elif name == 'state':
                statuses = vl_any.split("+")
                vl = set(getattr(PGState, status) for status in statuses)
            elif name == 'pgid':
                pool_s, pg_s = vl_any.split(".")
                vl = PGId(pool=int(pool_s), num=int(pg_s, 16), id=vl_any)
            else:
                raise ValueError(f"Unknown field tp {tp} for field {name}")
            dt[name] = vl
        pgs.append(PG(**dt))

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


version_rr = re.compile(r'ceph version\s+(?P<version>[^ ]*)\s+\((?P<hash>[^)]*?)\)')


def parse_ceph_versions(data: str) -> Dict[str, CephVersion]:
    vers: Dict[str, CephVersion] = {}
    for line in data.split("\n"):
        line = line.strip()
        if line:
            name, data_js = line.split(":", 1)
            rr = version_rr.match(json.loads(data_js)["version"])
            if not rr:
                logger.error("Can't parse version %r from %r", json.loads(data_js)["version"], line)
            major, minor, bugfix = map(int, rr.group("version").split("."))
            vers[name.strip()] = CephVersion(major, minor, bugfix, rr.group("hash"))
    return vers


def load_monitors(storage: TypedStorage, ver: CephVersions, hosts: Dict[str, Host]) -> Dict[str, CephMonitor]:
    if ver >= CephVersions.luminous:
        mons = storage.json.master.status['monmap']['mons']
        # mon_status = self.storage.json.master.mon_status
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
                          version=vers["mon." + srv["name"]])

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
                        root = info.mountpoint

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


def load_osd_perf_data(osd_id: int,
                       storage_type: OSDStoreType,
                       osd_perf_dump: Dict[int, List[Dict]]) -> Dict[str, numpy.ndarray]:
    osd_perf_dump = osd_perf_dump[osd_id]
    osd_perf: Dict[str, numpy.ndarray] = {}

    if storage_type == OSDStoreType.filestore:
        fstor = [obj["filestore"] for obj in osd_perf_dump]
        for field in ("apply_latency", "commitcycle_latency", "journal_latency"):
            count = [obj[field]["avgcount"] for obj in fstor]
            values = [obj[field]["sum"] for obj in fstor]
            osd_perf[field] = avg_counters(count, values)

        arr = numpy.array([obj['journal_wr_bytes']["avgcount"] for obj in fstor], dtype=numpy.float32)
        osd_perf["journal_ops"] = arr[1:] - arr[:-1]  # type: ignore
        arr = numpy.array([obj['journal_wr_bytes']["sum"] for obj in fstor], dtype=numpy.float32)
        osd_perf["journal_bytes"] = arr[1:] - arr[:-1]  # type: ignore
    else:
        bstor = [obj["bluestore"] for obj in osd_perf_dump]
        for field in ("commit_lat",):
            count = [obj[field]["avgcount"] for obj in bstor]
            values = [obj[field]["sum"] for obj in bstor]
            osd_perf[field] = avg_counters(count, values)

    return osd_perf




def load_pools(storage: TypedStorage, ver: CephVersions) -> Dict[int, Pool]:
    df_info: Dict[int, PoolDF] = {}

    for df_dict in storage.json.master.rados_df['pools']:
        if 'categories' in df_dict:
            assert len(df_dict['categories']) == 1
            df_dict = df_dict['categories'][0]

        del df_dict['name']
        id = df_dict.pop('id')
        df_info[id] = PoolDF(**df_dict)

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

        err_wrn = []
        for is_file, mon in self.storage.txt.mon:
            if not is_file:
                err_wrn = self.storage.txt.mon[f"{mon}/ceph_log_wrn_err"].split("\n")
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
        crush = load_crushmap(content=self.storage.txt.master.crushmap)

        if pg_dump:
            logger.debug("Load pgdump")
            pgs = parse_pg_dump(pg_dump)
        else:
            pgs = None

        return CephInfo(osds=self.load_osds(sum_per_osd, crush, pgs),
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
                        nodes_pg_info=None)

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

    def load_osd_devices(self, osd_id: int, host: Host) -> Union[FileStoreInfo, BlueStoreInfo]:
        osd_stor_node = self.storage.json.osd[str(osd_id)]
        osd_disks_info = osd_stor_node.devs_cfg

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
                  pgs: PGDump = None) -> Dict[int, CephOSD]:


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

        osd2rules = {}
        for rule in crush.rules.values():
            for osd_node in crush.iter_osds_for_rule(rule.id):
                osd2rules.setdefault(osd_node.id, []).append((rule.name, osd_node.weight))

        osd_perf_dump = {int(vals['id']): vals for vals in self.storage.json.master.osd_perf['osd_perf_infos']}

        for osd_data in self.storage.json.master.osd_dump['osds']:
            osd_id = osd_data['osd']
            osd_df_data = osd_df_map[osd_id]
            used_space = osd_df_data['kb_used'] * 1024
            free_space = osd_df_data['kb_avail']  * 1024
            cluster_ip = osd_data['cluster_addr'].split(":", 1)[0]

            config = AttredDict(**parse_txt_ceph_config(self.storage.txt.osd[str(osd_id)].config))

            try:
                host = self.ip2host[cluster_ip]
            except KeyError:
                logger.exception("Can't found host for osd %s, as no host own %r ip addr", osd_id, cluster_ip)
                raise

            status = OSDStatus.up if osd_data['up'] else OSDStatus.down
            storage_info = self.load_osd_devices(osd_id, host)

            #  CRUSH RULES/WEIGHTS
            crush_info: Dict[str, Tuple[bool, float, float]] = {}
            for rule, weight in osd2rules.get(osd_id, []):
                crush_info[rule] = (True, weight, storage_info.data.dev_info.size / (1024 ** 4))

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
            osds[osd_id] = CephOSD(id=osd_id,
                                   config=config,
                                   cluster_ip=cluster_ip,
                                   public_ip=osd_data['public_addr'].split(":", 1)[0],
                                   reweight=osd_rw_dict[osd_id],
                                   version=osd_versions[osd_id],
                                   pg_count=None if sum_per_osd is None else sum_per_osd[osd_id],
                                   used_space=used_space,
                                   free_space=free_space,
                                   free_perc=int((free_space * 100.0) / (free_space + used_space) + 0.5),
                                   host=host,
                                   storage_info=storage_info,
                                   run_info=self.load_osd_procinfo(osd_id) if status != OSDStatus.down else None,
                                   crush_rules_weights=crush_info,
                                   status=status,
                                   total_space=storage_info.data.dev_info.size,
                                   pgs=osd_pgs,
                                   pg_stats=pg_stats,
                                   d_pg_stats=None,
                                   historic_ops_storage_path=historic_ops_storage_path,
                                   osd_perf_counters=perf_cntrs,
                                   osd_perf_dump=osd_perf_dump[osd_id]['perf_stats'],
                                   class_name=crush.nodes_map[f'osd.{osd_id}'].class_name)


        return osds



# def get_osds_info(cluster: Cluster, ceph: CephInfo) -> OSDSInfo:
#
#     stor_set = set(osd.storage_type for osd in ceph.osds)
#     has_bs = OSDStoreType.bluestore in stor_set
#     has_fs = OSDStoreType.filestore in stor_set
#     assert has_fs or has_bs
#
#     all_vers = get_all_versions(ceph.osds)
#
#     osd2pg_map = collections.defaultdict(list)
#     if ceph.pgs:
#         for pg in ceph.pgs.pgs.values():
#             for osd_id in pg.acting:
#                 osd2pg_map[osd_id].append(pg)
#
#     osds: Dict[int, OSDInfo] = {}
#     for osd in ceph.osds:
#         disks = cluster.hosts[osd.host.name].disks
#         storage_devs = cluster.hosts[osd.host.name].storage_devs
#         assert osd.storage_info is not None
#
#         total_b = int(storage_devs[osd.storage_info.data_partition].size)
#
#
#         #  RUN INFO - FD COUNT, TCP CONN, THREADS
#         if osd.run_info is not None:
#             pinfo = osd.run_info.procinfo
#             opened_socks = sum(int(tp.get('inuse', 0)) for tp in pinfo.get('ipv4', {}).values())
#             opened_socks += sum(int(tp.get('inuse', 0)) for tp in pinfo.get('ipv6', {}).values())
#
#             sched = pinfo['sched']
#             if not sched:
#                 data = "\n".join(pinfo['sched_raw'].strip().split("\n")[2:])
#                 sched = parse_proc_file(data, ignore_err=True)
#
#             run_info = OSDProcessInfo(fd_count=pinfo["fd_count"],
#                                       opened_socks=opened_socks,
#                                       th_count=pinfo["th_count"],
#                                       cpu_usage=float(sched["se.sum_exec_runtime"]),
#                                       vm_rss=mem2bytes(pinfo["mem"]["VmRSS"]),
#                                       vm_size=mem2bytes(pinfo["mem"]["VmSize"]))
#         else:
#             run_info = None
#
#         #  USER DATA SIZE & TOTAL IO
#         if ceph.pgs:
#             pgs = osd2pg_map[osd.id]
#             bytes = sum(pg.stat_sum.num_bytes for pg in pgs)
#             reads = sum(pg.stat_sum.num_read for pg in pgs)
#             read_b = sum(pg.stat_sum.num_read_kb * 1024 for pg in pgs)
#             writes = sum(pg.stat_sum.num_write for pg in pgs)
#             write_b = sum(pg.stat_sum.num_write_kb * 1024 for pg in pgs)
#             scrub_err = sum(pg.stat_sum.num_scrub_errors for pg in pgs)
#             deep_scrub_err = sum(pg.stat_sum.num_deep_scrub_errors for pg in pgs)
#             shallow_scrub_err = sum(pg.stat_sum.num_shallow_scrub_errors for pg in pgs)
#
#             pg_stats = OSDPGStats(bytes, reads, read_b, writes, write_b, scrub_err, deep_scrub_err, shallow_scrub_err)
#
#             # USER DATA SIZE DELTA
#             if ceph.pgs_second:
#                 smap = ceph.pgs_second.pgs
#                 d_bytes = sum(smap[pg.pgid.id].stat_sum.num_bytes for pg in pgs) - bytes
#                 d_reads = sum(smap[pg.pgid.id].stat_sum.num_read for pg in pgs) - reads
#                 d_read_b = sum(smap[pg.pgid.id].stat_sum.num_read_kb * 1024 for pg in pgs) - read_b
#                 d_writes = sum(smap[pg.pgid.id].stat_sum.num_write for pg in pgs) - writes
#                 d_write_b = sum(smap[pg.pgid.id].stat_sum.num_write_kb * 1024 for pg in pgs) - write_b
#
#                 dtime = (ceph.pgs_second.collected_at - ceph.pgs.collected_at).total_seconds()
#
#                 d_stats = OSDDStats(d_bytes // dtime,
#                                     d_reads // dtime,
#                                     d_read_b // dtime,
#                                     d_writes // dtime,
#                                     d_write_b // dtime)
#             else:
#                 d_stats = None
#         else:
#             d_stats = None
#             pg_stats = None
#
#         # JOURNAL/WAL/DB info
#         sinfo = osd.storage_info
#         if isinstance(sinfo, FileStoreInfo):
#             collocation = sinfo.journal_dev != sinfo.data_dev
#             on_file = sinfo.journal_partition == sinfo.data_partition
#             drive_type = disks[sinfo.journal_dev].tp
#             size = None if collocation and on_file else storage_devs[sinfo.journal_partition].size
#             fs_info = JInfo(collocation, on_file, drive_type, size)
#             bs_info = None
#         else:
#             assert isinstance(sinfo, BlueStoreInfo)
#             wal_collocation = sinfo.wal_dev != sinfo.data_dev
#             wal_drive_type = disks[sinfo.wal_dev].tp
#
#             if sinfo.wal_partition not in (sinfo.db_partition, sinfo.data_partition):
#                 wal_size = int(storage_devs[sinfo.wal_partition].size)
#             else:
#                 wal_size = None
#
#             db_collocation = sinfo.db_dev != sinfo.data_dev
#             db_drive_type = disks[sinfo.db_dev].tp
#             if sinfo.db_partition != sinfo.data_partition:
#                 db_size = int(storage_devs[sinfo.db_partition].size)
#             else:
#                 db_size = None
#
#             fs_info = None
#             bs_info = WALDBInfo(wal_collocation=wal_collocation,
#                                 wal_drive_type=wal_drive_type,
#                                 wal_size=wal_size,
#                                 db_collocation=db_collocation,
#                                 db_drive_type=db_drive_type,
#                                 db_size=db_size)
#
#         osds[osd.id] = OSDInfo(pgs=osd2pg_map[osd.id] if ceph.pgs else None,
#                                total_space=total_b,
#                                used_space=0,
#                                free_space=0,
#                                total_user_data=0,
#                                crush_trees_weights=crush_info,
#                                data_drive_type=disks[osd.storage_info.data_dev].tp,
#                                data_part_size=storage_devs[osd.storage_info.data_partition].size,
#                                j_info=fs_info,
#                                wal_db_info=bs_info,
#                                run_info=run_info,
#                                pg_stats=pg_stats,
#                                d_stats=d_stats)
#
#     return OSDSInfo(has_bs=has_bs, has_fs=has_fs, all_versions=all_vers, largest_ver=max(all_vers),
#                     osds=osds)


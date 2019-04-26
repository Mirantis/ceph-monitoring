import logging
import collections
from ipaddress import IPv4Network
from typing import Dict, Any, List, Union, Tuple, Optional

from koder_utils import parse_info_file_from_proc, AttredDict, TypedStorage
from cephlib import (PGDump, PG, StatusRegion, Pool, PoolDF, CephVersion, CephMonitor,
                     MonRole, CephStatus, CephStatusCode, Host, CephInfo, CephOSD, OSDStatus, FileStoreInfo,
                     BlueStoreInfo, OSDProcessInfo, CephDevInfo, OSDPGStats,
                     parse_ceph_version, CephRelease, parse_txt_ceph_config, parse_pg_dump,
                     Crush, parse_ceph_report, parse_pg_distribution, OSDSpace)


logger = logging.getLogger("ceph_report")


NO_VALUE = -1


def load_monitors(storage: TypedStorage, ver: CephVersion, hosts: Dict[str, Host]) -> Dict[str, CephMonitor]:
    luminous = ver.release >= CephRelease.luminous
    if luminous:
        mons = storage.json.master.status['monmap']['mons']
    else:
        srv_health = storage.json.master.status['health']['health']['health_services']
        assert len(srv_health) == 1
        mons = srv_health[0]['mons']

    # vers = parse_ceph_version(storage.json.master.versions)
    result: Dict[str, CephMonitor] = {}
    mons_meta = {md['name']: md for md in storage.json.master.mons_metadata}
    for srv in mons:

        mon = CephMonitor(name=srv["name"],
                          status=None if luminous else srv["health"],
                          host=hosts[srv["name"]],
                          role=MonRole.unknown,
                          version=parse_ceph_version(mons_meta[srv["name"]]["ceph_version"]))

        if luminous:
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


def load_ceph_status(storage: TypedStorage, ver: CephVersion) -> CephStatus:
    mstorage = storage.json.master
    pgmap = mstorage.status['pgmap']
    health = mstorage.status['health']
    status = health['status'] if 'status' in health else health['overall_status']

    return CephStatus(status=CephStatusCode.from_str(status),
                      health_summary=health['checks' if ver.release >= CephRelease.luminous else 'summary'],
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


def load_pools(storage: TypedStorage, ver: CephVersion) -> Dict[int, Pool]:
    df_info: Dict[int, PoolDF] = {}

    for df_dict in storage.json.master.rados_df['pools']:
        if 'categories' in df_dict:
            assert len(df_dict['categories']) == 1
            df_dict = df_dict['categories'][0]

        del df_dict['name']

        # need 'ints' for older ceph version
        pool_id = int(df_dict.pop('id'))
        df_info[pool_id] = PoolDF(size_bytes=int(df_dict['size_bytes']),
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
                              crush_rule=int(info['crush_rule'] if ver.release >= CephRelease.luminous
                                             else info['crush_ruleset']),
                              extra=info,
                              df=df_info[pool_id],
                              apps=list(info.get('application_metadata', {}).keys()))
    return pools


def mem2bytes(vl: str) -> int:
    vl_sz, units = vl.split()
    assert units == 'kB'
    return int(vl_sz) * 1024


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
        self.report = parse_ceph_report(self.storage.json.master.report)

    def get_log_error_count(self) -> Tuple[Optional[List[str]], Any, Any]:
        errors_count = None
        status_regions = None
        err_wrn: Optional[List[str]] = None

        for is_file, mon in self.storage.txt.mon:
            if not is_file:
                err_wrn = self.storage.txt.mon[f"{mon}/ceph_log_wrn_err"].split("\n")
                try:
                    errors_count = self.storage.json.mon[f"{mon}/log_issues_count"]
                except KeyError:
                    pass

                try:
                    status_regions = [StatusRegion(*dt) for dt in self.storage.json.mon[f"{mon}/status_regions"]]
                except KeyError:
                    pass

                return err_wrn, errors_count, status_regions

    def load_ceph(self) -> CephInfo:
        settings = AttredDict(**parse_txt_ceph_config(self.storage.txt.master.default_config))

        err_wrn, errors_count, status_regions = self.get_log_error_count()

        logger.debug("Load pools")
        pools = load_pools(self.storage, self.report.version)
        name2pool = {pool.name: pool for pool in pools.values()}

        try:
            pg_dump = self.storage.json.master.pg_dump
        except AttributeError:
            pg_dump = None

        logger.debug("Load PG distribution")
        osd_pool_pg_2d, sum_per_pool, sum_per_osd = parse_pg_distribution(name2pool, pg_dump)

        if pg_dump:
            logger.debug("Load pgdump")
            pgs: Optional[PGDump] = parse_pg_dump(pg_dump)
        else:
            pgs = None

        logger.debug("Preparing PG/osd caches")

        osdid2rule: Dict[int, List[Tuple[int, float]]] = {}
        for rule in self.report.crushmap.rules.values():
            for osd_node in self.report.crushmap.iter_osds_for_rule(rule.id):
                osdid2rule.setdefault(osd_node.id, []).append((rule.id, osd_node.weight))

        osds = self.load_osds(sum_per_osd, self.report.crushmap, pgs, osdid2rule)
        osds4rule: Dict[int, List[CephOSD]] = {}

        for osd_id, rule2weights in osdid2rule.items():
            for rule_id, weight in rule2weights:
                osds4rule.setdefault(rule_id, []).append(osds[osd_id])

        logger.debug("Loading monitors/status")

        return CephInfo(osds=osds,
                        mons=load_monitors(self.storage, self.report.version, self.hosts),
                        status=load_ceph_status(self.storage, self.report.version),
                        version=self.report.version,
                        pools=name2pool,
                        osd_pool_pg_2d=osd_pool_pg_2d,
                        sum_per_pool=sum_per_pool,
                        sum_per_osd=sum_per_osd,
                        cluster_net=IPv4Network(settings['cluster_network'], strict=False),
                        public_net=IPv4Network(settings['public_network'], strict=False),
                        settings=settings,
                        pgs=pgs,
                        mgrs={},
                        radosgw={},
                        crush=self.report.crushmap,
                        log_err_warn=err_wrn,
                        osds4rule=osds4rule,
                        errors_count=errors_count,
                        status_regions=status_regions,
                        report=self.report)

    def load_osd_procinfo(self, osd_id: int) -> OSDProcessInfo:
        cmdln = [i.decode() for i in self.storage.raw.get_raw(f'osd/{osd_id}/cmdline.bin').split(b'\x00')]

        pinfo = self.storage.json.osd[str(osd_id)].procinfo

        sched = pinfo['sched']
        if not sched:
            data = "\n".join(pinfo['sched_raw'].strip().split("\n")[2:])
            sched = parse_info_file_from_proc(data, ignore_err=True)

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
            fc = self.storage.raw.get_raw('master/osd_versions.err').decode()

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
                bytes = sum(pg.stat_sum.bytes for pg in osd_pgs)
                reads = sum(pg.stat_sum.read for pg in osd_pgs)
                read_b = sum(pg.stat_sum.read_kb * 1024 for pg in osd_pgs)
                writes = sum(pg.stat_sum.write for pg in osd_pgs)
                write_b = sum(pg.stat_sum.write_kb * 1024 for pg in osd_pgs)
                scrub_err = sum(pg.stat_sum.scrub_errors for pg in osd_pgs)
                deep_scrub_err = sum(pg.stat_sum.deep_scrub_errors for pg in osd_pgs)
                shallow_scrub_err = sum(pg.stat_sum.shallow_scrub_errors for pg in osd_pgs)
                pg_stats = OSDPGStats(bytes, reads, read_b, writes, write_b, scrub_err, deep_scrub_err,
                                      shallow_scrub_err)

            perf_cntrs = None if self.osd_perf_counters_dump is None else self.osd_perf_counters_dump.get(osd_id)

            if free_space + used_space < 1:
                free_perc = None
            else:
                free_perc = int((free_space * 100.0) / (free_space + used_space) + 0.5)


            expected_weights = None if storage_info is None else (storage_info.data.dev_info.size / (1024 ** 4))
            total_space = None if storage_info is None else storage_info.data.dev_info.size
            osd_space = OSDSpace(free_perc=free_perc, used=used_space, total=total_space, free=free_space)

            osd_vers = {osd_meta.osd_id: osd_meta.version for osd_meta in self.report.osds}

            osds[osd_id] = CephOSD(id=osd_id,
                                   status=status,
                                   config=config,
                                   cluster_ip=cluster_ip,
                                   public_ip=osd_data['public_addr'].split(":", 1)[0],
                                   reweight=osd_rw_dict[osd_id],
                                   version=osd_vers.get(osd_id),
                                   pg_count=None if sum_per_osd is None else sum_per_osd.get(osd_id, 0),
                                   space=osd_space,
                                   host=host,
                                   storage_info=storage_info,
                                   run_info=self.load_osd_procinfo(osd_id) if status != OSDStatus.down else None,
                                   expected_weight=expected_weights,
                                   crush_rules_weights=crush_rules_weights,
                                   pgs=osd_pgs,
                                   pg_stats=pg_stats,
                                   osd_perf_counters=perf_cntrs,
                                   osd_perf_dump=osd_perf_dump[osd_id]['perf_stats'],
                                   class_name=crush.nodes_map[f'osd.{osd_id}'].class_name)

        return osds


# def load_osd_perf_data(osd_id: int,
#                        storage_type: OSDStoreType,
#                        osd_perf_dump: Dict[int, List[Dict]]) -> Dict[str, numpy.ndarray]:
#     osd_perf_dump = osd_perf_dump[osd_id]
#     osd_perf: Dict[str, numpy.ndarray] = {}
#
#     if storage_type == OSDStoreType.filestore:
#         fstor = [conn["filestore"] for conn in osd_perf_dump]
#         for field in ("apply_latency", "commitcycle_latency", "journal_latency"):
#             count = [conn[field]["avgcount"] for conn in fstor]
#             values = [conn[field]["sum"] for conn in fstor]
#             osd_perf[field] = avg_counters(count, values)
#
#         arr = numpy.array([conn['journal_wr_bytes']["avgcount"] for conn in fstor], dtype=numpy.float32)
#         osd_perf["journal_ops"] = arr[1:] - arr[:-1]  # type: ignore
#         arr = numpy.array([conn['journal_wr_bytes']["sum"] for conn in fstor], dtype=numpy.float32)
#         osd_perf["journal_bytes"] = arr[1:] - arr[:-1]  # type: ignore
#     else:
#         bstor = [conn["bluestore"] for conn in osd_perf_dump]
#         for field in ("commit_lat",):
#             count = [conn[field]["avgcount"] for conn in bstor]
#             values = [conn[field]["sum"] for conn in bstor]
#             osd_perf[field] = avg_counters(count, values)
#
#     return osd_perf

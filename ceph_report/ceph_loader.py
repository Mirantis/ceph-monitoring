import logging
import collections
from ipaddress import IPv4Network, IPv4Address
from pathlib import Path
from typing import Dict, Any, List, Union, Tuple, Optional

from koder_utils import parse_info_file_from_proc, AttredDict, TypedStorage
from cephlib import (PGDump, StatusRegion, MonRole, Host, CephInfo, CephOSD, OSDStatus, MonsMetadata, FileStoreInfo,
                     BlueStoreInfo, OSDProcessInfo, CephDevInfo, parse_ceph_version_simple,
                     parse_txt_ceph_config, Crush, parse_pg_distribution, OSDSpace, CephReport, parse_cmd_output,
                     CephStatus, MonMetadata, RadosDF, PGStat, CephIOStats, CephMonitor, Pool, OSDDf,
                     OSDDevCfg)


logger = logging.getLogger("report")


NO_VALUE = -1


def mem2bytes(vl: str) -> int:
    vl_sz, units = vl.split()
    assert units == 'kB'
    return int(vl_sz) * 1024


class CephLoader:
    def __init__(self,
                 storage: TypedStorage,
                 ip2host: Dict[IPv4Address, Host],
                 hosts: Dict[str, Host],
                 osd_perf_counters_dump: Any = None,
                 osd_historic_ops_paths: Any = None) -> None:
        self.storage = storage
        self.ip2host = ip2host
        self.hosts = hosts
        self.osd_perf_counters_dump = osd_perf_counters_dump
        self.osd_historic_ops_paths = osd_historic_ops_paths

        self.version = parse_ceph_version_simple(self.storage.txt.master.version)
        self.release = self.version.release

        self.report: CephReport = parse_cmd_output("ceph report", self.release, self.storage.json.master.report)
        self.status: CephStatus = parse_cmd_output("ceph status", self.release, self.storage.json.master.status)
        self.mons_meta: MonsMetadata = parse_cmd_output("ceph mon metadata", self.release,
                                                        self.storage.json.master.mon_metadata)
        self.osd_df: OSDDf = parse_cmd_output("ceph osd df", self.release, self.storage.json.master.osd_df)
        self.pg_dump: PGDump = parse_cmd_output("ceph pg dump", self.release, self.storage.json.master.pg_dump)
        self.rados_df: RadosDF = parse_cmd_output("rados df", self.release, self.storage.json.master.rados_df)
        self.mon_meta: Dict[str, MonMetadata] = {meta.name: meta for meta in self.mons_meta.mons}
        self.crush = Crush(self.report.crushmap)

        self.osd_dev_cfg: Dict[int, OSDDevCfg] = {}
        for osd in self.osd_df.nodes:
            try:
                self.osd_dev_cfg[osd.id] = OSDDevCfg.convert(self.storage.json.osd[str(osd.id)].devs_cfg)
            except KeyError:
                pass

    def get_log_error_count(self) -> Tuple[Optional[List[str]], Any, Any]:

        for is_file, mon in self.storage.txt.mon:
            if not is_file:
                err_wrn: List[str] = self.storage.txt.mon[f"{mon}/ceph_log_wrn_err"].split("\n")
                try:
                    errors_count = self.storage.json.mon[f"{mon}/log_issues_count"]
                except KeyError:
                    errors_count = None

                try:
                    status_regions: Any = [StatusRegion(*dt) for dt in self.storage.json.mon[f"{mon}/status_regions"]]
                except KeyError:
                    status_regions = None

                return err_wrn, errors_count, status_regions
        return None, None, None

    def load_ceph(self) -> CephInfo:

        settings = AttredDict(**parse_txt_ceph_config(self.storage.txt.master.default_config))

        err_wrn, errors_count, status_regions = self.get_log_error_count()

        logger.debug("Load pools")
        pools = self.load_pools()

        osd_pool_pg_2d = parse_pg_distribution(self.pg_dump)

        logger.debug("Load PG distribution")
        name2pool = {pool.name: pool for pool in pools.values()}
        osd_pool_name_pg_2d = {osd_id: {pools[pid].name: count for pid, count in pdata.items()}
                               for osd_id, pdata in osd_pool_pg_2d.items()}

        sum_per_pool = {pool.name: pool.pg for pool in pools.values()}
        sum_per_osd = {osd.osd: osd.num_pgs for osd in self.pg_dump.osd_stats}

        logger.debug("Preparing PG/osd caches")

        osdid2rule: Dict[int, List[Tuple[int, float]]] = {}
        for rule in self.report.crushmap.rules:
            for osd_id, weight in self.crush.iter_osds_for_rule(rule):
                osdid2rule.setdefault(osd_id, []).append((rule.rule_id, weight))

        osds = self.load_osds(sum_per_osd, osdid2rule)
        osds4rule: Dict[int, List[CephOSD]] = {}

        for osd_id, rule2weights in osdid2rule.items():
            for rule_id, weight in rule2weights:
                osds4rule.setdefault(rule_id, []).append(osds[osd_id])

        logger.debug("Loading monitors/status")

        return CephInfo(osds=osds,
                        mons=self.load_monitors(),
                        status=self.status,
                        version=self.report.version,
                        pools=name2pool,
                        osd_pool_pg_2d=osd_pool_name_pg_2d,
                        sum_per_pool=sum_per_pool,
                        sum_per_osd=sum_per_osd,
                        cluster_net=IPv4Network(settings['cluster_network'], strict=False),
                        public_net=IPv4Network(settings['public_network'], strict=False),
                        settings=settings,
                        pgs=self.pg_dump,
                        mgrs={},
                        radosgw={},
                        crush=self.crush,
                        log_err_warn=err_wrn,
                        osds4rule=osds4rule,
                        errors_count=errors_count,
                        status_regions=status_regions,
                        report=self.report)

    def load_osd_procinfo(self, osd_id: int) -> OSDProcessInfo:
        # cmdln = [i.decode() for i in self.storage.raw.get_raw(f'osd/{osd_id}/cmdline.bin').split(b'\x00')]

        pinfo = self.storage.json.osd[str(osd_id)].proc_info

        sched = pinfo['sched']
        if not sched:
            data = "\n".join(pinfo['sched_raw'].strip().split("\n")[2:])
            sched = parse_info_file_from_proc(data, ignore_err=True)

        return OSDProcessInfo(cmdline=None,
                              procinfo=pinfo,
                              fd_count=pinfo["fd_count"],
                              opened_socks=int(pinfo.get('sock_count', 0)),
                              th_count=pinfo["th_count"],
                              cpu_usage=float(sched["se.sum_exec_runtime"]),
                              vm_rss=mem2bytes(pinfo["status"]["VmRSS"]),
                              vm_size=mem2bytes(pinfo["status"]["VmSize"]))

    def load_osd_devices(self, osd_id: int, host: Host) -> Union[None, FileStoreInfo, BlueStoreInfo]:
        osd_disks_info = self.osd_dev_cfg.get(osd_id)
        if not osd_disks_info:
            return None

        def get_ceph_dev_info(devpath: Path, partpath: Path) -> CephDevInfo:
            dev_name = devpath.name
            partition_name = partpath.name
            return CephDevInfo(hostname=host.name,
                               dev_info=host.disks[dev_name],
                               partition_info=host.logic_block_devs[partition_name])

        data_dev = get_ceph_dev_info(osd_disks_info.r_data, osd_disks_info.data)

        if osd_disks_info.type == 'filestore':
            j_dev = get_ceph_dev_info(osd_disks_info.r_journal, osd_disks_info.journal)
            return FileStoreInfo(data_dev, j_dev)
        else:
            assert osd_disks_info.type == 'bluestore'
            db_dev = get_ceph_dev_info(osd_disks_info.r_db, osd_disks_info.db)
            wal_dev = get_ceph_dev_info(osd_disks_info.r_wal, osd_disks_info.wal)
            return BlueStoreInfo(data_dev, db_dev, wal_dev)

    def load_osds(self,
                  sum_per_osd: Optional[Dict[int, int]],
                  osdid2rule: Dict[int, List[Tuple[int, float]]]) -> Dict[int, CephOSD]:

        osd_rw_dict = {osd.id: osd.reweight for osd in self.osd_df.nodes}
        osd_df_map = {node.id: node for node in self.osd_df.nodes}
        osds: Dict[int, CephOSD] = {}

        osd2pg: Dict[int, List[PGStat]] = collections.defaultdict(list)
        for pg in self.pg_dump.pg_stats:
            for osd_id in pg.acting:
                osd2pg[osd_id].append(pg)

        osd_perf_dump = {int(vals['id']): vals for vals in self.storage.json.master.osd_perf['osd_perf_infos']}
        osd_vers = {osd_meta.id: osd_meta.ceph_version for osd_meta in self.report.osd_metadata}

        for osd in self.report.osdmap.osds:
            if osd.osd not in osd_df_map:
                logger.warning(f"Osd {osd.osd} has no disk space information - ignoring it")
                continue

            used_space = osd_df_map[osd.osd].kb_used * 1024
            free_space = osd_df_map[osd.osd].kb_avail * 1024

            try:
                cfg_txt = self.storage.txt.osd[str(osd.osd)].config
            except AttributeError:
                try:
                    cfg = self.storage.json.osd[str(osd.osd)].config
                except AttributeError:
                    cfg = None
            else:
                cfg = parse_txt_ceph_config(cfg_txt)

            config = AttredDict(**cfg) if cfg is not None else None

            cluster_ip = osd.cluster_addr.ip
            try:
                host = self.ip2host[cluster_ip]
            except KeyError:
                logger.exception(f"Can't found host for osd {osd.osd}, as no host own {cluster_ip} ip addr")
                raise

            status = OSDStatus.up if osd.up else OSDStatus.down
            storage_info = self.load_osd_devices(osd.osd, host)

            #  CRUSH RULES/WEIGHTS
            crush_rules_weights: Dict[int, float] = {}
            for rule_id, weight in osdid2rule.get(osd.osd, []):
                crush_rules_weights[rule_id] = weight

            pg_stats: Optional[CephIOStats] = None
            osd_pgs: Optional[List[PGStat]] = None

            if osd2pg and osd.osd in osd2pg and len(osd2pg[osd.osd]):
                osd_pgs = []
                pg_stats = osd2pg[osd.osd][0].stat_sum
                for pg in osd2pg[osd.osd][1:]:
                    pg_stats += pg.stat_sum
                    osd_pgs.append(pg)

            perf_cntrs = None if self.osd_perf_counters_dump is None else self.osd_perf_counters_dump.get(osd.osd)

            if free_space + used_space < 1:
                free_perc = None
            else:
                free_perc = int((free_space * 100.0) / (free_space + used_space) + 0.5)

            expected_weights = None if storage_info is None else (storage_info.data.dev_info.size / (1024 ** 4))
            total_space = None if storage_info is None else storage_info.data.dev_info.size
            osd_space = OSDSpace(free_perc=free_perc, used=used_space, total=total_space, free=free_space)

            osds[osd.osd] = CephOSD(id=osd.osd,
                                    status=status,
                                    config=config,
                                    cluster_ip=cluster_ip,
                                    public_ip=osd.public_addr.ip,
                                    reweight=osd_rw_dict[osd.osd],
                                    version=osd_vers.get(osd.osd),
                                    pg_count=None if sum_per_osd is None else sum_per_osd.get(osd.osd, 0),
                                    space=osd_space,
                                    host=host,
                                    storage_info=storage_info,
                                    run_info=self.load_osd_procinfo(osd.osd) if status != OSDStatus.down else None,
                                    expected_weight=expected_weights,
                                    crush_rules_weights=crush_rules_weights,
                                    pgs=osd_pgs,
                                    pg_stats=pg_stats,
                                    osd_perf_counters=perf_cntrs,
                                    osd_perf_dump=osd_perf_dump[osd.osd]['perf_stats'],
                                    class_name=self.crush.bucket_by_id(osd.osd).class_name)

        return osds

    def load_monitors(self) -> Dict[str, CephMonitor]:
        result: Dict[str, CephMonitor] = {}
        # luminous = self.release >= CephRelease.luminous
        for mon in self.status.monmap.mons:
            meta = self.mon_meta[mon.name]
            mon = CephMonitor(name=mon.name,
                              host=self.hosts[meta.hostname],
                              role=MonRole.unknown,
                              version=meta.ceph_version)

            # if luminous:
            #     mon.kb_avail = mon.kb_avail
            #     mon.avail_percent = mon.avail_percent
            # else:
            #     mon_db_root = "/var/lib/ceph/mon"
            #     root = ""
            #     for info in mon.host.logic_block_devs.values():
            #         if info.mountpoint is not None:
            #             if mon_db_root.startswith(info.mountpoint) and len(root) < len(info.mountpoint):
            #                 assert info.free_space is not None
            #                 mon.kb_avail = info.free_space
            #                 mon.avail_percent = info.free_space * 1024 * 100 // info.size
            #                 root = info.mountpoint
            #
            #     try:
            #         ceph_var_dirs_size = self.storage.txt.mon[f'{mon.name}/ceph_var_dirs_size']
            #     except KeyError:
            #         pass
            #     else:
            #         for ln in ceph_var_dirs_size.strip().split("\n"):
            #             sz, name = ln.split()
            #             if '/var/lib/ceph/mon' == name:
            #                 mon.database_size = int(sz)

            result[mon.name] = mon
        return result

    def load_pools(self) -> Dict[int, Pool]:
        df_info: Dict[str, RadosDF.RadosDFPoolInfo] = {pool.name: pool for pool in self.rados_df.pools}
        pools: Dict[int, Pool] = {}
        for pool in self.report.osdmap.pools:
            pools[pool.pool] = Pool(id=pool.pool,
                                    name=pool.pool_name,
                                    size=pool.size,
                                    min_size=pool.min_size,
                                    pg=pool.pg_num,
                                    pgp=pool.pg_placement_num,
                                    crush_rule=pool.crush_rule,
                                    extra=pool,
                                    df=df_info[pool.pool_name],
                                    apps=list(pool.application_metadata.keys()))
        return pools


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

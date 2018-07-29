from typing import List, Any, Dict, Tuple

import numpy

from .cluster_classes import CephOSD, DiskLoad, BlueStoreInfo, HDDsResourceUsage, CephDisksResourceUsage


def hdd_load(all_disks: Dict[Tuple[str, str], DiskLoad], times_len: int,
             node_time_bounds: Dict[str, Tuple[int, int]]) -> HDDsResourceUsage:
    shape = len(all_disks), times_len
    wio = numpy.empty(shape)
    wbytes = numpy.empty(shape)
    qd = numpy.empty(shape)
    rio = numpy.empty(shape)
    rbytes = numpy.empty(shape)

    for idx, ((hostname, dev), load) in enumerate(all_disks.items()):
        idx1, _ = node_time_bounds[hostname]
        idx2 = idx1 + times_len
        wio[idx] = load.write_iops_v[idx1:idx2]
        wbytes[idx] = load.write_bytes_v[idx1:idx2]
        qd[idx] = load.queue_depth_v[idx1:idx2]
        rio[idx] = load.read_iops_v[idx1:idx2]
        rbytes[idx] = load.read_bytes_v[idx1:idx2]

    return HDDsResourceUsage(wio=wio, wbytes=wbytes, rio=rio, rbytes=rbytes, qd=qd)


def get_hdd_resource_usage(perf_data: Any, osds: List[CephOSD]) -> CephDisksResourceUsage:
    collected = {host: host_data['collected_at'] for host, host_data in perf_data.items()}

    # select range, which present on all nodes
    lower = int(max(arr.min() for arr in collected.values()))
    upper = int(min(arr.max() for arr in collected.values()) + 0.999)

    node_bounds = {host: host_data['collected_at'].searchsorted((lower, upper + 1))
                   for host, host_data in perf_data.items()}

    min_sz = min((i2 - i1) for i1, i2 in node_bounds.values())

    data_disks: Dict[Tuple[str, str], DiskLoad] = {}
    journal_disks: Dict[Tuple[str, str], DiskLoad] = {}
    db_disks: Dict[Tuple[str, str], DiskLoad] = {}
    wal_disks: Dict[Tuple[str, str], DiskLoad] = {}

    for osd in osds:
        if osd.storage_info is None or osd.storage_info.data_stor_stats is None:
            # TODO: log warning - load data might be inaccurate
            continue
        data_disks[(osd.host.name, osd.storage_info.data_dev)] = osd.storage_info.data_stor_stats
        if isinstance(osd.storage_info, BlueStoreInfo):
            assert osd.storage_info.db_stor_stats is not None
            db_disks[(osd.host.name, osd.storage_info.db_dev)] = osd.storage_info.db_stor_stats
            assert osd.storage_info.wal_stor_stats is not None
            wal_disks[(osd.host.name, osd.storage_info.wal_dev)] = osd.storage_info.wal_stor_stats
        else:
            assert osd.storage_info.j_stor_stats is not None
            journal_disks[(osd.host.name, osd.storage_info.journal_dev)] = osd.storage_info.j_stor_stats

    data = hdd_load(data_disks, min_sz, node_bounds)
    journal = hdd_load(journal_disks, min_sz, node_bounds)
    db = hdd_load(db_disks, min_sz, node_bounds)
    wal = hdd_load(wal_disks, min_sz, node_bounds)

    return CephDisksResourceUsage(data=data, journal=journal, db=db, wal=wal)

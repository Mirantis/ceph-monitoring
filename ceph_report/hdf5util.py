from __future__ import annotations

import os
import sys
import logging
import argparse
import itertools
import collections
from dataclasses import dataclass
from typing import Iterator, Tuple, Iterable, List, Dict, Any, cast, Optional, Union, MutableMapping

import numpy
import pandas

from cephlib import RecId, to_unix_ms, OpType, get_hl_timings, parse_historic_file

# from . import setup_logging

logger = logging.getLogger("report")


# --- LOADERS AND CONVERTERS  ------------------------------------------------------------------------------------------


StorageType = MutableMapping[str, Union[pandas.DataFrame, pandas.Series]]


@dataclass
class ClusterInfo:
    osd2node: Dict[int, str]
    osd2class: Dict[int, str]
    pool_id2name: Dict[int, str]

    def store(self, storage: StorageType):
        osds = sorted(set(itertools.chain(self.osd2class.keys(), self.osd2node.keys())))
        classes = [self.osd2class.get(osd_id, "") for osd_id in osds]
        nodes = [self.osd2node.get(osd_id, "") for osd_id in osds]
        frame = pandas.DataFrame({"osd_ids": osds,
                                  "osd_class": classes,
                                  "osd_node": nodes})
        storage['osd_params'] = frame

        if self.pool_id2name:
            pools_ids, pools_names = map(list, zip(*self.pool_id2name.items()))
        else:
            pools_ids = []
            pools_names = []

        frame = pandas.DataFrame({"pool_name": pools_names, "pool_id": pools_ids})
        storage['pool_params'] = frame

    @classmethod
    def load(cls, storage: StorageType) -> 'ClusterInfo':
        frame = storage['osd_params']
        osd2node = {itm['osd_ids']: itm['osd_node']
                    for _, itm in frame[['osd_ids', 'osd_node']].iterrows()
                    if itm['osd_node'] != ""}
        osd2class = {itm['osd_ids']: itm['osd_class']
                     for _, itm in frame[['osd_ids', 'osd_class']].iterrows()
                     if itm['osd_class'] != ""}
        frame = storage['pool_params']
        pool_id2name = {itm['pool_id']: itm['pool_name']
                        for _, itm in frame[['pool_id', 'pool_name']].iterrows()
                        if itm['pool_name'] != ""}
        return cls(osd2node, osd2class, pool_id2name)


@dataclass
class LogInfo:
    data: pandas.DataFrame
    info: ClusterInfo
    pg_dump: pandas.DataFrame
    pg_dump_times: List[int]
    pool_info: pandas.DataFrame
    pool_info_times: List[int]

    def store(self, storage: StorageType):
        storage['data'] = self.data.drop(['host', 'pool_name'], axis=1)
        self.info.store(storage)
        storage['pg_dump'] = self.pg_dump
        storage['pg_dump_time'] = pandas.Series(self.pg_dump_times)
        storage['pool_info'] = self.pool_info
        storage['pool_info_times'] = pandas.Series(self.pool_info_times)

    @classmethod
    def load(cls, storage: StorageType) -> 'LogInfo':
        info = ClusterInfo.load(storage)
        data = storage['data']
        pool_names = [info.pool_id2name.get(pool_id) for pool_id in data['pool_id']]
        data['pool_name'] = pandas.Series(pool_names).astype('category')
        data['host'] = pandas.Series([info.osd2node.get(osd_id) for osd_id in data['osd_id']]).astype('category')

        return cls(data=data, info=info,
                   pg_dump=storage['pg_dump'],
                   pg_dump_times=list(storage['pg_dump_time']),
                   pool_info=storage['pool_info'],
                   pool_info_times=list(storage['pool_info_times']))


def load_hdf(fname: str) -> LogInfo:
    with pandas.HDFStore(fname) as fd:
        return LogInfo.load(fd)


def iterate_op_records(fnames: List[str]) -> Iterator[Tuple[RecId, Any]]:
    for fname in fnames:
        logger.info(f"Start processing {fname}")
        with open(fname, 'rb') as fd:
            # TODO: remove this
            logger.warning("Remove hostname hack!")
            yield RecId.params, {'hostname': fname.rsplit('/', 1)[-1].split('.', 1)[0]}
            for rec_id, op in parse_historic_file(fd):
                yield rec_id, op


def convert_to_hdfs(hdf5_target: str, fnames: List[str]):
    info = parse_logs_to_pd(iterate_op_records(fnames))
    with pandas.HDFStore(hdf5_target) as fd:
        info.store(fd)


def extract_from_pgdump(data: Dict[str, Any]) -> Tuple[int, Dict[str, List[int]]]:
    # ctime = to_unix_ms(data['stamp']) // 1000
    ctime = 0
    del data['acting']
    return ctime, data


def extract_from_pool_df(data: Dict[str, Any]) -> Dict[str, Union[List[str], List[int]]]:
    res: Dict[str, List[int]] = collections.defaultdict(list)

    for pool_info in data["pools"]:
        for name, val in pool_info.items():
            res[name].append(val)

    return res


def parse_logs_to_pd(ops_iterator: Iterable[Tuple[RecId, Any]]) -> LogInfo:

    osd_ids: List[int] = []
    durations: List[int] = []
    op_types: List[int] = []
    pool_ids: List[int] = []
    pgs: List[int] = []
    pool_name: List[str] = []
    hosts: List[str] = []

    local_io: List[int] = []
    wait_for_pg: List[int] = []
    download: List[int] = []
    wait_for_replica: List[int] = []
    rec_time: List[int] = []

    supported_types = (OpType.read,
                       OpType.write_secondary,
                       OpType.write_primary)

    pg_info: Dict[str, List[int]] = collections.defaultdict(list)
    pg_dump_times: List[int] = []
    pool_info: Dict[str, Union[List[int], List[int]]] = collections.defaultdict(list)
    pool_info_times: List[int] = []

    pool_id2name: Dict[int, str] = {}
    hostname: Optional[str] = None
    osd2hostname: Dict[int, str] = {}

    prev = 0

    for op_tp, params in ops_iterator:
        if len(durations) // 1000000 != prev // 1000000:
            logger.debug(f"{len(durations) // 1000000:,} millions of records processed")
        prev = len(durations)
        if op_tp == RecId.ops:
            for op in cast(List[Dict[str, Any]], params):
                tp = op['tp']
                if tp not in supported_types:
                    continue

                assert 'pool' in op, str(op)

                if tp in (OpType.write_primary, OpType.write_secondary):
                    if 'wait_for_pg' not in op:
                        download_tm, wait_for_pg_tm, local_io_tm, wait_for_replica_tm = get_hl_timings(tp, op)
                    else:
                        download_tm = op['download']
                        local_io_tm = op['local_io']
                        wait_for_replica_tm = op['wait_for_replica'] if tp is OpType.write_primary else 0

                        wait_for_pg_tm = op['wait_for_pg']
                else:
                    # print(op)
                    continue
                # elif op['tp'] is OpType.read:
                #     download_tm = op.get('download', 0)
                #     wait_for_replica_tm = op.get('wait_for_replica', 0)
                #     local_io_tm = op['local_io']
                #     wait_for_pg_tm = op['wait_for_pg']

                if op['pool'] in pool_id2name:
                    assert pool_id2name[op['pool']] == op['pool_name'], "Pool mapping changed, this case don't handled"
                else:
                    pool_id2name[op['pool']] = op['pool_name']

                assert hostname is not None
                osd2hostname[op['osd_id']] = hostname
                pool_ids.append(op['pool'])
                pgs.append(op['pg'])
                osd_ids.append(op['osd_id'])
                durations.append(op['duration'])
                op_types.append(tp.value)
                local_io.append(local_io_tm)
                wait_for_pg.append(wait_for_pg_tm)
                download.append(download_tm)
                wait_for_replica.append(wait_for_replica_tm)
                rec_time.append(op['time'])
                pool_name.append(op['pool_name'])
                hosts.append(hostname)

        elif op_tp == RecId.params:
            hostname = params['hostname']
        elif op_tp == RecId.pgdump:
            ctime, new_pg_data = extract_from_pgdump(params)
            for name, vals in new_pg_data.items():
                pg_info[name].extend(vals)
            pg_dump_times.append(ctime)
        elif op_tp == RecId.cluster_info:
            if "rados df" in params:
                new_pool_data = extract_from_pool_df(params["rados df"])
                data_len = None
                for name, vals in new_pool_data.items():
                    pool_info[name].extend(vals)
                    if data_len is None:
                        data_len = len(vals)
                    else:
                        assert data_len == len(vals)

                if data_len:
                    pool_info_times.extend([params.get('time', 0)] * data_len)

    logger.info(f"{len(durations):,} total record processed")

    cluster_info = ClusterInfo(osd2node=osd2hostname, osd2class={}, pool_id2name=pool_id2name)
    data = pandas.DataFrame({
        'download': numpy.array(download, dtype=numpy.uint16),
        'wait_for_pg': numpy.array(wait_for_pg, dtype=numpy.uint16),
        'local_io': numpy.array(local_io, dtype=numpy.uint16),
        'wait_for_replica': numpy.array(wait_for_replica, dtype=numpy.uint16),
        'osd_id': numpy.array(osd_ids, dtype=numpy.uint16),
        'duration': numpy.array(durations, dtype=numpy.uint32),
        'op_type': numpy.array(op_types, dtype=numpy.uint8),
        'pool_id': numpy.array(pool_ids, dtype=numpy.uint8),
        'pg': numpy.array(pgs, dtype=numpy.uint32),
        'time': numpy.array(rec_time, dtype=numpy.uint32),
        'pool_name': pandas.Series(pool_name).astype('category'),
        'host': pandas.Series(hosts).astype('category'),
    })

    return LogInfo(data, cluster_info,
                   pg_dump=pandas.DataFrame(pg_info),
                   pg_dump_times=pg_dump_times,
                   pool_info=pandas.DataFrame(pool_info),
                   pool_info_times=pool_info_times)


# ----------------------------------------------------------------------------------------------------------------------


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), help="log level", default='INFO')
    parser.add_argument("--log", help="log file")

    subparsers = parser.add_subparsers(dest='subparser_name')

    dbinfo_parsr = subparsers.add_parser('dbinfo', help="Show database info")
    dbinfo_parsr.add_argument("hdf5_file", help="HDF5 file to read data from")

    tohdf5_parser = subparsers.add_parser('tohdfs', help="Convert binary files to hds")
    tohdf5_parser.add_argument("target", help="HDF5 file path to store to")
    tohdf5_parser.add_argument("sources", nargs="+", help="record files to import")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    # setup_logger(logger, opts.log_level, opts.log)

    if opts.subparser_name == 'tohdfs':
        convert_to_hdfs(opts.target, opts.sources)
        return 0

    if opts.subparser_name == 'dbinfo':
        info = load_hdf(opts.hdf5_file)
        df = info.data
        print("On-disk size ", os.stat(opts.hdf5_file).st_size // 1024 // 1024, "MiB")
        print(df.info(memory_usage='deep'))

        for dtype in ['uint32', 'uint16', 'uint8', 'category']:
            selected_dtype = df.select_dtypes(include=[dtype])
            mean_usage_b = selected_dtype.memory_usage(deep=True).mean()
            mean_usage_mb = mean_usage_b / 1024 ** 2
            print(f"Average memory usage for {dtype} columns: {mean_usage_mb:03.2f} MiB")

        return 0

    print(f"Unknown command {opts.subparser_name}")
    return 1


if __name__ == "__main__":
    exit(main(sys.argv))

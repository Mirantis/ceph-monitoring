import argparse
import asyncio
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Any

from ceph_report.cli import pool_iostat, historic_iostat, historic_set, historic_get_settings

try:
    import stackprinter
except:
    stackprinter = None

import aiorpc.service
from koder_utils import make_storage

from . import get_file, setup_logging
from .collect_info import (do_preconfig, start_historic, status_historic, stop_historic, get_output_folder,
                           collect_historic, remove_historic, get_output_arch, run_collection, ReportFailed,
                           encrypt_and_upload, pack_output_folder)
from .utils import re_checker, CLIENT_NAME_RE, CLUSTER_NAME_RE


logger = logging.getLogger('collect')


def parse_args(argv: List[str]) -> Any:

    root_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparsers = root_parser.add_subparsers(dest='subparser_name')

    # ------------------------------------------------------------------------------------------------------------------
    collect_config = subparsers.add_parser('collect_config', help='Collect data')
    collect_config.add_argument("--config", default=None, type=Path, help="Config path")

    # ------------------------------------------------------------------------------------------------------------------
    collect_parser = subparsers.add_parser('collect', help='Collect data')
    collect_parser.add_argument("--dont-pack-result", action="store_true", help="Don't create archive")
    collect_parser.add_argument("--no-rbd-info", action='store_true', help="Don't collect info for rbd volumes")
    collect_parser.add_argument("--ceph-master-only", action="store_true",
                                help="Run only ceph master data collection, no info from " +
                                "osd/monitors would be collected")
    collect_parser.add_argument("--detect-only", action="store_true",
                                help="Don't collect any data, only detect cluster nodes")
    collect_parser.add_argument("--no-pretty-json", action="store_true", help="Don't prettify json data")
    collect_parser.add_argument("--collect-txt", action="store_true", help="Collect human-readable outputs(txt)")
    collect_parser.add_argument("--collect-rgw", action="store_true", help="Collect radosgw info")
    collect_parser.add_argument("--collect-maps", action="store_true", help="Collect txt/binary osdmap/crushmap")
    collect_parser.add_argument("--ceph-log-max-lines", default=10000, type=int,
                                help="Max lines from osd/mon log (%(default)s)")
    collect_parser.add_argument("--prometheus", default=None, help="Prometheus url to collect data")
    collect_parser.add_argument("--prometheus-interval", default=24 * 7, type=int,
                                help="For how many hours to the past grab data from prometheus")
    collect_parser.add_argument("--historic", action='store_true', help="Collect historic data")

    # ------------------------------------------------------------------------------------------------------------------

    historic_start = subparsers.add_parser('historic_start', help='Upload report to server')
    historic_start.add_argument('--size', metavar='OPS_TO_RECORD', default=200, type=int,
                                help='Collect X slowest requests for each given period')
    historic_start.add_argument('--duration', metavar='SECONDS', default=60, type=int, help='Ceph op keep duration')
    historic_start.add_argument('--min-duration', metavar='MS', default=50, type=int,
                                help='Min operation duration to collect')
    historic_start.add_argument('--max-file-size', metavar='SIZE_MiB', default=1024, type=int,
                                help='Max record file size in MiB')
    historic_start.add_argument('--max-collection-time', metavar='HOURS', default=24, type=int,
                                help='Max time to record in hours')
    historic_start.add_argument('--min-disk-free', metavar='SIZE_GiB', default=50, type=int,
                                help='Min disk free size in GiB to left')

    # ------------------------------------------------------------------------------------------------------------------

    historic_status = subparsers.add_parser('historic_status', help='Show status of historic collection')
    historic_stop = subparsers.add_parser('historic_stop', help='Stop historic collection')
    historic_collect = subparsers.add_parser('historic_collect', help='Download collected historic data')
    historic_remove = subparsers.add_parser('historic_remove', help='Wipe all historic records')

    # ------------------------------------------------------------------------------------------------------------------

    upload = subparsers.add_parser('upload', help='Upload report to server')
    upload.add_argument('--url', required=True, help="Url to upload to")
    upload.add_argument('--key', default=get_file("enc_key.pub"), type=Path,
                        help="Server open key for data encryption (%(default)s)")
    upload.add_argument('--cert', default=get_file("mira_report_storage.crt"), type=Path,
                        help="Storage server cert file (%(default)s)")
    upload.add_argument('--upload-script-path', type=Path,
                        default=get_file("upload.sh"), help="upload.sh path (%(default)s)")
    upload.add_argument('--http-creds', required=True,
                        help="Http user:password, as provided by mirantis support")
    upload.add_argument('report', help="path to report archive")

    # ------------------------------------------------------------------------------------------------------------------

    pool_load = subparsers.add_parser('iostat', help='Show pools load')
    pool_load.add_argument('-t', '--timeout', default=5, type=int, help='Diff timeout')

    historic_iostat = subparsers.add_parser('historic', help='Show slow requests info')
    historic_iostat.add_argument('--size', metavar='OPS_TO_RECORD', default=200, type=int,
                                help='Collect X slowest requests for each given period')
    historic_iostat.add_argument('--duration', metavar='SECONDS', default=60, type=int, help='Ceph op keep duration')
    historic_iostat.add_argument('--min-duration', metavar='MS', default=50, type=int,
                                help='Min operation duration to collect')
    historic_iostat.add_argument('--pool-timeout', metavar='SECONDS', default=None, type=int,
                                help='Pool new data every')
    historic_iostat.add_argument('--history-size', metavar='ticks', default=10, type=int)

    historic_set = subparsers.add_parser('historic_set', help='Set historic settings')
    historic_set.add_argument('--size', metavar='OPS_TO_RECORD', default=20, type=int,
                              help='Collect X slowest requests for each given period')
    historic_set.add_argument('--duration', metavar='SECONDS', default=600, type=int, help='Ceph op keep duration')

    historic_get = subparsers.add_parser('historic_get', help='Get current historic settings')

    # ------------------------------------------------------------------------------------------------------------------
    for parser in (collect_parser, historic_start, historic_stop, historic_collect,
                   historic_status, upload, historic_remove, historic_iostat, pool_load, historic_set, historic_get):
        parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                            help="Console log level, see logging.json for defaults")
        parser.add_argument("--persistent-log", action="store_true",
                            help="Log to /var/log/ceph_report_collector.log as well")
        parser.add_argument("--aiorpc-service-root", default=None, type=Path,
                            help="DIRTY HACK, USE FOR DEV ONLY - RPC agent installation root (%(default)s)")

    # ------------------------------------------------------------------------------------------------------------------

    for parser in (collect_parser, historic_collect):
        parser.add_argument("--cluster", help=f"Cluster name, should match {CLIENT_NAME_RE}",
                            type=re_checker(CLIENT_NAME_RE), required=True)
        parser.add_argument("--customer", help=f"Customer name, should match {CLUSTER_NAME_RE}",
                            type=re_checker(CLUSTER_NAME_RE), required=True)
        parser.add_argument("--output-folder", default=Path('/tmp'), type=Path,
                            help="Folder to put result to (%(default)s)")
        parser.add_argument("--wipe", action='store_true', help="Wipe results directory before store data")

    # ------------------------------------------------------------------------------------------------------------------

    for parser in (collect_parser, historic_start, historic_stop, historic_collect, historic_status, historic_remove,
                   historic_iostat, pool_load, historic_set, historic_get):
        parser.add_argument("--ceph-master", metavar="NODE", default=None,
                            help="Run all ceph cluster commands from NODE, (first inventory node by default)")
        parser.add_argument("--must-connect-to-all", action="store_true",
                            help="Must successfully connect to all ceph nodes")
        parser.add_argument("--extended-tb", action="store_true",
                            help="Show extended tb information")

    # ------------------------------------------------------------------------------------------------------------------
    if '--' in argv:
        ceph_extra_args = argv[argv.index('--'):]
        argv = argv[:argv.index('--') - 1]
    else:
        ceph_extra_args = []

    opts = root_parser.parse_args(argv[1:])
    opts.ceph_extra_args = ceph_extra_args

    return opts


async def historic_main(opts: Any, subparser: str):
    cfg = await do_preconfig(opts)

    async with cfg.conn_pool:
        if subparser == 'historic_start':
            await start_historic(cfg)
        elif subparser == 'historic_status':
            await status_historic(cfg.conn_pool, cfg.inventory)
        elif subparser == 'historic_stop':
            await stop_historic(cfg.conn_pool, cfg.inventory)
        elif subparser == 'historic_collect':
            output_folder = get_output_folder(opts.output_folder, opts.customer, opts.cluster, opts.wipe)
            assert output_folder
            path = output_folder / "historic"
            path.mkdir()
            storage = make_storage(str(path), existing=False, serializer='raw')
            logger.info(f"Collecting historic data to {path}")
            await collect_historic(cfg.conn_pool, cfg.inventory, storage)
        elif subparser == 'historic_remove':
            await remove_historic(cfg.conn_pool, cfg.inventory)
        else:
            raise RuntimeError(f"Unknown cmd {subparser}")


def do_collect(argv: List[str], opts: Any, log_config: Path) -> None:
    try:
        if opts.detect_only:
            output_folder = None
        else:
            output_folder = get_output_folder(opts.output_folder, opts.customer, opts.cluster, opts.wipe)

        if opts.detect_only or opts.dont_pack_result:
            output_arch = None
        else:
            output_arch = get_output_arch(opts.output_folder, opts.customer, opts.cluster)

        setup_logging(log_config, opts.log_level, output_folder, opts.persistent_log)
        logger.info(repr(argv))

        result = asyncio.run(run_collection(opts, output_folder, output_arch))

        if output_folder:
            if opts.dont_pack_result:
                logger.warning("Unpacked tree is kept as --dont-pack-result option is set, so no archive created")
                print(f"Result saved into {output_folder}")
            else:
                assert output_arch is not None
                assert output_folder is not None
                pack_output_folder(output_folder, output_arch)
                print(f"Result saved into {output_arch}")
                shutil.rmtree(output_folder)

        if result:
            res_str = "\n".join(f"       {key:>20s}: {val!r}" for key, val in result.items())
            if res_str.strip():
                logger.debug(f"Results:\n{res_str}")

    except Exception:
        logger.exception("During make_storage/collect")
        raise


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    log_config = get_file("logging.json")

    if opts.extended_tb:
        if stackprinter:

            def hook(*args):
                msg = stackprinter.format(args, style='color')
                print(msg)
                logger.critical(stackprinter.format(args, style='plaintext'))

            sys.excepthook = hook
        else:
            logger.error("Can't set extended tb, as no module 'stackprinter' installed ")

    # dirty hack to simplify development
    if hasattr(opts, 'aiorpc_service_root') and opts.aiorpc_service_root:
        aiorpc.service.INSTALL_PATH = opts.aiorpc_service_root

    if opts.subparser_name == 'collect_config':
        # make opts from config
        opts.subparser_name = 'collect'

    setup_logging(get_file("logging.json"), opts.log_level, None, False)

    try:
        if opts.subparser_name == 'collect':
            try:
                do_collect(argv, opts, log_config)
            except ReportFailed:
                return 1
        else:
            setup_logging(log_config, opts.log_level, None, opts.persistent_log)
            if opts.subparser_name == 'upload':
                try:
                    encrypt_and_upload(url=opts.url,
                                       report_file=opts.report,
                                       key_file=opts.key,
                                       web_cert_file=opts.cert,
                                       http_user_password=opts.http_creds)
                except subprocess.CalledProcessError:
                    pass
            elif opts.subparser_name in ('historic_start', 'historic_status',
                                         'historic_stop', 'historic_collect', 'historic_remove'):
                asyncio.run(historic_main(opts, opts.subparser_name))
            elif opts.subparser_name == 'iostat':
                asyncio.run(pool_iostat(opts.ceph_master, opts.timeout))
            elif opts.subparser_name == 'historic':
                asyncio.run(historic_iostat(opts))
            elif opts.subparser_name == 'historic_set':
                asyncio.run(historic_set(opts))
            elif opts.subparser_name == 'historic_get':
                asyncio.run(historic_get_settings(opts))
            else:
                print("Not implemented", file=sys.stderr)
                return 1
    except KeyboardInterrupt:
        return 1

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

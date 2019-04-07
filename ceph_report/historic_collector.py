import argparse
import asyncio
import logging
from typing import List, Any


ALLOWED_LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR']


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=ALLOWED_LOG_LEVELS, help="log level", default='INFO')
    parser.add_argument("--log", help="log file")

    subparsers = parser.add_subparsers(dest='subparser_name')

    set_parser = subparsers.add_parser('set', help="config osd's historic ops")
    set_parser.add_argument("--duration", required=True, type=int, help="Duration to keep")
    set_parser.add_argument("--size", required=True, type=int, help="Num request to keep")

    subparsers.add_parser('set_default', help="config osd's historic ops to default 20/600")

    record_parser = subparsers.add_parser('record', help="Dump osd's requests periodically")
    record_parser.add_argument("--http-server-addr", default=None, help="Addr for status http server")
    record_parser.add_argument("--duration", required=True, type=int, help="Duration to keep")
    record_parser.add_argument("--size", required=True, type=int, help="Num request to keep")

    record_parser.add_argument("--record-file-size-max-mb", default=1024, type=int, help="Max record file")
    record_parser.add_argument("--record-max-hours", default=7 * 24, type=int, help="Max hours to record")
    record_parser.add_argument("--min-free-disk-space-gb", default=50, type=int, help="Minimal disk free space to keep")

    record_parser.add_argument("--timeout", type=int, default=30, help="Timeout to run cli cmds")

    assert CompactPacker in ALL_PACKERS
    record_parser.add_argument("--packer", default=CompactPacker.name,
                               choices=[packer.name for packer in ALL_PACKERS], help="Select command packer")
    record_parser.add_argument("--min-duration", type=int, default=30,
                               help="Minimal duration in ms for op to be recorded")
    record_parser.add_argument("--record-cluster", type=int, help="Record cluster info every SECONDS seconds",
                               metavar='SECONDS', default=0)
    record_parser.add_argument("--record-pg-dump", type=int, help="Record cluster pg dump info every SECONDS seconds",
                               metavar='SECONDS', default=0)
    record_parser.add_argument("--compress-each", type=int, help="Compress each KB kilobytes of record file",
                               metavar='KiB', default=1024)
    record_parser.add_argument("--dump-unparsed-headers", action='store_true')
    record_parser.add_argument("output_file", help="Filename to append requests logs to it")

    parse_parser = subparsers.add_parser('parse', help="Parse records from file")
    parse_parser.add_argument("-l", "--limit", default=None, type=int, metavar="COUNT", help="Parse only COUNT records")
    parse_parser.add_argument("file", help="Log file")

    return parser.parse_args(argv[1:])


def setup_logger(configurable_logger: logging.Logger, log_level: str, log: str = None) -> None:
    assert log_level in ALLOWED_LOG_LEVELS
    configurable_logger.setLevel(getattr(logging, log_level))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, log_level))
    ch.setFormatter(formatter)

    if log:
        fh = logging.FileHandler(log)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        configurable_logger.addHandler(fh)

    configurable_logger.addHandler(ch)


async def async_main(loop: asyncio.AbstractEventLoop, opts: Any) -> int:
    if opts.subparser_name in ('set', 'set_default'):
        if opts.subparser_name == 'set':
            duration = opts.duration
            size = opts.size
        else:
            duration = DEFAULT_DURATION
            size = DEFAULT_SIZE
        osd_ids = await get_local_osds()
        failed_osds = await set_size_duration(osd_ids, duration=duration, size=size)
        if failed_osds:
            logger.error("Fail to set time/duration for next osds: %s", " ,".join(map(str, osd_ids)))
            return 1
        return 0
    else:
        assert opts.subparser_name == 'record'
        return await record_to_file(loop, opts)


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    setup_logger(logger, opts.log_level, opts.log)

    if opts.subparser_name == 'parse':
        print_records_from_file(opts.file, opts.limit)
        return 0

    loop = asyncio.get_event_loop()
    try:
        return loop.run_until_complete(async_main(loop, opts))
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    exit(main(sys.argv))

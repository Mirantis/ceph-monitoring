#!/usr/bin/env python3.5
import argparse
import json
import sys
import subprocess
import time
from pathlib import Path
import configparser
import logging
import logging.config
from typing import List, Any

from .collect_from_cfg import to_args


logger = logging.getLogger("service")
DONT_UPLOAD = 'dont_upload'
NO_CREDS = 'no_creds'


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("config", default="/etc/mirantis/ceph_report.cfg", help="Config file path  (%(default)s)")
    return parser.parse_args(argv[1:])


def main(argv: List[str]):
    opts = parse_args(argv)

    cfg = configparser.ConfigParser()
    cfg.read(opts.config)
    svc_config = cfg['service']
    logging.config.dictConfig(json.load(open(svc_config['logging'])))

    logger.info(f"Started with {argv}")

    report_every = int(svc_config['report_every'])
    upload_timeout = int(svc_config['upload_timeout'])
    report_timeout = int(svc_config['report_timeout'])

    upload_args = to_args(cfg['upload'])

    next_time = time.time()

    while True:
        sleep_time = next_time - time.time()
        time.sleep(sleep_time if sleep_time > 0 else 0)
        next_time += report_every
        try:
            cmd = [sys.executable, '-m', 'ceph_report.collect_from_cfg']
            logger.info(f"Started collecting with {cmd}, timeout={report_timeout}")

            res = subprocess.run(cmd, timeout=report_timeout, stdout=subprocess.PIPE)

            res.check_returncode()
            marker = "Will store results into"
            stdout = res.stdout.decode()

            if marker not in stdout:
                continue

            report = stdout[stdout.index(marker) + len(marker):].strip().split("\n")[0]
            report_path = Path(report)
            logger.info(f"Get new report {report_path}")

            slink_path = report_path.parent / 'not_uploaded' / report_path.name
            if not slink_path.parent.exists():
                slink_path.parent.mkdir(parents=True)

            slink_path.symlink_to(report_path)

            if cfg['upload']['url'] != DONT_UPLOAD:
                cmd = [sys.executable, "-m", "ceph_report.collect_info", "upload", *upload_args, str(report_path)]
                logger.info(f"Start upload with {cmd}, timeout={report_path}")
                subprocess.run(cmd, timeout=upload_timeout).check_returncode()
                logger.info("Upload successful")
                slink_path.unlink()
            else:
                logger.info("Skipping uploading, as it's disabled")

        except subprocess.TimeoutExpired:
            logger.error("Timeout expired")
        except subprocess.CalledProcessError as exc:
            logger.error(f"Child process failed with code {exc.returncode}")


if __name__ == "__main__":
    main(sys.argv)

import argparse
import sys
import subprocess
import time
from pathlib import Path
import logging.config
from typing import List, Any

from . import get_config, setup_logging, get_file


logger = logging.getLogger("service")
DONT_UPLOAD = 'dont_upload'
NO_CREDS = 'no_creds'


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--config", default=None, help="Config file path")
    return parser.parse_args(argv[1:])


def main(argv: List[str]):
    opts = parse_args(argv)
    cfg = get_config(opts.config)
    setup_logging(get_file("logging.json"), cfg.log_level, None, cfg.persistent_log)

    logger.info(f"Started with {argv}")

    upload_args = ["--url", cfg.url, "--http-creds", cfg.http_creds]

    next_time = time.time()

    while True:
        sleep_time = next_time - time.time()
        time.sleep(sleep_time if sleep_time > 0 else 0)
        next_time += cfg.report_every
        try:
            cmd = [sys.executable, '-m', 'ceph_report.collect_info', 'collect', '--config', str(cfg.cfg_file)]
            logger.info(f"Started collecting with {cmd}, timeout={cfg.report_timeout}")

            res = subprocess.run(cmd, timeout=cfg.report_timeout, stdout=subprocess.PIPE)

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

            if cfg.url != DONT_UPLOAD:
                cmd = [sys.executable, "-m", "ceph_report.collect_info", "upload", *upload_args, str(report_path)]
                logger.info(f"Start upload with {cmd}, timeout={report_path}")
                subprocess.run(cmd, timeout=cfg.upload_timeout).check_returncode()
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

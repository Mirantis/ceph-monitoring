#!/usr/bin/env python3.5
import argparse
import sys
import subprocess
import time
from pathlib import Path


def parse_args(argv):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--report-every", type=int, default=3600)
    parser.add_argument("--report-timeout", type=int, default=600)
    parser.add_argument("--upload-timeout", type=int, default=1200)
    parser.add_argument("--reporter")
    parser.add_argument("--agent")
    return parser.parse_args(argv[1:])


def main(argv):
    assert argv.count('--') == 2
    my_args = argv[:argv.index("--")]
    collector_and_uploader_args = argv[argv.index("--") + 1:]
    collector_args = collector_and_uploader_args[:collector_and_uploader_args.index("--")]
    uploader_args = collector_and_uploader_args[collector_and_uploader_args.index("--") + 1:]

    opts = parse_args(my_args)
    service_path = Path(opts.reporter)
    agent_path = Path(opts.agent)
    next_time = time.time()

    cert_folder = agent_path / 'agent_client_keys'

    while True:
        sleep_time = next_time - time.time()
        time.sleep(sleep_time if sleep_time > 0 else 0)
        next_time += opts.report_every
        try:
            res = subprocess.run([sys.executable, str(service_path / 'ceph_report/run.py'),
                                  str(service_path), str(agent_path), '--',
                                  'collect',
                                  "--api-key", str(cert_folder / "agent_api.key"),
                                  "--certs-folder", str(cert_folder),
                                  *collector_args],
                                 timeout=opts.report_timeout,
                                 stdout=subprocess.PIPE)

            res.check_returncode()
            marker = "Will store results into"
            stdout = res.stdout.decode("utf8")

            if marker not in stdout:
                continue

            report_path = stdout[stdout.index(marker):].strip().split("\n")[0]

            subprocess.run(
                [sys.executable, str(service_path / 'ceph_report/run.py'),
                 str(service_path), str(agent_path), '--',
                 'upload',
                 *uploader_args,
                 report_path],
                timeout=opts.upload_timeout).check_returncode()
        except subprocess.TimeoutExpired as exc:
            print(exc)
            print(" ".join(exc.cmd))
        except subprocess.CalledProcessError as exc:
            print(exc)
            print(" ".join(map(str, exc.cmd)))


if __name__ == "__main__":
    main(sys.argv)

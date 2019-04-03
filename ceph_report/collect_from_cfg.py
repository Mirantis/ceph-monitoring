import argparse
import configparser
import os
import sys
from pathlib import Path
from typing import Dict, List, Any


def to_args(section: Dict[str, str]) -> List[str]:
    res = []
    for key, val in section.items():
        if val in ('true', 'True', True, 'yes'):
            res.append('--' + key.replace("_", '-'))
        elif val in ('false', 'False', False, 'no'):
            pass
        else:
            res.append('--' + key.replace("_", '-'))
            res.append(str(val))
    return res


def prepare_collect_cmd(config: configparser.ConfigParser) -> List[str]:
    agent_path = Path(config['service']['agent'])
    cert_folder = agent_path / 'agent_client_keys'

    return ["-m", "ceph_report.collect_info", "collect",
            "--api-key", str(cert_folder / "agent_api.key"),
            "--certs-folder", str(cert_folder)] + to_args(config['collect'])


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--output-folder", default=None, help="Folder to save report to")
    parser.add_argument("config", default="/etc/mirantis/ceph_report.cfg", help="Config file path  (%(default)s)")
    return parser.parse_args(argv[1:])


def main(argv: List[str]):
    opts = parse_args(argv)
    cfg = configparser.ConfigParser()
    cfg.read(opts.config)
    if opts.output_folder:
        cfg.set('collect', 'output_folder', opts.output_folder)

    os.execve(sys.executable, [sys.executable] + prepare_collect_cmd(cfg), os.environ)


if __name__ == "__main__":
    main(sys.argv)

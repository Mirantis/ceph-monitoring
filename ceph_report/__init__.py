import json
import logging.config
from pathlib import Path
from typing import Optional

from .ceph_loader import CephLoader
from .cluster import Cluster, load_all, fill_usage, fill_cluster_nets_roles
from .checks import CheckMessage
from .report import Report


files_folder = Path(__file__).resolve().parent.parent / 'ceph_report_files'


def get_file(name: str) -> Path:
    return files_folder / name


def setup_logging(log_config_file: Path, log_level: str, out_folder: Optional[Path], persistent_log: bool = False):
    log_config = json.load(log_config_file.open())
    handlers = ["console"]

    if out_folder:
        handlers.append("log_file")
        log_file = out_folder / "log.txt"
        log_config["handlers"]["log_file"]["filename"] = str(log_file)
    else:
        del log_config["handlers"]["log_file"]

    if persistent_log:
        handlers.append("persistent_log_file")
    else:
        del log_config["handlers"]["persistent_log_file"]

    if log_level is not None:
        log_config["handlers"]["console"]["level"] = log_level

    for key in list(log_config['loggers']):
        log_config['loggers'][key]["handlers"] = handlers

    logging.config.dictConfig(log_config)


import configparser
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict
import logging.config


@dataclass
class ReporterConfig:
    cfg_file: Path
    root: Path
    agent: Path
    report_every: int
    report_timeout: int
    upload_timeout: int
    inventory: Path
    ceph_master: str
    storage : Path
    log_level: str
    persistent_log: bool
    cluster: str
    customer: str
    prometeus_url: Optional[str]
    prometheus_interval: Optional[int]
    url: Optional[str]
    http_creds: Optional[str]
    size: int
    duration: int
    min_duration: int


def get_config(path: Optional[Path]) -> ReporterConfig:
    cfg = configparser.ConfigParser()
    if not path:
        path = get_file('config.cfg')

    if not path.exists():
        raise RuntimeError(f"Can't find config file at {path}")

    cfg.read(path.open())
    common = cfg['common']
    if common['root'] == 'AUTO':
        path_formatters: Dict[str, str] = {'root': str(Path(__file__).parent.parent)}
    else:
        path_formatters = {'root': common['root']}

    path_formatters['agent'] = common['agent']

    def mkpath(val: str) -> Path:
        return Path(val.format(**path_formatters))

    return ReporterConfig(
        cfg_file=path,
        root=Path(common['root']),
        agent=Path(common['agent']),
        report_every=cfg.getint('service', 'report_every'),
        report_timeout=cfg.getint('service', 'report_timeout'),
        upload_timeout=cfg.getint('service', 'upload_timeout'),
        inventory=mkpath(cfg['collect']['inventory']),
        ceph_master=cfg['collect']['ceph_master'],
        storage=mkpath(cfg['collect']['storage']),
        log_level=common['log_level'],
        persistent_log=common['persistent_log'],
        cluster=cfg['collect']['cluster'],
        customer=cfg['collect']['customer'],
        url=cfg.get('upload', 'url', fallback=None),
        http_creds=cfg.get('upload', 'http_creds', fallback=None),
        prometeus_url=cfg.get('collect', 'prometeus_url', fallback=None),
        prometheus_interval=cfg.get('collect', 'prometheus_interval', fallback=None),
        duration=cfg.getint('historic', 'duration', fallback=None),
        size=cfg.getint('historic', 'size', fallback=None),
        min_duration=cfg.getint('historic', 'min_duration', fallback=None),
    )


FILES_DIRECTORY = Path(__file__).resolve().parent.parent / 'files'


def get_file(name: str) -> Path:
    return FILES_DIRECTORY / name


def setup_logging(log_config_file: Path, log_level: str, out_folder: Optional[str], persistent_log: bool = False):
    log_config = json.load(log_config_file.open())
    handlers = ["console"]

    if out_folder:
        handlers.append("log_file")
        log_file = os.path.join(out_folder, "log.txt")
        log_config["handlers"]["log_file"]["filename"] = log_file
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

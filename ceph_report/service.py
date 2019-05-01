import sys
import site
import time
import argparse
import subprocess
from typing import List, Any, cast
import configparser
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict
import logging.config

from . import get_file, setup_logging
from .utils import CLUSTER_NAME_RE, CLIENT_NAME_RE, re_checker


logger = logging.getLogger("service")
DONT_UPLOAD = 'dont_upload'
NO_CREDS = 'no_creds'


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
    storage: Path
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
        persistent_log=cfg.getboolean('common', 'persistent_log', fallback=False),
        cluster=cfg['collect']['cluster'],
        customer=cfg['collect']['customer'],
        url=cfg.get('upload', 'url', fallback=None),  # type: ignore
        http_creds=cfg.get('upload', 'http_creds', fallback=None),  # type: ignore
        prometeus_url=cfg.get('collect', 'prometeus_url', fallback=None),  # type: ignore
        prometheus_interval=cfg.getint('collect', 'prometheus_interval', fallback=None),  # type: ignore
        duration=cfg.getint('historic', 'duration', fallback=None),  # type: ignore
        size=cfg.getint('historic', 'size', fallback=None),  # type: ignore
        min_duration=cfg.getint('historic', 'min_duration', fallback=None),  # type: ignore
    )


def configure(opts: Any) -> None:
    params = {
        "CEPH_MASTER": opts.ceph_master,
        "UPLOAD_URL": opts.upload_url,
        "HTTP_CREDS": opts.http_creds,
        "CLUSTER": opts.cluster,
        "CUSTOMER": opts.customer,
        "PROMETEUS_URL": opts.customer,
        "PROMETEUS_INTERVAL": opts.customer,
    }

    config_file_content = get_file('config_templ.cfg').open().read()
    for name, val in params.items():
        config_file_content = config_file_content.replace("{" + name + "}", val)

    get_file("config.cfg").open("w").write(config_file_content)

    cfg = get_config(None)

    storage = Path(opts.storage)
    storage.mkdir(parents=True, exist_ok=True)

    for site_dir in map(Path, site.getsitepackages()):
        if site_dir.is_dir():
            (site_dir / 'ceph_report.pth').open("w").write(f"{cfg.root}\n{cfg.root / 'libs'}\n")
            break
    else:
        raise RuntimeError("Can't install pth file - no site folder found")


def install_service(svc_name: str, svc_target: Path, opts: Any) -> None:
    cfg = get_config(opts)
    params = {"INSTALL_FOLDER": str(cfg.root), "CONF_FILE": str(cfg.cfg_file)}

    service_file_content = get_file(svc_name).open().read()
    for name, val in params.items():
        service_file_content = service_file_content.replace("{" + name + "}", val)

    if not svc_target.parent.exists():
        svc_target.parent.mkdir(parents=True)

    svc_target.open("w").write(service_file_content)
    subprocess.run(["systemctl", "daemon-reload"]).check_returncode()

    if not opts.dont_enable:
        subprocess.run(["systemctl", "enable", svc_name]).check_returncode()

    if not opts.dont_start:
        subprocess.run(["systemctl", "start", svc_name]).check_returncode()


def unistall_service(svc_name: str, svc_target: Path) -> None:
    if subprocess.run(["systemctl", "status", svc_name], stdout=subprocess.DEVNULL).returncode == 0:
        subprocess.run(["systemctl", "disable", svc_name]).check_returncode()
        subprocess.run(["systemctl", "stop", svc_name]).check_returncode()
        svc_target.unlink()
        subprocess.run(["systemctl", "daemon-reload"]).check_returncode()


def parse_args(argv: List[str]) -> Any:

    class Formatter(argparse.ArgumentDefaultsHelpFormatter):
        def __init__(self, *args, **kwargs) -> None:
            kwargs['width'] = 120
            argparse.ArgumentDefaultsHelpFormatter.__init__(self, *args, **kwargs)

    parser = argparse.ArgumentParser(formatter_class=Formatter)

    sub_parsers = parser.add_subparsers(dest='subparser_name')

    configure_parser = sub_parsers.add_parser("configure")
    configure_parser.add_argument("--config", default=None, help="Config file")
    configure_parser.add_argument("--ceph-master", default='-',
                                  help="Node to run whole-cluster collection, first inventory node by default")
    configure_parser.add_argument("--http-creds", default=NO_CREDS,
                                  help="Upload HTTP credentials, must be provided if --upload-url is provided")
    configure_parser.add_argument("--upload-url", default=DONT_UPLOAD,
                                  help="Report upload URL, if not provided - reports would not be uploaded")
    configure_parser.add_argument("--cluster", required=True, type=re_checker(CLUSTER_NAME_RE),
                                  help="Cluster name. Should create unique pair " +
                                       f"with --customer and match {CLUSTER_NAME_RE}")
    configure_parser.add_argument("--customer", required=True, type=re_checker(CLIENT_NAME_RE),
                                  help=f"Customer name, should be unique and match {CLIENT_NAME_RE}")
    configure_parser.add_argument("--no-service", action='store_true', default=False, help="Don't install service")
    configure_parser.add_argument("--prometeus-url", default="", help="Prometheus URL to pull data")
    configure_parser.add_argument("--prometeus-interval", default=0, type=int, metavar="HOURS",
                                  help="Prometheus data pull hor how many hours")

    install_parser = sub_parsers.add_parser("install_service")
    install_parser.add_argument("--dont-start", action='store_true', default=False, help="Don't start service")
    install_parser.add_argument("--dont-enable", action='store_true', default=False,
                                help="Don't enable auto start on boot")

    sub_parsers.add_parser("uninstall_service")

    run_parser = sub_parsers.add_parser("run")
    run_parser.add_argument("--config", default=None, help="Config file path")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    svc_name = 'mirantis_ceph_reporter.service'
    svc_target = Path("/lib/systemd/system") / svc_name

    if opts.subparser_name == 'configure':
        configure(opts)
    elif opts.subparser_name == 'install_service':
        install_service(svc_name, svc_target, opts)
    elif opts.subparser_name == 'uninstall_service':
        unistall_service(svc_name, svc_target)
    elif opts.subparser_name == 'run':
        cfg = get_config(opts.config)
        setup_logging(get_file("logging.json"), cfg.log_level, None, cfg.persistent_log)

        logger.info(f"Started with {argv}")

        assert cfg.url
        assert cfg.http_creds

        upload_args: List[str] = ["--url", cfg.url, "--http-creds", cfg.http_creds]  # type: ignore

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
    else:
        print(f"Unknown cmd {opts.subparser_name}")
        return 1
    return 0


if __name__ == "__main__":
    main(sys.argv)

import sys
import site
import argparse
import subprocess
from pathlib import Path
from typing import List, Any

from . import get_file, get_config
from .service import DONT_UPLOAD, NO_CREDS
from .utils import CLUSTER_NAME_RE, CLIENT_NAME_RE, re_checker


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
    parser = argparse.ArgumentParser(
        formatter_class=lambda *args, **kwargs: argparse.ArgumentDefaultsHelpFormatter(*args, **kwargs, width=120))

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

    install_service = sub_parsers.add_parser("install_service")
    install_service.add_argument("--dont-start", action='store_true', default=False, help="Don't start service")
    install_service.add_argument("--dont-enable", action='store_true', default=False,
                                 help="Don't enable auto start on boot")

    sub_parsers.add_parser("uninstall_service")

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
    else:
        print(f"Unknown cmd {opts.subparser_name}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

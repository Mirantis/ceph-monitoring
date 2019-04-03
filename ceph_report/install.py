import shutil
import sys
import argparse
import subprocess
from pathlib import Path
from typing import List, Any

try:
    import agent
except ImportError:
    agent = None

from .service import DONT_UPLOAD, NO_CREDS
from .utils import CLUSTER_NAME_RE, CLIENT_NAME_RE, re_checker, get_file


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(
        formatter_class=lambda *args, **kwargs: argparse.ArgumentDefaultsHelpFormatter(*args, **kwargs, width=120))

    sub_parsers = parser.add_subparsers(dest='subparser_name')

    install_parser = sub_parsers.add_parser("install")
    install_parser.add_argument("--inventory", required=True,
                                help="Inventory file")
    install_parser.add_argument("--ceph-master", default='-',
                                help="Node to run whole-cluster collection, first inventory node by default")
    install_parser.add_argument("--http-creds", default=NO_CREDS,
                                help="Upload HTTP credentials, must be provided if --upload-url is provided")
    install_parser.add_argument("--upload-url", default=DONT_UPLOAD,
                                help="Report upload URL, if not provided - reports would not be uploaded")
    install_parser.add_argument("--cluster", required=True, type=re_checker(CLUSTER_NAME_RE),
                                help="Cluster name. Should create unique pair " +
                                f"with --customer and match {CLUSTER_NAME_RE}")
    install_parser.add_argument("--customer", required=True, type=re_checker(CLIENT_NAME_RE),
                                help=f"Customer name, should be unique and match {CLIENT_NAME_RE}")
    if agent:
        install_parser.add_argument("--agent-dir", default=str(Path(agent.__file__).parent),
                                    help="RPC agent installation directory (%(default)s)")
    else:
        install_parser.add_argument("--agent-dir", required=True, help="RPC agent installation directory")

    install_parser.add_argument("--storage", default='/var/lib/ceph-report',
                                help="Reports storage folder (%(default)s)")
    install_parser.add_argument("--config-folder", default='/etc/mirantis',
                                help="Config folder (%(default)s)")
    install_parser.add_argument("--no-service", action='store_true', default=False, help="Don't install service")
    install_parser.add_argument("--dont-start", action='store_true', default=False, help="Don't start service")
    install_parser.add_argument("--dont-enable", action='store_true', default=False,
                                help="Don't enable auto start on boot")
    install_parser.add_argument("--prometeus-url", default=None, help="Prometheus URL to pull data")

    return parser.parse_args(argv[1:])


def install(svc_name: str, svc_target: Path, config_folder: Path, opts: Any) -> None:

    config_file = config_folder / "ceph_report.cfg"
    log_config_file = config_folder / 'service_log_config.json'
    reporter_folder = Path(__file__).resolve().parent.parent

    paths = [
        Path(opts.agent_dir).parent,
        Path(opts.agent_dir).parent / 'libs',
        reporter_folder,
        reporter_folder / 'libs'
    ]

    paths_s = ":".join(map(str, paths))

    params = {
        "INVENTORY_FILE": opts.inventory,
        "MASTER_NODE": opts.ceph_master,
        "UPLOAD_URL": opts.upload_url,
        "HTTP_CREDS": opts.http_creds,
        "CLUSTER": opts.cluster,
        "CUSTOMER": opts.customer,
        "INSTALL_FOLDER": str(reporter_folder),
        "AGENT_FOLDER": str(Path(opts.agent_dir).parent),
        "CONF_FILE": str(config_file),
        "STORAGE_FOLDER": opts.storage,
        "LOGGING_CONFIG": str(log_config_file),
        "PYTHONPATH": paths_s,
        "PYTHONHOME": str(Path(sys.executable).parent)
    }

    config_file_content = get_file('config.cfg').open().read()
    for name, val in params.items():
        config_file_content = config_file_content.replace("{" + name + "}", val)

    if not config_folder.exists():
        config_folder.mkdir(parents=True)

    config_file.open("w").write(config_file_content)
    shutil.copyfile(str(get_file('service_log_config.json')), str(log_config_file))

    storage = Path(opts.storage)
    if not storage.exists():
        storage.mkdir(parents=True)

    if not opts.no_service:
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


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    svc_name = 'mirantis_ceph_reporter.service'
    svc_target = Path("/lib/systemd/system") / svc_name

    if opts.subparser_name == 'install':
        install(svc_name, svc_target, Path(opts.config_folder), opts)
    else:
        assert opts.subparser_name == 'uninstall'
        if subprocess.run(["systemctl", "status", svc_name]).returncode == 0:
            subprocess.run(["systemctl", "disable", svc_name]).check_returncode()
            subprocess.run(["systemctl", "stop", svc_name]).check_returncode()
            svc_target.unlink()
            subprocess.run(["systemctl", "daemon-reload"]).check_returncode()

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

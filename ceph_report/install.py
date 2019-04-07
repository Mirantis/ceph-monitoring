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


def configure(config_folder: Path, opts: Any) -> None:

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
        "LOG_FOLDER": str(opts.log_folder),
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

    svc_config = get_file('service_log_config.json').open().read().replace('{LOG_FOLDER}', str(opts.log_folder))
    log_config_file.open("w").write(svc_config)

    collector_config = get_file('logging.json').open().read().replace('{LOG_FOLDER}', str(opts.log_folder))
    get_file('logging.json').open("w").write(collector_config)

    storage = Path(opts.storage)
    if not storage.exists():
        storage.mkdir(parents=True)


def install_service(svc_name: str, svc_target: Path, config_folder: Path, opts: Any) -> None:

    config_file = config_folder / "ceph_report.cfg"
    reporter_folder = Path(__file__).resolve().parent.parent

    params = {"INSTALL_FOLDER": str(reporter_folder), "CONF_FILE": str(config_file)}

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
    configure_parser.add_argument("--inventory", required=True,
                                help="Inventory file")
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
    if agent:
        configure_parser.add_argument("--agent-dir", default=str(Path(agent.__file__).parent),
                                    help="RPC agent installation directory (%(default)s)")
    else:
        configure_parser.add_argument("--agent-dir", required=True, help="RPC agent installation directory")

    configure_parser.add_argument("--storage", default='/var/lib/ceph_report',
                                help="Reports storage folder (%(default)s)")
    configure_parser.add_argument("--config-folder", default='/etc/mirantis',
                                help="Config folder (%(default)s)")
    configure_parser.add_argument("--no-service", action='store_true', default=False, help="Don't install service")
    configure_parser.add_argument("--log-folder", default='/var/log/ceph_report', help="Folder to store logs to")
    configure_parser.add_argument("--prometeus-url", default=None, help="Prometheus URL to pull data")

    install_service = sub_parsers.add_parser("install_service")
    install_service.add_argument("--config-folder", default='/etc/mirantis',
                                 help="Config folder (%(default)s)")
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
        configure(Path(opts.config_folder), opts)
    elif opts.subparser_name == 'install_service':
        install_service(svc_name, svc_target, Path(opts.config_folder), opts)
    elif opts.subparser_name == 'uninstall_service':
        unistall_service(svc_name, svc_target)
    else:
        print(f"Unknown cmd {opts.subparser_name}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

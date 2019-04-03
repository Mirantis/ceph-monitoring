import shutil
import sys
import argparse
import subprocess
from pathlib import Path
from typing import List, Any

from .service import DONT_UPLOAD, NO_CREDS
from .utils import CLUSTER_NAME_RE, CLIENT_NAME_RE, re_checker

BASE_FILE_PATH = Path(sys.argv[0]).parent.parent
DEFAULT_AGENT_PATH = BASE_FILE_PATH.parent / 'agent'


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sub_parsers = parser.add_subparsers(dest='subparser_name')

    install_parser = sub_parsers.add_parser("install")
    install_parser.add_argument("--inventory", required=True,
                                help="Inventory file")
    install_parser.add_argument("--ceph-master", default='-',
                                help="Node to run whole-cluster collection, first inventory node by default")
    install_parser.add_argument("--http-creds", default=NO_CREDS,
                                help="Upload HTTP credentials, if not provided --upload-url should not be set as well")
    install_parser.add_argument("--upload-url", default=DONT_UPLOAD,
                                help="Report upload URL, if not provided - reports would not be uploaded")
    install_parser.add_argument("--cluster", required=True, type=re_checker(CLUSTER_NAME_RE),
                                help="Cluster name. Should create unique pair " +
                                f"with --customer and match {CLUSTER_NAME_RE}")
    install_parser.add_argument("--customer", required=True, type=re_checker(CLIENT_NAME_RE),
                                help=f"Customer name, should be unique and match {CLIENT_NAME_RE}")
    install_parser.add_argument("--agent-dir", default=str(DEFAULT_AGENT_PATH),
                                help="RPC agent installation directory (%(default)s)")
    install_parser.add_argument("--storage", default='/var/lib/ceph-report',
                                help="Reports storage folder (%(default)s)")
    install_parser.add_argument("--config-folder", default='/etc/mirantis',
                                help="Config folder (%(default)s)")
    install_parser.add_argument("--dont-start", action='store_true', default=False, help="Don't start service")
    install_parser.add_argument("--dont-enable", action='store_true', default=False,
                                help="Don't enable autostart on boot")
    sub_parsers.add_parser("uninstall")

    return parser.parse_args(argv[1:])


def install(svc_name: str, svc_target: Path, config_folder: Path, opts: Any) -> None:

    config_file = config_folder / "ceph_report.cfg"
    log_config_file = config_folder / 'service_log_config.json'

    params = {
        "INVENTORY_FILE": opts.inventory,
        "MASTER_NODE": opts.ceph_master,
        "UPLOAD_URL": opts.upload_url,
        "HTTP_CREDS": opts.http_creds,
        "CLUSTER": opts.cluster,
        "CUSTOMER": opts.customer,
        "INSTALL_FOLDER": str(BASE_FILE_PATH),
        "AGENT_FOLDER": str(opts.agent_dir),
        "CONF_FILE": str(config_file),
        "STORAGE_FOLDER": opts.storage,
        "LOGGING_CONFIG": str(log_config_file)
    }

    config_file_content = (BASE_FILE_PATH / 'ceph_report/files/config.cfg').open().read()
    service_file_content = (BASE_FILE_PATH / svc_name).open().read()
    for name, val in params.items():
        config_file_content = config_file_content.replace("{" + name + "}", val)
        service_file_content = service_file_content.replace("{" + name + "}", val)

    if not svc_target.parent.exists():
        svc_target.parent.mkdir(parents=True)

    if not config_folder.exists():
        config_folder.mkdir(parents=True)

    svc_target.open("w").write(service_file_content)
    config_file.open("w").write(config_file_content)
    shutil.copyfile(str(BASE_FILE_PATH / 'service_log_config.json'), str(log_config_file))

    storage = Path(opts.storage)
    if not storage.exists():
        storage.mkdir(parents=True)

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
        subprocess.run(["systemctl", "disable", svc_name]).check_returncode()
        subprocess.run(["systemctl", "stop", svc_name]).check_returncode()
        svc_target.unlink()
        subprocess.run(["systemctl", "daemon-reload"]).check_returncode()

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

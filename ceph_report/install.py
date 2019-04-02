import sys
import argparse
import subprocess
from pathlib import Path


def parse_args(argv):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sub_parsers = parser.add_subparsers(dest='subparser_name')

    install_parser = sub_parsers.add_parser("install")
    install_parser.add_argument("--inventory", required=True)
    install_parser.add_argument("--ceph-master", required=True)
    install_parser.add_argument("--http-creds", required=True)
    install_parser.add_argument("--upload-url", required=True)
    install_parser.add_argument("--cluster", required=True)
    install_parser.add_argument("--customer", required=True)

    sub_parsers.add_parser("uninstall")

    return parser.parse_args(argv[1:])


def install(base_files_path, svc_name, svc_target, opts):
    data = (base_files_path / svc_name).open().read()
    svc = data.replace("{INVENTORY_FILE}", opts.inventory)
    svc = svc.replace("{MASTER_NODE}", opts.ceph_master)
    svc = svc.replace("{UPLOAD_URL}", opts.upload_url)
    svc = svc.replace("{HTTP_CREDS}", opts.http_creds)
    svc = svc.replace("{CLUSTER}", opts.cluster)
    svc = svc.replace("{CUSTOMER}", opts.customer)

    with svc_target.open("w") as target:
        target.write(svc)

    subprocess.run(["systemctl", "daemon-reload"]).check_returncode()
    subprocess.run(["systemctl", "enable", svc_name]).check_returncode()
    subprocess.run(["systemctl", "start", svc_name]).check_returncode()


def main(argv):
    opts = parse_args(argv)
    base_files_path = Path(sys.argv[0]).parent.parent
    svc_name = 'mirantis_ceph_reporter.service'
    svc_target = Path("/lib/systemd/system") / svc_name

    if opts.subparser_name == 'install':
        install(base_files_path, svc_name, svc_target, opts)
    else:
        assert opts.subparser_name == 'uninstall'
        subprocess.run(["systemctl", "disable", svc_name]).check_returncode()
        subprocess.run(["systemctl", "stop", svc_name]).check_returncode()
        svc_target.unlink()
        subprocess.run(["systemctl", "daemon-reload"]).check_returncode()

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))


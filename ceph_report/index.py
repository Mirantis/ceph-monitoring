import re
import sys
import time
import shutil
import pathlib
import datetime
import argparse
from typing import Iterator, Tuple, List

from .visualize import setup_logging, make_report, prepare_path


fname_re = re.compile(r"(?:ceph_report\.)?(?P<name>.*?)" +
                      r"[._](?P<datetime>20[12]\d_[A-Za-z]{3}_\d{1,2}\.\d\d_\d\d)" +
                      r"\.(?P<ext>tar\.gz|html)$")


def iter_fname_re(path: pathlib.Path, ext: str) -> Iterator[Tuple[str, datetime.datetime, pathlib.Path]]:
    for file in path.iterdir():
        rr = fname_re.match(file.name)
        if rr and rr.group("ext") == ext:
            yield rr.group('name'), datetime.datetime.strptime(rr.group('datetime'), "%Y_%b_%d.%H_%M"), file


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                   default=None, help="Console log level")
    p.add_argument("archive_folder", help="Folder with collected cluster archives")
    p.add_argument("html_folder", help="Output folder for reports")
    return p.parse_args(argv[1:])


def generate_index_for(name, all_reports):
    links = "<br>\n".join(f'<a href="{fpath.name}">{date}</a>' for (cname, date), fpath in all_reports.items()
                          if cname == name)
    return f"<html><body>{links}</body></html>"


def generate_index(all_reports):
    all_names = {name for name, _ in all_reports}
    links = "<br>\n".join(f'<a href="{name}.html">{name}</a>' for name in all_names)
    return f"<html><body>{links}</body></html>"


def main(argv: List[str]):
    opts = parse_args(argv)
    setup_logging(opts.log_level)
    htmlpath = pathlib.Path(opts.html_folder)

    regenerate_index_for = set()

    while True:
        all_reports = {(name, date): path for name, date, path in iter_fname_re(htmlpath, 'html')}
        for arch_name, arch_date, arch_path in iter_fname_re(pathlib.Path(opts.archive_folder), 'tar.gz'):
            if (arch_name, arch_date) not in all_reports:
                remove_data_folder, data_folder = prepare_path(arch_path)
                try:
                    report = make_report(name=f"{arch_name} {arch_date}",
                                         d1_path=pathlib.Path(data_folder),
                                         d2_path=None,
                                         plot=True,
                                         encrypt_passwd=None,
                                         embed=True,
                                         pretty_html=False)
                    rep_file = htmlpath / f"{arch_name}_{arch_date:%Y_%b_%d.%H_%M}.html"
                    rep_file.open("w").write(report)
                    regenerate_index_for.add(arch_name)
                    all_reports[(arch_name, arch_date)] = rep_file
                finally:
                    if remove_data_folder:
                        shutil.rmtree(str(data_folder))

        for name in regenerate_index_for:
            (htmlpath / f"{name}.html").open("w").write(generate_index_for(name, all_reports))

        if regenerate_index_for:
            (htmlpath / f"index.html").open("w").write(generate_index(all_reports))

        time.sleep(1)


if __name__ == "__main__":
    main(sys.argv)

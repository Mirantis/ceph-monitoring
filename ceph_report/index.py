import os
import threading

import subprocess
import sys
import json
import time
import shutil
import logging
import pathlib
import argparse
import datetime
import collections
from typing import Iterator, List, Dict, Tuple, Any, NamedTuple, Set

from ceph_report.visualize_utils import table_to_doc
from koder_utils import Table, Column, doc_to_string

from . import Report, get_file
from .checks import CheckMessage, Severity, ErrTarget, ServiceType
from .visualize import make_report, prepare_path
from .utils import parse_file_name, FileType, FileInfo, FileInfoId, ClusterId
from .web_storage import SyncApi
from . import setup_logging


logger = logging.getLogger("index")


def href(text: str, link: str) -> str:
    return f'<a href="{link}">{text}</a>'


def iter_file_type(path: pathlib.Path, tp: FileType) -> Iterator[FileInfo]:
    for file in path.iterdir():
        finfo = parse_file_name(file.name)
        if finfo and finfo.ftype == tp:
            yield finfo


def issue_info_to_jsonable(issues: Dict[str, List[CheckMessage]]) -> Dict[str, Any]:
    res = {}
    for key, messages in issues.items():
        res[key] = [
            (mess.reporter_id, mess.severity.name, mess.message,
             mess.affected_service.name, mess.affected_service.type.name)
            for mess in messages
        ]
    return res


def issue_info_from_jsonable(js_data: Dict[str, Any]) -> Dict[str, List[CheckMessage]]:
    res = {}
    for key, issues in js_data.items():
        res[key] = []
        for reporter_id, sev, mess, affected, affected_type in issues:
            res[key].append(
                CheckMessage(reporter_id=reporter_id,
                             severity=getattr(Severity, sev),
                             message=mess,
                             affected_service=ErrTarget(affected, getattr(ServiceType, affected_type)))
            )
    return res


def count_per_type(issues: Dict[str, List[CheckMessage]]) -> Dict[Severity, int]:
    issues_per_type = collections.Counter()
    for issues in issues.values():
        for issue in issues:
            issues_per_type[issue.severity] += 1
    return dict(issues_per_type.items())


class ClusterLatestInfo(NamedTuple):
    collect_datetime: datetime.datetime
    link_to_report: str
    link_to_issues: str
    issues_summary: str


class ReportsHTMLIndex:
    css = ["bootstrap.min.css", "report.css"]

    def __init__(self, archive_folder: str, html_folder: str, show_limit: int = 20) -> None:
        self.archive_folder = pathlib.Path(archive_folder)
        self.html_folder = pathlib.Path(html_folder)
        self.per_customer_cluster_info: Dict[ClusterId, ClusterLatestInfo] = {}
        self.current_reports_meta: Dict[FileInfoId, Dict[str, Any]] = {}
        self.current_reports: Dict[FileInfoId, FileInfo] = {}
        self.show_limit = show_limit
        self.css_part = "".join(f'<link href="{css}" rel="stylesheet" type="text/css">' for css in self.css)

    def load_ready(self):
        self.current_reports = {finfo.id: finfo for finfo in iter_file_type(self.html_folder, FileType.report_html)}

        # remove reports, which has no info, and load metainfo for those, who has
        for html_id, html_info in list(self.current_reports.items()):
            if html_id not in self.current_reports_meta:
                minfo = html_info.copy(ftype=FileType.meta, ref_ftype=html_info.ftype)
                mpath = self.html_folder / minfo.name
                if not mpath.exists():
                    del self.current_reports[html_id]
                else:
                    self.current_reports_meta[html_id] = issue_info_from_jsonable(json.load(mpath.open()))

    def generate_index_for_cluster(self, customer: str, cluster: str) -> Tuple[str, ClusterLatestInfo]:
        infos = [html_info for html_info in self.current_reports.values()
                 if html_info.customer == customer and html_info.cluster == cluster]
        assert infos != []

        infos.sort(key=lambda x: -time.mktime(x.collect_datetime.timetuple()))
        infos = infos[:self.show_limit]
        latest_info = None

        class ClusterTable(Table):
            last_report = Column.s(converter=lambda x: f"{x:%Y %b %d} {x:%H %M}")
            link = Column.s()
            issues_link = Column.s()
            status = Column.s()

        ct = ClusterTable()
        for info in infos:
            status = []
            cpt = count_per_type(self.current_reports_meta[info.id])
            for sev in sorted(Severity, key=lambda x: x.value):
                if sev in cpt:
                    status.append(f"{sev.name.capitalize()}: {cpt[sev]}")

            ct.add_named_row(last_report=info.collect_datetime,
                             link=href("report", info.name),
                             issues_link=href("issues", f"{info.name}#issues"),
                             status=", ".join(status))

            if info is infos[0]:
                latest_info = ClusterLatestInfo(collect_datetime=info.collect_datetime,
                                                link_to_report=info.name,
                                                link_to_issues=f"{info.name}#issues",
                                                issues_summary=", ".join(status))

        tbl_html = doc_to_string(table_to_doc(ct, sortable=False))
        html = f"<html><title>Reports for customer {customer}, cluster {cluster}</title><head>" + \
            f"{self.css_part}</head><body><center>{tbl_html}</center></body></html>"

        assert latest_info is not None
        return html, latest_info

    def generate_index(self, not_updated_clusters: Set[ClusterId] = None) -> None:
        all_clusters: Set[ClusterId] = {(info.customer, info.cluster) for info in self.current_reports.values()}

        class IndexTable(Table):
            customer = Column.s()
            cluster = Column.s()
            collect_time = Column.s(converter=lambda x: f"{x:%Y %b %d} {x:%H %M}")
            link = Column.s()
            issues_link = Column.s()
            status = Column.s()

        tb = IndexTable()

        for customer, cluster in sorted(all_clusters):
            if not_updated_clusters is None or (customer, cluster) not in not_updated_clusters:
                html, latest_info = self.generate_index_for_cluster(customer, cluster)

                with (self.html_folder / f"ceph_report.{customer}.{cluster}.index.html").open("w") as fd:
                    fd.write(html)

                tb.add_named_row(customer=customer, cluster=cluster, collect_time=latest_info.collect_datetime,
                                 link=href("report", latest_info.link_to_report),
                                 issues_link=href("issues", latest_info.link_to_issues),
                                 status=latest_info.issues_summary)
                self.per_customer_cluster_info[(customer, cluster)] = latest_info
            else:
                latest_info = self.per_customer_cluster_info[(customer, cluster)]
                tb.add_named_row(customer=customer, cluster=cluster, collect_time=latest_info.collect_datetime,
                                 link=href("report", latest_info.link_to_report),
                                 issues_link=href("issues", latest_info.link_to_issues),
                                 status=latest_info.issues_summary)

        tbl_html = doc_to_string(table_to_doc(tb, sortable=False))
        html = f"<html><title>Mirantis customers ceph reports</title><head>" + \
            f"{self.css_part }</head><body><center>{tbl_html}</center></body></html>"

        with (self.html_folder / f"ceph_report.index.html").open("w") as fd:
            fd.write(html)

    def process_archive(self, finfo: FileInfo) -> Tuple[FileInfo, Report]:
        remove_data_folder, data_folder = prepare_path(self.archive_folder / finfo.name)
        try:
            report = make_report(name=f"{finfo.name} {finfo.collect_datetime}",
                                 d1_path=pathlib.Path(data_folder),
                                 d2_path=None,
                                 plot=True)
            html_info = finfo.copy(ftype=FileType.report_html)
            with (self.html_folder / html_info.name).open("w") as fd:
                fd.write(report.render(encrypt=None, embed=True, pretty_html=False))

            html_meta = html_info.copy(ftype=FileType.meta, ref_ftype=html_info.ftype)
            with (self.html_folder / html_meta.name).open("w") as fd:
                fd.write(json.dumps(issue_info_to_jsonable(report.issues)))

        finally:
            if remove_data_folder:
                shutil.rmtree(str(data_folder))
        return html_info, report

    def regenerate(self):
        updated: Set[ClusterId] = set()
        for finfo in iter_file_type(self.archive_folder, FileType.report_arch):
            if finfo.id not in self.current_reports:
                html_info, report = self.process_archive(finfo)
                updated.add((html_info.customer, html_info.cluster))
                self.current_reports_meta[finfo.id] = report.issues
                self.current_reports[finfo.id] = html_info

        all_customers = {(info.customer, info.cluster) for info in self.current_reports.values()}
        self.generate_index(all_customers - updated)


def sync_thread(url: str, user: str, password: str, decrypt_key_file: str,
                target_dir: str,
                tmp_dir: str = None,
                sync_timeout: int = 60, max_decrypt_timeout: int = 30):
    api = SyncApi(url, user, password)
    target = pathlib.Path(target_dir)
    decrypt = get_file("decrypt.sh")
    bad_files = set()

    if tmp_dir is None:
        tmp_dir = target / 'tmp'

    if not tmp_dir.exists():
        tmp_dir.mkdir()

    while True:
        curr_names = set(target.iterdir())
        for name in set(api.list()) - curr_names:
            with (tmp_dir / name).open("wb") as fd:
                api.save_to(name, fd)
            os.rename(tmp_dir / name, target / name)

        # decrypt
        for enc in iter_file_type(target, FileType.enc):
            if enc.name in bad_files:
                continue
            key_file = target / enc.copy(ftype=FileType.key).name
            if not key_file.exists():
                continue
            dec_file = target / enc.ref_name
            if not dec_file.exists():
                try:
                    subprocess.check_call([str(decrypt), str(tmp_dir / enc.name), str(dec_file),
                                           str(key_file), decrypt_key_file], timeout=max_decrypt_timeout,
                                          stdout=subprocess.DEVNULL)
                except subprocess.CalledProcessError as exc:
                    bad_files.add(enc.name)
                    logger.error(f"Failed to decrypt {enc.name}: {exc}")
                else:
                    os.rename(tmp_dir / enc.name, target / enc.name)
        time.sleep(sync_timeout)


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                   default=None, help="Console log level")
    p.add_argument("--one-shoot", action="store_true", help="Run one time")
    p.add_argument("--sync-url", help="Url to sync files from")
    p.add_argument("--http-user", help="HTTP user")
    p.add_argument("--http-password", help="HTTP password")
    p.add_argument("--decrypt-key", help="Files decrypt key path")
    p.add_argument("--sync-timeout", type=int, default=60, help="Check timeout")
    p.add_argument("archive_folder", help="Folder with collected cluster archives")
    p.add_argument("html_folder", help="Output folder for reports")
    return p.parse_args(argv[1:])


def main(argv: List[str]):
    opts = parse_args(argv)
    setup_logging(get_file("logging.json"), opts.log_level, None)
    indexer = ReportsHTMLIndex(html_folder=opts.html_folder, archive_folder=opts.archive_folder)

    indexer.load_ready()
    indexer.generate_index()

    if opts.sync_url:
        if not hasattr(opts, "http_user") or not hasattr(opts, "http_password") or not hasattr(opts, "decrypt_key"):
            logger.error("Not all required arguments for sync provided")
            exit(1)
        threading.Thread(target=sync_thread,
                         args=(opts.sync_url, opts.http_user,
                               opts.http_password, opts.decrypt_key, opts.archive_folder),
                         kwargs={"sync_timeout": opts.sync_timeout}, daemon=True).start()

    while True:
        indexer.regenerate()
        if opts.one_shoot:
            break
        time.sleep(1)


if __name__ == "__main__":
    main(sys.argv)

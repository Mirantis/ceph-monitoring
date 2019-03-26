import datetime
import re
from enum import Enum
from typing import NamedTuple, Optional, Dict, Tuple


class FileType(Enum):
    report_arch = 1
    report_html = 2
    historic = 3
    key = 4
    enc = 5
    meta = 6


type_mapping: Dict[FileType, Tuple[str, Optional[str]]] = {
    FileType.report_arch: (".tar.gz", "ceph_report."),
    FileType.report_html: (".html", "ceph_report."),
    FileType.historic: (".bin", "ceph_historic."),
    FileType.enc: (".enc", None),
    FileType.key: (".key", None),
    FileType.meta: (".meta", None),
}


encapsulated_types = {FileType.key, FileType.meta, FileType.enc}


customer_re = re.compile("[0-9a-z_-]+$")
cluster_re = customer_re


FileInfoId = Tuple[str, str, datetime.datetime]
ClusterId = Tuple[str, str]


class FileInfo(NamedTuple):
    ftype: FileType
    ref_name: Optional[str]
    ref_ftype: Optional[FileType]
    customer: str
    cluster: str
    collect_datetime: datetime.datetime

    @property
    def id(self) -> FileInfoId:
        return self.customer, self.cluster, self.collect_datetime

    @property
    def name(self) -> str:
        body = f"{self.customer}.{self.cluster}.{self.collect_datetime:%Y_%b_%d}.{self.collect_datetime:%H_%M}"
        if self.ftype and self.ftype in encapsulated_types:
            base_suffix, base_prefix = type_mapping[self.ref_ftype]
            body = f"{'' if base_prefix is None else base_prefix}{body}{base_suffix}"
        suffix, prefix = type_mapping[self.ftype]
        return f"{'' if prefix is None else prefix}{body}{suffix}"

    def copy(self, **update) -> 'FileInfo':
        attrs = {name: getattr(self, name) for name in self.__annotations__ if name not in update}
        for name in update:
            assert name in self.__annotations__
        attrs.update(update)
        return FileInfo(**attrs)


def get_file_type(name: str) -> Tuple[Optional[FileType], Optional[str]]:
    for tp, (suffix, prefix) in type_mapping.items():
        if name.endswith(suffix):
            if tp in encapsulated_types:
                return tp, name[:-len(suffix)]
            elif name.startswith(prefix):
                return tp, name[len(prefix):-len(suffix)]
    else:
        return None, None


def parse_file_name(name: str) -> Optional[FileInfo]:
    ftype, rest = get_file_type(name)
    if ftype is None:
        return None

    if ftype in encapsulated_types:
        ref_ftype, rest = get_file_type(rest)
        ref_name = rest
        if ref_ftype in encapsulated_types:
            return None
    else:
        ref_ftype = ref_name = None

    parts = rest.split(".")
    if len(parts) != 4:
        return None

    customer, cluster, cdate, ctime = parts

    if not (customer_re.match(customer) and cluster_re.match(cluster)):
        return None

    try:
        dtm = datetime.datetime.strptime(f"{cdate}.{ctime}", "%Y_%b_%d.%H_%M")
    except ValueError:
        return None

    return FileInfo(ftype=ftype, ref_name=ref_name,
                    ref_ftype=ref_ftype, customer=customer, cluster=cluster,
                    collect_datetime=dtm)


import bisect
from typing import Union, Iterable, Callable, Sequence, Tuple, List, TypeVar, Any

import numpy

from koder_utils import XMLDocument, SimpleTable, Table, table_to_html


class StopError(Exception):
    pass


def tab(name: str) -> Callable[[Callable], Callable]:
    def closure(func: Callable) -> Callable:
        func.report_name = name  # type: ignore
        return func
    return closure


def perf_info_required(func):
    func.perf_info_required = True
    return func


def plot(func):
    func.plot = True
    return func


CMap = Sequence[Tuple[float, Tuple[float, float, float]]]


# replace with hvs mapping
def_color_map: CMap = (
    (0.0, (0.500, 0.000, 1.000)),
    (0.1, (0.304, 0.303, 0.988)),
    (0.2, (0.100, 0.588, 0.951)),
    (0.3, (0.096, 0.805, 0.892)),
    (0.4, (0.300, 0.951, 0.809)),
    (0.5, (0.504, 1.000, 0.705)),
    (0.6, (0.700, 0.951, 0.588)),
    (0.7, (0.904, 0.805, 0.451)),
    (0.8, (1.000, 0.588, 0.309)),
    (0.9, (1.000, 0.303, 0.153)),
    (1.0, (1.000, 0.000, 0.000))
)


def table_to_doc(table: Union[SimpleTable, Table], **attrs) -> XMLDocument:
    doc = table_to_html(table)
    doc(**attrs)
    return doc


def val_to_color(val: float, color_map: CMap = def_color_map) -> str:
    idx = [i[0] for i in color_map]
    assert idx == sorted(idx)
    pos = bisect.bisect_left(idx, val)
    if pos <= 0:
        ncolor: Sequence[float] = color_map[0][1]
    elif pos >= len(idx):
        ncolor = color_map[-1][1]
    else:
        color1 = color_map[pos - 1][1]
        color2 = color_map[pos][1]

        dx1 = (val - idx[pos - 1]) / (idx[pos] - idx[pos - 1])
        dx2 = (idx[pos] - val) / (idx[pos] - idx[pos - 1])

        ncolor = [(v1 * dx2 + v2 * dx1)
                  for v1, v2 in zip(color1, color2)]

    ncolor = [int((channel * 255 + 255) / 2) for channel in ncolor]
    return f"#{ncolor[0]:02X}{ncolor[1]:02X}{ncolor[2]:02X}"


def to_html_histo(vals: Sequence[Union[int, float]],
                  show_int: bool = True,
                  short: bool = True,
                  tostr: Callable[[Any], str] = None) -> Tuple[str, Union[int, float]]:

    if tostr is None:
        fmt = (lambda x: str(int(x + 0.4999))) if show_int else "{0:.2f}".format
    elif show_int:
        fmt = lambda x: tostr(int(x))
    else:
        fmt = tostr

    vals_s = set(vals)
    if len(vals_s) == 1:
        return f"{fmt(vals[0])}", vals[0]  # type: ignore
    elif len(vals_s) < (3 if short else 7):
        msg = "<br>".join(f"{fmt(val)}: {vals.count(val)}" for val in sorted(vals_s))  # type: ignore
        return msg, sum(vals) / len(vals)
    else:
        if short:
            p0, p50, p100 = numpy.percentile(vals, [0, 50, 100])  # type: ignore
            return f"min = {fmt(p0)}<br>mediana = {fmt(p50)}<br>max = {fmt(p100)}", p50
        else:
            p0, p25, p50, p75, p90, p95, p100 = numpy.percentile(vals, [0, 25, 50, 75, 90, 95, 100])  # type: ignore

            msg = f"min = {fmt(p0)}<br>25% < {fmt(p25)}<br>mediana = {fmt(p50)}<br>75% < {fmt(p75)}"  # type: ignore
            msg += f"<br>90% < {fmt(p90)}<br>95% < {fmt(p95)}<br>max = {fmt(p100)}"  # type: ignore
            return msg, p50


T = TypeVar('T')


def partition(items: Iterable[T], size: int) -> Iterable[List[T]]:
    curr = []
    for idx, val in enumerate(items):
        curr.append(val)
        if (idx + 1) % size == 0:
            yield curr
            curr = []

    if curr:
        yield curr


def partition_by_len(items: Iterable[Union[T, Tuple[T, int]]],
                     chars_per_line: int, delimiter_len: int) -> Iterable[List[T]]:
    curr: List[T] = []
    curr_len = 0
    for el_r in items:
        if isinstance(el_r, tuple):
            el, el_len = el_r
        else:
            el = el_r
            el_len = len(str(el))
        if curr_len + delimiter_len + el_len <= chars_per_line:
            curr.append(el)
            curr_len += delimiter_len + el_len
        else:
            yield curr
            curr = [el]
            curr_len = el_len
    if curr:
        yield curr


def table_id(tid: str) -> Callable[[Callable], Callable]:
    def closure(func):
        func.html_id = tid
        return func
    return closure

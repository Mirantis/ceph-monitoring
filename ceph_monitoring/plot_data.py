import json
import logging
import warnings
import subprocess
from io import BytesIO
from typing import Callable, Any, AnyStr, Optional, Tuple, Dict, Iterator, Set, Union

from cephlib.units import b2ssize
from dataclasses import dataclass

import distutils.spawn
from typing_extensions import Protocol

import numpy

from matplotlib import pyplot

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import seaborn

from cephlib.plot import plot_histo, hmap_from_2d, plot_hmap_with_histo
from cephlib.crush import Node, Rule


from .ceph_loader import NO_VALUE, CephInfo
# from .osd_ops import calc_stages_time, iter_ceph_ops, ALL_STAGES
# from .perf_parser import STAGES_PRINTABLE_NAMES
from .visualize_utils import perf_info_required, plot, tab
from .report import Report

# from .resource_usage import get_hdd_resource_usage


# ----------------------------------------------------------------------------------------------------------------------

logger = logging.getLogger('cephlib.report')

# ----------------------------------------------------------------------------------------------------------------------


class ReportProto(Protocol):
    def add_block(self, tag: str, name: str, img: AnyStr):
        return


def get_img(plt: Any, format: str = 'svg') -> AnyStr:
    "returns svg image"
    bio = BytesIO()
    if format in ('png', 'jpg'):
        plt.savefig(bio, format=format)
        return bio.getvalue()  # type: ignore
    assert format == 'svg', f"Unknown format {format}"
    plt.savefig(bio, format='svg')
    img_start1 = b"<!-- Created with matplotlib (http://matplotlib.org/) -->"
    img_start2 = b"<!-- Created with matplotlib (https://matplotlib.org/) -->"
    img_svg = bio.getvalue()
    img_start = img_start1 if img_start1 in img_svg else img_start2
    assert img_start in img_svg, "SVG format of matplotlib output has changed, update code accordingly"
    return bio.getvalue().split(img_start, 1)[1].decode("utf8")  # type: ignore


def plot_img(func: Callable, *args, **kwargs) -> str:
    fs = kwargs.pop('figsize', (6, 4))
    tl = kwargs.pop('tight_layout', True)

    if kwargs.pop('ax', False):
        fig, ax = pyplot.subplots(figsize=fs, tight_layout=tl)
        func(ax, *args, **kwargs)
    else:
        fig = pyplot.figure(figsize=fs, tight_layout=tl)
        func(fig, *args, **kwargs)

    return get_img(fig)


@plot
@tab("OSD used space")
def show_osd_used_space_histo(ceph: CephInfo) -> Optional[AnyStr]:
    min_osd: int = 3
    vals = [(100 - osd.free_perc) for osd in ceph.osds.values() if osd.free_perc is not None]
    if len(vals) >= min_osd:
        return plot_img(plot_histo, numpy.array(vals), left=0, right=100, ax=True,   # type: ignore
                        xlabel="OSD used space GiB", y_ticks=True)
    return None


def get_histo_img(vals: numpy.ndarray, **kwargs) -> str:
    return plot_img(plot_histo, vals, left=min(vals) * 0.9, right=max(vals) * 1.1, ax=True, **kwargs)


def get_kde_img(vals: numpy.ndarray) -> str:
    fig, ax = pyplot.subplots(figsize=(6, 4), tight_layout=True)
    seaborn.kdeplot(vals, ax=ax, clip=(vals.min(), vals.max()))
    return get_img(fig)


# @plot
# @perf_info_required
# def show_osd_load(report: ReportProto, cluster: Cluster, ceph: CephInfo):
#     max_xbins: int = 25
#     logger.info("Plot osd load")
#     usage = get_hdd_resource_usage(cluster.perf_data, ceph.osds)
#
#     hm_vals, ranges = hmap_from_2d(usage.data.wbytes, max_xbins=max_xbins)
#     hm_vals /= 1024. ** 2
#     img = plot_img(plot_hmap_with_histo, hm_vals, ranges)
#
#     logger.warning("Show only data disk load for now")
#
#     report.add_block("OSD_write_MiBps", "OSD data write (MiBps)", img)
#
#     pyplot.clf()
#     data, chunk_ranges = hmap_from_2d(usage.data.wio, max_xbins=max_xbins)
#     img = plot_img(plot_hmap_with_histo, data, chunk_ranges)
#     report.add_block("OSD_write_IOPS", "OSD data write IOPS", img)
#
#     data, chunk_ranges = hmap_from_2d(usage.data.qd, max_xbins=max_xbins)
#     img = plot_img(plot_hmap_with_histo, data, chunk_ranges)
#     report.add_block("OSD_write_QD", "OSD data write QD", img)

    # if not usage.colocated_journals:
    #     hm_vals, ranges = hmap_from_2d(usage.ceph_j_dev_wbytes, max_xbins=max_xbins)
    #     hm_vals /= 1024. ** 2
    #     img = plot_img(plot_hmap_with_histo, hm_vals, ranges)
    #     report.add_block(6, "OSD journal write (MiBps)", img)
    #
    #     data, chunk_ranges = hmap_from_2d(usage.ceph_j_dev_wio, max_xbins=max_xbins)
    #     img = plot_img(plot_hmap_with_histo, data, chunk_ranges)
    #     report.add_block(6, "OSD journal write IOPS", img)
    #
    #     pyplot.clf()
    #     data, chunk_ranges = hmap_from_2d(usage.ceph_j_dev_qd, max_xbins=max_xbins)
    #     img = plot_img(plot_hmap_with_histo, data, chunk_ranges)
    #     report.add_block(6, "OSD journal QD", img)


# @plot
# @perf_info_required
# def show_osd_lat_heatmaps(report: ReportProto, ceph: CephInfo):
#     max_xbins: int = 25
#     logger.info("Plot osd latency heatmaps")
#
#     for field, header in (('journal_latency', "Journal latency, ms"),
#                           ('apply_latency',  "Apply latency, ms"),
#                           ('commitcycle_latency', "Commit cycle latency, ms")):
#
#         if field not in ceph.osds[0].osd_perf:
#             continue
#
#         min_perf_len = min(osd.osd_perf[field].size for osd in ceph.osds)
#
#         lats = numpy.concatenate([osd.osd_perf[field][:min_perf_len] for osd in ceph.osds])
#         lats = lats.reshape((len(ceph.osds), min_perf_len))
#
#         print(field)
#         hmap, ranges = hmap_from_2d(lats, max_xbins=max_xbins, noval=NO_VALUE)
#
#         if len(hmap) != 0:
#             img = plot_img(plot_hmap_with_histo, hmap, ranges)
#             report.add_block("OSD_" + field, header, img)

#
# @plot
# @perf_info_required
# def show_osd_ops_boxplot(ceph: CephInfo) -> Tuple[str, Any]:
#     logger.info("Loading ceph historic ops data")
#     ops = []
#
#     for osd in ceph.osds:
#         fd = ceph.storage.raw.get_fd(osd.historic_ops_storage_path, mode='rb')
#         ops.extend(iter_ceph_ops(fd))
#
#     stimes = {}
#     for op in ops:
#         for name, vl in calc_stages_time(op).items():
#             stimes.setdefault(name, []).append(vl)
#
#     logger.info("Plotting historic boxplots")
#
#     stage_times_map = {name: numpy.array(values) / 1000. for name, values in stimes.items()}
#
#     # cut upper 2%
#     for arr in stage_times_map.values():
#         numpy.clip(arr, 0, numpy.percentile(arr, 98), arr)
#
#     stage_names, stage_times = zip(*sorted(stage_times_map.items(), key=lambda x: ALL_STAGES.index(x[0])))
#     pyplot.clf()
#     plt, ax = pyplot.subplots(figsize=(12, 9))
#
#     seaborn.boxplot(data=stage_times, ax=ax)
#     ax.set_yscale("log")
#     ax.set_xticklabels([STAGES_PRINTABLE_NAMES[name] for name in stage_names], size=14, rotation=35)
#     return "Ceph OPS time", get_img(plt)

# def show_crush(ceph: CephInfo) -> Tuple[str, Any]:
#     logger.info("Plot crushmap")
#
#     G = networkx.DiGraph()
#
#     G.add_node("ROOT")
#     for i in range(5):
#         G.add_node("Child_%i" % i)
#         G.add_node("Grandchild_%i" % i)
#         G.add_node("Greatgrandchild_%i" % i)
#
#         G.add_edge("ROOT", "Child_%i" % i)
#         G.add_edge("Child_%i" % i, "Grandchild_%i" % i)
#         G.add_edge("Grandchild_%i" % i, "Greatgrandchild_%i" % i)
#
#     pyplot.clf()
#     pos = networkx.fruchterman_reingold_layout(G)
#     networkx.draw(G, pos, with_labels=False, arrows=False)
#     return "nodes", get_img(pyplot)


colormap_host = (numpy.array([(0.5274894271434064, 0.304959630911188, 0.03729334871203383),
                             (0.7098039215686275, 0.46897347174163784, 0.14955786236063054),
                             (0.8376009227220299, 0.6858131487889272, 0.39792387543252583),
                             (0.9328719723183391, 0.8572087658592848, 0.6678200692041522),
                             (0.9625528642829682, 0.9377931564782775, 0.8723567858515955),
                             (0.8794309880815072, 0.9413302575932334, 0.932487504805844),
                             (0.6821222606689737, 0.8775086505190313, 0.8482122260668975),
                             (0.415455594002307, 0.7416378316032297, 0.6991926182237602),
                             (0.16785851595540177, 0.554479046520569, 0.5231064975009612),
                             (0.003537101114955786, 0.3838523644752019, 0.35094194540561324)]
                             ) * 256).astype(numpy.uint8)


colormap_repl = (numpy.array([(0.11864667435601693, 0.37923875432525955, 0.6456747404844292),
                              (0.2366013071895425, 0.5418685121107266, 0.7470203767781622),
                              (0.481430219146482, 0.714878892733564, 0.8394463667820069),
                              (0.7324106113033448, 0.8537485582468282, 0.9162629757785467),
                              (0.9014225297962322, 0.9367935409457901, 0.9562475970780469),
                              (0.9792387543252595, 0.9191080353710112, 0.8837370242214534),
                              (0.9797001153402538, 0.7840830449826992, 0.6848904267589392),
                              (0.9222606689734718, 0.5674740484429068, 0.4486735870818917),
                              (0.8115340253748559, 0.32110726643598614, 0.27581699346405225),
                              (0.6692041522491349, 0.08489042675893888, 0.16401384083044984)]
                             ) * 256).astype(numpy.uint8)


def get_color(w: float, cmap: numpy.ndarray, min_w:float, max_w: float) -> Tuple[int, int, int]:
    if abs(w - max_w) < 1e-5:
        return tuple(cmap[-1])  # type: ignore

    idx = (w - min_w) / (max_w - min_w) * (len(cmap) - 1)
    lidx = int(idx)
    c1 = 1.0 - (idx - lidx)
    return (c1 * cmap[lidx] + (1.0 - c1) * cmap[lidx + 1]).astype(numpy.uint8)


def to_dot_name(name: str) -> str:
    return name.replace("-", "_").replace('.', '_')


@dataclass
class ValsSummary:
    max: float
    min: float
    values: Dict[str, float]


def do_get_values(node: Node, vals: Dict[str, float], res: Dict[str, ValsSummary]):
    vs = res.get(node.type)
    if vs:
        assert node.name not in vs.values
    else:
        vs = res[node.type] = ValsSummary(-1, -1, {})
    vs.values[node.name] = vals[node.name]

    for ch in node.childs:
        do_get_values(ch, vals, res)


def get_values(node: Node, vals: Dict[str, float]) -> Dict[str, ValsSummary]:
    res: Dict[str, ValsSummary] = {}
    do_get_values(node, vals, res)
    for vs in res.values():
        not_none = [vl for vl in vs.values.values() if vl is not None]
        if not_none:
            vs.max = max(not_none)
            vs.min = min(not_none)
        else:
            vs.max = vs.min = None

    return res


def calc_min_max(summaries: Dict[str, ValsSummary],
                 minmax_coef: Union[float, Dict[str, float]]) -> Dict[str, Tuple[float, float]]:

    sett: Dict[str, Tuple[float, float]] = {}
    for level, vl in summaries.items():
        if isinstance(minmax_coef, float):
            min_v = min(vl.min, minmax_coef * vl.max)
        elif level not in minmax_coef:
            min_v = vl.min
        else:
            min_v = min(vl.min, minmax_coef[level] * vl.max)

        sett[level] = (min_v, vl.max)

    return sett


def get_weight_colors(root_node: Node, ceph: CephInfo,
                      cmaps: Dict[str, numpy.ndarray]) -> Dict[str, Tuple[str, str]]:

    nodes_weights: Dict[str, float] = {}

    def copy_weight(node: Node):
        nodes_weights[node.name] = node.weight
        for ch in node.childs:
            copy_weight(ch)

    copy_weight(root_node)
    weights = get_values(root_node, nodes_weights)
    sett = calc_min_max(weights, 0.6)
    return val2colors(weights, sett, cmaps, lambda x: f"{x:.2f}")


def get_used_space_colors(root_node: Node, ceph: CephInfo,
                          cmaps: Dict[str, numpy.ndarray]) -> Dict[str, Tuple[str, str]]:
    used: Dict[str, float] = {}

    def set_used_space(node: Node) -> float:
        if node.name not in used:
            if node.type == 'osd':
                used[node.name] = ceph.osds[node.id].used_space
            else:
                used[node.name] = sum(map(set_used_space, node.childs))
        return used[node.name]

    set_used_space(root_node)
    used_space = get_values(root_node, used)
    return val2colors(used_space, calc_min_max(used_space, 0.6), cmaps, b2ssize)


def get_data_size_colors(root_node: Node, ceph: CephInfo,
                         cmaps: Dict[str, numpy.ndarray]) -> Dict[str, Tuple[str, str]]:
    sizes: Dict[str, float] = {}

    def set_data_size(node: Node) -> float:
        if node.name not in sizes:
            if node.type == 'osd':
                sizes[node.name] = ceph.osds[node.id].pg_stats.bytes
            else:
                sizes[node.name] = sum(map(set_data_size, node.childs))
        return sizes[node.name]

    set_data_size(root_node)
    data_size = get_values(root_node, sizes)
    return val2colors(data_size, calc_min_max(data_size, 0.6), cmaps, b2ssize)


def get_free_space_colors(root_node: Node, ceph: CephInfo,
                          cmaps: Dict[str, numpy.ndarray]) -> Dict[str, Tuple[str, str]]:
    free_space: Dict[str, float] = {}

    def set_free_space(node: Node) -> float:
        if node.name not in free_space:
            if node.type == 'osd':
                free_space[node.name] = ceph.osds[node.id].free_space
            else:
                free_space[node.name] = sum(map(set_free_space, node.childs))
        return free_space[node.name]

    set_free_space(root_node)
    free_summ = get_values(root_node, free_space)
    return val2colors(free_summ, calc_min_max(free_summ, 0.6), cmaps, b2ssize)


def get_free_perc_colors(root_node: Node, ceph: CephInfo,
                         cmaps: Dict[str, numpy.ndarray]) -> Dict[str, Tuple[str, str]]:
    free_perc: Dict[str, float] = {}

    def set_free_perc(node: Node) -> Optional[float]:
        if node.name not in free_perc:
            if node.type == 'osd':
                free_perc[node.name] = ceph.osds[node.id].free_perc
            else:
                free_prc = [set_free_perc(ch) for ch in node.childs]
                free_prc = [i for i in free_prc if i is not None]
                free_perc[node.name] = None if len(free_prc) == 0 else min(free_prc)
        return free_perc[node.name]

    set_free_perc(root_node)
    free_summ = get_values(root_node, free_perc)
    return val2colors(free_summ, calc_min_max(free_summ, 0.6), cmaps, lambda x: f"{int(x)}%" if x is not None else "?")


def val2colors(vals: Dict[str, ValsSummary],
               sett: Dict[str, Tuple[float, float]],
               cmaps: Dict[str, numpy.ndarray],
               tostr: Callable[[float], str]) -> Dict[str, Tuple[str, str]]:
    res: Dict[str, Tuple[str, str]] = {}
    a = 160
    for key, (max_v, min_v) in sett.items():
        for name, val in vals[key].values.items():
            if key in cmaps and val is not None:
                r, g, b = get_color(val, cmaps[key], min_v, max_v)
            else:
                r = g = b = 255
            assert name not in res
            res[name] = f"#{r:02X}{g:02X}{b:02X}{a:02X}", tostr(val)
    return res


def make_dot(node: Node, idmap: Dict[str, str], id_prefix: str = "") -> Iterator[str]:
    dname = to_dot_name(node.name)
    htmlid = id_prefix + dname

    # 00.00 is a stub for data, later will be filled into svg by js
    yield f'{dname} [label="{node.name}\\n00.00", id="{htmlid}"]'
    assert node.name not in idmap
    idmap[node.name] = htmlid
    for ch in node.childs:
        if ch.type != 'osd':
            yield from make_dot(ch, idmap, id_prefix)
            yield f"{dname} -> {to_dot_name(ch.name)};"


@plot
def plot_crush_rules(ceph: CephInfo, report: Report):
    neato_path = distutils.spawn.find_executable('neato')

    if not neato_path:
        logger.warning("Neato tool not found, probably graphviz package is not installed. " +
                       "Rules graphs would not be generated")
    else:
        for rule in ceph.crush.rules.values():
            root_node = ceph.crush.copy_tree_for_rule(rule)
            if not root_node:
                continue

            cmaps = {"host": colormap_host}
            if rule.replicated_on != "host":
                cmaps[rule.replicated_on] = colormap_repl

            # maps crush node id to html node id
            idmap: Dict[str, str] = {}

            dot = f"digraph {rule.name} {{\n    overlap = scale;\n    "
            dot += "\n    ".join(make_dot(root_node, idmap, id_prefix=rule.name + "_")) + "\n}"
            svg = subprocess.check_output("neato -Tsvg", shell=True, input=dot.encode('utf8')).decode("utf8")
            svg = svg[svg.index("<svg "):]

            targets = []
            div_id = f'div_{rule.name}'
            for func, name in [(get_weight_colors, 'weight'),
                               (get_used_space_colors, 'used_space'),
                               (get_data_size_colors, 'data_size'),
                               (get_free_space_colors, 'free_space'),
                               (get_free_perc_colors, 'free_perc')]:

                colors = func(root_node, ceph, cmaps)
                colors_js = {idmap[name]: v for name, v in colors.items() if not name.startswith("osd.")}
                varname = f"colors_{rule.name}_{name}"
                report.scripts.append(f"const {varname} = {json.dumps(colors_js)}")

                linkid = f'ptr_crush_{rule.name}_{name}'
                targets.append(
                    f'''<span class="crushlink" onclick="setColors({varname}, '{div_id}', '{linkid}')" 
                    id="{linkid}">{name}</span>''')

            report.onload.append(f"setColors(colors_{rule.name}_weight, '{div_id}', 'ptr_crush_{rule.name}_weight')")
            targets = ',&nbsp;&nbsp;&nbsp;'.join(targets)
            report.add_block(f"crush_svg_{rule.name}",
                             None,
                             f'''<div id="{div_id}"><center>{targets}</center></div><br>\n{svg}''',
                             f"Tree for '{rule.name}'")

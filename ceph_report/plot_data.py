import collections
import json
import logging
import warnings
import subprocess
from io import BytesIO
from typing import Callable, Any, AnyStr, Optional, Tuple, Dict, Iterator, Union, List

from dataclasses import dataclass

import distutils.spawn
from typing_extensions import Protocol

import numpy

from matplotlib import pyplot

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import seaborn

from koder_utils import b2ssize, plot_histo, XMLBuilder, RawContent
from cephlib import CrushMap, Crush, CephInfo, get_rule_replication_level

from .visualize_utils import plot, tab
from .report import Report

# from .resource_usage import get_hdd_resource_usage


# ----------------------------------------------------------------------------------------------------------------------

logger = logging.getLogger('report')

# ----------------------------------------------------------------------------------------------------------------------


class ReportProto(Protocol):
    def add_block(self, tag: str, name: str, img: AnyStr):
        return


def get_img(plt: Any, format: str = 'svg') -> AnyStr:
    """ returns svg image """
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
    return bio.getvalue().split(img_start, 1)[1].decode()  # type: ignore


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
def show_osd_used_space_histo(ceph: CephInfo) -> Optional[RawContent]:
    min_osd: int = 3
    vals = [(100 - osd.space.free_perc) for osd in ceph.osds.values() if osd.space.free_perc is not None]
    if len(vals) >= min_osd:
        img = plot_img(plot_histo, numpy.array(vals), left=0, right=100, ax=True,   # type: ignore
                       xlabel="OSD used space GiB",
                       y_ticks=True,
                       ylogscale=True)
        return RawContent(img)
    return None


def get_histo_img(vals: numpy.ndarray, **kwargs) -> str:
    return plot_img(plot_histo, vals, left=min(vals) * 0.9, right=max(vals) * 1.1, ax=True, **kwargs)


def get_kde_img(vals: numpy.ndarray) -> str:
    fig, ax = pyplot.subplots(figsize=(6, 4), tight_layout=True)
    seaborn.kdeplot(vals, ax=ax, clip=(vals.min(), vals.max()))
    return get_img(fig)


#  TODO: matplotlib palette??
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


def get_color(w: float, cmap: numpy.ndarray, min_w: float, max_w: float) -> Tuple[int, int, int]:
    if abs(w - max_w) < 1e-5:
        return tuple(cmap[-1])  # type: ignore

    idx = (w - min_w) / (max_w - min_w) * (len(cmap) - 1)
    lidx = int(idx)
    c1 = 1.0 - (idx - lidx)
    return (c1 * cmap[lidx] + (1.0 - c1) * cmap[lidx + 1]).astype(numpy.uint8)


def to_dot_name(name: str) -> str:
    return name.replace("-", "_").replace('.', '_').replace("~", "_")


@dataclass
class ValsSummary:
    max: float
    min: float
    values: Dict[str, float]


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


def vals_summary_from_list(vals: Dict[str, float]) -> ValsSummary:
    not_none = [vl for vl in vals.values() if vl is not None]
    if not_none:
        max_v: float = max(not_none)
        min_v: float = min(not_none)
    else:
        assert False, "Fix me"
        # max_v = min_v = None

    return ValsSummary(max_v, min_v, vals)


def vals_summary_dct(vals: Dict[str, Dict[str, float]]) -> Dict[str, ValsSummary]:
    summary_per_class: Dict[str, ValsSummary] = {}
    for class_name, values in vals.items():
        summary_per_class[class_name] = vals_summary_from_list(values)
    return summary_per_class


def get_weight_colors(root: CrushMap.Bucket,
                      ceph: CephInfo,
                      cmaps: Dict[str, numpy.ndarray]) -> Dict[str, Tuple[str, str]]:

    weights_per_class: Dict[str, Dict[str, float]] = collections.defaultdict(dict)
    for bid, w in ceph.crush.iter_childs(root):
        bucket = ceph.crush.bucket_by_id(bid)
        weights_per_class[bucket.type_name][bucket.name] = w

    weights_summary_per_class = vals_summary_dct(weights_per_class)
    sett = calc_min_max(weights_summary_per_class, 0.6)
    return val2colors(weights_summary_per_class, sett, cmaps, lambda x: f"{x:.2f}")


def get_tree_property(root: CrushMap.Bucket,
                      crush: Crush,
                      osd_func: Callable[[CrushMap.Bucket], float],
                      bucket_func: Callable[[CrushMap.Bucket, List[float]], float]) -> Dict[str, Dict[str, float]]:

    per_bucket: Dict[str, float] = {}
    per_class_bucket: Dict[str, Dict[str, float]] = collections.defaultdict(dict)

    def dive(node: CrushMap.Bucket) -> float:
        if node.name not in per_bucket:
            if node.is_osd:
                per_bucket[node.name] = osd_func(node)
            else:
                childs = [dive(crush.bucket_by_id(itm.id)) for itm in node.items]
                per_bucket[node.name] = bucket_func(node, childs)
            per_class_bucket[node.type_name][node.name] = per_bucket[node.name]
        return per_bucket[node.name]

    dive(root)
    return per_class_bucket


def get_used_space_colors(root: CrushMap.Bucket,
                          ceph: CephInfo,
                          cmaps: Dict[str, numpy.ndarray]) -> Dict[str, Tuple[str, str]]:

    used_per_class = get_tree_property(root, ceph.crush,
                                       lambda node: ceph.osds[node.id].space.used,
                                       lambda node, ch_vals: sum(ch_vals))

    used_per_class_summary = vals_summary_dct(used_per_class)
    minmax = calc_min_max(used_per_class_summary, 0.6)
    return val2colors(used_per_class_summary, minmax, cmaps, b2ssize)


def get_data_size_colors(root: CrushMap.Bucket,
                         ceph: CephInfo,
                         cmaps: Dict[str, numpy.ndarray]) -> Dict[str, Tuple[str, str]]:
    data_per_class = get_tree_property(root, ceph.crush,
                                       lambda node: ceph.osds[node.id].pg_stats.num_bytes,
                                       lambda node, ch_vals: sum(ch_vals))

    data_per_class_summary = vals_summary_dct(data_per_class)
    minmax = calc_min_max(data_per_class_summary, 0.6)
    return val2colors(data_per_class_summary, minmax, cmaps, b2ssize)


def get_free_space_colors(root: CrushMap.Bucket,
                          ceph: CephInfo,
                          cmaps: Dict[str, numpy.ndarray]) -> Dict[str, Tuple[str, str]]:
    data_per_class = get_tree_property(root, ceph.crush,
                                       lambda node: ceph.osds[node.id].space.free,
                                       lambda node, ch_vals: sum(ch_vals))

    free_summ = vals_summary_dct(data_per_class)
    minmax = calc_min_max(free_summ, 0.6)
    return val2colors(free_summ, minmax, cmaps, b2ssize)


def get_free_perc_colors(root: CrushMap.Bucket,
                         ceph: CephInfo,
                         cmaps: Dict[str, numpy.ndarray]) -> Dict[str, Tuple[str, str]]:
    def free_for_bucket(_: CrushMap.Bucket, child_vals: List[float]) -> Optional[float]:
        free_prc = [i for i in child_vals if i is not None]
        return None if len(free_prc) == 0 else min(free_prc)

    free_perc = get_tree_property(root, ceph.crush, # type: ignore
                                  lambda node: ceph.osds[node.id].space.free_perc,
                                  free_for_bucket)

    free_summ = vals_summary_dct(free_perc)
    minmax = calc_min_max(free_summ, 0.6)
    return val2colors(free_summ, minmax, cmaps, lambda x: f"{int(x)}%" if x is not None else "?")


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


def make_dot(node: CrushMap.Bucket, crush: Crush, idmap: Dict[str, str], id_prefix: str = "") -> Iterator[str]:
    dname = to_dot_name(node.name)
    htmlid = id_prefix + dname

    # 00.00 is a stub for data, later will be filled into svg by js
    yield f'{dname} [label="{node.name}\\n00.00", id="{htmlid}"]'
    assert node.name not in idmap
    idmap[node.name] = htmlid
    for child_itm in node.items:
        child = crush.bucket_by_id(child_itm.id)
        if not child.is_osd:
            yield from make_dot(child, crush, idmap, id_prefix)
            yield f"{dname} -> {to_dot_name(child.name)};"


@plot
def plot_crush_rules(ceph: CephInfo, report: Report) -> None:
    neato_path = distutils.spawn.find_executable('neato')
    if not neato_path:
        logger.warning("Neato tool not found, probably graphviz package is not installed. " +
                       "Rules graphs would not be generated")
    else:
        for rule in ceph.crush.crushmap.rules:
            root_node = ceph.crush.get_root_bucket_for_rule(rule)
            if not root_node:
                continue

            cmaps = {"host": colormap_host}
            replicated_on = get_rule_replication_level(rule)
            if replicated_on != "host":
                cmaps[replicated_on] = colormap_repl

            # maps crush node id to html node id
            idmap: Dict[str, str] = {}

            rule_name = rule.rule_name.replace("-", '_')
            dot = f"digraph {rule_name} {{\n    overlap = scale;\n    "
            dot += "\n    ".join(make_dot(root_node, ceph.crush, idmap, id_prefix=rule_name + "_")) + "\n}"

            try:
                svg = subprocess.check_output("neato -Tsvg", shell=True, input=dot.encode()).decode()
            except subprocess.CalledProcessError as exc:
                logger.error("Failed to convert .dot to svg with 'neato -Tsvg': %s\n%s", exc, dot)
                svg = None

            if svg:
                div_id = f'div_{rule_name}'
                doc = XMLBuilder()
                with doc.center(id=div_id):
                    for func, name in [(get_weight_colors, 'weight'),
                                       (get_used_space_colors, 'used_space'),
                                       (get_data_size_colors, 'data_size'),
                                       (get_free_space_colors, 'free_space'),
                                       (get_free_perc_colors, 'free_perc')]:

                        colors = func(root_node, ceph, cmaps)
                        colors_js = {idmap[name]: v for name, v in colors.items() if not name.startswith("osd.")}
                        varname = f"colors_{rule_name}_{name}"
                        report.scripts.append(f"const {varname} = {json.dumps(colors_js)}")

                        linkid = f'ptr_crush_{rule_name}_{name}'
                        doc.span(name, class_="crushlink", onclick=f"setColors({varname}, '{div_id}', '{linkid}')",
                                 id=f"{linkid}")

                    report.onload.append(f"setColors(colors_{rule_name}_weight, '{div_id}',"
                                         f"'ptr_crush_{rule_name}_weight')")
                doc.br()
                doc << RawContent(svg[svg.index("<svg "):])
                report.add_block(f"crush_svg_{rule_name}", None, doc, f"Tree for '{rule_name}'")

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

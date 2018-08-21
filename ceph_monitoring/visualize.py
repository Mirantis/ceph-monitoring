import io
import sys
import json
import pstats
import shutil
import inspect
import logging
import tempfile
import cProfile
import argparse
import subprocess
import logging.config
from pathlib import Path
from typing import List, Tuple

import matplotlib


matplotlib.use('Agg')
del matplotlib

import seaborn
seaborn.set()
del seaborn

from cephlib.storage import make_storage, TypedStorage

from .cluster import load_all, fill_usage, fill_cluster_nets_roles
from .obj_links import host_link
from .report import Report

from .visualize_utils import StopError
from .visualize_cluster import show_cluster_summary, show_issues_table, show_primary_settings, show_ruleset_info, \
                               show_io_status, show_mons_info, show_cluster_err_warn, show_whole_cluster_nets, \
                               show_cluster_err_warn_summary
from .visualize_pools_pgs import show_pools_info, show_pg_state, show_pg_size_kde, show_pools_lifetime_load, \
                                 show_pools_curr_load
from .visualize_hosts import show_hosts_config, show_host_io_load_in_color, show_host_network_load_in_color, \
                             host_info, show_hosts_status, show_hosts_pg_info
from .visualize_osds import show_osd_state, show_osd_info, show_osd_perf_info, show_osd_pool_pg_distribution, \
                            show_osd_pool_agg_pg_distribution, show_osd_proc_info, show_osd_proc_info_agg
from .plot_data import plot_crush_rules, show_osd_used_space_histo

logger = logging.getLogger('report')


def setup_logging(opts):
    log_config = json.load((Path(__file__).parent / 'logging.json').open())

    del log_config["handlers"]['log_file']

    if opts.log_level is not None:
        log_config["handlers"]["console"]["level"] = opts.log_level

    for settings in log_config["loggers"].values():
        settings['handlers'].remove('log_file')

    logging.config.dictConfig(log_config)


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                   default=None, help="Console log level")
    p.add_argument("-n", "--no-plots", help="Don't draw any plots, generate tables only", action="store_true")
    p.add_argument("-N", "--name", help="Report name", default="Nemo")
    p.add_argument("-o", '--out', help="report output folder", required=True)
    p.add_argument("-w", '--overwrite', action='store_true',  help="Overwrite result folder data")
    p.add_argument("-p", "--pretty-html", help="Prettify index.html", action="store_true")
    p.add_argument("--profile", help="Profile report creation", action="store_true")
    p.add_argument("-e", "--embed", action='store_true', help="Embed js/css files into report to make it stand-alone")
    p.add_argument("path", help="Folder with data, or .tar.gz archive")
    p.add_argument("old_path", nargs='?', help="Older folder with data, or .tar.gz archive to calculate load")
    return p.parse_args(argv[1:])


def prepare_path(path: str) -> Tuple[bool, str]:
    if Path(path).is_file():
        folder = tempfile.mkdtemp()
        logger.info("Unpacking %r to temporary folder %r", path, folder)
        subprocess.call(f"tar -zxvf {path} -C {folder} >/dev/null 2>&1", shell=True)
        return True, folder
    elif not Path(path).is_dir():
        logger.error("Path argument (%r) should be a folder with data or path to archive", path)
        raise ValueError()
    else:
        return False, path


def main(argv: List[str]):
    opts = parse_args(argv)
    setup_logging(opts)

    logger.info("Generating report from %r to %r", opts.path, opts.out)

    if opts.profile:
        prof = cProfile.Profile()
        prof.enable()

    remove_d1, d1_path = prepare_path(opts.path)

    if opts.old_path:
        remove_d2, d2_path = prepare_path(opts.old_path)
    else:
        remove_d2 = False
        d2_path = None  # type: ignore

    out_p = Path(opts.out)
    index_path = out_p / 'index.html'
    if index_path.exists():
        if not opts.overwrite:
            logger.error("%r already exists. Pass -w/--overwrite to overwrite files", index_path)
            return 1
    elif not out_p.exists():
        out_p.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Loading collected data info into RAM")
        if d2_path:
            cluster, ceph = load_all(TypedStorage(make_storage(d1_path, existing=True)))
            cluster2, ceph2 = load_all(TypedStorage(make_storage(d2_path, existing=True)))
            fill_usage(cluster, cluster2, ceph, ceph2)
            cluster.has_second_report = True
        else:
            cluster, ceph = load_all(TypedStorage(make_storage(d1_path, existing=True)))

        fill_cluster_nets_roles(cluster, ceph)

        logger.info("Done")

        report = Report(opts.name, "index.html")

        cluster_reporters = [
            show_cluster_summary,
            show_issues_table,
            show_primary_settings,
            show_pools_info,
            show_pools_lifetime_load,
            show_pools_curr_load,
            show_ruleset_info,
            show_io_status,
            show_hosts_config,
            show_hosts_status,
            show_hosts_pg_info,
            show_mons_info,
            show_osd_state,
            show_osd_info,
            show_osd_proc_info,
            show_osd_proc_info_agg,
            show_osd_perf_info,
            show_pg_state,
            show_cluster_err_warn_summary,
            show_cluster_err_warn,
            (show_osd_pool_agg_pg_distribution if len(ceph.osds) > 20 else show_osd_pool_pg_distribution),
            show_host_io_load_in_color,
            show_host_network_load_in_color,
            show_whole_cluster_nets,
            show_osd_used_space_histo,
            # show_osd_pg_histo,
            # show_osd_load,
            # show_osd_lat_heatmaps,
            # show_osd_ops_boxplot,
            show_pg_size_kde,
            plot_crush_rules
        ]

        params = {"ceph": ceph, "cluster": cluster, "report": report, "uptime": not cluster.has_second_report}

        for reporter in cluster_reporters:
            if getattr(reporter, "perf_info_required", False) and not cluster.has_second_report:
                continue
            if getattr(reporter, 'plot', False) and opts.no_plots:
                continue

            sig = inspect.signature(reporter)
            curr_params = {name: params[name] for name in sig.parameters}
            new_block = reporter(**curr_params)

            if new_block:
                assert 'report' not in sig.parameters
                report.add_block(reporter.__name__.replace("show_", ""), reporter.report_name, new_block)

        for _, host in sorted(cluster.hosts.items()):
            report.add_block(host_link(host.name).id, None, host_info(host, ceph))

        report.save_to(Path(opts.out), opts.pretty_html, embed=opts.embed)
        logger.info("Report successfully stored to %r", index_path)
    except StopError:
        pass
    finally:
        if remove_d1:
            shutil.rmtree(d1_path)
        if remove_d2:
            assert d2_path
            shutil.rmtree(d2_path)

    if opts.profile:
        prof.disable()  # type: ignore
        s = io.StringIO()
        ps = pstats.Stats(prof, stream=s).sort_stats('time', 'calls')
        ps.print_stats(40)
        print(s.getvalue())


if __name__ == "__main__":
    exit(main(sys.argv))

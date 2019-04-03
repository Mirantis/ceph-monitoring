#!/usr/bin/env python3.5
import argparse
import configparser
import logging
import logging.config
import json
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger("run")


def load_config(path: Path) -> Optional[Dict[str, Any]]:
    if path.exists():
        cfg = configparser.ConfigParser()
        cfg.read(str(path))
        if cfg.has_section("run"):
            return cfg["run"]
    return None


def parse_args(argv):
    default_config_path = "/etc/mirantis/ceph_report.cfg"
    def_cfg = load_config(Path(default_config_path))

    pythonhome = def_cfg.get("pythonhome") if def_cfg else None
    pythonpath = def_cfg.get("pythonpath").split(":") if def_cfg else []

    parser = argparse.ArgumentParser()

    if pythonhome:
        parser.add_argument("--pythonhome", default=None, metavar="PYTHONHOME",
                            help="Path to python home ({})".format(pythonhome))
    else:
        parser.add_argument("--pythonhome", required=True, metavar="PYTHONHOME", help="Path to python home")

    parser.add_argument("--pythonpath",
                        nargs="*",
                        default=[],
                        metavar="LIBPATH",
                        help="Path to extra libs folder ({})".format(pythonpath))

    parser.add_argument("--config", default=default_config_path, help="Path config file (%(default)s)")
    return parser.parse_args(argv[1:])


default_logging_cfg = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "simple": {
            "format": "%(asctime)s - %(levelname)s - %(message)s",
            "datefmt": "%H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        },
    },
    "loggers": {
        "run": {"level": "DEBUG", "handlers": ["console"]}
    }
}


def main(argv):
    if '--' not in argv:
        tool_cmd = []
        my_args = argv
    else:
        tool_cmd = argv[argv.index("--") + 1:]
        my_args = argv[:argv.index("--")]

    opts = parse_args(my_args)

    config = load_config(Path(opts.config))
    if not config:
        config = {}

    log_cfg = json.load(open(config['log_config'])) if 'log_config' in config else default_logging_cfg
    logging.config.dictConfig(log_cfg)

    # can't check this in lines above - need to make --help works
    if not tool_cmd:
        logger.error("No tool opts provided. Use [opts] -- [tool opts]")
        exit(1)

    environ = os.environ.copy()
    pythonhome = config.get("pythonhome") if opts.pythonhome is None else opts.pythonhome

    if pythonhome is None:
        logger.error("No pythonhome provided")
        exit(1)

    pythonbins = list(Path(pythonhome).glob("python3.[789]"))
    if len(pythonbins) == 0:
        logger.error("Can't find appropriate python version at %r, only 3.7+ supported", pythonhome)
        exit(1)

    pythonbin = sorted(str(bin) for bin in pythonbins)[0]

    environ["PYTHONHOME"] = pythonhome

    pypath = environ.get("PYTHONPATH", "")
    pythonpath = config.get("pythonpath", "").split(":") if not opts.pythonpath else opts.pythonpath
    for lib_dir in pythonpath:
        if Path(lib_dir).is_dir():
             pypath += ":" + lib_dir

    environ["PYTHONPATH"] = pypath

    logger.info("Starting tool with python=%r PYTHONHOME=%r, PYTHONPATH=%r",
                 pythonbin, environ.get('PYTHONHOME', ''), environ.get('PYTHONPATH', ''))

    cmd = [pythonbin, *tool_cmd]
    logger.info("CMD=%s", cmd)

    for handler in logger.handlers:
        handler.flush()

    sys.stdout.flush()
    sys.stderr.flush()

    os.execve(pythonbin, cmd, environ)


if __name__ == "__main__":
    main(sys.argv)

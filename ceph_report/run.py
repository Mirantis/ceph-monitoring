#!/usr/bin/env python3.5
import argparse
import configparser
import logging
import logging.config
import json
import os
import sys
from pathlib import Path


logger = logging.getLogger("run")
base_files_path = Path(sys.argv[0]).parent


def parse_args(argv):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--reporter",
                        default=str(base_files_path.parent),
                        help="Path to reporter installation (%(default)s)")
    parser.add_argument("--agent",
                        default=str(base_files_path.parent.parent / 'agent'),
                        help="Path to agent installation (%(default)s)")
    parser.add_argument("--config",
                        default="/etc/mirantis/ceph_report.cfg",
                        help="Path config file (%(default)s)")
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

    # can't check this in lines above - need to make --help works
    if not tool_cmd:
        print("No tool provided. Use [opts] -- [tool opts]")
        exit(1)

    if Path(opts.config).exists():
        cfg = configparser.ConfigParser()
        cfg.read(opts.config)
        config = cfg['run']
    else:
        config = {
            'agent': opts.agent,
            'reporter': opts.reporter,
        }

    log_cfg = json.load(open(config['log_config'])) if 'log_config' in config else default_logging_cfg
    logging.config.dictConfig(log_cfg)

    service_path = Path(opts.reporter)
    agent_path = Path(opts.agent)

    environ = os.environ.copy()
    pypath = environ.get("PYTHONPATH", "")

    for lib_dir in (service_path / 'libs', agent_path / 'libs', agent_path / 'agent'):
        if lib_dir.is_dir():
             pypath += ":" + str(lib_dir)

    pypath += ":" + str(service_path)
    environ["PYTHONPATH"] = pypath

    for python_path in (service_path / 'python', agent_path / 'python'):
        if python_path.is_dir():
            environ['PYTHONHOME'] = str(python_path)
            pythonbins = list(python_path.glob("python3.[789]"))
            assert len(pythonbins) == 1
            pythonbin = str(pythonbins[0])
        else:
            pythonbin = sys.executable

    environ["RPC_AGENT_PATH"] = str(agent_path)

    cmd = [pythonbin, *tool_cmd]
    logging.info("Starting tool with PYTHONHOME=%r, PYTHONPATH=%r, RPC_AGENT_PATH=%r",
                 environ.get('PYTHONHOME', ''),
                 environ.get('PYTHONPATH', ''),
                 environ.get('RPC_AGENT_PATH', ''))
    logging.info("CMD=%s", cmd)

    for handler in logger.handlers:
        handler.flush()

    sys.stdout.flush()
    sys.stderr.flush()

    os.execve(pythonbin, cmd, environ)


if __name__ == "__main__":
    main(sys.argv)

#!/usr/bin/env python3.5
import argparse
import os
import sys
from pathlib import Path


def parse_args(argv):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("reporter_path")
    parser.add_argument("agent_path")
    return parser.parse_args(argv[1:])


def main(argv):
    collector_args = argv[argv.index("--") + 1:]
    my_args = argv[:argv.index("--")]
    opts = parse_args(my_args)
    service_path = Path(opts.reporter_path)
    agent_path = Path(opts.agent_path)

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

    sys.stdout.flush()
    sys.stderr.flush()

    os.execve(pythonbin, [pythonbin, "-m", "ceph_report.collect_info", *collector_args], environ)


if __name__ == "__main__":
    main(sys.argv)

.PHONY: mypy pylint pylint_e prepare archive archive_local

ALL_FILES=$(shell find ceph_report/ -type f -name '*.py')
# ALL_FILES=ceph_report/collect_info.py
# ALL_FILES=ceph_report/visualize_cluster.py ceph_report/resource_usage.py ceph_report/osd_ops.py ceph_report/hw_info.py ceph_report/cluster.py
STUBS="/home/koder/workspace/typeshed"

mypy:
		MYPYPATH=${STUBS} python -m mypy --ignore-missing-imports --follow-imports=skip ${ALL_FILES}

PYLINT_FMT=--msg-template={path}:{line}: [{msg_id}({symbol}), {obj}] {msg}

pylint:
		python -m pylint '${PYLINT_FMT}' --rcfile=pylint.rc ${ALL_FILES}

pylint_e:
		python3 -m pylint -E '${PYLINT_FMT}' --rcfile=pylint.rc ${ALL_FILES}

archive:
		bash scripts/makearch.sh binary/ceph_report.sh

archive_local:
		bash -x scripts/makearch.sh --local binary/ceph_report.sh

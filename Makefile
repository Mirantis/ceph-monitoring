.PHONY: mypy pylint pylint_e prepare archive mypy_collect whl

ALL_FILES=$(shell find ceph_report/ -type f -name '*.py')
COLLECT_FILES=$(shell find ceph_report/ -type f -name 'collect*.py')
STUBS="/home/koder/workspace/typeshed"
PYLINT_FMT=--msg-template={path}:{line}: [{msg_id}({symbol}), {obj}] {msg}

CURR_PATH=$(dir $(abspath $(lastword $(MAKEFILE_LIST))))
MAKE_ARCH=/home/koder/workspace/pet/agent/agent/make_arch.py


mypy:
		MYPYPATH=${STUBS} python -m mypy --ignore-missing-imports --follow-imports=skip ${ALL_FILES}

mypy_collect:
		MYPYPATH=${STUBS} python -m mypy --ignore-missing-imports --follow-imports=skip ${COLLECT_FILES}

pylint:
		python -m pylint '${PYLINT_FMT}' --rcfile=pylint.rc ${ALL_FILES}

pylint_e:
		python3 -m pylint -E '${PYLINT_FMT}' --rcfile=pylint.rc ${ALL_FILES}

archive:
		python '${MAKE_ARCH}' --config arch_config.txt '${CURR_PATH}' '${CURR_PATH}/binary/ceph_report.sh'

whl:
		python setup.py sdist bdist_wheel
.PHONY: mypy pylint pylint_e prepare archive mypy_collect whl

ALL_FILES=$(shell find ceph_report/ -type f -name '*.py')
COLLECT_FILES=$(shell find ceph_report/ -type f -name 'collect*.py')
STUBS="/home/koder/workspace/typeshed"
PYLINT_FMT=--msg-template={path}:{line}: [{msg_id}({symbol}), {obj}] {msg}

CURR_PATH=$(dir $(abspath $(lastword $(MAKEFILE_LIST))))
ARCH_LIB=$(dir $(abspath $(shell python -c "import koder_utils ; print(koder_utils.__file__)")))


mypy:
		MYPYPATH=${STUBS} python3 -m mypy --ignore-missing-imports --follow-imports=skip ${ALL_FILES}

mypy_collect:
		MYPYPATH=${STUBS} python3 -m mypy --ignore-missing-imports --follow-imports=skip ${COLLECT_FILES}

pylint:
		python3 -m pylint '${PYLINT_FMT}' --rcfile=pylint.rc ${ALL_FILES}

pylint_e:
		python3 -m pylint -E '${PYLINT_FMT}' --rcfile=pylint.rc ${ALL_FILES}

archive:
		python3  -m koder_utils.tools.make_arch '${CURR_PATH}' '${CURR_PATH}/binary/ceph_report.sh'

whl:
		python3 setup.py sdist bdist_wheel
#!/usr/bin/env bash


LIBS="--pythonpath /opt/mirantis/ceph_report /opt/mirantis/ceph_report/libs"
readonly LIBS="${LIBS} /opt/mirantis/agent/libs /opt/mirantis/agent"
readonly RUNNER=/opt/mirantis/ceph_report/ceph_report/run.py

python3.5 "${RUNNER}" ${LIBS} --pythonhome /opt/mirantis/agent/python -- -m ceph_report.install configure "$@"

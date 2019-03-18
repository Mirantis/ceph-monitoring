#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly CLUSTER=""
readonly INVENTORY=/opt/ceph_report/inventory
readonly TIMEOUT=$((60 * 60 * 24))
readonly ARCH=/opt/ceph_report/ceph_report.sh
readonly TARGET_DIR=/var/ceph_reports
readonly MAX_CEPH_REPORTS_SIZE_KB=$((10 * 1024 * 1024))
readonly CEPH_MASTER=$(head -1 "${INVENTORY}")

while true ; do

    while true ; do
        curr_sz=$(du -s "${TARGET_DIR}" | awk '{print $1}')
        if (( curr_sz >= MAX_CEPH_REPORTS_SIZE_KB )) ; then
            latest=$(ls -rt1 "${TARGET_DIR}" | head -n 1)
            rm -f "${latest}"
        fi
    done

    bash "${ARCH}" collect --log-level DEBUG --ceph-master "${CEPH_MASTER}" \
        --output-folder "${TARGET_DIR}" --inventory "${INVENTORY}"
    sleep "${TIMEOUT}"
done
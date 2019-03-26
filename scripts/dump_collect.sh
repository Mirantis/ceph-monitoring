#!/usr/bin/env bash

readonly OSDIDS=$(ls -1 /var/run/ceph/ceph-osd.* | awk -F. '{print $2}')
readonly RESULT_FILE="/tmp/report.txt"
readonly DUMP_TIMEOUT=600

while true ; do
    for osd_id in ${OSDIDS} ; do
        ceph --admin-daemon "/var/run/ceph/ceph-osd.${osd_id}.asok" dump_historic_ops >> "${RESULT_FILE}"
    done
    sleep "${DUMP_TIMEOUT}"
done

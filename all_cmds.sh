#!/usr/bin/env bash

readonly INSTALL_PATH=/opt/koder
readonly SOURCE_PATH=/home/koder/workspace
readonly ARCH=/tmp/arch.sh
readonly INV=/home/koder/.inventories/local


alias py="${INSTALL_PATH}/usr/bin/python3.7"
pushd "${SOURCE_PATH}"
#bash aiorpc/make_arch.sh "${ARCH}" "${SOURCE_PATH}"
#py -m aiorpc.service uninstall
#rm -rf ${INSTALL_PATH}/*

bash "${ARCH}" --install "${INSTALL_PATH}"

# --aiorpc-service-root /opt/koder/usr/local/lib/python3.7/dist-packages/aiorpc_service
py -m aiorpc.service install --inventory "${INV}"
py -m ceph_report iostat

py -m ceph_report historic_start --min-disk-free 10 --duration 10 --size 50 --min-duration 0
py -m ceph_report historic_status
py -m ceph_report historic
py -m ceph_report historic_stop

py -m ceph_report collect --cluster local --customer koder --dont-pack-result --historic --wipe --output-folder /tmp/rr

#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly CONTAINER_ID="${1}"
readonly INSTALL_ROOT=/opt/mirantis
readonly REPORT_INSTALL="${INSTALL_ROOT}/ceph_report"
readonly AGENT_INSTALL="${INSTALL_ROOT}/agent"
readonly CONFIGS=/etc/mirantis
readonly REPORTS=/var/lib/ceph_report
readonly LOGS_DIR=/var/log/ceph_report
readonly AGENT_SOURCE=/home/koder/workspace/pet/agent
readonly REPORT_SOURCE=/home/koder/workspace/ceph-related-repos/ceph_report
readonly INVENTORY=/home/koder/.inventories/local

for CDIR in "${INSTALL_ROOT}" "${CONFIGS}" "${REPORTS}" "${LOGS_DIR}"; do
    if [[ ! -d "${CDIR}" ]] ; then
        sudo mkdir "${CDIR}"
        sudo chown koder.koder "${CDIR}"
    fi
done

#if [[ -d "${REPORT_INSTALL}" ]] ; then
#    sudo python3.5 "${REPORT_INSTALL}/ceph_report/run.py" -- -m ceph_report.install uninstall_service
#fi

if [[ -d "${AGENT_INSTALL}" ]] ; then
    bash -x "${AGENT_INSTALL}/deploy.sh" uninstall "${INVENTORY}"
fi

rm -rf "${CONFIGS}/*" "${LOGS_DIR}/*" "${REPORT_INSTALL}" "${REPORTS}/*" "${AGENT_INSTALL}"

bash -x "${AGENT_SOURCE}/dev_rebuild_redeploy.sh" "${CONTAINER_ID}" "${AGENT_INSTALL}" "${INVENTORY}"

source ~/workspace/venvs/ceph_report/bin/activate
make -C "${REPORT_SOURCE}" archive

set +o nounset
deactivate
set -o nounset

bash "${REPORT_SOURCE}/binary/ceph_report.sh" "${REPORT_INSTALL}"
bash "${REPORT_INSTALL}/files/mira_default_configure.sh" --inventory "${INVENTORY}" --cluster local --customer koder --no-service

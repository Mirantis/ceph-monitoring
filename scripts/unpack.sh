#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly base_path="${PWD}"
readonly archname="${0}"
readonly tmpdir=$(mktemp -d)
readonly arch_content_pos=$(awk '/^__ARCHIVE_BELOW__/ {print NR + 1; exit 0; }' "$0")
readonly python3_path=$(which python3 2>&1 || true)

if [[ "${python3_path}" == *"not found"* ]] ; then
    echo "You need python3.4+ to run this tool. 'python3' alias should be created"
    exit 1
fi

readonly python3_ver=$(python3 --version | awk '{print $2}')
if [[ ! "${python3_ver}" =~ ^3\.[456789]\. ]] ; then
    echo "You need python3.4+ to run this tool, but python3 point to '${python3_ver}'"
    exit 1
fi

tail "-n+${arch_content_pos}" "${archname}" | tar -zx -C "${tmpdir}"

pushd "${tmpdir}" >/dev/null 2>&1
env PYTHONPATH=typing.whl python3 collect_info.py "$@" --base-folder "${base_path}"
code=$?
popd >/dev/null

rm -rf "${tmpdir}"
exit "${code}"

__ARCHIVE_BELOW__

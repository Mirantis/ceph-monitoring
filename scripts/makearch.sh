#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly typing_whl_url="https://files.pythonhosted.org/packages/4a/bd/eee1157fc2d8514970b345d69cb9975dcd1e42cd7e61146ed841f6e68309/typing-3.6.6-py3-none-any.whl"

readonly my_name="${0}"
readonly my_path=$(realpath "${my_name}")
readonly libs="cephlib agent xmlbuilder3"
readonly start_dir=$(pwd)
readonly temp_dir=$(mktemp -d)
readonly arch_file=$(mktemp)

if [[ "${1}" == "--local" ]] ; then
    echo "Using local libs for archive"
    readonly output_file=$(realpath "${2}")
    readonly download_deps="0"
    lib_paths=""
    for lib in $libs ; do
        init_path=$(python -c "import ${lib}; print(${lib}.__file__)")
        lib_path=$(dirname "${init_path}")
        while [[ -L "${lib_path}" ]] ; do
            lib_path=$(readlink -f "${lib_path}")
        done
        lib_paths="${lib_paths} ${lib_path}"
        echo "Using ${lib} from ${lib_path}"
    done
else
    readonly download_deps="1"
    readonly output_file=$(realpath "${1}")
fi


mkdir -p "${temp_dir}"
pushd "${temp_dir}" >/dev/null

if [[ "${download_deps}" == "1" ]] ; then
    for name in ${libs} ; do
        lib_url="https://github.com/koder-ua/${name}/archive/master.tar.gz"
        echo "Downloading ${name} from ${lib_url}"
        wget --no-check-certificate "${lib_url}"
        tar --strip-components=1 -xzf master.tar.gz "${name}-master/${name}"
        rm master.tar.gz
    done
else
    for lib_path in $lib_paths ; do
        cp -r "${lib_path}" .
        local_lib_dir=$(basename "${lib_path}")
        rm -rf "${local_lib_dir}/__pycache__"
        rm -rf $(find "${local_lib_dir}" -iname '*.pyc')
    done
fi

if [[ "${download_deps}" == "1" ]] ; then
    echo "Downloading typing from ${typing_whl_url}"
    wget "${typing_whl_url}" -O typing.whl
else
    scripts_dir=$(dirname "${my_path}")
    cmdir=$(dirname "${scripts_dir}")
    echo "Using local copy of typing.whl from "${cmdir}/binary/typing.whl""
    cp "${cmdir}/binary/typing.whl" .
fi

readonly files="collect_info.py files/logging.json files/enc_key.pub files/upload.sh files/mira_report_storage.crt"
for file in ${files} ; do
    mkdir -p $(dirname "${file}")
    cp "${start_dir}/ceph_report/${file}" "${file}"
done

tar -zcf "$arch_file" ./*
cat "${start_dir}/scripts/unpack.sh" "${arch_file}" > "${output_file}"
chmod +x "${output_file}"

popd >/dev/null

rm -rf "${temp_dir}"
rm "${arch_file}"

echo "Executable archive stored into ${output_file}"
readonly md5=$(md5sum ${output_file} | awk '{print $1}')
readonly size=$(ls -l ${output_file} | awk '{print $5}')
echo "Size = ${size}, md5 = ${md5}"

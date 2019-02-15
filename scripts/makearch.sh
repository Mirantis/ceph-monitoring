#!/usr/bin/env bash
set -ex
set -o pipefail

binpath=$(realpath "$0")
libs="cephlib agent xmlbuilder3"

if [[ $1 == "--local" ]] ; then
    shift 1
    download_deps="0"
    libpaths=""
    for lib in $libs ; do
        initpath=$(python -c "import ${lib}; print(${lib}.__file__)")
        libpath=$(dirname "$initpath")
        while [ -L "$libpath" ] ; do
            libpath=$(readlink -f "$libpath")
        done
        libpaths="${libpaths} ${libpath}"
    done
else
    download_deps="1"
fi

output_file=$(realpath "$1")
cdir=$(pwd)
tdir=$(mktemp -d)
archfile=$(mktemp)

mkdir -p "$tdir"
pushd "$tdir"

if [[ $download_deps == "1" ]] ; then
    for name in $libs ; do
        wget --no-check-certificate "https://github.com/koder-ua/${name}/archive/master.tar.gz"
        tar --strip-components=1 -xzf master.tar.gz "${name}-master/${name}"
        rm master.tar.gz
    done
else
    for libpath in $libpaths ; do
        cp -r "$libpath" .
    done
fi

if [[ $download_deps == "1" ]] ; then
    URL="https://files.pythonhosted.org/packages/ec/cc/28444132a25c113149cec54618abc909596f0b272a74c55bab9593f8876c/typing-3.6.4.tar.gz"
    wget $URL
else
    scripts_dir=$(dirname "$binpath")
    cmdir=$(dirname "$scripts_dir")
    cp "${cmdir}/binary/typing-3.6.4.tar.gz" .
fi

cp "${cdir}/ceph_monitoring/collect_info.py" .
cp "${cdir}/ceph_monitoring/logging.json" .
tar -zcf "$archfile" ./*
cat "${cdir}/scripts/unpack.sh" "$archfile" > "$output_file"
chmod +x "$output_file"

popd

rm -rf "$tdir"
rm "$archfile"

echo "Executable archive stored into $output_file"

#!/usr/bin/env bash


bash aiorpc_service/make_arch.sh /tmp/arch.sh /home/koder/workspace
bash /tmp/arch.sh --install /opt/koder

# --aiorpc-service-root /opt/koder/usr/local/lib/python3.7/dist-packages/aiorpc_service
python -m ceph_report.cli iostat

python -m ceph_report.collect_info historic_start --min-disk-free 10 --duration 10 --size 50 --min-duration 0
python -m ceph_report.collect_info historic_status
python -m ceph_report.collect_info historic_stop

python -m ceph_report.cli historic

python -m ceph_report.collect_info collect --cluster local --customer koder --dont-pack-result --historic --wipe --output-folder /tmp/rr

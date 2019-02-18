Ceph cluster data collect tool.
-------------------------------

Tool aim to collect and render ceph cluster information into html report.


How to collect data:
--------------------

* Only Jewel(10) and more resent versions of ceph are supported, for older ceph see 'old' branch
* ssh to any node, which has ssh access to all ceph nodes
* python3.4+ required on collection node, python2.7 required in all other nodes
* Passwordless ssh access need to be setup to all required nodes. If you key are encrypted you need to decrypt it,
  put decrypted key as default ~/.ssh/id_rsa file, run tool and then remove decrypted key. Usually you can
  decrypt ssh key with `openssl rsa –in enc.key -out dec.key`.
* If SSH daemons didn't listen on ceph client network - you need provide inventory file, which have IP's or names
  of all ceph nodes, usable for ssh. Inventory must be in format one IP_or_name per line.
* root or user with passwordless sudo need to used. In second case pass `-u USER_NAME --sudo` options.
* Run `curl -o ceph_report.sh 'https://raw.githubusercontent.com/Mirantis/ceph-monitoring/master/binary/ceph_report.sh'`

* In simplest case collect cluster info can be done with (see below for MCP example):
  Please take into account that all file paths must be absolute.

  `bash ceph_report.sh -c node,ceph -l DEBUG ADDITIONAL_OPTIONS`

Additional options (in most cases you will need them all):
- For passing inventory `--inventory INV_FILE`. Path to inventory must be absolute.
- For non-root user `-u SSH_USER --sudo`
- If running from node, where no `ceph` cmd available `--ceph-master MASTER_NODE_NAME`
- To overwrite previous report data `-w`

For MCP in most cases you need to run it as

    `bash ceph_report.sh -c node,ceph -l DEBUG --sudo --inventory INV_FILE_PATH --ceph-master ANY_CEPH_NODE`

* See `bash ceph_report.sh --help` for usage, if needed
* Follow the logs, in case of error it should give you a hint what's broken, or what package is missing
* The last log message should looks like:
  18:21:12 - INFO - Result saved into `/tmp/tmpv7tvwtlr.tar.gz`
* If in doubt you can ignore warnings, if they appears in log. The file name in last log line is the name of results archive.


How to visualize result:
------------------------

* python3.6+ is required
* Run next commands:
    - `git clone https://github.com/Mirantis/ceph-monitoring.git`
    - `cd ceph-monitoring`
    - `bash prepare.sh`
    - `source venv3/bin/activate`
* read `python -m ceph_monitoring.visualize_cluster --help` and update next command line if nessesary
* `python -m ceph_monitoring.visualize_cluster -l DEBUG -w --embed -o REPORT_OUTPUT_FOLDER RESULT_ARCHIVE_PATH`
* Open REPORT_OUTPUT_FOLDER/index.html in browser

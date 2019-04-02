Ceph cluster data collect tool.
-------------------------------

Tool aim to collect and render ceph cluster information into html report.


How to make archive:

        make archive

How to deploy:
    
* Install agent firsh
      
* ceph_report.sh /opt/mirantis/ceph_report

Create and start service:
    
        python3.5 /opt/mirantis/ceph_report/ceph_report/install.py --inventory INV --ceph-master XXX --http-creds USER:PASSDW --upload-url https://..... --cluster CLUSTER --customer CUSTOMER
        

Run collector manually:

/usr/bin/python3.5 /opt/mirantis/ceph_report/ceph_report/run.py /opt/mirantis/ceph_report /opt/mirantis/agent -- collect --api-key /opt/mirantis/agent/agent_client_keys/agent_api.key --certs-folder /opt/mirantis/agent/agent_client_keys --inventory /home/koder/.inventories/local --ceph-master ceph01 --output-folder /var/ceph_reports --log-level DEBUG --persistent-log --cluster local --customer koder

Upload:

/usr/bin/python3.5 /opt/mirantis/ceph_report/ceph_report/run.py /opt/mirantis/ceph_report /opt/mirantis/agent -- upload --url URL --http-creds CREDS ARCH_PATH




OLD_DOCS
--------



How to collect data:
--------------------

* Only Jewel(10) and more resent versions of ceph are supported, for older ceph see 'old' branch
* ssh to any node, which has ssh access to all ceph nodes
* python3.4+ required on collection node, python2.7 required in all other nodes
* Passwordless ssh access need to be setup to all required nodes. If you key are encrypted you need to
  decrypt it,put decrypted key as default ~/.ssh/id_rsa file, run tool and then remove decrypted key.
  Usually you can decrypt ssh key with `openssl rsa –in enc.key -out dec.key`.
* In case if you can't put you private ssh key to master node you need to add it on you local computer to
  ssh-agent (https://help.github.com/en/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#adding-your-ssh-key-to-the-ssh-agent) and add '-A' to ssh, when connecting to remote host. In case if
  jumphost is used you need to map master node 22 port to some port on jumphost first in order to -A
  option to work. As example:

        ssh -L 2234:target_host:22 jumphost
  
  After that from another shell (don't exit from first ssh):
  
        ssh target_user@jumphost -p 2234 -k target_system_key

* If SSH daemons didn't listen on ceph client network - you need provide inventory file, which have IP's or names
  of all ceph nodes, usable for ssh. Inventory must be in format one IP_or_name per line.
* root or user with passwordless sudo need to used. In second case pass `-u USER_NAME --sudo` options.
* Run `curl -o ceph_report.sh 'https://raw.githubusercontent.com/Mirantis/ceph-monitoring/master/binary/ceph_report.sh'`

* In simplest case collect cluster info can be done with (see below for MCP example):
  Please take into account that all file paths must be absolute.

    `bash ceph_report.sh collect --log-level DEBUG --cluster CLUSTER_NAME ADDITIONAL_OPTIONS`

Additional options (in most cases you will need them all):
- For passing inventory `--inventory INV_FILE`
- If running from node, where no `ceph` cmd available `--ceph-master MASTER_NODE_NAME`
- To overwrite previous report data `--wipe`

For MCP in most cases you need to run it as

    bash ceph_report.sh collect  --log-level DEBUG --cluster CLUSTER_NAME --inventory INV_FILE_PATH --ceph-master ANY_CEPH_NODE

* See `bash ceph_report.sh --help` for usage, if needed
* Follow the logs, in case of error it should give you a hint what's broken, or what package is missing
* The last log message should looks like:
  18:21:12 - INFO - Result saved into `/tmp/CLUSTER_NAME.CURR_DATETIME.tar.gz`
* If in doubt you can ignore warnings, if they appears in log. The file name in last log line is the name of results archive.
* By default file placed into /tmp, you can change it with `--output-folder`.
  Name template of the output file can not be changed.


How to upload file to mirantis report center:

    bash ceph_report.sh upload --log-level DEBUG --url MIRANTIS_SERVER_URL --http-creds HTTP_USER:HTTP_PASSWD REPORT_FILE_PATH


How to visualize result:
------------------------

* python3.6+ is required
* Run next commands:
    - `git clone https://github.com/Mirantis/ceph-monitoring.git`
    - `cd ceph-monitoring`
    - `bash prepare.sh`
    - `apt install graphviz`
    - `source venv3/bin/activate`
* read `python -m ceph_report.visualize --help` and update next command line if nessesary
* `python -m ceph_report.visualize -l DEBUG -w --embed -o REPORT_OUTPUT_FOLDER RESULT_ARCHIVE_PATH`
* Open REPORT_OUTPUT_FOLDER/index.html in browser

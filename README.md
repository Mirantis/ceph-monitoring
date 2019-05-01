Introduction
------------




Limitations and prerequisites
-----------------------------

* Only Jewel(10) and more resent versions of ceph are supported. Check ceph version with ceph --version on any node.
* python3.5+ required on all nodes
* You need to run everythin from any node, which has ssh access to all ceph nodes
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

Installation
------------

Agent installation
------------------

Collector v3 needs agent to communicate with remote nodes.
Check if agent already installed and running - check /opt/mirantis/agent
folder. It mush contains agent, agent_client_keys, python and libs folders, along with
deploy.sh and other files. Also check /opt/mirantis/inventory file - it must
contains all ceph nodes. If all files presents - check that agent actually running with
  
    bash /opt/mirantis/agent/deploy.sh status /opt/mirantis/inventory
  
You should see 'RUN' agains every node. If this all working - skip rest of this part.

Download agent binary from https://raw.githubusercontent.com/koder-ua/agent/v3/arch/agent.sh

    curl -o agent.sh "https://raw.githubusercontent.com/koder-ua/agent/v3/arch/agent.sh"

Install agent locally with:

    sudo agent.sh /opt/mirantis/agent

Create inventory file, which contains all ceph nodes, one node name/ip per line, store it to
/opt/mirantis/inventory. All names/ips in inventory file must be accesible via ssh.
Install agent globally:

    sudo bash /opt/mirantis/agent/deploy.sh install /opt/mirantis/inventory --ssh-user YOUR_SSH_USER_NAME

Check that installation working 

    bash /opt/mirantis/agent/deploy.sh status /opt/mirantis/inventory

You should see 'RUN' agains every node.

Agent uninstallation
--------------------

    bash /opt/mirantis/agent/deploy.sh uninstall /opt/mirantis/inventory

Collector installation
----------------------

Check whenever collector already installer first. Check /opt/mirantis/ceph_report folder.
If it installed - you can either use current installation or reinstall it. For collector
uninstallation see section below.

Download ceph_report binary:


    curl -o ceph_report.sh "https://raw.githubusercontent.com/Mirantis/ceph-monitoring/v3/binary/ceph_report.sh"


Install collector:
    
    sudo ceph_report.sh /opt/mirantis/ceph_report

Configure it:


    sudo bash /opt/mirantis/ceph_report/files/mirantis_default_install.sh --inventory /opt/mirantis/inventory --cluster CLUSTER_NAME --customer CUSTOMER_NAME --no-service

    
Collect report:

    sudo python3.5 /opt/mirantis/ceph_report/ceph_report/run.py -- -m ceph_report.collect_from_cfg [--output-folder XXX]

If collection went fine list line would contains report archive path

For all subsequent collection you only need to run last command.


Turn on prometheus data collection
---------------------------------

Open /etc/mirantis/ceph_report.cfg and add next lines to section '[collect]'

prometeus_url=http://PROMETHEUS_URL
prometheus_interval=HOURS

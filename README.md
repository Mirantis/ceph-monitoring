Introduction
------------

Limitations and prerequisites
-----------------------------

* Only Jewel(10) and more resent versions of ceph are supported. Check ceph version with ceph --version on any node.
* Collection need to be runned from any node, which has passwordless ssh access to all target nodes
  If you key are encrypted you need to decrypt it,put decrypted key as default ~/.ssh/id_rsa file,
  run tool and then remove decrypted key. Usually you can decrypt ssh key with `openssl rsa –in enc.key -out dec.key`.
* In case if you can't put you private ssh key to master node you need to add it on you local computer to
  ssh-agent (https://help.github.com/en/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#adding-your-ssh-key-to-the-ssh-agent) and add '-A' to ssh, when connecting to remote host. In case if
  jumphost is used you need to map master node 22 port to some port on jumphost first in order to -A
  option to work. As example:

        ssh -L 2234:target_host:22 jumphost
  
  After that from another shell (don't exit from first ssh):
  
        ssh target_user@jumphost -p 2234 -k target_system_key

* root or user with passwordless sudo need to used. In second case pass `-u USER_NAME --sudo` options.
* systemd must be used on all nodes

Installation
------------

* copy recent collect_data.sh from github releases https://github.com/Mirantis/ceph-monitoring/releases/
  to master node
* create inventory file with all target(ceph) node names, one node by line, like

        osd01
        osd02
        osd03
        mon01
        mon02
        mon03

You should be able to ssh to nodes via this names without password

* Install collector locally

        bash collect_data.sh --install /opt/ceph_collector

* Create an alias to python in installation folder
  
        alias py=/opt/ceph_collector/usr/bin/python3.7

* Install agent service on all nodes

        py -m aiorpc install --inventory inventory

* Check installation status

        py -m aiorpc status

Run collection
--------------
        py -m ceph_report --cluster CLUSTER_NAME --customer CUSTOMER_NAME -l DEBUG


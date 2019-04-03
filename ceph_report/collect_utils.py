import asyncio
import re
import socket
import ipaddress
import traceback
from typing import Optional, Tuple, Set, Dict, Any, List, NamedTuple, Union

from agent.client import AsyncRPCClient
from cephlib.discover import OSDConfig, get_mons_nodes, get_osds_nodes

from .collect_node_classes import INode, Node


CephServices = NamedTuple('CephServices', [('mons', Dict[str, str]), ('osds', Dict[str, List[OSDConfig]])])

# ------------ HELPER FUNCTIONS: GENERAL -------------------------------------------------------------------------------


def ip_and_hostname(ip_or_hostname: str) -> Tuple[str, Optional[str]]:
    """returns (ip, maybe_hostname)"""
    try:
        ipaddress.ip_address(ip_or_hostname)
        return ip_or_hostname, None
    except ValueError:
        return socket.gethostbyname(ip_or_hostname), ip_or_hostname


# ------------ HELPER FUNCTIONS: PARSERS -------------------------------------------------------------------------------


def parse_ipa4(data: str) -> Set[str]:
    """
    parse 'ip -o -4 a' output
    """
    res: Set[str] = set()
    # 26: eth0    inet 169.254.207.170/16
    for line in data.split("\n"):
        line = line.strip()
        if line:
            _, dev, _, ip_sz, *_ = line.split()
            ip, sz = ip_sz.split('/')
            ipaddress.IPv4Address(ip)
            res.add(ip)
    return res


def parse_proc_file(fc: str, ignore_err: bool = False) -> Dict[str, str]:
    res: Dict[str, str] = {}
    for ln in fc.split("\n"):
        ln = ln.strip()
        if ln:
            try:
                name, val = ln.split(":")
            except ValueError:
                if not ignore_err:
                    raise
            else:
                res[name.strip()] = val.strip()
    return res


def parse_devices_tree(lsblkdct: Dict[str, Any]) -> Dict[str, str]:
    def fall_down(fnode: Dict[str, Any], root: str, res_dict: Dict[str, str]):
        res_dict['/dev/' + fnode['name']] = root
        for ch_node in fnode.get('children', []):
            fall_down(ch_node, root, res_dict)

    res: Dict[str, str] = {}
    for node in lsblkdct['blockdevices']:
        fall_down(node, '/dev/' + node['name'], res)
        res['/dev/' + node['name']] = '/dev/' + node['name']
    return res


def parse_sockstat_file(fc: str) -> Optional[Dict[str, Dict[str, str]]]:
    res: Dict[str, Dict[str, str]] = {}
    for ln in fc.split("\n"):
        if ln.strip():
            if ':' not in ln:
                return None
            name, params = ln.split(":", 1)
            params_l = params.split()
            if len(params_l) % 2 != 0:
                return None
            res[name] = dict(zip(params_l[:-1:2], params_l[1::2]))
    return res


# ------------ HELPER FUNCTIONS: RPC -----------------------------------------------------------------------------------


async def get_host_interfaces(rpc: INode) -> List[Tuple[bool, str]]:
    """Return list of host interfaces, returns pair (is_physical, name)"""

    res: List[Tuple[bool, str]] = []
    content = (await rpc.run("ls -l /sys/class/net")).stdout

    for line in content.strip().split("\n")[1:]:
        if not line.startswith('l'):
            continue

        params = line.split()
        if len(params) < 11:
            continue

        res.append(('/devices/virtual/' not in params[10], params[8]))
    return res


async def get_device_for_file(node: Node, fname: str) -> Tuple[str, str]:
    """Find storage device, on which file is located"""

    dev = await node.rpc.fs.get_dev_for_file(fname)
    dev = dev.decode('utf8')
    assert dev.startswith('/dev'), "{!r} is not starts with /dev".format(dev)
    root_dev = dev = dev.strip()
    rr = re.match('^(/dev/[shv]d.*?)\\d+', root_dev)
    if rr:
        root_dev = rr.group(1)
    return root_dev, dev


async def discover_ceph_services(master_node: INode, opts: Any) -> CephServices:
    """Find ceph monitors and osds using ceph osd dump and ceph mon_map"""

    async def exec_func(cmd: str) -> str:
        return (await master_node.run_exc(cmd)).stdout

    mons: Dict[str, str] = {}

    for mon_id, (ip, name) in (await get_mons_nodes(exec_func, opts.ceph_extra)).items():
        assert ip not in mons
        mons[ip] = name

    osds: Dict[str, List[OSDConfig]] = {}
    osd_nodes = await get_osds_nodes(exec_func, opts.ceph_extra, get_config=False)
    for ip, osds_info in osd_nodes.items():
        assert ip not in osds
        osds[ip] = osds_info

    return CephServices(mons, osds)


async def init_rpc_and_fill_data(ips_or_hostnames: List[str],
                                 access_key: str,
                                 certificates: Dict[str, Any]) -> Tuple[List[Node], List[Tuple[str, str]]]:
    """Connect to nodes and fill Node object with basic node info: ips and hostname"""

    async def connect_coro(hostname_or_ip: str):
        try:
            rpc = AsyncRPCClient(hostname_or_ip,
                                 access_key=access_key,
                                 ssl_cert_file=certificates.get(hostname_or_ip))
            await rpc.__aenter__()
            await rpc.sys.ping()
            res = await rpc.run("hostname", node_name=hostname_or_ip, merge_err=True)
            node_or_exc: Union[Node, Exception] = \
                Node(conn_endpoint=hostname_or_ip, rpc=rpc, hostname=res.stdout.strip())
        except Exception as exc:
            node_or_exc = exc
            traceback.print_exc()
        return hostname_or_ip, node_or_exc

    rpc_nodes: List[Node] = []
    failed_nodes: List[Tuple[str, str]] = []

    results = await asyncio.gather(*map(connect_coro, ips_or_hostnames), return_exceptions=True)

    for hostname_or_ip, result in results:
        if isinstance(result, Exception):
            failed_nodes.append((hostname_or_ip, str(result)))
        else:
            assert isinstance(result, Node)
            rpc_nodes.append(result)

    return rpc_nodes, failed_nodes

import re
import xml.etree.ElementTree as ET
from typing import Dict, Optional


from .cluster_classes import LSHWCPUInfo, LSHWInfo, LSHWNetInfo, LSHWDiskInfo


def get_data(rr: str, data: str) -> str:
    return re.search("(?ims)" + rr, data).group(0)  # type: ignore


def parse_hw_info(lshw_out: str) -> Optional[LSHWInfo]:
    lshw_et = ET.fromstring(lshw_out)

    try:
        hostname = lshw_et.find("node").attrib['id']  # type: ignore
    except (AttributeError, KeyError):
        hostname = None

    try:
        sys_name = lshw_et.find("node/vendor").text + " " + lshw_et.find("node/product").text  # type: ignore
        sys_name = sys_name.lower().replace("(to be filled by o.e.m.)", "")
    except AttributeError:
        sys_name = None

    core = lshw_et.find("node/node[@id='core']")
    if core is None:
        return None

    try:
        mb: Optional[str] = " ".join(core.find(node).text for node in ['vendor', 'product', 'version'])  # type: ignore
    except AttributeError:
        mb = None

    cpu_info = []
    for cpu in core.findall("node[@class='processor']"):
        try:
            model = cpu.find('product').text  # type: ignore
            threads_node = cpu.find("configuration/setting[@id='threads']")
            cores = 1 if threads_node is None else int(threads_node.attrib['value'])  # type: ignore
        except (AttributeError, KeyError):
            pass
        else:
            assert isinstance(model, str)
            cpu_info.append(LSHWCPUInfo(model, cores))

    ram_size = 0
    for mem_node in core.findall(".//node[@class='memory']"):
        descr = mem_node.find('description')
        try:
            if descr is not None and descr.text == 'System Memory':
                mem_sz = mem_node.find('size')
                if mem_sz is None:
                    for slot_node in mem_node.find("node[@class='memory']"):  # type: ignore
                        slot_sz = slot_node.find('size')
                        if slot_sz is not None:
                            assert slot_sz.attrib['units'] == 'bytes'
                            ram_size += int(slot_sz.text)  # type: ignore
                else:
                    assert mem_sz.attrib['units'] == 'bytes'
                    ram_size += int(mem_sz.text)  # type: ignore
        except (AttributeError, KeyError, ValueError):
            pass

    net_info = {}
    for net in core.findall(".//node[@class='network']"):
        try:
            link = net.find("configuration/setting[@id='link']")
            if link.attrib['value'] == 'yes':  # type: ignore

                speed_node = net.find("configuration/setting[@id='speed']")
                speed = None if speed_node is None else speed_node.attrib['value']

                dup_node = net.find("configuration/setting[@id='duplex']")
                dup = False if dup_node is None else dup_node.attrib['value'] == "full"

                name = net.find("logicalname").text  # type: ignore
                assert isinstance(name, str)
                net_info[name] = LSHWNetInfo(name=name, speed=speed, duplex=dup)
        except (AttributeError, KeyError):
            pass

    storage_controllers = []
    for controller in core.findall(".//node[@class='storage']"):
        try:
            description = getattr(controller.find("description"), 'text', "")
            product = getattr(controller.find("product"), 'text', "")
            vendor = getattr(controller.find("vendor"), 'text', "")
            dev = getattr(controller.find("logicalname"), 'text', "")
            storage_controllers.append((f"{dev}: " if dev else "") + f"{description} {vendor} {product}")
        except AttributeError:
            pass

    disks_raw_info: Dict[str, str] = {}
    disks_info = {}
    for disk in core.findall(".//node[@class='disk']"):
        try:
            lname_node = disk.find('logicalname')
            if lname_node is not None:

                dev = lname_node.text.split('/')[-1]  # type: ignore
                if dev == "" or dev[-1].isdigit():
                    continue

                sz_node = disk.find('size')
                assert sz_node.attrib['units'] == 'bytes'  # type: ignore
                sz = int(sz_node.text)  # type: ignore
                disks_info[dev] = LSHWDiskInfo(size=sz, mount_point=None, device=None)
            else:
                description = disk.find('description').text  # type: ignore
                product = disk.find('product').text  # type: ignore
                vendor = disk.find('vendor').text  # type: ignore
                version = disk.find('version').text  # type: ignore
                serial = disk.find('serial').text  # type: ignore
                businfo = disk.find('businfo').text  # type: ignore
                assert isinstance(businfo, str)
                disks_raw_info[businfo] = f"{description} {product} {vendor} {version} {serial}"
        except (AttributeError, KeyError):
            pass

    return LSHWInfo(raw=lshw_out,
                    disks_raw_info=disks_raw_info,
                    disks_info=disks_info,
                    sys_name=sys_name,
                    hostname=hostname,
                    mb=mb,
                    cpu_info=cpu_info,
                    storage_controllers=storage_controllers,
                    net_info=net_info,
                    ram_size=ram_size)


def get_dev_file_name(path_or_name: str) -> str:
    res = path_or_name[len("/dev/"):] if path_or_name.startswith('/dev/') else path_or_name
    assert '/' not in res
    return res
import abc
import subprocess
import zlib
import errno
from pathlib import Path
from typing import Iterable, List, Set, Optional, Dict, Any, Union

from agent.utils import CMDResult, run, CmdType
from agent.client import AsyncRPCClient

from cephlib.discover import OSDConfig


class INode(metaclass=abc.ABCMeta):
    name: str = ""

    @abc.abstractmethod
    async def run_exc(self, cmd: CmdType, **kwargs) -> CMDResult:
        pass

    @abc.abstractmethod
    async def get_file(self, name: Union[str, Path], compress: bool = False) -> bytes:
        pass

    async def run(self, cmd: CmdType, **kwargs) -> CMDResult:
        try:
            return await self.run_exc(cmd, **kwargs)
        except subprocess.CalledProcessError as exc:
            return CMDResult(cmd, exc.output, exc.stderr, exc.returncode)
        except subprocess.TimeoutExpired as exc:
            return CMDResult(cmd, exc.output, exc.stderr, errno.ETIMEDOUT)

    @abc.abstractmethod
    async def exists(self, fname: Union[str, Path]) -> bool:
        pass

    @abc.abstractmethod
    async def listdir(self, path: Union[str, Path]) -> Iterable[Path]:
        pass


class Node(INode):
    def __init__(self, conn_endpoint: str, hostname: str, rpc: AsyncRPCClient, cmd_run_timeout: int = 60) -> None:
        self.conn_endpoint = conn_endpoint
        self.name = hostname
        self.rpc = rpc
        self.osds: List[OSDConfig] = []
        self.all_ips: Set[str] = {conn_endpoint}
        self.mon: Optional[str] = None
        self.load_config: Dict[str, Any] = {}
        self.cmd_run_timeout = cmd_run_timeout

    def __cmp__(self, other: 'Node') -> int:
        x = (self.name, self.conn_endpoint)
        y = (other.name, other.conn_endpoint)
        if x == y:
            return 0
        return 1 if x > y else -1

    @property
    def hostname(self) -> str:
        return self.name

    def dct(self) -> Dict[str, Any]:
        return {'name': self.name, 'endpoint': self.conn_endpoint, 'all_ips': list(self.all_ips)}

    def __str__(self) -> str:
        return f"Node(name={self.name}, endpoint={self.conn_endpoint})"

    def merge(self, other: 'Node', overwrite_ssh: bool = True):

        if self.mon and not other.mon:
            self.mon = other.mon

        self.osds = list(set(self.osds + other.osds))

        if overwrite_ssh:
            self.conn_endpoint = other.conn_endpoint

        if not self.name and other.name:
            self.name = other.name

        self.all_ips.update(other.all_ips)

    async def run_exc(self, cmd: CmdType, **kwargs) -> CMDResult:
        assert self.rpc is not None
        assert 'node_name' not in kwargs

        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.cmd_run_timeout

        return await self.rpc.run(cmd, node_name=self.name, **kwargs)

    async def get_file(self, name: Union[str, Path], compress: bool = False) -> bytes:
        data = await self.rpc.get_file(name, compress=compress)
        return zlib.decompress(data) if compress else data

    def ceph_info(self) -> str:
        info = self.name
        if self.mon:
            info += " mon=" + self.mon
        if self.osds:
            info += " osds=[" + ",".join(str(osd.osd_id) for osd in self.osds) + "]"
        return info

    async def exists(self, fname: Union[str, Path]) -> bool:
        return await self.rpc.fs.file_exists(str(fname))

    async def listdir(self, path: Union[str, Path]) -> Iterable[Path]:
        return map(Path, await self.rpc.fs.listdir(str(path)))


class Local(INode):
    name = 'localhost'

    async def run_exc(self, cmd: CmdType, **kwargs) -> CMDResult:
        assert 'node_name' not in kwargs
        return CMDResult(*(await run(cmd, **kwargs)))

    def __str__(self) -> str:
        return "Localhost()"

    async def get_file(self, name: Union[str, Path], compress: bool = False) -> bytes:
        return Path(name).open('rb').read()

    async def exists(self, fname: Union[str, Path]) -> bool:
        return Path(fname).exists()

    async def listdir(self, path: Union[str, Path]) -> Iterable[Path]:
        return Path(path).iterdir()



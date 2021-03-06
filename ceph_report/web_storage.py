import re
import sys
import ssl
import json
import time
import shutil
import os.path
import hashlib
import argparse
import functools
from urllib.parse import urljoin
from typing import List, Dict, Tuple, BinaryIO

import requests
from aiohttp import web, BasicAuth, ClientSession

from .utils import parse_file_name, FileType


MAX_FILE_SIZE = 1 << 30
MAX_PASSWORD_LEN = 1 << 10


def upload(func):
    func.auth_role = 'upload'
    return func


def download(func):
    func.auth_role = 'download'
    return func


def allowed_file_name(data: str) -> bool:
    return re.match(r"[-a-zA-Z0-9_.]+$", data) is not None


def encrypt_password(password: str, salt: str = None) -> Tuple[str, str]:
    if salt is None:
        salt = "".join(f"{i:02X}" for i in ssl.RAND_bytes(16))
    return hashlib.sha512(password.encode('utf-8') + salt.encode('utf8')).hexdigest(), salt


def check_passwd(db: Dict[str, Tuple[str, str, List[str]]], user_name: str, passwd: str) -> List[str]:
    if user_name not in db:
        return []
    correct_passwd, salt, roles = db[user_name]
    curr_password, _ = encrypt_password(passwd, salt)
    if curr_password == correct_passwd:
        return roles
    return []


def basic_auth_middleware(pwd_db: Dict[str, Tuple[str, str, List[str]]]):
    @web.middleware
    async def basic_auth(request, handler):
        basic_auth = request.headers.get('Authorization')
        if basic_auth:
            auth = BasicAuth.decode(basic_auth)
            if handler.auth_role in check_passwd(pwd_db, auth.login, auth.password):
                return await handler(request)

        headers = {'WWW-Authenticate': 'Basic realm="XXX"'}
        return web.HTTPUnauthorized(headers=headers)
    return basic_auth


@upload
async def handle_put(target_dir: str, min_free_space: int, request: web.Request):
    target_name = request.headers.get('Arch-Name')
    enc_passwd = request.headers.get('Enc-Password')

    if request.content_length is None or target_name is None or enc_passwd is None :
        return web.HTTPBadRequest(reason="Some required header(s) missing (Arch-Name/Enc-Password/Content-Length)")

    # explicit redundancy with parse_file_name to be sure that no malicious file would ever pass into
    if not allowed_file_name(target_name):
        return web.HTTPBadRequest(reason=f"Incorrect file name: {target_name}")

    finfo = parse_file_name(target_name)
    if finfo is None or finfo.ftype != FileType.enc or finfo.ref_ftype != FileType.report_arch:
        return web.HTTPBadRequest(reason=f"Incorrect file name: {target_name}")

    if request.content_length > MAX_FILE_SIZE:
        return web.HTTPBadRequest(reason=f"File too large, max {MAX_FILE_SIZE} size allowed")

    if len(enc_passwd) > MAX_PASSWORD_LEN:
        return web.HTTPBadRequest(reason=f"Password too large, max {MAX_PASSWORD_LEN} size allowed")

    if shutil.disk_usage("/").free - request.content_length - len(enc_passwd) < min_free_space:
        return web.HTTPBadRequest(reason="No space left on device")

    assert finfo.ref_name is not None
    target_path = os.path.join(target_dir, finfo.ref_name)
    key_file = target_path + ".key"
    meta_file = target_path + ".meta"

    if any(map(os.path.exists, [target_path, key_file, meta_file])):
        return web.HTTPBadRequest(reason="File exists")

    print(f"Receiving {request.content_length} bytes from {request.remote} to file {target_name}")

    try:
        with open(target_path, "wb") as fd:
            while True:
                data = await request.content.read(1 << 20)
                if len(data) == 0:
                    break
                fd.write(data)

        with open(key_file, "wb") as fd:
            fd.write(enc_passwd.encode("utf8"))

        with open(meta_file, "w") as fd2:
            fd2.write(json.dumps({'upload_time': time.time(), 'src_addr': request.remote}))

    except:
        os.unlink(target_path)
        raise

    return web.Response(text="Saved ")


@download
async def handle_list(target_dir: str, request: web.Request):
    return web.Response(text=json.dumps(os.listdir(target_dir)), content_type="application/json")


@download
async def handle_get(target_dir: str, request: web.Request):
    target_name = (await request.read()).decode()

    if not allowed_file_name(target_name):
        return web.HTTPBadRequest(reason=f"Incorrect file name: {target_name}")

    return web.FileResponse(os.path.join(target_dir, target_name))


@download
async def handle_del(target_dir: str, request: web.Request):
    target_name = (await request.read()).decode()

    if not allowed_file_name(target_name):
        return web.HTTPBadRequest(reason=f"Incorrect file name: {target_name}")

    os.unlink(os.path.join(target_dir, target_name))
    return web.Response(text="removed")


class API:
    def __init__(self, src_url: str, http_user: str, http_password: str) -> None:
        assert ' ' not in src_url

        self.auth = BasicAuth(http_user, http_password)
        self.list_url = urljoin(src_url, "list")
        self.get_url = urljoin(src_url, "get")
        self.del_url = urljoin(src_url, "del")

    async def list(self) -> List[str]:
        async with ClientSession() as client:
            async with client.get(self.list_url) as resp:
                assert resp.status == 200
                return await resp.json()

    async def save_to(self, name: str, fd: BinaryIO):
        async with ClientSession() as client:
            async with client.post(self.get_url, data=name.encode("utf8")) as resp:
                assert resp.status == 200
                while True:
                    data = await resp.content.read(2 << 20)
                    if not data:
                        break
                    fd.write(data)

    async def delete(self, name: str):
        async with ClientSession() as client:
            async with client.post(self.get_url, data=name.encode("utf8")) as resp:
                assert resp.status == 200


class SyncApi:
    def __init__(self, src_url: str, http_user: str, http_password: str) -> None:
        assert ' ' not in src_url

        self.http_user = http_user
        self.http_password = http_password
        self.auth = BasicAuth(http_user, http_password)
        self.list_url = urljoin(src_url, "list")
        self.get_url = urljoin(src_url, "get")
        self.del_url = urljoin(src_url, "del")

    def list(self) -> List[str]:
        resp = requests.get(self.list_url, auth=(self.http_user, self.http_password))
        assert resp.status_code == 200
        return resp.json()

    def save_to(self, name: str, fd: BinaryIO):
        resp = requests.post(self.get_url, auth=(self.http_user, self.http_password), data=name)
        assert resp.status_code == 200
        for chunk in resp.iter_content(chunk_size=2 << 20):
            fd.write(chunk)

    def delete(self, name: str):
        resp = requests.post(self.del_url, auth=(self.http_user, self.http_password), data=name)
        assert resp.status_code == 200


def parse_args(argv: List[str]):
    p = argparse.ArgumentParser()
    subparsers = p.add_subparsers(dest='subparser_name')

    server = subparsers.add_parser('server', help='Run web server')
    server.add_argument("--cert", required=True, help="cert file path")
    server.add_argument("--key", required=True, help="key file path")
    server.add_argument("--password-db", required=True, help="Json file with password database")
    server.add_argument("--storage-folder", required=True, help="Path to store archives")
    server.add_argument("addr", default="0.0.0.0:80", help="Address to listen on")
    server.add_argument("--min-free-space", type=int, default=5,
                        help="Minimal free space should always be available on device in gb")

    user_add = subparsers.add_parser('user_add', help='Add user to db')
    user_add.add_argument("--role", required=True, choices=('download', 'upload'), nargs='+', help="User role")
    user_add.add_argument("--user", required=True, help="User name")
    user_add.add_argument("--password", default=None, help="Password")
    user_add.add_argument("db", help="Json password db")

    user_rm = subparsers.add_parser('user_rm', help='Add user to db')
    user_rm.add_argument("--user", required=True, help="User name")
    user_rm.add_argument("db", help="Json password db")

    return p.parse_args(argv[1:])


def main(argv: List[str]):
    opts = parse_args(argv)

    if opts.subparser_name == 'server':
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(opts.cert, opts.key)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        auth = basic_auth_middleware(json.load(open(opts.password_db)))
        app = web.Application(middlewares=[auth],
                              client_max_size=MAX_FILE_SIZE)
        app.add_routes([web.put('/archives', functools.partial(handle_put, opts.storage_folder, opts.min_free_space)),
                        web.get('/list', functools.partial(handle_list, opts.storage_folder)),
                        web.post('/get', functools.partial(handle_get, opts.storage_folder)),
                        web.post('/del', functools.partial(handle_del, opts.storage_folder))])

        host, port = opts.addr.split(":")

        web.run_app(app, host=host, port=int(port), ssl_context=ssl_context)
    elif opts.subparser_name == 'user_add':
        if os.path.exists(opts.db):
            db = json.load(open(opts.db))
        else:
            db = {}

        if opts.password is None:
            if opts.user not in db:
                print("User not in db yet, provide password")
                exit(1)
            enc_password, salt, _ = db[opts.user]
        else:
            enc_password, salt = encrypt_password(opts.password)
        db[opts.user] = [enc_password, salt, opts.role]
        js = json.dumps(db, indent=4, sort_keys=True)
        open(opts.db, "w").write(js)
    else:
        assert opts.subparser_name == 'user_rm'
        db = json.load(open(opts.db))

        if opts.user not in db:
            exit(0)
        del db[opts.user]

        js = json.dumps(db, indent=4, sort_keys=True)
        open(opts.db, "w").write(js)


if __name__ == "__main__":
    main(sys.argv)

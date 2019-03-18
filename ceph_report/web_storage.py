import re
import sys
import ssl
import json
import os.path
import argparse
import functools
import time
from typing import List, Dict

from aiohttp import web, BasicAuth

MAX_FILE_FIZE = 1 << 30


fname_re = r"(?:ceph_report\.)?(?P<name>.*?)" + \
           r"[._](?P<datetime>20[12]\d_[A-Za-z]{3}_\d{1,2}\.\d\d_\d\d)" + \
           r"\.(?P<ext>tar\.gz|html)$"

fname_rr = re.compile(fname_re)


def upload(func):
    func.auth = 'upload'
    return func


def download(func):
    func.auth = 'download'
    return func


def basic_auth_middleware(pwd_db: Dict[str, Dict[str, str]]):
    @web.middleware
    async def basic_auth(request, handler):
        basic_auth = request.headers.get('Authorization')
        if basic_auth:
            auth = BasicAuth.decode(basic_auth)
            realm = getattr(handler, 'auth')
            if auth.login == pwd_db[realm]['login'] and auth.password == pwd_db[realm]['password']:
                return await handler(request)

        headers = {'WWW-Authenticate': 'Basic realm="{}"'.format('XXX')}
        return web.HTTPUnauthorized(headers=headers)
    return basic_auth


def allowed_file_name(data: str) -> bool:
    return re.match(r"[-a-zA-Z0-9_.]+$", data) is not None


@upload
async def handle_put(target_dir: str, request: web.Request):
    target_name = request.headers.get('Arch-Name')
    enc_passwd = request.headers.get('Enc-Password')

    if request.content_length is None or target_name is None or enc_passwd is None :
        return web.HTTPBadRequest(reason="Some required header(s) missing (Arch-Name/Enc-Password/Content-Length)")

    if not allowed_file_name(target_name):
        return web.HTTPBadRequest(reason="Incorrect file name: {}".format(target_name))

    if not target_name.endswith('.enc'):
        return web.HTTPBadRequest(reason="Incorrect file name: {}".format(target_name))

    if not fname_rr.match(target_name[:-len('.enc')]):
        return web.HTTPBadRequest(reason="Incorrect file name: {}".format(target_name))

    if request.content_length > MAX_FILE_FIZE:
        return web.HTTPBadRequest(reason="File too large, max {} size allowed".format(MAX_FILE_FIZE))

    target_path = os.path.join(target_dir, target_name)
    key_file = target_path + ".key"
    meta_file = target_path + ".meta"

    if any(map(os.path.exists, [target_path, key_file, meta_file])):
        return web.HTTPBadRequest(reason="File exists")

    print("Receiving {} bytes from {} to file {}".format(request.content_length, request.remote, target_name))

    try:
        with open(target_path, "wb") as fd:
            while True:
                data = await request.content.read(1 << 20)
                if len(data) == 0:
                    break
                fd.write(data)

        with open(target_path + ".key", "wb") as fd:
            fd.write(enc_passwd.encode("utf8"))

        with open(target_path + ".meta", "w") as fd:
            fd.write(json.dumps({'upload_time': time.time(), 'src_addr': request.remote}))

    except:
        os.unlink(target_path)
        raise

    return web.Response(text="Saved ")


@download
async def handle_list(target_dir: str, request: web.Request):
    return web.Response(text=json.dumps(os.listdir(target_dir)), content_type="text/json")


@download
async def handle_get_file(target_dir: str, request: web.Request):
    target_name = (await request.read()).decode("utf8")

    if not allowed_file_name(target_name):
        return web.HTTPBadRequest(reason="Incorrect file name: {}".format(target_name))

    return web.FileResponse(os.path.join(target_dir, target_name))


@download
async def handle_del_file(target_dir: str, request: web.Request):
    target_name = (await request.read()).decode("utf8")

    if not allowed_file_name(target_name):
        return web.HTTPBadRequest(reason="Incorrect file name: {}".format(target_name))

    os.unlink(os.path.join(target_dir, target_name))
    return web.Response(text="removed")


def parse_args(argv: List[str]):
    p = argparse.ArgumentParser()
    p.add_argument("--cert", required=True, help="cert file path")
    p.add_argument("--key", required=True, help="key file path")
    p.add_argument("--password-db", required=True, help="Json file with password database")
    p.add_argument("--storage-folder", required=True, help="Path to store archives")
    p.add_argument("addr", default="0.0.0.0:80", help="Address to listen on")
    return p.parse_args(argv[1:])


def main(argv: List[str]):
    opts = parse_args(argv)
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(opts.cert, opts.key)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    auth = basic_auth_middleware(json.load(open(opts.password_db)))
    app = web.Application(middlewares=[],
                          client_max_size=MAX_FILE_FIZE)
    app.add_routes([web.put('/archives', functools.partial(handle_put, opts.storage_folder))])
    app.add_routes([web.get('/list', functools.partial(handle_list, opts.storage_folder))])
    app.add_routes([web.post('/get', functools.partial(handle_get_file, opts.storage_folder))])
    app.add_routes([web.post('/del', functools.partial(handle_del_file, opts.storage_folder))])

    host, port = opts.addr.split(":")

    web.run_app(app, host=host, port=int(port), ssl_context=ssl_context)


if __name__ == "__main__":
    main(sys.argv)

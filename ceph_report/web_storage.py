import re
import sys
import ssl
import json
import os.path
import argparse
import functools
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


@upload
async def handle_put(target_dir: str, request):
    target_name = request.headers.get('Arch-Name')
    enc_passwd = request.headers.get('Enc-Password')

    if request.content_length is None or target_name is None or enc_passwd is None :
        return web.HTTPBadRequest(reason="Some required header(s) missing (Arch-Name/Enc-Password/Content-Length)")

    if not re.match(r"[-a-zA-Z0-9_.]+$", target_name):
        return web.HTTPBadRequest(reason="Incorrect file name: {}".format(target_name))

    if not target_name.endswith('.enc'):
        return web.HTTPBadRequest(reason="Incorrect file name: {}".format(target_name))

    if not fname_rr.match(target_name[:-len('.enc')]):
        return web.HTTPBadRequest(reason="Incorrect file name: {}".format(target_name))

    if request.content_length > MAX_FILE_FIZE:
        return web.HTTPBadRequest(reason="File too large, max {} size allowed".format(MAX_FILE_FIZE))

    target_path = os.path.join(target_dir, target_name)
    if os.path.exists(target_path):
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

    except:
        os.unlink(target_path)
        raise

    return web.Response(text="Saved ")


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

    auth = basic_auth_middleware(json.load(open(opts.password_db)))
    app = web.Application(middlewares=[],
                          client_max_size=MAX_FILE_FIZE)
    app.add_routes([web.put('/archives', functools.partial(handle_put, opts.storage_folder))])

    host, port = opts.addr.split(":")

    web.run_app(app, host=host, port=int(port), ssl_context=ssl_context)


if __name__ == "__main__":
    main(sys.argv)

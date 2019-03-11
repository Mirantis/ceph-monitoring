import shutil
import logging
import urllib.request
from pathlib import Path
from urllib.error import URLError
from typing import Optional, List, Any, Tuple

logger = logging.getLogger('report')

from . import html


class Report:
    def __init__(self, cluster_name: str, output_file_name: str) -> None:
        self.cluster_name = cluster_name
        self.output_file_name = output_file_name
        self.style: List[str] = []
        self.style_links: List[str] = []
        self.script_links: List[str] = []
        self.scripts: List[str] = []
        self.divs: List[Tuple[str, Optional[str], Optional[str], str]] = []
        self.onload: List[str] = ["onHashChanged()"]

    def add_block(self, name: str, header: Optional[str], block_obj: Any, menu_item: str = None):
        if menu_item is None:
            menu_item = header
        self.divs.append((name, header, menu_item, str(block_obj)))

    def insert_js_css(self, link: str, embed: bool, static_files_dir: Path, output_dir: Path) -> Tuple[bool, str]:
        def get_path(link: str) -> Tuple[bool, str]:
            if link.startswith("http://") or link.startswith("https://"):
                return False, link
            fname = link.rsplit('/', 1)[-1]
            return True, str(static_files_dir / fname)

        local, fname = get_path(link)
        data = None

        if local:
            if embed:
                data = open(fname, 'rb').read().decode("utf8")
            else:
                shutil.copyfile(fname, output_dir / Path(fname).name)
        else:
            try:
                data = urllib.request.urlopen(fname, timeout=10).read().decode("utf8")
            except (TimeoutError, URLError):
                logger.warning(f"Can't retrieve {fname}")

        if data is not None:
            return True, data
        else:
            return False, link

    def make_body(self, embed: bool, output_dir: Path, static_files_dir) -> html.Doc:
        self.style_links.append("bootstrap.min.css")
        self.style_links.append("report.css")
        self.script_links.append("report.js")
        self.script_links.append("sorttable_utf.js")

        links: List[str] = []

        for link in self.style_links + self.script_links:
            is_data, link_or_data = self.insert_js_css(link, embed, static_files_dir, output_dir)
            if is_data:
                if link in self.style_links:
                    self.style.append(link_or_data)
                else:
                    self.scripts.append(link_or_data)
            else:
                links.append(link_or_data)

        css_links = links[:len(self.style_links)]
        js_links = links[len(self.style_links):]

        doc = html.Doc()
        with doc.html:
            with doc.head:
                doc.title("Ceph cluster report: " + self.cluster_name)

                for url in css_links:
                    doc.link(href=url, rel="stylesheet", type="text/css")

                if self.style:
                    doc.style("\n".join(self.style), type="text/css")

                for url in js_links:
                    doc.script(type="text/javascript", src=url)

                onload = "    " + ";\n    ".join(self.onload)
                self.scripts.append(f'function onload(){{\n{onload};\n}}')
                code = ";\n".join(self.scripts)

                if embed:
                    doc.script(code, type="text/javascript")
                else:
                    (output_dir / "onload.js").open("w").write(code)
                    doc.script(type="text/javascript", src="onload.js")
            with doc.body(onload="onload()"):
                with doc.div(_class="menu-ceph"):
                    with doc.ul():
                        for idx, div in enumerate(self.divs):
                            if div is not None:
                                name, _, menu, _ = div
                                if menu:
                                    if menu.endswith(":"):
                                        menu = menu[:-1]
                                    doc.li.span(menu,
                                                _class="menulink",
                                                onclick=f"clicked('{name}')",
                                                id=f"ptr_{name}")

                for div in self.divs:
                    doc("\n")
                    if div is not None:
                        name, header, menu_item, block = div
                        with doc.div(_class="data-ceph", id=name):
                            if header is None:
                                doc(block)
                            else:
                                doc.H3.center(header)
                                doc.br

                                if block != "":
                                    doc.center(block)
        return doc

    def encrypt(self, doc: html.Doc, password: str, static_files_dir: Path, output_dir: Path):
        body_s = str(doc) + " "
        if len(body_s) % 32:
            body_s += " " * (32 - len(body_s) % 32)

        import os
        import base64
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.backends import default_backend

        backend = default_backend()
        iv = os.urandom(16)
        cipher = Cipher(algorithms.AES(password), modes.CBC(iv), backend=backend)
        encryptor = cipher.encryptor()
        encrypted_body = iv + encryptor.update(body_s.encode("utf8")) + encryptor.finalize()
        encrypted_body_base64 = base64.b64encode(encrypted_body)

        step = 128
        header = 'encrypted = "'
        idx = step - len(header)
        header += encrypted_body_base64[0: idx] + '"'
        parts = [header]
        while idx < len(encrypted_body_base64):
            parts.append(' + "' + encrypted_body_base64[idx: idx + step] + '"')
            idx += step

        enc_code = " \\\n      ".join(parts)
        doc = html.Doc()
        _, link_or_data = self.insert_js_css("aesjs.js", True, static_files_dir, output_dir)

        with doc.head:
            doc.script(type="text/javascript", src=link_or_data)
            doc.script(type="text/javascript")(enc_code)

        with doc.body.center:
            doc('Report encrypted, to encrypt enter password and press "decrypt": ')
            doc.input(type="password", id="password")
            doc.input(type="button",
                      onclick="decode(document.getElementById('password').value)",
                      value="Decrypt")

    def save_to(self, output_dir: Path, pretty_html: bool = False,
                embed: bool = False, encrypt: str = None):

        static_files_dir = Path(__file__).absolute().parent.parent / "html_js_css"
        doc = self.make_body(embed, output_dir, static_files_dir)

        if encrypt:
            logger.info("Encrypting report with AES")
            assert embed
            self.encrypt(doc, encrypt, static_files_dir, output_dir)

        index = f"<!doctype html>{doc}"
        index_path = output_dir / self.output_file_name
        try:
            if pretty_html:
                from bs4 import BeautifulSoup
                index = BeautifulSoup.BeautifulSoup(index).prettify()
        except:
            pass
        index_path.open("w").write(index)


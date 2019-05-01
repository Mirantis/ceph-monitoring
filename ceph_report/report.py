import shutil
import logging
import urllib.request
from pathlib import Path
from urllib.error import URLError
from typing import Optional, List, Tuple, Dict

from koder_utils import XMLBuilder, RawContent, AnyXML

from .checks import CheckMessage


logger = logging.getLogger('report')


class Report:
    def __init__(self, cluster_name: str, output_file_name: str) -> None:
        self.cluster_name = cluster_name
        self.output_file_name = output_file_name
        self.style: List[str] = []
        self.style_links: List[str] = []
        self.script_links: List[str] = []
        self.scripts: List[str] = []
        self.divs: List[Tuple[str, Optional[str], Optional[str], AnyXML]] = []
        self.onload: List[str] = ["onHashChanged()"]
        self.issues: Dict[str, List[CheckMessage]] = {}

    def add_block(self, name: str, header: Optional[str], block_obj: AnyXML, menu_item: str = None):
        if menu_item is None:
            menu_item = header
        self.divs.append((name, header, menu_item, block_obj))

    def insert_js_css(self, link: str, embed: bool, static_files_dir: Path,
                      output_dir: Optional[Path]) -> Tuple[bool, str]:
        def get_path(link: str) -> Tuple[bool, str]:
            if link.startswith("http://") or link.startswith("https://"):
                return False, link
            fname = link.rsplit('/', 1)[-1]
            return True, str(static_files_dir / fname)

        local, fname = get_path(link)
        data = None

        if local:
            if embed:
                data = open(fname, 'rb').read().decode()
            else:
                assert output_dir is not None
                shutil.copyfile(fname, output_dir / Path(fname).name)
        else:
            try:
                data = urllib.request.urlopen(fname, timeout=10).read().decode()
            except (TimeoutError, URLError):
                logger.warning(f"Can't retrieve {fname}")

        if data is not None:
            return True, data
        else:
            return False, link

    def make_body(self, embed: bool, output_dir: Optional[Path], static_files_dir) -> XMLBuilder:
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
        doc = XMLBuilder()
        with doc.html:
            with doc.head:
                doc.title("Ceph cluster report: " + self.cluster_name)

                for url in css_links:
                    doc.link(href=url, rel="stylesheet", type="text/css")

                if self.style:
                    doc.style(RawContent("\n".join(self.style)), type="text/css")

                for url in js_links:
                    doc.script(type="text/javascript", src=url)

                onload = "    " + ";\n    ".join(self.onload)
                self.scripts.append(f'function onload(){{\n{onload};\n}}')
                code = ";\n".join(self.scripts)

                if embed:
                    doc.script(RawContent(code), type="text/javascript")
                else:
                    assert output_dir is not None
                    (output_dir / "onload.js").open("w").write(code)
                    doc.script(type="text/javascript", src="onload.js")

            with doc.body(onload="onload()"):
                with doc.div(class_="menu-ceph"):
                    with doc.ul():
                        for idx, div in enumerate(self.divs):
                            if div is not None:
                                name, _, menu, _ = div
                                if menu:
                                    if menu.endswith(":"):
                                        menu = menu[:-1]
                                    doc.li.span(menu,
                                                class_="menulink",
                                                onclick=f"clicked('{name}')",
                                                id=f"ptr_{name}")

                for div in self.divs:
                    doc("\n")
                    if div is not None:
                        name, header, menu_item, block = div
                        with doc.div(class_="data-ceph", id=name):
                            if header is None:
                                doc << block
                            else:
                                doc.H3.center(header)
                                doc.br()
                                doc << block
        return doc

    def render(self, pretty_html: bool = False, embed: bool = False) -> str:

        static_files_dir = Path(__file__).absolute().parent.parent / "html_js_css"
        assert embed
        doc = self.make_body(embed, None, static_files_dir)

        index = f"<!doctype html>{doc}"
        try:
            if pretty_html:
                from bs4 import BeautifulSoup
                index = BeautifulSoup(index, features="html.parser").prettify()
        except ImportError:
            logger.error("Can't prettify - BeautifulSoup is not installed")
        except Exception as exc:
            logger.error(f"Failed to prettify {exc}")

        # TODO: I dunno why it need's this
        return index.replace("window.onload = sorttable.init", "")

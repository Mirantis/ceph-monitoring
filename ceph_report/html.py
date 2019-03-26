import sys
from enum import Enum
from typing import Callable, List, Dict, Union, Iterable, Optional, Tuple

from cephlib.units import b2ssize, b2ssize_10


assert sys.version_info >= (3, 6), "This python module must run on 3.6, it requires buidin dict ordering"


class RTag:
    def __getattr__(self, name: str) -> Callable:
        def closure(text: str = "", **attrs: str) -> str:
            name2 = name.replace("_", '-')

            if '_class' in attrs:
                attrs['class'] = attrs.pop('_class')

            if len(attrs) == 0:
                sattrs = ""
            else:
                sattrs = " " + " ".join(f'{name2}="{val}"' for name2, val in attrs.items())

            if name2 == 'br':
                assert text == ""
                assert attrs == {}
                return "<br>"
            elif text == "" and name2 not in ('script', 'link'):
                return f"<{name2}{sattrs} />"
            elif name2 == 'link':
                assert text == ''
                return f"<link{sattrs}>"
            else:
                return f"<{name2}{sattrs}>{text}</{name2}>"
        return closure


rtag = RTag()


class TagProxy:
    def __init__(self, doc: 'Doc', name :str) -> None:
        self.__doc = doc
        self.__name = name
        self.__text = ""
        self.__attrs: Dict[str, str] = {}
        self.__childs: List[Union[str, TagProxy]] = []

    def __call__(self, text: str = "", **attrs) -> 'TagProxy':
        self.__childs.append(text)
        self.__attrs.update(attrs)
        return self

    def __getattr__(self, name: str) -> 'TagProxy':
        tagp = TagProxy(self.__doc, name)
        self.__childs.append(tagp)
        return tagp

    def __enter__(self) -> 'TagProxy':
        self.__doc += self
        return self

    def __exit__(self, x, y, z):
        self.__doc -= self

    def __str__(self) -> str:
        inner = "".join(map(str, self.__childs))
        return getattr(rtag, self.__name)(inner, **self.__attrs)


class Doc:
    def __init__(self) -> None:
        self.__stack: List[TagProxy] = []
        self.__childs: List[TagProxy] = []

    def __getattr__(self, name):
        if len(self.__stack) == 0:
            tagp = TagProxy(self, name)
            self.__childs.append(tagp)
        else:
            tagp = getattr(self.__stack[-1], name)
        return tagp

    def _enter(self, name, text="", **attrs):
        self += getattr(self, name)
        self(text, **attrs)

    def _exit(self):
        self -= self.__stack[-1]

    def __str__(self):
        assert self.__stack == []
        return "".join(map(str, self.__childs))

    def __iadd__(self, tag: TagProxy) -> 'Doc':
        self.__stack.append(tag)
        return self

    def __isub__(self, tag: TagProxy) -> 'Doc':
        assert self.__stack.pop() is tag
        return self

    def __call__(self, text: str = "", **attrs: str):
        assert self.__stack != []
        return self.__stack[-1](text, **attrs)


def ok(text: str) -> TagProxy:
    return rtag.font(text, color="green")


def fail(text: str) -> TagProxy:
    return rtag.font(text, color="red")


def href(text: str, link: str) -> str:
    return f'<a href="{link}">{text}</a>'


class TableAlign(Enum):
    center = 0
    left_right = 1
    center_right = 2
    left_center = 3


class HTMLTable:
    default_classes = {'table-bordered', 'sortable', 'zebra-table'}

    def __init__(self,
                 id: str = None,
                 headers: Iterable[str] = None,
                 table_attrs: Dict[str, str] = None,
                 zebra: bool = True,
                 header_attrs: Dict[str, str] = None,
                 extra_cls: Iterable[str] = None,
                 sortable: bool = True,
                 align: TableAlign = TableAlign.center) -> None:

        assert not isinstance(extra_cls, str)
        self.table_attrs = table_attrs.copy() if table_attrs is not None else {}
        classes = self.default_classes.copy()

        if extra_cls:
            classes.update(extra_cls)

        if not zebra:
            classes.remove('zebra-table')

        if not sortable:
            classes.remove('sortable')

        if align == TableAlign.center:
            classes.add('table_c')
        elif align == TableAlign.left_right:
            classes.add('table_lr')
        elif align == TableAlign.center_right:
            classes.add('table_cr')
        elif align == TableAlign.left_center:
            classes.add('table_lc')
        else:
            raise ValueError(f"Unknown align type: {align}")

        if id is not None:
            self.table_attrs['id'] = id

        self.table_attrs['class'] = " ".join(classes)

        if header_attrs is None:
            header_attrs = {}

        if headers is not None:
            self.headers: List[Tuple[str, Optional[Dict[str, str]]]] = [(header, header_attrs) for header in headers]
        else:
            self.headers = []
        self.cells: List[List] = [[]]

    def add_header(self, text: str, attrs: Dict[str, str] = None):
        self.headers.append((text, attrs))

    def add_cell(self, data: Union[str, TagProxy], **attrs: str):
        self.cells[-1].append((data, attrs))

    def add_cell_b2ssize(self, data: Union[int, float], **attrs: str):
        assert 'sorttable_customkey' not in attrs
        self.add_cell(b2ssize(data), sorttable_customkey=str(data), **attrs)

    def add_cell_b2ssize_10(self, data: Union[int, float], **attrs: str):
        assert 'sorttable_customkey' not in attrs
        self.add_cell(b2ssize_10(data), sorttable_customkey=str(data), **attrs)

    def add_cells(self, *cells: Union[str, TagProxy], **attrs: str):
        self.add_row(cells, **attrs)

    def add_row(self, data: Iterable[Union[str, TagProxy]], **attrs: str):
        for val in data:
            self.add_cell(val, **attrs)
        self.next_row()

    def next_row(self):
        self.cells.append([])

    def __str__(self):
        t = Doc()

        with t.table('', **self.table_attrs):
            with t.thead.tr:
                if self.headers:
                    for header, attrs in self.headers:
                        t.th(header, **attrs)

            with t.tbody:
                for line in self.cells:
                    if line == [] and line is self.cells[-1]:
                        continue
                    with t.tr:
                        for cell, attrs in line:
                            t.td(cell, **attrs)

        return str(t)


H = rtag
HTML_UNKNOWN = H.font('???', color="orange")

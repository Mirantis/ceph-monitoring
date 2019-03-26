import weakref
from typing import Any, Union, Optional, Callable, Iterable, Tuple, List, Dict, Type, Set
from dataclasses import dataclass

from cephlib.units import b2ssize, b2ssize_10

from .html import TagProxy, HTMLTable, ok, fail
from .visualize_utils import partition_by_len, partition


@dataclass
class Field:
    tp: Any
    name: Optional[str] = None
    converter: Callable[[Any], Union[str, TagProxy]] = lambda x: str(x)
    custom_sort: Callable[[Any], str] = lambda x: str(x)
    allow_none: bool = True
    skip_if_no_data: bool = True
    null_sort_key: str = ''
    null_value: str = ''
    dont_sort: bool = False
    help: Optional[str] = None

    def __post_init__(self):
        assert self.tp is not tuple


@dataclass
class ExtraColumns:
    names: Dict[str, str]
    base_type: Field

    def __getattr__(self, name: str) -> Any:
        if name == 'name':
            raise AttributeError(f"Instance of class {self.__class__} has no attribute 'name'")
        return getattr(self.base_type, name)


def count(name: str = None, **kwargs) -> Field:
    return Field(int, name, b2ssize_10, null_sort_key='-1', **kwargs)


def exact_count(name: str = None, **kwargs) -> Field:
    return Field(int, name, null_sort_key='-1', **kwargs)


def bytes_sz(name: str = None, **kwargs) -> Field:
    return Field(int, name, b2ssize, null_sort_key='-1', **kwargs)


def ident(name: str = None, **kwargs) -> Field:
    return Field(str, name, **kwargs)


def to_str(name: str = None, **kwargs) -> Field:
    return Field(None, name, **kwargs)


def float_vl(name: str = None, **kwargs) -> Field:
    return Field((int, float), name, converter=lambda x: f"{x:.2f}", **kwargs)


def seconds(name: str = None, **kwargs) -> Field:
    return count(name, **kwargs)


def idents_list(name: str = None, delim: str = "<br>", chars_per_line: int = None,
                partition_size: int = 1, part_delim: str = ', ', **kwargs) -> Field:
    def converter(vals: List[Any]) -> str:
        if chars_per_line is not None:
            assert partition_size == 1
            data = [((el_r[0], len(el_r[1])) if isinstance(el_r, tuple) else (el_r, len(str(el_r)))) for el_r in vals]
            res = []
            for line in partition_by_len(data, chars_per_line, len(part_delim) if part_delim != ', ' else 1):
                res.append(part_delim.join(map(str, line)))
            return delim.join(res)
        elif partition != 1:
            assert all(not isinstance(vl, tuple) for vl in vals)
            return delim.join(part_delim.join(part) for part in partition(map(str, vals), partition_size))
        else:
            assert all(not isinstance(vl, tuple) for vl in vals)
            return delim.join(map(str, vals))
    return Field(list, name, converter=converter, dont_sort=True, **kwargs)


def ok_or_fail(name: str = None, **kwargs) -> Field:
    return Field(bool, name, converter=lambda val: ok('ok') if val else fail('fail'), **kwargs)


def yes_or_no(name: str = None, true_fine: bool = True, **kwargs) -> Field:
    def converter(val: bool) -> str:
        if true_fine:
            return str(ok('yes') if val else fail('no'))
        else:
            return str(fail('yes') if val else ok('no'))
    return Field(bool, name, converter=converter, **kwargs)


def extra_columns(tp: Field, **names: str) -> ExtraColumns:
    return ExtraColumns(names, tp)


class WithAttrs:
    def __init__(self, val: Any, **attrs: str) -> None:
        self.val = val
        self.attrs = attrs


def prepare_field(table: 'Table', name: str, val: Any, must_fld: bool = True) -> Any:
    fld: Union[Field, ExtraColumns] = getattr(table, name)

    if must_fld:
        assert isinstance(fld, Field)

    if isinstance(fld, ExtraColumns):
        fld = fld.base_type

    if isinstance(val, tuple):
        val, sort_by = val
        rval = WithAttrs(val, sorttable_customkey=str(sort_by))
    else:
        rval = val

    if fld.tp:
        assert isinstance(val, fld.tp) or (fld.allow_none and val is None), \
            f"Field {name} requires type {fld.tp}{' or None' if fld.allow_none else ''} " + \
            f"but get {val!r} of type {type(val)}"

    return rval


class Separator:
    pass


class Row:
    def __init__(self, table: 'Table', target: Union[Type[Separator], Dict[str, Any]]) -> None:
        self.__dict__['_target__'] = target
        self.__dict__['_table__'] = weakref.ref(table)

    def __setattr__(self, name: str, val: Any) -> None:
        table = self._table__()
        assert table
        self._target__[name] = prepare_field(table, name, val)

    def __getattr__(self, name) -> Any:
        table = self._table__()
        assert table
        fld: Union[Field, ExtraColumns] = getattr(table, name)
        assert isinstance(fld, ExtraColumns)

        class Extra:
            def __init__(self, table: 'Table', target: Dict[str, Any]) -> None:
                self.table = weakref.ref(table)
                self.target = target

            def __setitem__(self, key: str, val: Any):
                table = self.table()
                assert table
                self.target[key] = prepare_field(table, name, val, must_fld=False)

        return Extra(table, self._target__)


class Table:

    def __init__(self) -> None:
        self.rows: List[Union[Type[Separator], Dict[str, Any]]] = []
        self.all_names = [name for name, _, _ in self.all_fields()]

    @classmethod
    def all_fields(cls) -> Iterable[Tuple[str, Field, Optional[str]]]:
        for key, val in cls.__dict__.items():
            if isinstance(val, Field):
                yield (key, val, val.name)
            elif isinstance(val, ExtraColumns):
                yield from ((sname, val.base_type, pname) for sname, pname in val.names.items())

    def __init_subclass__(cls, **kwargs):
        for key, val, ext_name in cls.all_fields():
            if isinstance(val, Field) and ext_name is None:
                val.name = key.replace("_", " ").capitalize()

    def next_row(self) -> Row:
        self.rows.append({})
        return Row(self, self.rows[-1])

    def all_headers(self) -> Tuple[List[str], List[str], Dict[str, Field]]:
        items = list(self.all_fields())
        names = []
        printable_names = {}
        types = {}

        for key, fld, pname in items:
            names.append(key)
            printable_names[key] = key if pname is None else pname
            types[key] = fld

        names_set = set(names)

        # find all used keys
        all_keys: Set[Tuple[str, ...]] = set()
        for row in self.rows:
            if row is not Separator:
                assert set(row.keys()).issubset(names_set), f"{row.keys()} {names_set}"  # type: ignore
                all_keys.update(row.keys())  # type: ignore

        headers = [name for name, val, _ in items if name in all_keys or not val.skip_if_no_data]
        headers.sort(key=names.index)
        header_names = [printable_names[name] for name in headers]
        return headers, header_names, types

    def add_row(self, *vals: Any):
        self.rows.append(dict(zip(self.all_names, vals)))

    def add_named_row(self, **vals: Any):
        assert sorted(self.all_names) == sorted(vals.keys()), f"{sorted(self.all_names)}, {sorted(vals.keys())}"
        self.rows.append(vals)

    def html(self, id=None, **kwargs) -> HTMLTable:
        headers, header_names, types = self.all_headers()

        for name, val in getattr(getattr(self, 'html_params', None), '__dict__', {}).items():
            if not name.startswith("__") and name not in kwargs:
                kwargs[name] = val

        sortable = kwargs.get('sortable', True)
        table = HTMLTable(id, headers=header_names, **kwargs)

        for row in self.rows:
            table.next_row()
            if row is Separator:
                table.add_row(["----"] * len(headers))
                continue

            for attr_name in headers:
                val: Any = row.get(attr_name)  # type: ignore
                field = types[attr_name]

                if isinstance(val, WithAttrs):
                    attrs = val.attrs
                    val = val.val
                else:
                    attrs = {}

                h_val = field.null_value if val is None else field.converter(val)  # type: ignore
                if not field.dont_sort and sortable and 'sorttable_customkey' not in attrs:
                    attrs['sorttable_customkey'] = field.null_sort_key \
                                                   if val is None else field.custom_sort(val)   # type: ignore

                table.add_cell(h_val, **attrs)

        return table

    def add_separator(self):
        self.rows.append(Separator)

    @classmethod
    def help(cls) -> str:
        return ""

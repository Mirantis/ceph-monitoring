import math
from collections import Counter
from typing import Iterable, TypeVar, Sequence, Dict, Any, Union, Tuple, List


T = TypeVar('T')


def auto_group_by(items: Sequence[T], border: float = 0.3) -> Iterable[Iterable[T]]:
    all_attrs = set(items[0].__dict__)
    assert all(set(item.__dict__) == all_attrs for item in items)
    entropy = {}
    for attr in all_attrs:
        vals: Dict[str, int] = Counter()
        for item in items:
            vals[getattr(item, attr)] += 1

        entropy[attr] = -sum(vals[val] / len(items) * math.log2(vals[val] / len(items)) for val in vals)

    mutable_keys = [attr for attr, val in entropy.items() if val > border]
    for group in group_by((item.__dict__ for item in items), mutable_keys):
        yield [items[idx] for idx in group]


def group_by(items: Iterable[Dict[str, Any]], mutable_keys=Union[str, Tuple[str, ...]]) -> Iterable[List[int]]:
    if isinstance(mutable_keys, str):
        mutable_keys = (mutable_keys,)

    mutable_keys_s = set(mutable_keys)

    grouped: Dict[Tuple, List[int]] = {}
    for idx, dct in enumerate(items):
        group_idx = tuple(dct[key] for key in dct if key not in mutable_keys_s)
        grouped.setdefault(group_idx, []).append(idx)

    return grouped.values()

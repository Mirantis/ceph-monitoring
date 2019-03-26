import asyncio

from typing import List, Tuple, Dict, Sequence
from urllib.parse import urljoin

import aiohttp


async def query_dev_metric(url: str,
                           name: str,
                           hosts: Sequence[str],
                           devs: Sequence[str],
                           range_minutes: int,
                           offset_minutes: int = 0) -> Dict[Tuple[str, str], List[float]]:

    query = '{name}{{host=~"{hosts}", name=~"{devs}"}}[{range}m]'.format(name=name,
                                                                         hosts="|".join(hosts),
                                                                         devs="|".join(devs),
                                                                         range=range_minutes)

    if offset_minutes != 0:
        query += " offset {}m".format(offset_minutes)

    print(query)

    async with aiohttp.ClientSession() as sess:
        async with sess.get(urljoin(url, "/api/v1/query"), params={'query': query}) as resp:
            json = await resp.json()

    assert json['status'] == 'success', json

    res = {}

    for item in json['data']['result']:
        metric = item['metric']
        key = (metric['host'], metric['name'])
        res[key] = [float(vl) for _, vl in item['values']]

    return res


async def get_block_devs_loads(url: str, hosts: List[str], range_minutes: int,
                               devs: Sequence[str] = ("sd[a-z][a-z]?", "nvme[0-9]n[0-9]", "hd[a-z][a-z]?")) \
                                    -> Dict[str, Dict[Tuple[str, str], List[float]]]:
    res = {}

    metrics = [
        "diskio_io_time",
        "diskio_read_bytes",
        "diskio_read_time",
        "diskio_reads",
        "diskio_weighted_io_time",
        "diskio_write_bytes",
        "diskio_write_time",
        "diskio_writes"
    ]
    for metric in metrics:
        res[metric] =await query_dev_metric(url, name="diskio_io_time",
                                            hosts=hosts, devs=devs, range_minutes=range_minutes)
    return res


# loop = asyncio.get_event_loop()
# vals = loop.run_until_complete(get_block_devs_loads("http://mon01:15010",
#                                                     hosts=["osd[0-9]+"],
#                                                     range_minutes=1))
# loop.close()
#
# import pprint
# pprint.pprint(vals)

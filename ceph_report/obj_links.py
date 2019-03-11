from typing import NamedTuple


Link = NamedTuple('Link', [('link', str), ('id', str)])


def host_link(hostname: str) -> Link:
    return Link(f"""<span class="objlink" onclick="clicked('host-{hostname}')">{hostname}</span>""",
                f'host-{hostname}')


def osd_link(osd_id: int) -> Link:
    return Link(f"""<span class="objlink" onclick="clicked('osd-{osd_id}')">{osd_id}</span>""",
                f'osd-{osd_id}')


def mon_link(name: str) -> Link:
    return Link(f"""<span class="objlink" onclick="clicked('mon-{name}')">{name}</span>""",
                f'mon-{name}')


def err_link(reporter_id: str, mess: str = None) -> Link:
    return Link(f"""<span class="objlink" onclick="clicked('err-{reporter_id}')">{mess}</span>""",
                f'err-{reporter_id}')


def rule_link(rule_name: str, rule_id: int) -> Link:
    return Link(f"""<span class="objlink" onclick="clicked('ruleset_info')">{rule_name}({rule_id})</span>""",
                'ruleset_info')


def pool_link(pool_name: str) -> Link:
    return Link(f"""<span class="objlink" onclick="clicked('pools_info')">{pool_name}</span>""",
                'pools_info')


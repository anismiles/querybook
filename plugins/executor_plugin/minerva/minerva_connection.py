from typing import NamedTuple, Optional
import re

from lib.query_executor.connection_string.helpers.common import split_hostport, random_choice
from executor_plugin.minerva.const import connection_regex, apikey_regex

class MinervaConnectionConf(NamedTuple):
    protocol: str
    host: str
    port: Optional[int]
    depot: Optional[str]
    collection: Optional[str]


def get_minerva_connection_conf(connection_string: str) -> MinervaConnectionConf:
    match = re.search(
        connection_regex,
        connection_string,
    )

    protocol = match.group(1)
    raw_hosts = match.group(2)
    depot = (match.group(3) or "/hive")[1:]
    collection = (match.group(4) or "/default")[1:]

    parsed_hosts = [split_hostport(hostport) for hostport in raw_hosts.split(",")]
    hostname, port = random_choice(parsed_hosts, default=(None, None))

    return MinervaConnectionConf(
        host=hostname, port=port, depot=depot, collection=collection, protocol=protocol
    )

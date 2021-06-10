from typing import NamedTuple, Optional
import re

from .helpers.common import split_hostport, random_choice


class MinervaConnectionConf(NamedTuple):
    protocol: str
    host: str
    port: Optional[int]
    depot: Optional[str]
    collection: Optional[str]


def get_minerva_connection_conf(connection_string: str) -> MinervaConnectionConf:
    match = re.search(
        r"^(http|https):\/\/([\w.-]+(?:\:\d+)?(?:,[\w.-]+(?:\:\d+)?)*)(\/\w+)?(\/\w+)?(\?[\w.-]+=[\w.-]+(?:&[\w.-]+=[\w.-]+)*)?$",
        connection_string,  # https://easily-champion-frog.dataos.io:7432/depot/collection
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

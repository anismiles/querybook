from pyhive import presto

from lib.logger import get_logger
from lib.query_executor.base_client import ClientBaseClass, CursorBaseClass
from executor_plugin.minerva.minerva_connection import get_minerva_connection_conf

from logic.user import get_user_by_name

LOG = get_logger(__file__)


class MinervaClient(ClientBaseClass):
    def __init__(
        self, connection_string, apikey=None, proxy_user=None, *args, **kwargs
    ):
        minerva_conf = get_minerva_connection_conf(connection_string)
        current_user = get_user_by_name(proxy_user)
        current_user_apikey = current_user.properties["heimdall"]

        protocol = minerva_conf.protocol
        host = minerva_conf.host
        port = 7432 if not minerva_conf.port else minerva_conf.port
        username = current_user_apikey or apikey
        catalog = minerva_conf.depot
        schema = minerva_conf.collection
        source = f"Storybook/" # TODO: Add Version

        LOG.debug(f"MinervaClient => address: {protocol}://{host}:{port} source: {source}")

        # default to querybook credentials if user/pwd is not supplied
        # we pass auth credentials through requests_kwargs instead of
        # using requests library's builtin auth to bypass the https requirement
        # and set the proper Authorization header
        req_kwargs = {}

        connection = presto.connect(
            host,
            port=port,
            username=username,
            catalog=catalog,
            schema=schema,
            protocol=protocol,
            source=source,
            requests_kwargs=req_kwargs,
        )
        self._connection = connection
        super(MinervaClient, self).__init__()

    def cursor(self):
        return MinervaCursor(cursor=self._connection.cursor())


class MinervaCursor(CursorBaseClass):
    def __init__(self, cursor):
        self._cursor = cursor
        self._init_query_state_vars()

    def _init_query_state_vars(self):
        self._tracking_url = None
        self._percent_complete = 0

    def run(self, query: str):
        self._init_query_state_vars()
        self._cursor.execute(query)

    def cancel(self):
        self._cursor.cancel()

    def poll(self):
        poll_result = self._cursor.poll()

        # PyHive does not support presto async, so we need to hack
        status = self._cursor._state
        # Finished if status is not running or none
        completed = status not in (
            self._cursor._STATE_RUNNING,
            self._cursor._STATE_NONE,
        )

        if poll_result:
            self._update_percent_complete(poll_result)
            self._update_tracking_url(poll_result)

        return completed

    def get_one_row(self):
        return self._cursor.fetchone()

    def get_n_rows(self, n: int):
        return self._cursor.fetchmany(size=n)

    def get_columns(self):
        description = self._cursor.description
        if description is None:
            # Not a select query, no return
            return None
        else:
            columns = list(map(lambda d: d[0], description))
            return columns

    @property
    def tracking_url(self):
        return self._tracking_url

    @property
    def percent_complete(self):
        return self._percent_complete

    def _update_percent_complete(self, poll_result):
        stats = poll_result.get("stats", {})
        completed_splits = stats.get("completedSplits", 0)
        total_splits = max(stats.get("totalSplits", 1), 1)
        self._percent_complete = (completed_splits * 100) / total_splits

    def _update_tracking_url(self, poll_result):
        if self._tracking_url is None:
            self._tracking_url = poll_result.get("infoUri", None)

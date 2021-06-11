import re
import time
from pyhive import presto
from pyhive.exc import DatabaseError
from typing import Dict, List, Tuple

from lib.query_executor.connection_string.helpers.common import (
    split_hostport,
    random_choice,
)
from lib.form import StructFormField, FormField
from lib.metastore.base_metastore_loader import (
    BaseMetastoreLoader,
    DataTable,
    DataColumn,
)

from lib.logger import get_logger

LOG = get_logger(__file__)

connection_regex = r"^(http|https):\/\/([\w.-]+(?:\:\d+)?(?:,[\w.-]+(?:\:\d+)?)*)(\/\w+)?(\/\w+)?(\?[\w.-]+=[\w.-]+(?:&[\w.-]+=[\w.-]+)*)?$"
depot_regex = r"^[A-Za-z0-9_]+$"
apikey_regex = r"^[A-Za-z0-9]+$"

"""
SELECT
  table_cat,
  table_schem,
  table_name,
  column_Name,
  data_type,
  type_name,
  column_size
FROM
  system.jdbc.columns
WHERE
  table_schem NOT IN ('pg_catalog', 'information_schema', 'definitions')
  AND table_cat NOT IN ('system')
"""


def _parse_connection(connection_string: str):
    match = re.search(connection_regex, connection_string,)

    protocol = match.group(1)
    raw_hosts = match.group(2)
    parsed_hosts = [split_hostport(hostport) for hostport in raw_hosts.split(",")]
    hostname, port = random_choice(parsed_hosts, default=(None, None))

    return protocol, hostname, port


class MinervaMetadataLoader(BaseMetastoreLoader):
    def __init__(self, metastore_dict: Dict):
        protocol, hostname, port = _parse_connection(
            metastore_dict.get("metastore_params").get("connection")
        )
        self.protocol = protocol
        self.hostname = hostname
        self.port = port
        self.depot = metastore_dict.get("metastore_params").get("depot")
        self.apikey = metastore_dict.get("metastore_params").get("apikey")

        LOG.info(f"********** MinervaMetadataLoader/{self.depot} **********")
        super(MinervaMetadataLoader, self).__init__(metastore_dict)

    @classmethod
    def get_metastore_params_template(cls):
        return StructFormField(
            connection=FormField(
                required=True,
                regex=connection_regex,
                helper="""
<p>Format
<code>https://[host:port]</code></p>
<p>`Depot` and `Collection` are optional.</p>
<p>See [here](https://faq.tmdc.io) for more details.</p>""",
            ),
            depot=FormField(
                required=True,
                regex=depot_regex,
                helper="""
<p>Application apikey</p>
            """,
            ),
            apikey=FormField(
                required=True,
                regex=apikey_regex,
                helper="""
<p>Application apikey</p>
        """,
            ),
        )

    def run_query(self, query: str):
        req_kwargs = {}
        connection = presto.connect(
            protocol=self.protocol,
            host=self.hostname,
            port=self.port,
            username=self.apikey,
            catalog=self.depot,
            requests_kwargs=req_kwargs,
        )

        cursor = connection.cursor()
        try:
            LOG.debug(f"run_query: {query}")
            cursor.execute(query)
            columns = list(map(lambda d: d[0], cursor.description))
            rows = cursor.fetchall()
        except DatabaseError:
            LOG.error("***** ERROR")

        return columns, rows

    def get_all_schema_names(self) -> List[str]:
        LOG.info("get_all_schema_names")
        query = f"""
            SELECT
                DISTINCT CONCAT(table_cat, '.', table_schem)
            FROM
                system.jdbc.columns
            WHERE
                table_schem NOT IN ('pg_catalog', 'information_schema', 'definitions')
                AND table_cat NOT IN ('system')
        """
        columns, rows = self.run_query(query)
        schemas = [row[0] for row in rows]
        LOG.info(f">> schemas: {schemas}")
        return schemas

    def get_all_table_names_in_schema(self, schema_name: str) -> List[str]:
        LOG.info(f"get_all_table_names_in_schema: {schema_name}")
        [catalog, schema] = schema_name.split(".")
        query = f"""
            SELECT
                DISTINCT table_name
            FROM
                system.jdbc.columns
            WHERE
                table_cat = '{catalog}'
                AND table_schem = '{schema}'
        """
        columns, rows = self.run_query(query)
        tables = [row[0] for row in rows]
        LOG.info(f">> tables: {tables}")
        return tables

    def get_table_and_columns(
        self, schema_name: str, table_name: str
    ) -> Tuple[DataTable, List[DataColumn]]:
        LOG.info(f"get_table_and_columns: {schema_name}/{table_name}")
        [catalog, schema] = schema_name.split(".")
        query = f"""
            SELECT
                table_cat,
                table_schem,
                table_name,
                column_Name,
                type_name,
                remarks,
                data_type,
                column_size
            FROM
                system.jdbc.columns
            WHERE
                table_cat = '{catalog}'
                AND table_schem = '{schema}'
                AND table_name = '{table_name}'
        """
        columns, rows = self.run_query(query)

        table = DataTable(
            name=table_name,
            table_created_at=int(time.time()),
            table_updated_at=int(time.time()),
        )

        columns = [DataColumn(row[3], row[4], row[5]) for row in rows]

        return table, columns

        # glue_table = self.glue_client.get_table(schema_name, table_name).get("Table")
        #
        # if self.load_partitions:
        #     partitions = self.glue_client.get_hms_style_partitions(
        #         schema_name, table_name
        #     )
        # else:
        #     partitions = []
        #
        # table = DataTable(
        #     name=glue_table.get("Name"),
        #     type=glue_table.get("TableType"),
        #     owner=glue_table.get("Owner"),
        #     table_created_at=int(
        #         glue_table.get("CreateTime", datetime(1970, 1, 1)).timestamp()
        #     ),
        #     table_updated_at=int(
        #         glue_table.get("UpdateTime", datetime(1970, 1, 1)).timestamp()
        #     ),
        #     location=glue_table.get("StorageDescriptor").get("Location"),
        #     partitions=partitions,
        #     raw_description=glue_table.get("Description"),
        # )
        #
        # columns = [
        #     DataColumn(col.get("Name"), col.get("Type"), col.get("Comment"))
        #     for col in glue_table.get("StorageDescriptor").get("Columns")
        # ]
        #
        # columns.extend(
        #     [
        #         DataColumn(col.get("Name"), col.get("Type"), col.get("Comment"))
        #         for col in glue_table.get("PartitionKeys")
        #     ]
        # )
        #
        # return table, columns

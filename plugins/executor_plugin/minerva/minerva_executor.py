from pyhive.exc import Error

from const.query_execution import QueryExecutionErrorType
from lib.query_executor.base_executor import QueryExecutorBaseClass
from lib.query_executor.utils import get_parsed_syntax_error
from lib.form import FormField, StructFormField

from executor_plugin.minerva.const import connection_regex, apikey_regex
from executor_plugin.minerva.minerva_client import MinervaClient

minerva_executor_template = StructFormField(
    connection_string=FormField(
        required=True,
        regex=connection_regex,
        helper="""
<p>Format
<code>https://[host:port]/[depot]/[collection]</code></p>
<p>`Depot` and `Collection` are optional.</p>
<p>See [here](https://faq.tmdc.io) for more details.</p>""",
    ),
    apikey=FormField(
        required=False,
        regex=apikey_regex,
        helper="""
<p>Application apikey</p>
        """,
    ),
)


def get_minerva_error_dict(e):
    if hasattr(e, "args") and e.args[0] is not None:
        error_arg = e.args[0]
        if type(error_arg) is dict:
            return error_arg
    return None


class MinervaQueryExecutor(QueryExecutorBaseClass):
    @classmethod
    def _get_client(cls, client_setting):
        return MinervaClient(**client_setting)

    @classmethod
    def EXECUTOR_NAME(cls):
        return "minerva"

    @classmethod
    def EXECUTOR_LANGUAGE(cls):
        return "minerva"

    @classmethod
    def EXECUTOR_TEMPLATE(cls):
        return minerva_executor_template

    def _parse_exception(self, e):
        error_type = QueryExecutionErrorType.INTERNAL.value
        error_str = str(e)
        error_extracted = None

        try:
            if isinstance(e, Error):
                error_type = QueryExecutionErrorType.ENGINE.value
                error_dict = get_minerva_error_dict(e)
                if error_dict:
                    error_extracted = error_dict.get("message", None)
                    # In Presto, only context free syntax error are labelled as
                    # SYNTAX_ERROR, and context sensitive errors are user errors
                    # However in both cases errorLocation is provided
                    if "errorLocation" in error_dict:
                        return get_parsed_syntax_error(
                            error_extracted,
                            error_dict["errorLocation"].get("lineNumber", 1) - 1,
                            error_dict["errorLocation"].get("columnNumber", 1) - 1,
                        )

        except Exception:
            pass
        return error_type, error_str, error_extracted

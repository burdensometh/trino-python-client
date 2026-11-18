# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import threading
import uuid
from decimal import Decimal
from unittest.mock import patch

import httpretty
import pytest
from httpretty import httprettified
from requests import Session

import trino.exceptions
from tests.unit.oauth_test_utils import _get_token_requests
from tests.unit.oauth_test_utils import _post_statement_requests
from tests.unit.oauth_test_utils import GetTokenCallback
from tests.unit.oauth_test_utils import PostStatementCallback
from tests.unit.oauth_test_utils import REDIRECT_RESOURCE
from tests.unit.oauth_test_utils import RedirectHandler
from tests.unit.oauth_test_utils import SERVER_ADDRESS
from tests.unit.oauth_test_utils import TOKEN_RESOURCE
from trino import constants
from trino import sqltypes
from trino.auth import BasicAuthentication
from trino.auth import OAuth2Authentication
from trino.dbapi import Binary
from trino.dbapi import connect
from trino.dbapi import Connection
from trino.dbapi import Cursor


@patch("trino.dbapi.trino.client")
def test_http_session_is_correctly_passed_in(mock_client):
    # GIVEN
    test_session = Session()
    test_session.proxies = {"http": "some.http.proxy:81", "https": "some.http.proxy:81"}

    # WHEN
    with connect("sample_trino_cluster:443", http_session=test_session) as conn:
        conn.cursor().execute("SOME FAKE QUERY")

    # THEN
    request_args, _ = mock_client.TrinoRequest.call_args
    assert test_session in request_args


@patch("trino.dbapi.trino.client")
def test_http_session_is_defaulted_when_not_specified(mock_client):
    # WHEN
    with connect("sample_trino_cluster:443") as conn:
        conn.cursor().execute("SOME FAKE QUERY")

    # THEN
    request_args, _ = mock_client.TrinoRequest.call_args
    assert mock_client.TrinoRequest.http.Session.return_value in request_args


@httprettified
def test_token_retrieved_once_per_auth_instance(sample_post_response_data, sample_get_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    get_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_get_response_data)

    # bind post statement to submit query
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

    # bind get statement for result retrieval
    httpretty.register_uri(
        method=httpretty.GET,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}/20210817_140827_00000_arvdv/1",
        body=get_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    with connect(
            "coordinator",
            user="test",
            auth=OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
            http_scheme=constants.HTTPS
    ) as conn:
        conn.cursor().execute("SELECT 1")
        conn.cursor().execute("SELECT 2")
        conn.cursor().execute("SELECT 3")

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    with connect(
            "coordinator",
            user="test",
            auth=OAuth2Authentication(redirect_auth_url_handler=redirect_handler),
            http_scheme=constants.HTTPS
    ) as conn2:
        conn2.cursor().execute("SELECT 1")
        conn2.cursor().execute("SELECT 2")
        conn2.cursor().execute("SELECT 3")

    assert len(_get_token_requests(challenge_id)) == 1


@httprettified
def test_token_retrieved_once_when_authentication_instance_is_shared(sample_post_response_data,
                                                                     sample_get_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    get_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_get_response_data)

    # bind post statement to submit query
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

    # bind get statement for result retrieval
    httpretty.register_uri(
        method=httpretty.GET,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}/20210817_140827_00000_arvdv/1",
        body=get_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    authentication = OAuth2Authentication(redirect_auth_url_handler=redirect_handler)

    with connect(
            "coordinator",
            user="test",
            auth=authentication,
            http_scheme=constants.HTTPS
    ) as conn:
        conn.cursor().execute("SELECT 1")
        conn.cursor().execute("SELECT 2")
        conn.cursor().execute("SELECT 3")

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    with connect(
            "coordinator",
            user="test",
            auth=authentication,
            http_scheme=constants.HTTPS
    ) as conn2:
        conn2.cursor().execute("SELECT 1")
        conn2.cursor().execute("SELECT 2")
        conn2.cursor().execute("SELECT 3")

    assert len(_post_statement_requests()) == 7
    assert len(_get_token_requests(challenge_id)) == 1


@httprettified
def test_token_retrieved_once_when_multithreaded(sample_post_response_data, sample_get_response_data):
    token = str(uuid.uuid4())
    challenge_id = str(uuid.uuid4())

    redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
    token_server = f"{TOKEN_RESOURCE}/{challenge_id}"

    post_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_post_response_data)
    get_statement_callback = PostStatementCallback(redirect_server, token_server, [token], sample_get_response_data)

    # bind post statement to submit query
    httpretty.register_uri(
        method=httpretty.POST,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
        body=post_statement_callback)

    # bind get statement for result retrieval
    httpretty.register_uri(
        method=httpretty.GET,
        uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}/20210817_140827_00000_arvdv/1",
        body=get_statement_callback)

    # bind get token
    get_token_callback = GetTokenCallback(token_server, token)
    httpretty.register_uri(
        method=httpretty.GET,
        uri=token_server,
        body=get_token_callback)

    redirect_handler = RedirectHandler()

    authentication = OAuth2Authentication(redirect_auth_url_handler=redirect_handler)

    conn = connect(
        "coordinator",
        user="test",
        auth=authentication,
        http_scheme=constants.HTTPS
    )

    class RunningThread(threading.Thread):
        lock = threading.Lock()

        def __init__(self):
            super().__init__()

        def run(self) -> None:
            with RunningThread.lock:
                conn.cursor().execute("SELECT 1")

    threads = [
        RunningThread(),
        RunningThread(),
        RunningThread()
    ]

    # run and join all threads
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(_get_token_requests(challenge_id)) == 1


@patch("trino.dbapi.trino.client")
def test_tags_are_set_when_specified(mock_client):
    client_tags = ["TAG1", "TAG2"]
    with connect("sample_trino_cluster:443", client_tags=client_tags) as conn:
        conn.cursor().execute("SOME FAKE QUERY")

    _, passed_client_tags = mock_client.ClientSession.call_args
    assert passed_client_tags["client_tags"] == client_tags


@patch("trino.dbapi.trino.client")
def test_role_is_set_when_specified(mock_client):
    roles = {"system": "finance"}
    with connect("sample_trino_cluster:443", roles=roles) as conn:
        conn.cursor().execute("SOME FAKE QUERY")

    _, passed_role = mock_client.ClientSession.call_args
    assert passed_role["roles"] == roles


def test_hostname_parsing():
    https_server_with_port = Connection("https://mytrinoserver.domain:9999")
    assert https_server_with_port.host == "mytrinoserver.domain"
    assert https_server_with_port.port == 9999
    assert https_server_with_port.http_scheme == constants.HTTPS

    https_server_without_port = Connection("https://mytrinoserver.domain")
    assert https_server_without_port.host == "mytrinoserver.domain"
    assert https_server_without_port.port == constants.DEFAULT_TLS_PORT
    assert https_server_without_port.http_scheme == constants.HTTPS

    http_server_with_port = Connection("http://mytrinoserver.domain:9999")
    assert http_server_with_port.host == "mytrinoserver.domain"
    assert http_server_with_port.port == 9999
    assert http_server_with_port.http_scheme == constants.HTTP

    http_server_without_port = Connection("http://mytrinoserver.domain")
    assert http_server_without_port.host == "mytrinoserver.domain"
    assert http_server_without_port.port == constants.DEFAULT_PORT
    assert http_server_without_port.http_scheme == constants.HTTP

    http_server_with_path = Connection("http://mytrinoserver.domain/some_path")
    assert http_server_with_path.host == "mytrinoserver.domain/some_path"
    assert http_server_with_path.port == constants.DEFAULT_PORT
    assert http_server_with_path.http_scheme == constants.HTTP

    only_hostname = Connection("mytrinoserver.domain")
    assert only_hostname.host == "mytrinoserver.domain"
    assert only_hostname.port == constants.DEFAULT_PORT
    assert only_hostname.http_scheme == constants.HTTP

    only_hostname_with_path = Connection("mytrinoserver.domain/some_path")
    assert only_hostname_with_path.host == "mytrinoserver.domain/some_path"
    assert only_hostname_with_path.port == constants.DEFAULT_PORT
    assert only_hostname_with_path.http_scheme == constants.HTTP


def test_description_is_none_when_cursor_is_not_executed():
    connection = Connection("sample_trino_cluster:443")
    with connection.cursor() as cursor:
        assert hasattr(cursor, 'description')


@pytest.mark.parametrize(
    "host, port, http_scheme_input_argument, http_scheme_set",
    [
        # Infer from hostname
        ("https://mytrinoserver.domain:9999", None, None, constants.HTTPS),
        ("http://mytrinoserver.domain:9999", None, None, constants.HTTP),
        # Infer from port
        ("mytrinoserver.domain", constants.DEFAULT_TLS_PORT, None, constants.HTTPS),
        ("mytrinoserver.domain", constants.DEFAULT_PORT, None, constants.HTTP),
        # http_scheme parameter has higher precedence than port parameter
        ("mytrinoserver.domain", constants.DEFAULT_TLS_PORT, constants.HTTP, constants.HTTP),
        ("mytrinoserver.domain", constants.DEFAULT_PORT, constants.HTTPS, constants.HTTPS),
        # Set explicitly by http_scheme parameter
        ("mytrinoserver.domain", None, constants.HTTPS, constants.HTTPS),
        # Default
        ("mytrinoserver.domain", None, None, constants.HTTP),
    ],
)
def test_setting_http_scheme(host, port, http_scheme_input_argument, http_scheme_set):
    connection = Connection(host, port, http_scheme=http_scheme_input_argument)
    assert connection.http_scheme == http_scheme_set


@patch("trino.client.CODECS_UNAVAILABLE", {"lz4": "Not installed", "zstd": "Not installed"})
def test_default_encoding_no_compression():
    connection = Connection("host", 8080, user="test")
    assert connection._client_session.encoding == ["json"]


@patch("trino.client.CODECS_UNAVAILABLE", {"zstd": "Not installed"})
def test_default_encoding_lz4():
    connection = Connection("host", 8080, user="test")
    assert connection._client_session.encoding == ["json+lz4", "json"]


@patch("trino.client.CODECS_UNAVAILABLE", {"lz4": "Not installed"})
def test_default_encoding_zstd():
    connection = Connection("host", 8080, user="test")
    assert connection._client_session.encoding == ["json+zstd", "json"]


@patch("trino.client.CODECS_UNAVAILABLE", {})
def test_default_encoding_all():
    connection = Connection("host", 8080, user="test")
    assert connection._client_session.encoding == ["json+zstd", "json+lz4", "json"]


def test_error_when_auth_over_http():
    with pytest.raises(trino.exceptions.TrinoAuthError, match="TLS/SSL is required for authentication"):
        Connection("mytrinoserver.domain", http_scheme=constants.HTTP, auth=BasicAuthentication("u", "p"))


def test_no_error_when_auth_over_https():
    Connection("mytrinoserver.domain", http_scheme=constants.HTTPS, auth=BasicAuthentication("u", "p"))


def test_error_when_auth_over_http_mentions_allow_insecure_auth():
    with pytest.raises(trino.exceptions.TrinoAuthError, match="allow_insecure_auth=True"):
        Connection("mytrinoserver.domain", http_scheme=constants.HTTP, auth=BasicAuthentication("u", "p"))


def test_no_error_when_auth_over_http_with_allow_insecure_auth():
    connection = Connection(
        "mytrinoserver.domain",
        http_scheme=constants.HTTP,
        auth=BasicAuthentication("u", "p"),
        allow_insecure_auth=True,
    )
    assert connection.http_scheme == constants.HTTP
    # Ensure the flag doesn't just suppress the constructor check, but also
    # doesn't prevent building a request object that would actually be used
    # to send the (insecure) authenticated requests.
    request = connection._create_request()
    assert request._http_scheme == constants.HTTP


def _statement_uri(query_id, token):
    return f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}/{query_id}/{token}"


@httprettified
def test_cursor_close_does_not_cancel_finished_update_query():
    """Regression test for https://github.com/trinodb/trino-python-client/issues/601

    An update statement (INSERT/UPDATE/DELETE) reports its affected row count as
    a single synthetic row while Trino still returns a final nextUri. Closing the
    cursor without fetching must drain that nextUri instead of issuing a DELETE,
    otherwise the already-completed statement is reported as USER_CANCELED.
    """
    query_id = "20210817_140827_00000_arvdv"
    statement_path = f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}"

    post_response = {
        "id": query_id,
        "nextUri": _statement_uri(query_id, 1),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{query_id}",
        "stats": {"state": "QUEUED"},
    }
    # The update-count row arrives together with a still-present nextUri.
    update_response = {
        "id": query_id,
        "nextUri": _statement_uri(query_id, 2),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{query_id}",
        "updateType": "INSERT",
        "updateCount": 1000,
        "columns": [{
            "name": "rows",
            "type": "bigint",
            "typeSignature": {"rawType": "bigint", "arguments": [], "typeArguments": []},
        }],
        "data": [[1000]],
        "stats": {"state": "FINISHED"},
    }
    # Final response transitions the query to a terminal state (no nextUri).
    # Trino keeps repeating updateType/updateCount on the trailing pages.
    final_response = {
        "id": query_id,
        "infoUri": f"{SERVER_ADDRESS}/query.html?{query_id}",
        "updateType": "INSERT",
        "updateCount": 1000,
        "columns": update_response["columns"],
        "stats": {"state": "FINISHED"},
    }

    httpretty.register_uri(method=httpretty.POST, uri=statement_path, body=json.dumps(post_response))
    httpretty.register_uri(method=httpretty.GET, uri=_statement_uri(query_id, 1), body=json.dumps(update_response))
    httpretty.register_uri(method=httpretty.GET, uri=_statement_uri(query_id, 2), body=json.dumps(final_response))
    httpretty.register_uri(method=httpretty.DELETE, uri=_statement_uri(query_id, 2), status=204)

    with connect("coordinator", user="test", http_scheme=constants.HTTPS) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO some_table VALUES (1), (2), (3)")
        # execute() must have drained the query to a terminal state.
        assert cur._query.finished is True
        assert cur.rowcount == 1000
        cur.close()

    delete_requests = [r for r in httpretty.latest_requests() if r.method == "DELETE"]
    assert delete_requests == [], "closing a finished update query must not issue a cancel"


@httprettified
def test_cursor_close_cancels_unfinished_query():
    """Closing a cursor whose result set has not been fully consumed must still
    cancel the running query so the server can free its resources.
    """
    query_id = "20210817_140827_00000_arvdv"
    statement_path = f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}"

    post_response = {
        "id": query_id,
        "nextUri": _statement_uri(query_id, 1),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{query_id}",
        "stats": {"state": "QUEUED"},
    }
    # A SELECT that returns a first page of data with more still pending.
    data_response = {
        "id": query_id,
        "nextUri": _statement_uri(query_id, 2),
        "infoUri": f"{SERVER_ADDRESS}/query.html?{query_id}",
        "columns": [{
            "name": "x",
            "type": "bigint",
            "typeSignature": {"rawType": "bigint", "arguments": [], "typeArguments": []},
        }],
        "data": [[1]],
        "stats": {"state": "RUNNING"},
    }

    httpretty.register_uri(method=httpretty.POST, uri=statement_path, body=json.dumps(post_response))
    httpretty.register_uri(method=httpretty.GET, uri=_statement_uri(query_id, 1), body=json.dumps(data_response))
    httpretty.register_uri(method=httpretty.DELETE, uri=_statement_uri(query_id, 2), status=204)

    with connect("coordinator", user="test", http_scheme=constants.HTTPS) as conn:
        cur = conn.cursor()
        cur.execute("SELECT x FROM some_table")
        assert cur._query.finished is False
        cur.close()

    delete_requests = [r for r in httpretty.latest_requests() if r.method == "DELETE"]
    assert len(delete_requests) == 1, "closing an unfinished query must cancel it"


@pytest.mark.parametrize(
    "value,expected",
    [
        (b"", b""),
        (b"hello", b"hello"),
        (b"\xff\xfe\x00\x01", b"\xff\xfe\x00\x01"),
        (bytearray(b"abc"), b"abc"),
        (memoryview(b"xyz"), b"xyz"),
        ("hello", b"hello"),
    ]
)
def test_binary(value, expected):
    # Binary() previously called .encode() unconditionally, which raised
    # AttributeError for bytes/bytearray/memoryview input.
    result = Binary(value)
    assert isinstance(result, bytes)
    assert result == expected


@pytest.mark.parametrize("value", [1, None, 1.5, [b"a"]])
def test_binary_rejects_non_bytes_non_str(value):
    with pytest.raises(TypeError):
        Binary(value)


@pytest.mark.parametrize(
    "value,expected",
    [
        (b"", "X''"),
        (b"hello", "X'68656c6c6f'"),
        (b"\xca\xfe\xba\xbe", "X'cafebabe'"),
        (bytearray(b"abc"), "X'616263'"),
        (memoryview(b"abc"), "X'616263'"),
    ]
)
def test_format_prepared_param_binary(value, expected):
    cursor = Cursor.__new__(Cursor)
    assert cursor._format_prepared_param(value) == expected
    # Round trip through Binary(), as SQLAlchemy's _Binary.bind_processor does.
    assert cursor._format_prepared_param(Binary(value)) == expected


class _StubCursorConnection:
    """Minimal stand-in for a DBAPI connection used by Cursor.execute()."""

    host = "coordinator"
    port = 8080

    def __init__(self, legacy_prepared_statements=False):
        self.legacy_prepared_statements = legacy_prepared_statements

    def _use_legacy_prepared_statements(self):
        return self.legacy_prepared_statements

    def _create_request(self):
        return object()


class _RecordingQuery:
    """Replacement for trino.client.TrinoQuery that records the SQL."""

    statements = []

    def __init__(self, request, query, **kwargs):
        type(self).statements.append(query)

    @property
    def update_type(self):
        return "INSERT"

    def execute(self):
        return iter([])


@pytest.fixture
def recording_cursor(monkeypatch):
    _RecordingQuery.statements = []
    monkeypatch.setattr("trino.dbapi.trino.client.TrinoQuery", _RecordingQuery)

    def build(legacy_prepared_statements=False):
        cursor = Cursor.__new__(Cursor)
        cursor._connection = _StubCursorConnection(legacy_prepared_statements)
        cursor._request = object()
        cursor._legacy_primitive_types = False
        cursor._stats_callback = None
        return cursor

    return build


@pytest.mark.parametrize(
    "value,declaration,expected",
    [
        ([], "array(bigint)", "CAST(ARRAY[] AS ARRAY(bigint))"),
        ([1, 2], "array(integer)", "CAST(ARRAY[1,2] AS ARRAY(integer))"),
        ({}, "map(varchar, bigint)",
         "MAP(CAST(ARRAY[] AS ARRAY(varchar)), CAST(ARRAY[] AS ARRAY(bigint)))"),
        ({"a": 1}, "map(varchar, bigint)",
         "MAP(CAST(ARRAY['a'] AS ARRAY(varchar)), CAST(ARRAY[1] AS ARRAY(bigint)))"),
        ((None, None), "row(x bigint, y varchar)", "CAST(ROW(NULL,NULL) AS ROW(x bigint, y varchar))"),
        ((1, "a"), "row(x bigint, y varchar)", "CAST(ROW(1,'a') AS ROW(x bigint, y varchar))"),
        (None, "array(bigint)", "CAST(NULL AS ARRAY(bigint))"),
        (
            [[1], [2]],
            "array(array(bigint))",
            "CAST(ARRAY[CAST(ARRAY[1] AS ARRAY(bigint)),CAST(ARRAY[2] AS ARRAY(bigint))] "
            "AS ARRAY(ARRAY(bigint)))",
        ),
        (
            ([], {}),
            "row(x array(bigint), y map(varchar, integer))",
            "CAST(ROW(CAST(ARRAY[] AS ARRAY(bigint)),"
            "MAP(CAST(ARRAY[] AS ARRAY(varchar)), CAST(ARRAY[] AS ARRAY(integer)))) "
            "AS ROW(x ARRAY(bigint), y MAP(varchar, integer)))",
        ),
        ({"a": []}, "map(varchar, array(bigint))",
         "MAP(CAST(ARRAY['a'] AS ARRAY(varchar)), "
         "CAST(ARRAY[CAST(ARRAY[] AS ARRAY(bigint))] AS ARRAY(ARRAY(bigint))))"),
    ]
)
def test_format_typed_container_param(value, declaration, expected):
    cursor = Cursor.__new__(Cursor)
    assert cursor._format_prepared_param(value, sql_type=declaration) == expected


def test_format_typed_param_accepts_sqltype_instances():
    cursor = Cursor.__new__(Cursor)
    declaration = sqltypes.MapType(
        sqltypes.ScalarType("VARCHAR"),
        sqltypes.ArrayType(sqltypes.ScalarType("BIGINT")),
    )
    assert cursor._format_prepared_param({}, sql_type=declaration) == \
        "MAP(CAST(ARRAY[] AS ARRAY(VARCHAR)), CAST(ARRAY[] AS ARRAY(ARRAY(BIGINT))))"


@pytest.mark.parametrize(
    "value",
    [[], (), {}],
)
def test_untyped_uninferrable_container_raises(value):
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param(value)
    assert str(excinfo.value).startswith("params[0]: cannot infer")


def test_untyped_all_null_row_raises():
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError, match=r"params\[0\]: cannot infer"):
        cursor._format_prepared_param((None, None))


def test_untyped_nested_empty_container_reports_full_path():
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param([[1, 2], []])
    assert str(excinfo.value).startswith("params[0][1]: cannot infer")


def test_integer_out_of_range_reports_full_path():
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param([1, 2 ** 40], sql_type="array(integer)")
    message = str(excinfo.value)
    assert message.startswith("params[0][1]:")
    assert "INTEGER" in message
    assert "out of range" in message


def test_typed_scalar_rejects_incompatible_python_type():
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param({"k": "value"}, sql_type="map(varchar, bigint)")
    assert str(excinfo.value).startswith("params[0]['k']:")


def test_decimal_precision_and_scale_are_enforced():
    cursor = Cursor.__new__(Cursor)
    assert cursor._format_prepared_param(Decimal("1.235"), sql_type="decimal(6,2)") == \
        "CAST(DECIMAL '1.235' AS DECIMAL(6, 2))"
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param(Decimal("10000.00"), sql_type="decimal(6,2)")
    assert "does not fit in DECIMAL(6, 2)" in str(excinfo.value)


def test_typed_nested_decimal_reports_full_path():
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param(
            [Decimal("1.00"), Decimal("100.00")],
            sql_type="array(decimal(4,2))",
        )
    assert str(excinfo.value).startswith("params[0][1]:")


def test_row_field_mismatch_reports_full_path():
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param(
            {"outer": (1, "a", True)},
            sql_type="map(varchar, row(x bigint, y varchar))",
        )
    assert "ROW value has 3 field(s)" in str(excinfo.value)


def test_row_mapping_field_mismatch_lists_missing_and_unexpected_fields():
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param({"x": 1, "z": 2}, sql_type="row(x bigint, y varchar)")
    message = str(excinfo.value)
    assert "missing ROW field(s) ['y']" in message


def test_row_mapping_rejects_unexpected_fields():
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param({"x": 1, "y": "a", "z": 2}, sql_type="row(x bigint, y varchar)")
    assert "unexpected ROW field(s) ['z']" in str(excinfo.value)


def test_map_key_conflict_reports_both_keys():
    cursor = Cursor.__new__(Cursor)
    # NaN is not equal to itself, so Python keeps both keys in the mapping,
    # but Trino treats them as the same DOUBLE key.
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param({float("nan"): "a", float("nan"): "b"}, sql_type="map(double, varchar)")
    message = str(excinfo.value)
    assert message.startswith("params[0].keys[1]:")
    assert "duplicate map key" in message


def test_map_key_conflict_inside_nested_value_reports_full_path():
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param(
            [("a", {float("nan"): 1, float("nan"): 2})],
            sql_type="array(row(name varchar, values map(double, bigint)))",
        )
    assert str(excinfo.value).startswith("params[0][0][1].keys[1]:")
    assert "duplicate map key" in str(excinfo.value)


def test_typed_requires_one_declaration_per_param(recording_cursor):
    cursor = recording_cursor()
    with pytest.raises(trino.exceptions.ProgrammingError, match="exactly one entry per parameter"):
        cursor.execute("SELECT ?, ?", ([], [1]), typed=["array(bigint)"])
    with pytest.raises(trino.exceptions.ProgrammingError, match="typed requires params"):
        cursor.execute("SELECT 1", typed=["array(bigint)"])
    assert _RecordingQuery.statements == []


def test_execute_immediate_applies_typed_cast(recording_cursor):
    cursor = recording_cursor(legacy_prepared_statements=False)
    cursor.execute("SELECT ?", ([],), typed=["array(bigint)"])
    assert _RecordingQuery.statements[-1] == \
        "EXECUTE IMMEDIATE 'SELECT ?' USING CAST(ARRAY[] AS ARRAY(bigint))"


def test_execute_immediate_mixes_typed_and_untyped_params(recording_cursor):
    cursor = recording_cursor(legacy_prepared_statements=False)
    cursor.execute("SELECT ?, ?", (1, {}), typed=[None, "map(varchar, integer)"])
    assert _RecordingQuery.statements[-1] == (
        "EXECUTE IMMEDIATE 'SELECT ?, ?' USING 1,"
        "MAP(CAST(ARRAY[] AS ARRAY(varchar)), CAST(ARRAY[] AS ARRAY(integer)))"
    )


def test_legacy_prepared_statements_apply_typed_cast(recording_cursor):
    cursor = recording_cursor(legacy_prepared_statements=True)
    cursor.execute("SELECT ?", ({},), typed=["map(varchar, integer)"])
    statements = _RecordingQuery.statements
    assert statements[0].startswith("PREPARE ")
    assert "USING MAP(CAST(ARRAY[] AS ARRAY(varchar)), CAST(ARRAY[] AS ARRAY(integer)))" in statements[1]
    assert statements[2].startswith("DEALLOCATE PREPARE ")


def test_executemany_applies_typed_to_every_parameter_sequence(recording_cursor):
    cursor = recording_cursor(legacy_prepared_statements=False)
    cursor.executemany("INSERT INTO t VALUES (?)", [([],), ([1],)], typed=["array(bigint)"])
    executed = [statement for statement in _RecordingQuery.statements if statement.startswith("EXECUTE")]
    assert len(executed) == 2
    assert all("CAST(ARRAY[%s] AS ARRAY(bigint))" % ("" if first else "1") in statement
               for first, statement in zip([True, False], executed))


def test_null_map_key_is_rejected_with_path():
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(trino.exceptions.TrinoParameterError) as excinfo:
        cursor._format_prepared_param({None: 1}, sql_type="map(varchar, bigint)")
    message = str(excinfo.value)
    assert message.startswith("params[0].keys[0]:")
    assert "cannot be NULL" in message


def test_primitive_declaration_wraps_value_in_cast():
    cursor = Cursor.__new__(Cursor)
    assert cursor._format_prepared_param(1, sql_type="integer") == "CAST(1 AS integer)"
    assert cursor._format_prepared_param("{}", sql_type="json") == "CAST('{}' AS json)"
    assert cursor._format_prepared_param(None, sql_type="integer") == "CAST(NULL AS integer)"
    # Inside a declared container the container's CAST already covers the value.
    assert cursor._format_prepared_param(None, sql_type="integer", cast_scalar=False) == "NULL"


def test_typed_param_wrapper():
    cursor = Cursor.__new__(Cursor)
    assert cursor._format_prepared_param(sqltypes.Typed([], "array(bigint)")) == \
        "CAST(ARRAY[] AS ARRAY(bigint))"
    with pytest.raises(trino.exceptions.TrinoParameterError, match="cannot be combined"):
        cursor._format_prepared_param(sqltypes.Typed([], "array(bigint)"), sql_type="array(integer)")


def test_legacy_prepared_statements_do_not_prepare_when_validation_fails(recording_cursor):
    cursor = recording_cursor(legacy_prepared_statements=True)
    with pytest.raises(trino.exceptions.TrinoParameterError):
        cursor.execute("SELECT ?", params=([], ), typed=None)
    assert _RecordingQuery.statements == []

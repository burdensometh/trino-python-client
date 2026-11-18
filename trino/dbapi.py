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
"""

This module implements the Python DBAPI 2.0 as described in
https://www.python.org/dev/peps/pep-0249/ .

Fetch methods returns rows as a list of lists on purpose to let the caller
decide to convert then to a list of tuples.
"""
import datetime
import math
import uuid
from collections import OrderedDict
from collections.abc import Mapping
from decimal import Decimal
from decimal import localcontext
from decimal import ROUND_HALF_UP
from itertools import islice
from threading import Lock
from time import time
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Union
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import trino.client
import trino.exceptions
import trino.logging
from trino import constants
from trino import sqltypes
from trino.constants import LENGTH_TYPES
from trino.constants import PRECISION_TYPES
from trino.constants import SCALE_TYPES
from trino.exceptions import DatabaseError
from trino.exceptions import DataError
from trino.exceptions import Error
from trino.exceptions import IntegrityError
from trino.exceptions import InterfaceError
from trino.exceptions import InternalError
from trino.exceptions import NotSupportedError
from trino.exceptions import OperationalError
from trino.exceptions import ProgrammingError
from trino.exceptions import Warning
from trino.transaction import IsolationLevel
from trino.transaction import NO_TRANSACTION
from trino.transaction import Transaction

__all__ = [
    # https://www.python.org/dev/peps/pep-0249/#globals
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Cursor",
    # https://www.python.org/dev/peps/pep-0249/#exceptions
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]


apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"

logger = trino.logging.get_logger(__name__)


class TimeBoundLRUCache:
    """A bounded LRU cache which expires entries after a configured number of seconds.
    Note that expired entries will be evicted only on an attempted access (or through
    the LRU policy)."""
    def __init__(self, capacity: int, ttl_seconds: int):
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.cache = OrderedDict()
        self.lock = Lock()

    def get(self, key):
        with self.lock:
            if key not in self.cache:
                return None
            value, timestamp = self.cache[key]
            if time() - timestamp > self.ttl_seconds:
                self.cache.pop(key)
                return None
            self.cache.move_to_end(key)
            return value

    def put(self, key, value):
        with self.lock:
            self.cache[key] = value, time()
            self.cache.move_to_end(key)
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

    def __repr__(self):
        return f"LRUCache(capacity: {self.capacity}, ttl: {self.ttl_seconds} seconds, {self.cache})"


must_use_legacy_prepared_statements = TimeBoundLRUCache(1024, 3600)


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database.

    See class :py:class:`Connection` for arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


_USE_DEFAULT_ENCODING = object()


class Connection:
    """Trino supports transactions and the ability to either commit or rollback
    a sequence of SQL statements. A single query i.e. the execution of a SQL
    statement, can also be cancelled. Transactions are not supported by this
    client implementation yet.

    """

    def __init__(
        self,
        host: str,
        port=None,
        user=None,
        source=constants.DEFAULT_SOURCE,
        catalog=constants.DEFAULT_CATALOG,
        schema=constants.DEFAULT_SCHEMA,
        session_properties=None,
        http_headers=None,
        http_scheme=None,
        auth=constants.DEFAULT_AUTH,
        extra_credential=None,
        max_attempts=constants.DEFAULT_MAX_ATTEMPTS,
        request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,
        isolation_level=IsolationLevel.AUTOCOMMIT,
        verify=True,
        http_session=None,
        client_tags=None,
        legacy_primitive_types=False,
        legacy_prepared_statements=None,
        roles=None,
        timezone=None,
        encoding: Union[str, List[str]] = _USE_DEFAULT_ENCODING,
        heartbeat_interval: Optional[float] = constants.DEFAULT_HEARTBEAT_INTERVAL,
        allow_insecure_auth: bool = False,
    ):
        # Automatically assign http_schema, port based on hostname
        parsed_host = urlparse(host, allow_fragments=False)

        if encoding is _USE_DEFAULT_ENCODING:
            encoding = [
                enc
                for enc in trino.client.ENCODINGS
                if (enc.split("+")[1] if "+" in enc else None) not in trino.client.CODECS_UNAVAILABLE
            ]

        self.host = host if parsed_host.hostname is None else parsed_host.hostname + parsed_host.path
        self.user = user
        self.source = source
        self.catalog = catalog
        self.schema = schema
        self.session_properties = session_properties
        self._client_session = trino.client.ClientSession(
            user=user,
            catalog=catalog,
            schema=schema,
            source=source,
            properties=session_properties,
            headers=http_headers,
            transaction_id=NO_TRANSACTION,
            extra_credential=extra_credential,
            client_tags=client_tags,
            roles=roles,
            timezone=timezone,
            encoding=encoding,
            heartbeat_interval=heartbeat_interval,
        )
        # mypy cannot follow module import
        if http_session is None:
            self._http_session = trino.client.TrinoRequest.http.Session()
            self._http_session.verify = verify
        else:
            self._http_session = http_session
        self.http_headers = http_headers

        # Set http_scheme
        if parsed_host.scheme:
            self.http_scheme = parsed_host.scheme
        elif http_scheme:
            self.http_scheme = http_scheme
        elif port == constants.DEFAULT_TLS_PORT:
            self.http_scheme = constants.HTTPS
        elif port == constants.DEFAULT_PORT:
            self.http_scheme = constants.HTTP
        else:
            self.http_scheme = constants.HTTP

        if auth is not None and self.http_scheme == constants.HTTP and not allow_insecure_auth:
            raise trino.exceptions.TrinoAuthError(
                "TLS/SSL is required for authentication. "
                "To use HTTPS, specify 'https://' in the host URL (which takes precedence "
                "over http_scheme), or, if the host URL has no scheme, pass http_scheme='https'. "
                "If your connection is encrypted below the application layer (for example behind an mTLS "
                "service mesh sidecar), pass allow_insecure_auth=True and ensure "
                "http-server.authentication.allow-insecure-over-http=true is set on the coordinator if it "
                "has HTTPS enabled."
            )

        # Infer connection port: `hostname` takes precedence over explicit `port` argument
        # If none is given, use default based on HTTP protocol
        default_port = constants.DEFAULT_TLS_PORT if self.http_scheme == constants.HTTPS else constants.DEFAULT_PORT
        self.port = (
            parsed_host.port if parsed_host.port is not None
            else port if port is not None
            else default_port
        )

        self.auth = auth
        self.extra_credential = extra_credential
        self.max_attempts = max_attempts
        self.request_timeout = request_timeout
        self.client_tags = client_tags

        self._isolation_level = isolation_level
        self._request = None
        self._transaction = None
        self.legacy_primitive_types = legacy_primitive_types
        self.legacy_prepared_statements = legacy_prepared_statements

    @property
    def isolation_level(self):
        return self._isolation_level

    @property
    def transaction(self):
        return self._transaction

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.commit()
        except Exception:
            self.rollback()
        else:
            self.close()

    def close(self):
        # TODO cancel outstanding queries?
        self._http_session.close()

    def start_transaction(self):
        self._transaction = Transaction(self._create_request())
        self._transaction.begin()
        return self._transaction

    def commit(self):
        if self.transaction is None:
            return
        self._transaction.commit()
        self._transaction = None

    def rollback(self):
        if self.transaction is None:
            raise RuntimeError("no transaction was started")
        self._transaction.rollback()
        self._transaction = None

    def _create_request(self):
        return trino.client.TrinoRequest(
            self.host,
            self.port,
            self._client_session,
            self._http_session,
            self.http_scheme,
            self.auth,
            self.max_attempts,
            self.request_timeout,
        )

    def cursor(
            self,
            cursor_style: str = "row",
            legacy_primitive_types: bool = None,
            stats_callback: Optional[Callable[[Dict[str, Any]], None]] = None):
        """Return a new :py:class:`Cursor` object using the connection."""
        if self.isolation_level != IsolationLevel.AUTOCOMMIT:
            if self.transaction is None:
                self.start_transaction()
        if self.transaction is not None:
            request = self.transaction.request
        else:
            request = self._create_request()

        cursor_class = {
            # Add any custom Cursor classes here
            "segment": SegmentCursor,
            "row": Cursor
        }.get(cursor_style.lower(), Cursor)

        return cursor_class(
            self,
            request,
            legacy_primitive_types=(
                legacy_primitive_types
                if legacy_primitive_types is not None
                else self.legacy_primitive_types
            ),
            stats_callback=stats_callback
        )

    def _use_legacy_prepared_statements(self):
        if self.legacy_prepared_statements is not None:
            return self.legacy_prepared_statements

        value = must_use_legacy_prepared_statements.get((self.host, self.port))
        if value is None:
            try:
                query = trino.client.TrinoQuery(
                    self._create_request(),
                    query="EXECUTE IMMEDIATE 'SELECT 1'")
                query.execute()
                value = False
            except Exception as e:
                logger.warning(
                    "EXECUTE IMMEDIATE not available for %s:%s; defaulting to legacy prepared statements (%s)",
                    self.host, self.port, e)
                value = True
            must_use_legacy_prepared_statements.put((self.host, self.port), value)
        return value


class DescribeOutput(NamedTuple):
    name: str
    catalog: str
    schema: str
    table: str
    type: str
    type_size: int
    aliased: bool

    @classmethod
    def from_row(cls, row: List[Any]):
        return cls(*row)


class ColumnDescription(NamedTuple):
    name: str
    type_code: int
    display_size: int
    internal_size: int
    precision: int
    scale: int
    null_ok: bool

    @classmethod
    def from_column(cls, column: Dict[str, Any]):
        type_signature = column["typeSignature"]
        raw_type = type_signature["rawType"]
        arguments = type_signature["arguments"]
        return cls(
            column["name"],  # name
            column["type"],  # type_code
            None,  # display_size
            arguments[0]["value"] if raw_type in LENGTH_TYPES else None,  # internal_size
            arguments[0]["value"] if raw_type in PRECISION_TYPES else None,  # precision
            arguments[1]["value"] if raw_type in SCALE_TYPES else None,  # scale
            None  # null_ok
        )


class Cursor:
    """Database cursor.

    Cursors are not isolated, i.e., any changes done to the database by a
    cursor are immediately visible by other cursors or connections.

    """

    def __init__(
            self,
            connection,
            request,
            legacy_primitive_types: bool = False,
            stats_callback: Optional[Callable[[Dict[str, Any]], None]] = None):
        if not isinstance(connection, Connection):
            raise ValueError(
                "connection must be a Connection object: {}".format(type(connection))
            )
        self._connection = connection
        self._request = request

        self.arraysize = 1
        self._iterator = None
        self._query = None
        self._legacy_primitive_types = legacy_primitive_types
        self._stats_callback = stats_callback

    def __iter__(self):
        return self._iterator

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def connection(self):
        return self._connection

    @property
    def info_uri(self):
        if self._query is not None:
            return self._query.info_uri
        return None

    @property
    def update_type(self):
        if self._query is not None:
            return self._query.update_type
        return None

    @property
    def description(self) -> List[ColumnDescription]:
        if self._query is None or self._query.columns is None:
            return None

        # [ (name, type_code, display_size, internal_size, precision, scale, null_ok) ]
        return [
            ColumnDescription.from_column(col) for col in self._query.columns
        ]

    @property
    def rowcount(self):
        """The rowcount will be returned for INSERT, UPDATE, DELETE, MERGE
        and CTAS statements based on `update_count` returned by the Trino
        API.

        If the rowcount can't be determined, -1 will be returned.

        Trino cannot reliably determine the number of rows returned for DQL
        queries. For example, the result of a SELECT query is streamed and
        the number of rows is only known when all rows have been retrieved.

        See https://peps.python.org/pep-0249/#rowcount
        """
        if self._query is not None and self._query.update_count is not None:
            return self._query.update_count
        return -1

    @property
    def stats(self):
        if self._query is not None:
            return self._query.stats
        return None

    @property
    def query_id(self) -> Optional[str]:
        if self._query is not None:
            return self._query.query_id
        return None

    @property
    def query(self) -> Optional[str]:
        if self._query is not None:
            return self._query.query
        return None

    @property
    def warnings(self):
        if self._query is not None:
            return self._query.warnings
        return None

    def setinputsizes(self, sizes):
        raise trino.exceptions.NotSupportedError

    def setoutputsize(self, size, column):
        raise trino.exceptions.NotSupportedError

    def _prepare_statement(self, statement: str, name: str) -> None:
        """
        Registers a prepared statement for the provided `operation` with the
        `name` assigned to it.

        :param statement: sql to be executed.
        :param name: name that will be assigned to the prepared statement.
        """
        sql = f"PREPARE {name} FROM {statement}"
        query = trino.client.TrinoQuery(self.connection._create_request(), query=sql,
                                        legacy_primitive_types=self._legacy_primitive_types)
        query.execute()

    def _execute_prepared_statement(
        self,
        statement_name,
        params,
        typed=None,
        formatted_params=None,
    ):
        if formatted_params is None:
            formatted_params = self._format_params(params, typed)
        sql = 'EXECUTE ' + statement_name + ' USING ' + ','.join(formatted_params)
        return trino.client.TrinoQuery(
            self._request,
            query=sql,
            legacy_primitive_types=self._legacy_primitive_types,
            stats_callback=self._stats_callback)

    def _execute_immediate_statement(self, statement: str, params, typed=None):
        """
        Binds parameters and executes a statement in one call.

        :param statement: sql to be executed.
        :param params: parameters to be bound.
        :param typed: optional type declarations, one per parameter.
        """
        sql = "EXECUTE IMMEDIATE '" + statement.replace("'", "''") + \
              "' USING " + ",".join(self._format_params(params, typed))
        return trino.client.TrinoQuery(
            self.connection._create_request(),
            query=sql,
            legacy_primitive_types=self._legacy_primitive_types,
            stats_callback=self._stats_callback)

    def _format_params(self, params, typed=None):
        """Format every parameter, applying the optional type declarations.

        ``typed`` is either ``None`` or a sequence with exactly one entry per
        parameter.  An entry is a type declaration (see :mod:`trino.sqltypes`)
        or ``None`` when the parameter should be formatted as before.
        """
        if typed is None:
            return [self._format_prepared_param(param, path=f"params[{index}]")
                    for index, param in enumerate(params)]

        typed = list(typed)
        if len(typed) != len(params):
            raise trino.exceptions.ProgrammingError(
                "typed must have exactly one entry per parameter: "
                f"got {len(typed)} type declarations for {len(params)} parameters"
            )

        return [
            self._format_prepared_param(param, path=f"params[{index}]", sql_type=type_declaration)
            for index, (param, type_declaration) in enumerate(zip(params, typed))
        ]

    def _format_prepared_param(self, param, path="params[0]", sql_type=None, cast_scalar=True):
        """
        Formats parameters to be passed in an
        EXECUTE statement.

        :param param: the Python value to encode.
        :param path: location of the value, used in error messages.
        :param sql_type: optional declared :class:`trino.sqltypes.SqlType`.
            Containers with a declaration are encoded with a ``CAST`` so that
            empty arrays and empty maps get a known type.  Values of primitive
            types are validated against the declaration, and wrapped in a
            ``CAST`` when ``cast_scalar`` is true so the declared type is
            actually applied.
        :param cast_scalar: whether a primitive declaration should produce a
            ``CAST``.  Elements of a declared container are already covered
            by the container's own ``CAST``, so they are not wrapped again.
        """
        if isinstance(param, sqltypes.Typed):
            if sql_type is not None:
                raise trino.exceptions.TrinoParameterError(
                    f"{path}: a Typed value and an explicit type declaration cannot be combined"
                )
            sql_type = param.type
            param = param.value

        if sql_type is not None:
            sql_type = sqltypes.parse_type(sql_type)

        if isinstance(sql_type, sqltypes.ArrayType):
            return self._format_typed_array(param, sql_type, path)

        if isinstance(sql_type, sqltypes.MapType):
            return self._format_typed_map(param, sql_type, path)

        if isinstance(sql_type, sqltypes.RowType):
            return self._format_typed_row(param, sql_type, path)

        if isinstance(sql_type, sqltypes.DecimalType):
            return self._format_typed_decimal(param, sql_type, path)

        if sql_type is not None:
            # A primitive declaration validates the value and, at the top
            # level, wraps the regular literal in a CAST so the parameter has
            # the declared type, for example a JSON, CHAR or integer type.
            self._validate_typed_value(param, sql_type, path)
            if param is None and cast_scalar:
                # An untyped NULL has the unknown type, which would contradict
                # the declaration.
                return f"CAST(NULL AS {sql_type.to_sql()})"
            literal = self._format_prepared_param(param, path=path)
            if cast_scalar and param is not None:
                return f"CAST({literal} AS {sql_type.to_sql()})"
            return literal

        if param is None:
            return "NULL"

        if isinstance(param, bool):
            return "true" if param else "false"

        if isinstance(param, int):
            # TODO represent numbers exceeding 64-bit (BIGINT) as DECIMAL
            return "%d" % param

        if isinstance(param, float):
            if param == float("+inf"):
                return "infinity()"
            if param == float("-inf"):
                return "-infinity()"
            if math.isnan(param):
                return "nan()"
            return "DOUBLE '%s'" % param

        if isinstance(param, str):
            return ("'%s'" % param.replace("'", "''"))

        if isinstance(param, (bytes, bytearray, memoryview)):
            return "X'%s'" % bytes(param).hex()

        if isinstance(param, datetime.datetime) and param.tzinfo is None:
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
            return "TIMESTAMP '%s'" % datetime_str

        if isinstance(param, datetime.datetime) and param.tzinfo is not None:
            datetime_str = param.strftime("%Y-%m-%d %H:%M:%S.%f")
            # named timezones
            if isinstance(param.tzinfo, ZoneInfo):
                return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.key)
            # offset-based timezones
            return "TIMESTAMP '%s %s'" % (datetime_str, param.tzinfo.tzname(param))

        # We can't calculate the offset for a time without a point in time
        if isinstance(param, datetime.time) and param.tzinfo is None:
            time_str = param.strftime("%H:%M:%S.%f")
            return "TIME '%s'" % time_str

        if isinstance(param, datetime.time) and param.tzinfo is not None:
            time_str = param.strftime("%H:%M:%S.%f")
            # named timezones
            if isinstance(param.tzinfo, ZoneInfo):
                utc_offset = datetime.datetime.now(tz=param.tzinfo).strftime('%z')
                return "TIME '%s %s:%s'" % (time_str, utc_offset[:3], utc_offset[3:])
            # offset-based timezones
            return "TIME '%s %s'" % (time_str, param.strftime('%Z')[3:])

        if isinstance(param, datetime.date):
            date_str = param.strftime("%Y-%m-%d")
            return "DATE '%s'" % date_str

        if isinstance(param, list):
            if not param:
                raise self._missing_type_error(path, "an empty array")
            return "ARRAY[%s]" % ','.join(
                self._format_prepared_param(item, path=f"{path}[{index}]")
                for index, item in enumerate(param)
            )

        if isinstance(param, tuple):
            if not param:
                raise self._missing_type_error(path, "an empty row", "row(a bigint)")
            if all(item is None for item in param):
                raise self._missing_type_error(path, "a row whose fields are all NULL", "row(a bigint)")
            return "ROW(%s)" % ','.join(
                self._format_prepared_param(item, path=f"{path}[{index}]")
                for index, item in enumerate(param)
            )

        if isinstance(param, Mapping):
            if not param:
                raise self._missing_type_error(path, "an empty map", "map(varchar, bigint)")
            keys = list(param.keys())
            values = [param[key] for key in keys]
            return "MAP({}, {})".format(
                self._format_prepared_param(keys, path=f"{path}.keys"),
                self._format_prepared_param(values, path=f"{path}.values")
            )

        if isinstance(param, uuid.UUID):
            return "UUID '%s'" % param

        if isinstance(param, Decimal):
            return "DECIMAL '%s'" % format(param, "f")

        raise trino.exceptions.NotSupportedError(
            f"{path}: query parameter of type '{type(param).__name__}' is not supported."
        )

    # -- typed container encoding -------------------------------------------

    @staticmethod
    def _missing_type_error(path: str, description: str, example: str = "array(bigint)"):
        return trino.exceptions.TrinoParameterError(
            f"{path}: cannot infer the Trino type of {description}. "
            f'Declare it with the typed argument, for example typed=["{example}"].'
        )

    def _format_typed_decimal(self, param, sql_type, path):
        if param is None:
            return f"CAST(NULL AS {sql_type.to_sql()})"
        if isinstance(param, bool) or not isinstance(param, (Decimal, int, float)):
            raise trino.exceptions.TrinoParameterError(
                f"{path}: expected a decimal value for {sql_type.to_sql()}, "
                f"got {type(param).__name__}"
            )
        if isinstance(param, float):
            if math.isnan(param) or math.isinf(param):
                raise trino.exceptions.TrinoParameterError(
                    f"{path}: {param!r} cannot be represented as {sql_type.to_sql()}"
                )
            value = Decimal(str(param))
        elif isinstance(param, int):
            value = Decimal(param)
        else:
            value = param

        if not value.is_finite():
            raise trino.exceptions.TrinoParameterError(
                f"{path}: {param!r} cannot be represented as {sql_type.to_sql()}"
            )

        # Round half up the same way a CAST to DECIMAL does and make sure the
        # result fits into the declared precision. A local context is needed
        # because the default context only has 28 digits of precision.
        with localcontext() as context:
            context.prec = max(len(value.as_tuple().digits), sql_type.precision) + 2
            scaled = value.scaleb(sql_type.scale)
            rounded = scaled.to_integral_value(rounding=ROUND_HALF_UP)
        if rounded != 0 and rounded.adjusted() + 1 > sql_type.precision:
            raise trino.exceptions.TrinoParameterError(
                f"{path}: value {param!r} does not fit in {sql_type.to_sql()}"
            )
        return f"CAST(DECIMAL '{format(value, 'f')}' AS {sql_type.to_sql()})"

    def _validate_typed_value(self, param, sql_type, path):
        """Validate a value against a primitive type declaration."""
        name = sql_type.name.strip().lower()
        base = name.split("(", 1)[0].strip()
        if param is None:
            return
        if base in ("boolean", "bool"):
            allowed = isinstance(param, bool)
        elif base in ("tinyint", "smallint", "int", "integer", "bigint"):
            allowed = isinstance(param, int) and not isinstance(param, bool)
            if allowed:
                allowed = self._integer_fits(param, base, path)
        elif base in ("real", "double"):
            allowed = isinstance(param, (int, float)) and not isinstance(param, bool)
        elif base in ("decimal", "numeric"):
            allowed = isinstance(param, (int, float, Decimal)) and not isinstance(param, bool)
        elif base in ("varchar", "char", "json", "uuid", "ipaddress"):
            allowed = isinstance(param, str) or (
                base == "uuid" and isinstance(param, uuid.UUID)
            )
        elif base == "varbinary":
            allowed = isinstance(param, (bytes, bytearray, memoryview))
        elif base in ("date",):
            allowed = isinstance(param, datetime.date) and not isinstance(param, datetime.datetime)
        elif base in ("time", "time with time zone"):
            allowed = isinstance(param, datetime.time)
        elif base in ("timestamp", "timestamp with time zone"):
            allowed = isinstance(param, datetime.datetime)
        elif base == "interval day to second":
            allowed = isinstance(param, datetime.timedelta)
        else:
            # Unknown primitive declarations are validated by Trino itself.
            return
        if not allowed:
            raise trino.exceptions.TrinoParameterError(
                f"{path}: expected a value compatible with {sql_type.to_sql()}, "
                f"got {type(param).__name__}"
            )
        return True

    @staticmethod
    def _integer_fits(value: int, base: str, path: str) -> bool:
        limits = {
            "tinyint": (-2 ** 7, 2 ** 7 - 1),
            "smallint": (-2 ** 15, 2 ** 15 - 1),
            "int": (-2 ** 31, 2 ** 31 - 1),
            "integer": (-2 ** 31, 2 ** 31 - 1),
            "bigint": (-2 ** 63, 2 ** 63 - 1),
        }
        if base not in limits:
            return True
        low, high = limits[base]
        if not low <= value <= high:
            raise trino.exceptions.TrinoParameterError(
                f"{path}: value {value!r} is out of range for {base.upper()} "
                f"[{low}, {high}]"
            )
        return True

    def _format_typed_array(self, param, sql_type, path):
        if param is None:
            return f"CAST(NULL AS {sql_type.to_sql()})"
        if not isinstance(param, (list, tuple)):
            raise trino.exceptions.TrinoParameterError(
                f"{path}: expected a list for {sql_type.to_sql()}, got {type(param).__name__}"
            )
        item_type = sql_type.item_type
        elements = [
            self._format_prepared_param(item, path=f"{path}[{index}]", sql_type=item_type, cast_scalar=False)
            for index, item in enumerate(param)
        ]
        sql = "ARRAY[%s]" % ",".join(elements)
        return f"CAST({sql} AS {sql_type.to_sql()})"

    def _format_typed_map(self, param, sql_type, path):
        if param is None:
            return f"CAST(NULL AS {sql_type.to_sql()})"
        if not isinstance(param, Mapping):
            raise trino.exceptions.TrinoParameterError(
                f"{path}: expected a mapping for {sql_type.to_sql()}, got {type(param).__name__}"
            )
        keys = []
        values = []
        seen = {}
        for index, key in enumerate(param.keys()):
            key_path = f"{path}.keys[{index}]"
            if key is None:
                raise trino.exceptions.TrinoParameterError(
                    f"{key_path}: a map key cannot be NULL in {sql_type.to_sql()}"
                )
            key_sql = self._format_prepared_param(
                key, path=key_path, sql_type=sql_type.key_type, cast_scalar=False
            )
            key_identity = self._map_key_identity(key, sql_type.key_type, key_sql)
            if key_identity in seen:
                raise trino.exceptions.TrinoParameterError(
                    f"{key_path}: duplicate map key {key!r} conflicts with {seen[key_identity]!r}"
                )
            seen[key_identity] = key
            keys.append(key_sql)
            values.append(self._format_prepared_param(
                param[key], path=f"{path}[{key!r}]", sql_type=sql_type.value_type, cast_scalar=False
            ))
        # Cast the key and value arrays instead of the map itself: array
        # casts are supported for every element type, while a cast between two
        # map types is not universally available.  The resulting map already
        # has the declared type.
        return "MAP(CAST(ARRAY[%s] AS ARRAY(%s)), CAST(ARRAY[%s] AS ARRAY(%s)))" % (
            ",".join(keys),
            sql_type.key_type.to_sql(),
            ",".join(values),
            sql_type.value_type.to_sql(),
        )

    @staticmethod
    def _map_key_identity(value, sql_type, formatted_sql):
        """Normalize a map key so that keys Trino considers equal collide.

        Map keys may be any Trino type, including nested containers.  For
        primitive keys the Python value is coerced to the declared type so
        that, for example, ``1`` and ``Decimal('1.0')`` are seen as the same
        ``DECIMAL`` key.  Nested containers fall back to their SQL text.
        """
        if isinstance(sql_type, (sqltypes.ArrayType, sqltypes.MapType, sqltypes.RowType)):
            return "sql", formatted_sql
        base = sql_type.name.strip().lower().split("(", 1)[0].strip() if isinstance(
            sql_type, sqltypes.ScalarType
        ) else "decimal"
        try:
            if base in ("tinyint", "smallint", "int", "integer", "bigint"):
                return "int", int(value)
            if base in ("real", "double"):
                if isinstance(value, float) and math.isnan(value):
                    return "float", "nan"
                return "float", float(value)
            if base in ("decimal", "numeric"):
                # A DecimalType declaration also normalizes to a decimal key.
                return "decimal", Decimal(str(value)).normalize()
            if base in ("boolean", "bool"):
                return "bool", bool(value)
            if base == "varbinary":
                return "bytes", bytes(value)
            if base in ("varchar", "char", "json", "uuid", "ipaddress"):
                return "str", str(value)
        except (TypeError, ValueError, ArithmeticError):
            pass
        return "sql", formatted_sql

    def _format_typed_row(self, param, sql_type, path):
        if param is None:
            return f"CAST(NULL AS {sql_type.to_sql()})"
        if isinstance(param, Mapping):
            field_names = [name for name, _ in sql_type.fields]
            if any(name is None for name in field_names):
                raise trino.exceptions.TrinoParameterError(
                    f"{path}: {sql_type.to_sql()} declares positional ROW fields and cannot "
                    "be populated from a mapping"
                )
            missing = [name for name in field_names if name not in param]
            if missing:
                raise trino.exceptions.TrinoParameterError(
                    f"{path}: missing ROW field(s) {missing} for {sql_type.to_sql()}"
                )
            unexpected = sorted(
                (str(name) for name in param if name not in field_names)
            )
            if unexpected:
                raise trino.exceptions.TrinoParameterError(
                    f"{path}: unexpected ROW field(s) {unexpected} for {sql_type.to_sql()}"
                )
            values = [
                self._format_prepared_param(
                    param[name], path=f"{path}.{name}", sql_type=field_type, cast_scalar=False
                )
                for name, field_type in sql_type.fields
            ]
        elif isinstance(param, (list, tuple)):
            if len(param) != len(sql_type.fields):
                raise trino.exceptions.TrinoParameterError(
                    f"{path}: ROW value has {len(param)} field(s) but "
                    f"{sql_type.to_sql()} declares {len(sql_type.fields)}"
                )
            values = [
                self._format_prepared_param(
                    item, path=f"{path}[{index}]", sql_type=field_type, cast_scalar=False
                )
                for index, (item, (_, field_type)) in enumerate(zip(param, sql_type.fields))
            ]
        else:
            raise trino.exceptions.TrinoParameterError(
                f"{path}: expected a tuple or mapping for {sql_type.to_sql()}, "
                f"got {type(param).__name__}"
            )
        sql = "ROW(%s)" % ",".join(values)
        return f"CAST({sql} AS {sql_type.to_sql()})"

    def _deallocate_prepared_statement(self, statement_name: str) -> None:
        sql = 'DEALLOCATE PREPARE ' + statement_name
        query = trino.client.TrinoQuery(self.connection._create_request(), query=sql,
                                        legacy_primitive_types=self._legacy_primitive_types)
        query.execute()

    def _generate_unique_statement_name(self):
        return 'st_' + uuid.uuid4().hex.replace('-', '')

    def execute(self, operation, params=None, typed=None):
        """Execute a statement, optionally binding query parameters.

        :param operation: the SQL statement.
        :param params: a list or tuple of parameter values.
        :param typed: optional type declarations, one per parameter.  Each
            entry is a :class:`trino.sqltypes.SqlType` instance or a SQL type
            string such as ``"array(bigint)"``.  Use ``None`` for parameters
            that should be encoded by inference.  Type declarations are
            required for empty arrays, empty maps, all-``NULL`` rows, and
            any value whose type cannot be inferred.
        """
        if typed is not None:
            if params is None:
                raise trino.exceptions.ProgrammingError(
                    "typed requires params: type declarations cannot be applied without parameters"
                )
            if not isinstance(params, (list, tuple)):
                raise trino.exceptions.ProgrammingError(
                    "params must be a list or tuple when typed is provided"
                )
            params = tuple(params)
            typed = tuple(
                sqltypes.parse_type(declaration) if declaration is not None else None
                for declaration in typed
            )
            if len(typed) != len(params):
                raise trino.exceptions.ProgrammingError(
                    "typed must have exactly one entry per parameter: "
                    f"got {len(typed)} type declarations for {len(params)} parameters"
                )
            if not typed:
                typed = None

        if params:
            assert isinstance(params, (list, tuple)), (
                'params must be a list or tuple containing the query '
                'parameter values'
            )

            if self.connection._use_legacy_prepared_statements():
                # Encode the parameters first so that validation errors are
                # raised before the PREPARE request is sent.
                formatted_params = self._format_params(params, typed)
                statement_name = self._generate_unique_statement_name()
                self._prepare_statement(operation, statement_name)

                try:
                    # Send execute statement and assign the return value to `results`
                    # as it will be returned by the function
                    self._query = self._execute_prepared_statement(
                        statement_name, params, typed, formatted_params=formatted_params
                    )
                    self._iterator = iter(self._query.execute())
                finally:
                    # Send deallocate statement
                    # At this point the query can be deallocated since it has already
                    # been executed
                    # TODO: Consider caching prepared statements if requested by caller
                    self._deallocate_prepared_statement(statement_name)
            else:
                self._query = self._execute_immediate_statement(operation, params, typed)
                self._iterator = iter(self._query.execute())

        else:
            if typed is not None:
                raise trino.exceptions.ProgrammingError(
                    "typed requires params: no type declarations can be applied to an empty "
                    "parameter list"
                )
            self._query = trino.client.TrinoQuery(self._request, query=operation,
                                                  legacy_primitive_types=self._legacy_primitive_types,
                                                  stats_callback=self._stats_callback)
            self._iterator = iter(self._query.execute())
        return self

    def executemany(self, operation, seq_of_params, typed=None):
        """
        PEP-0249: Prepare a database operation (query or command) and then
        execute it against all parameter sequences or mappings found in the sequence seq_of_parameters.
        Modules are free to implement this method using multiple calls to
        the .execute() method or by using array operations to have the
        database process the sequence as a whole in one call.

        Use of this method for an operation which produces one or more result
        sets constitutes undefined behavior, and the implementation is permitted (but not required)
        to raise an exception when it detects that a result set has been created by an invocation of the operation.

        The same comments as for .execute() also apply accordingly to this method.

        ``typed`` is applied to every parameter sequence, and follows the same
        rules as for :meth:`execute`.

        Return values are not defined.
        """
        for parameters in seq_of_params[:-1]:
            self.execute(operation, parameters, typed=typed)
            self.fetchall()
            if self._query.update_type is None:
                raise NotSupportedError("Query must return update type")
        if seq_of_params:
            self.execute(operation, seq_of_params[-1], typed=typed)
        else:
            self.execute(operation)
        return self

    def fetchone(self) -> Optional[List[Any]]:
        """

        PEP-0249: Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """

        try:
            assert self._iterator is not None
            return next(self._iterator)
        except StopIteration:
            return None
        except trino.exceptions.HttpError as err:
            raise trino.exceptions.OperationalError(str(err))

    def fetchmany(self, size=None) -> List[List[Any]]:
        """
        PEP-0249: Fetch the next set of rows of a query result, returning a
        sequence of sequences (e.g. a list of tuples). An empty sequence is
        returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If
        it is not given, the cursor's arraysize determines the number of rows
        to be fetched. The method should try to fetch as many rows as indicated
        by the size parameter. If this is not possible due to the specified
        number of rows not being available, fewer rows may be returned.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        Note there are performance considerations involved with the size
        parameter. For optimal performance, it is usually best to use the
        .arraysize attribute. If the size parameter is used, then it is best
        for it to retain the same value from one .fetchmany() call to the next.
        """

        if size is None:
            size = self.arraysize

        return list(islice(iter(self.fetchone, None), size))

    def describe(self, sql: str) -> List[DescribeOutput]:
        """
        List the output columns of a SQL statement, including the column name (or alias), catalog, schema, table, type,
        type size in bytes, and a boolean indicating if the column is aliased.

        :param sql: SQL statement
        """
        statement_name = self._generate_unique_statement_name()
        self._prepare_statement(sql, statement_name)
        try:
            sql = f"DESCRIBE OUTPUT {statement_name}"
            self._query = trino.client.TrinoQuery(
                self._request,
                query=sql,
                legacy_primitive_types=self._legacy_primitive_types,
            )
            result = self._query.execute()
        finally:
            self._deallocate_prepared_statement(statement_name)

        return list(map(lambda x: DescribeOutput.from_row(x), result))

    def genall(self):
        return self._query.result

    def fetchall(self) -> List[List[Any]]:
        return list(iter(self.fetchone, None))

    def cancel(self):
        if self._query is None:
            return
        self._query.cancel()

    def close(self):
        self.cancel()
        # TODO: Cancel not only the last query executed on this cursor
        #  but also any other outstanding queries executed through this cursor.


class SegmentCursor(Cursor):
    def __init__(
            self,
            connection,
            request,
            legacy_primitive_types: bool = False,
            stats_callback: Optional[Callable[[Dict[str, Any]], None]] = None):
        super().__init__(
            connection, request, legacy_primitive_types=legacy_primitive_types, stats_callback=stats_callback)
        if self.connection._client_session.encoding is None:
            raise ValueError("SegmentCursor can only be used if encoding is set on the connection")

    def execute(self, operation, params=None, typed=None):
        if params or typed is not None:
            # TODO: refactor code to allow for params to be supported
            raise ValueError("params not supported")

        self._query = trino.client.TrinoQuery(self._request, query=operation,
                                              legacy_primitive_types=self._legacy_primitive_types,
                                              fetch_mode="segments",
                                              stats_callback=self._stats_callback)
        self._iterator = iter(self._query.execute())
        return self


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
DateFromTicks = datetime.date.fromtimestamp
TimestampFromTicks = datetime.datetime.fromtimestamp


def TimeFromTicks(ticks):
    return datetime.time(*datetime.localtime(ticks)[3:6])


def Binary(value):
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(f"Binary() expects bytes, bytearray, memoryview or str, got {type(value).__name__}")


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = [v.lower() for v in values]

    def __eq__(self, other):
        return other.lower() in self.values


STRING = DBAPITypeObject("VARCHAR", "CHAR", "VARBINARY", "JSON", "IPADDRESS")

BINARY = DBAPITypeObject(
    "ARRAY", "MAP", "ROW", "HyperLogLog", "P4HyperLogLog", "QDigest"
)

NUMBER = DBAPITypeObject(
    "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "DECIMAL"
)

DATETIME = DBAPITypeObject(
    "DATE",
    "TIME",
    "TIME WITH TIME ZONE",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "INTERVAL YEAR TO MONTH",
    "INTERVAL DAY TO SECOND",
)

ROWID = DBAPITypeObject()  # nothing indicates row id in Trino

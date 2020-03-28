from typing import Iterable, Tuple, Optional, List, ContextManager, \
    Union, IO
from ..xtypes import KwargsDict
from .xtypes import Query, Table, Columns, IndexColumn, OmniColumns, \
    ColumnConfigDict

from enum import Enum
from contextlib import contextmanager
import psycopg2 as pg
from psycopg2 import sql as pgs, extras as pgx, extensions as pge
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement

from . import queries
from ..logger import empty_logger_hub
from .template_query import TemplateQuery
from .tools import \
    coalesce_to_list, \
    split_table_identifier_string, \
    wrap_query_debug, \
    parse_create_table_cols_config, \
    parse_iterable_to_sql, \
    parse_rows_to_sql, \
    sql_block_select, \
    sql_block_where, \
    sql_block_order_by, \
    stack_sql_lines, \
    column_config_dict_to_list
from .config import SQL, DEFAULT, PART, SUFFIX, BLOCK

default_page_size = 1000
default_temp = False


class ResultFormat(Enum):
    """Result format for Session.execute_query"""
    DATAFRAME = 'dataframe'
    LIST = 'list'
    CURSOR = 'cursor'


# NOTE: Enum not used to allow for custom join types if necessary
class TableJoin:
    """PostgreSQL Table Join Type"""
    INNER_JOIN = "INNER JOIN"
    LEFT_OUTER_JOIN = "LEFT OUTER JOIN"
    RIGHT_OUTER_JOIN = "RIGHT OUTER JOIN"
    FULL_OUTER_JOIN = "FULL OUTER JOIN"


QueryResult = Union[None, pge.cursor, pd.DataFrame, List[List]]


class Session:
    """
    PostGIS connection object with methods to read and write data and perform
    spatial analyses.
    """

    def __init__(
            self,
            host: str,
            dbname: str,
            user: str,
            password=DEFAULT.password,
            port=DEFAULT.port,
            log_func=lambda x: None,
            ensure_table_schema=True,
            connection_kwargs: Optional[KwargsDict] = None,
            default_schema=DEFAULT.schema,
            default_geom_col=DEFAULT.col_geom,
            default_cursor_kwargs: Optional[KwargsDict] = None,
            sql_alchemy_engine_kwargs: Optional[KwargsDict] = None
    ):

        # establish connection
        self._connection_parameters = dict(
            host=host, port=port, dbname=dbname, user=user, password=password
        )
        self._connection_kwargs = connection_kwargs or {}

        self.connection = pg.connect(
            **self._connection_parameters,
            **self._connection_kwargs
        )
        self.__connected = True

        # logger for execute query
        self.log = log_func

        # create sql alchemy engine
        self._sql_alchemy_engine = self._create_sqlalchemy_engine(
            **(sql_alchemy_engine_kwargs or {})
        )

        # properties
        self.ensure_table_schema = ensure_table_schema

        # defaults
        self.default_schema = default_schema
        self.default_geom_col = default_geom_col
        self.default_cursor_kwargs = default_cursor_kwargs or {}

    def commit(self):
        """Commit changes to database"""
        self.connection.commit()

    def close(self):
        """Close PostGIS connection"""
        self.connection.close()  # closes connection again with no issue
        self.__connected = False

    def check_connected(self):
        """Assert that session is connected to PostGIS"""
        if not self.__connected:
            raise pg.InterfaceError("Session connection already closed")

    def _cursor(self, **kwargs) -> pge.cursor:
        """
        Return a PostGIS cursor object for manipulating the database.
        **kwargs passed directly to cursor
        """
        self.check_connected()
        return self.connection.cursor(**kwargs)

    def _build_sqlalchemy_login_string(self) -> str:
        """
        Return a string from the login details that be used to connect
        to the PostgreSQL database through SQLAlchemy create_engine
        """

        user, password, host, dbname = [
            self._connection_parameters[key] for key in
            ['user', 'password', 'host', 'dbname']
        ]

        return f"postgresql://{user}:{password}@{host}/{dbname}"

    def _create_sqlalchemy_engine(
            self,
            create_engine_kwargs: Optional[KwargsDict] = None
    ) -> Engine:
        login_string = self._build_sqlalchemy_login_string()

        return create_engine(login_string, **(create_engine_kwargs or {}))

    @contextmanager
    def sqlalchemy_connection(
            self,
            connection_kwargs: Optional[KwargsDict] = None
    ):
        """
        Context manager wrapper for SQLAlchemy connection to the database.
        Used by `pandas` and `geopandas` dataframes to write/read to/from
        the PostGIS database referenced in the parent `Session`.
        """
        conn = self._sql_alchemy_engine.connect(
            **(connection_kwargs or {})
        )
        try:
            yield conn
        finally:
            conn.close()

    def _split_table_identifier(
            self, table: Table
    ) -> Tuple[str, str]:
        """
        Splits table identifier string using local `default_schema` attribute
        if no schema is given. Returns identifiers. Tuple(`schema`,`table`)

        """
        return split_table_identifier_string(table, self.default_schema)

    def _table_with_schema(
            self, table: Table
    ) -> Table:
        """
        Returns `table` with the default schema if none is provided and if
        `self.ensure_table_schema` is True.
        """
        if self.ensure_table_schema:
            return ".".join(self._split_table_identifier(table))
        else:
            return table

    def format_query(self, query: Query, args, kwargs):
        """
        Formats the query through TemplateQuery but applies a schema to
        all tables if `self.ensure_table_schema` is True.
        """
        if self.ensure_table_schema:
            return TemplateQuery(query, self.default_schema).format(
                *(args or []), **(kwargs or {})
            )
        else:
            return TemplateQuery(query).format(*args, **kwargs)

    def _process_query(self, query: Query, context=None) -> str:
        """
        Process query object in the database context and return as a string
        """
        context = context or self.connection
        if isinstance(query, pgs.Composable):
            return query.as_string(context).strip()
        else:
            return query.strip()

    def execute_query(
            self, query: Query, fetch=False,
            result_format=ResultFormat.DATAFRAME, as_cols=False,
            single_response=False,
            cursor_kwargs: Optional[KwargsDict] = None,
            convert_array_to_tuple=False,
            log_query_string=False
    ) -> QueryResult:

        """
        Execute `query` in parent `Session`. If `fetch` is `True` then return
        the fetched query result with a format corresponding to `result_type`.
        If `single_response` is `True` and `result_type` is not `CURSOR`, then
        the first value of the first row (top-left-most) of the result table
        will be the only value returned.

        `QueryResultFormat` Result Types:
            DATAFRAME (default): `pandas.DataFrame` object is returned
            LIST: A `List` of the result rows as python `List` objects/
                if `as_cols` is `True` then a  `List` of columns as `List`
                objects is returned
            CURSOR: the cursor as a `psycopg2.extras.cursor object` is
                returned

        """

        self.check_connected()

        cursor_kwargs = cursor_kwargs or self.default_cursor_kwargs

        with self._cursor(**cursor_kwargs) as cursor:
            # psycopg2 2.7.5 requires string query
            query_string = self._process_query(query, cursor)

            # BREAKPOINT HERE \/
            if log_query_string:
                self.log(wrap_query_debug(query_string))
            cursor.execute(query_string)

            if fetch:

                if result_format == ResultFormat.CURSOR:
                    return cursor

                rows = cursor.fetchall()

                if convert_array_to_tuple:
                    # get indices of columns that have list values
                    # assume that first row is representative of entire column
                    # type
                    list_col_indices = [
                        index for index, value in enumerate(rows[0])
                        if isinstance(value, list)
                    ]

                    # force conversion of columns that are lists to tuple
                    if len(list_col_indices) > 0:
                        for row_index in range(len(rows)):
                            row = list(rows[row_index])
                            for index in list_col_indices:
                                row[index] = tuple(row[index])
                            rows[row_index] = tuple(row)

                if single_response:
                    return rows[0][0]

                if result_format == ResultFormat.LIST:
                    return list(zip(*rows)) if as_cols else rows
                elif result_format == ResultFormat.DATAFRAME:
                    return pd.DataFrame(
                        rows,
                        columns=[col.name for col in cursor.description]
                    )
                else:
                    raise ValueError(
                        "result_type must be a valid QueryResultFormat"
                    )

    def table_exists(self, table: Table):
        schema, table = self._split_table_identifier(table)
        query = self.format_query(
            queries.table_exists, None,
            dict(schema=schema, table=table)
        )
        return self.execute_query(query, fetch=True, single_response=True)

    def any_tables_not_exist(self, *tables: Table):
        return any(self.table_exists(table) for table in tables)

    def check_table_exists(self, table: Table):
        if not self.table_exists(table):
            msg = "table {} does not exist"
            raise pg.ProgrammingError(msg.format(table))

    def check_table_not_exists(self, table: Table):
        if self.table_exists(table):
            msg = "table {} already exists"
            raise pg.ProgrammingError(msg.format(table))

    def table_cols_info(self, table: Table):
        """Retrieve column information from `information_schema` for `table`"""
        self.check_table_exists(table)

        schema, table = self._split_table_identifier(table)
        query = self.format_query(
            queries.table_cols_info, [],
            dict(schema=schema, table=table)
        )
        return self.execute_query(
            query, fetch=True, result_format=ResultFormat.DATAFRAME
        )

    def table_col_names(self, table: Table):
        return list(self.table_cols_info(table).column_name)

    def _coalesce_single_col(
            self,
            table: Table,
            col: IndexColumn,
            _cached_table_cols: Optional[Iterable[str]] = None
    ) -> str:
        """
        Coalesces column `col` to a string column name that exists in `table`
        from either the column string name or an integer representing its index,
        if the name or index exists in `table`.
        """

        table = self._table_with_schema(table)

        if _cached_table_cols is None:
            table_cols = self.table_col_names(table)
            num_cached_cols = 0
        else:
            table_cols = list(_cached_table_cols)
            num_cached_cols = len(table_cols)

        if isinstance(col, int):
            try:
                return table_cols[col]
            except IndexError:
                raise IndexError(
                    f"Index {col} is out of range of table {table}"
                )
        else:
            if col in table_cols:
                return col
            else:
                raise KeyError(
                    f"Column '{col}' is not in table {table}"
                    + (
                        f". Cached table columns provided: {num_cached_cols}"
                        if num_cached_cols > 0 else ""
                    )
                )

    def _coalesce_cols(
            self,
            table: Table,
            cols: Optional[OmniColumns],
            allow_no_cols=False,
    ) -> Union[str, List[str]]:
        """
        Coalesces columns `cols` to a list of string column names that exists
        in `table` from either the column string name or an integer representing
        its index, if the name or index exists in `table`.

        Will return an empty list if `cols` is `None` and `allow_no_cols` is
        `True`.
        """

        table = self._table_with_schema(table)

        if cols is None:
            if allow_no_cols:
                return []
            else:
                raise ValueError(
                    "Must provide `cols` if `allow_no_cols` is `False`"
                )

        table_cols = self.table_col_names(table)
        coalesced_cols = coalesce_to_list(cols)

        return [
            self._coalesce_single_col(table, col, table_cols)
            for col in coalesced_cols
        ]

    def create_table(
            self, table: Table, column_config: Iterable[Tuple[str, str]],
            temp=default_temp, log_query_string=True
    ):
        """
        Create a table with name `table`. `column_configs` is a list of
        tuples (`name`,`config`) where `name` is a column name and `config`
        is a SQL expression describing the column configuration.

        A temporary table is created if `temp` is `True`.
        """
        table = self._table_with_schema(table)

        if not temp:
            self.check_table_not_exists(table)

        parsed_config = parse_create_table_cols_config(column_config)
        temp_fragment = PART.temp if temp else ""
        query = self.format_query(
            queries.create_table, None,
            dict(table=table, config=parsed_config, temp=temp_fragment)
        )
        self.execute_query(query, log_query_string=log_query_string)

    def create_table_as(
            self, table: Table, query: Query, temp=default_temp,
            log_query_string=True
    ):
        """
        Wrap `query` with a CREATE TABLE `table` AS statement.

        A temporary table is created if `temp` is `True`.
        """
        table = self._table_with_schema(table)

        if not temp:
            self.check_table_not_exists(table)

        temp_fragment = PART.temp if temp else ""
        if not isinstance(query, pgs.Composable):
            query = pgs.SQL(query)

        full_query = self.format_query(
            queries.create_table_as, None,
            dict(table=table, query=query, temp=temp_fragment)
        )
        self.execute_query(full_query, log_query_string=log_query_string)

    def create_table_from_rows(
            self,
            table: Table,
            column_config: ColumnConfigDict,
            rows: Iterable[Iterable],
            page_size=default_page_size,
            log_query_string=True,
            temp=default_temp
    ):
        table = self._table_with_schema(table)

        column_config_create = column_config_dict_to_list(column_config)

        self.create_table(table, column_config_create, temp, log_query_string)

        self.insert(
            table, rows, page_size=page_size, log_query_string=log_query_string
        )

    def create_table_from_dataframe(
            self,
            table: Table,
            column_config: ColumnConfigDict,
            dataframe: pd.DataFrame,
            page_size=default_page_size,
            log_query_string=True,
            temp=default_temp,
            fillna=True,
            fillna_with=None
    ):
        table = self._table_with_schema(table)

        if fillna:
            dataframe.replace({np.nan: fillna_with}, inplace=True)

        # remove columns other than the ones in column_config
        keep_columns = list(column_config.keys())
        drop_columns = list(dataframe.columns.difference(keep_columns))
        dataframe.drop(drop_columns, axis=1, inplace=True)

        missing_columns = set(keep_columns) - set(dataframe.columns)

        if missing_columns:
            raise ValueError(f"Data is missing columns: {missing_columns}")

        rows = dataframe.values.tolist()

        # NOTE: rows will actually be Iterable[Iterable]
        self.create_table_from_rows(
            table, column_config, rows, page_size, log_query_string, temp
        )

    def create_table_from_csv(
            self,
            table: Table,
            column_config: ColumnConfigDict,
            filepath_or_buffer: Union[str, IO],
            read_csv_kwargs: Optional[KwargsDict] = None,
            page_size=default_page_size,
            log_query_string=True,
            temp=default_temp

    ):
        df = pd.read_csv(
            filepath_or_buffer, **(read_csv_kwargs or {})
        )

        self.create_table_from_dataframe(
            table, column_config, df, page_size,
            log_query_string, temp
        )

    def create_table_from_shp(
            self,
            table: Table,
            file_path: str,
            column_config: ColumnConfigDict,
            sql_geom_col_name=None,
            read_file_kwargs: Optional[KwargsDict] = None,
            to_sql_kwargs: Optional[KwargsDict] = None,
            connection_kwargs: Optional[KwargsDict] = None,
            dataframe_geometry_column='geometry',

    ):
        """
        Create `table` in the parent `Session` using the shp data at `file_path`
        and the non-spatial column structure specified by `column_config`
        """
        self.check_table_not_exists(table)

        # resolve output geometry column name
        sql_geom_col_name = sql_geom_col_name or self.default_geom_col

        # read shapefile to geodataframe
        gdf = gpd.read_file(file_path, **(read_file_kwargs or {}))

        gdf_geom_col = gdf[dataframe_geometry_column]

        # geometry column srid as integer
        geom_epgs = gdf_geom_col.crs.to_epsg()

        # geometry column geometry type
        geom_type = gdf_geom_col[0].geometryType()

        # create new (or alter old) geometry column
        gdf[sql_geom_col_name] = gdf_geom_col.apply(
            lambda x: WKTElement(x.wkt, srid=geom_epgs)
        )

        # drop old geometry column only if they are not the same
        if sql_geom_col_name != dataframe_geometry_column:
            gdf.drop(dataframe_geometry_column, 1, inplace=True)

        # NOTES: table has to be created by to_sql otherwise it creates problems
        # create table from config and add a geometry column
        # create_column_config = [tuple(p) for p in column_config.items()]
        # self.create_table(table, create_column_config)
        #
        # self.add_geom_col(
        #     table, geom_epgs, geom_type, dimension, sql_geom_col_name
        # )
        # self.commit()

        # remove columns other than the ones in column_config
        keep_columns = list(column_config.keys()) + [sql_geom_col_name]
        gdf.drop(gdf.columns.difference(keep_columns), axis=1)

        # write to sql database
        schema, table = self._split_table_identifier(table)
        to_sql_kwargs = {
            'index': False,
            'if_exists': 'fail',
            'dtype': {
                **column_config,
                **{sql_geom_col_name: Geometry(geom_type, srid=geom_epgs)}
            },
            **(to_sql_kwargs or {})
        }

        with self.sqlalchemy_connection(connection_kwargs) as conn:
            gdf.to_sql(table, conn, schema, **to_sql_kwargs)

    def drop_table(self, table: Table, if_exists=True):
        """
        Drop table named `table`
        """
        table = self._table_with_schema(table)

        exists_block = PART.if_exists if if_exists else SQL.empty
        query = self.format_query(
            queries.drop_table, None,
            dict(table=table, exists=exists_block)
        )
        self.execute_query(query)

    def insert(
            self,
            table: Table,
            rows: Iterable[Iterable],
            target_cols: Iterable[str] = None,
            template: Union[pgs.Composable, str, None] = None,
            page_size=100,
            cursor_kwargs: Optional[KwargsDict] = None,
            log_query_string=False
    ):
        """
        Insert `rows` into `target_cols` of `table` using `template`.
        """
        table = self._table_with_schema(table)

        self.check_table_exists(table)
        cursor_kwargs = cursor_kwargs or self.default_cursor_kwargs

        with self._cursor(**cursor_kwargs) as cursor:

            if target_cols is None:
                cols_block = pgs.SQL("")
            else:
                cols_block = self.format_query(
                    "({I@c})", None, {'c': target_cols}
                )

            if template and isinstance(template, pgs.Composable):
                template = template.as_string(cursor)

            query = self.format_query(
                queries.insert, None,
                dict(table=table, cols_block=cols_block)
            )
            query_string = self._process_query(query, cursor)
            if log_query_string:
                self.log(wrap_query_debug(query_string))

            pgx.execute_values(cursor, query_string, rows, template, page_size)

    def select(
            self,
            table: Table,
            cols: Optional[OmniColumns] = None,
            where: Optional[Iterable[str]] = None,
            order_by: Optional[Iterable[str]] = None,
            result_format=ResultFormat.DATAFRAME,
            exclude_cols: Optional[OmniColumns] = None,
            as_cols=False,
            single_response=False,
            auto_order_by_first_n_cols: Optional[int] = 1,
            convert_array_to_tuple=False,
            log_query_string=True,
            cursor_kwargs: Optional[KwargsDict] = None
    ) -> QueryResult:
        """
        Select columns `cols` (excluding columns in `exclude_cols`) from `table`
        with WHERE and ORDER BY clauses built from items in `where` and
        `order_by` respectively.

        If `order_by` is `None`, `auto_order_by_first_n_cols` will be used. If
        value is `None` then no ORDER BY clause will be used. If the value is
        an integer (n) the first (n) columns will be in used to order. If the
        value is `-1` all columns will be used to order.


        Other arguments passed to `execute_query`.
        """

        table = self._table_with_schema(table)

        # get names of columns to be selected except those excluded
        if cols is None:
            cols = self.table_col_names(table)
        use_cols = self._coalesce_cols(table, cols)

        if exclude_cols is not None:
            # diff with lists instead of sets to maintain column order
            excluded_cols = self._coalesce_cols(table, exclude_cols)
            selected_cols = [
                col for col in use_cols if col not in excluded_cols
            ]
        else:
            selected_cols = list(use_cols)

        lines = [
            sql_block_select(selected_cols),  # SELECT
            self.format_query("FROM {T@table}", None, dict(table=table))  # FROM
        ]

        if where:
            lines.append(
                sql_block_where((pgs.SQL(i) for i in where))
            )

        # create ORDER BY clause using either the ORDER BY argument
        # or the first_n_cols if supplied
        if order_by:
            lines.append(
                sql_block_order_by((pgs.SQL(i) for i in (order_by or [])))
            )
        elif auto_order_by_first_n_cols:

            n = auto_order_by_first_n_cols

            if n > 0 or n == -1:

                if n > 0:
                    order_cols_to_index = min((
                        len(selected_cols), auto_order_by_first_n_cols
                    ))
                else:
                    order_cols_to_index = len(selected_cols)

                lines.append(
                    sql_block_order_by((
                        pgs.Identifier(col) for col in
                        selected_cols[:order_cols_to_index]
                    ))
                )

        combined_query = stack_sql_lines(*lines)

        return self.execute_query(
            combined_query, True, result_format, as_cols, single_response,
            cursor_kwargs, convert_array_to_tuple, log_query_string
        )

    def _make_table_cols_sets(self, table: Table, cols: Iterable[str]):
        table = self._table_with_schema(table)
        return set(cols), set(self.table_col_names(table))

    def cols_in_table(self, table: Table, *cols: str):
        """Returns columns in `cols` that are present in `table`"""
        table = self._table_with_schema(table)
        target_cols, table_cols = self._make_table_cols_sets(table, cols)
        return list(target_cols.intersection(table_cols))

    def cols_not_in_table(self, table: Table, *cols: str):
        """Returns columns in `cols` that are not present in `table`"""
        table = self._table_with_schema(table)
        target_cols, table_cols = self._make_table_cols_sets(table, cols)
        return list(target_cols.difference(table_cols))

    def table_has_all_of_cols(self, table: Table, *cols: str):
        """Returns whether any column in `cols` is present in `table`"""
        table = self._table_with_schema(table)
        target_cols = set(cols)
        table_cols = set(self.table_col_names(table))

        return target_cols.issubset(table_cols)

    def table_has_none_of_cols(self, table: Table, *cols: str):
        """Returns whether all columns in `cols` are present in `table`"""
        table = self._table_with_schema(table)
        target_cols = set(cols)
        table_cols = set(self.table_col_names(table))

        return target_cols.isdisjoint(table_cols)

    def check_table_has_all_of_cols(self, table: Table, *cols: str):
        """
        Raises exception if any column in `cols` is not present in `table`
        """
        table = self._table_with_schema(table)
        if not self.table_has_all_of_cols(table, *cols):
            msg = '"{}" is missing columns {}'
            raise pg.ProgrammingError(msg.format(
                table, self.cols_not_in_table(table, *cols)
            ))

    def check_table_has_none_of_cols(self, table: Table, *cols: str):
        """
        Raises exception if any column in `cols` is present in `table`
        """
        table = self._table_with_schema(table)
        if not self.table_has_none_of_cols(table, *cols):
            msg = '"{}" is missing columns {}'
            raise pg.ProgrammingError(msg.format(
                table, self.cols_in_table(table, *cols)
            ))

    def set_primary_key(self, table: Table, columns: OmniColumns):
        table = self._table_with_schema(table)

        columns = self._coalesce_cols(table, columns)

        query = self.format_query(
            queries.add_primary_key, None,
            dict(table=table, col_names=columns)
        )
        self.execute_query(query)

    def add_geom_col(
            self, table: Table, srid: int, geom_type: str, geom_dim: int,
            col_geom: Optional[str] = None, log_query_string=True
    ):
        """
        Add a PostGIS geometry column to `table` with coordinate system `srid`,
        geometry type `geom_type` and dimensions `geom_dim`.

        Created column will be named using the Session default_geom_col if not
        specified in `col_geom`.
        """

        col_geom = col_geom or self.default_geom_col

        self.check_table_exists(table)
        self.check_table_has_none_of_cols(table, col_geom)

        schema, table = self._split_table_identifier(table)

        query = self.format_query(
            queries.add_geom_col, None, dict(
                schema=schema, table=table, col_geom=col_geom,
                srid=srid,
                geom_type=geom_type, geom_dim=geom_dim
            )
        )
        self.execute_query(query, log_query_string=log_query_string)

    def calc_geom_from_xy(
            self, table: Table, col_x: IndexColumn, col_y: IndexColumn,
            srid_input,
            srid_output: Optional[int] = None, col_geom: Optional[str] = None,
            log_query_string=True
    ):
        """
        Set the value of PostGIS 2D geometry column in `table` with coordinate
        system `srid_output` from the existing `col_x` and `col_y` columns with
        coordinate system `srid_input`.

        `srid_output` will be the same as `srid_input` if not specified.

        Modified column name is based on the Session default_geom_col if not
        specified in `col_geom`.
        """
        table = self._table_with_schema(table)

        col_geom = col_geom or self.default_geom_col
        col_x, col_y = self._coalesce_cols(table, [col_x, col_y])

        self.check_table_exists(table)
        self.check_table_has_all_of_cols(table, col_geom, col_x, col_y)

        if srid_output is None or srid_output == srid_input:
            base_query = queries.calc_geom_col
        else:
            base_query = queries.calc_geom_col_reproj

        query = self.format_query(
            base_query, None, dict(
                table=table, col_geom=col_geom, col_x=col_x,
                col_y=col_y,
                srid_input=srid_input, srid_output=srid_output
            )
        )
        self.execute_query(query, log_query_string=log_query_string)

    def add_geom_col_from_xy(
            self, table: Table, col_x: str, col_y: str, srid_input,
            geom_type: str, geom_dim: int,
            srid_output: Optional[int] = None, col_geom: Optional[str] = None,
            log_query_string=True
    ):
        """
        Create a PostGIS 2D geometry column in `table` with coordinate
        system `srid_output` from the existing `col_x` and `col_y` columns with
        coordinate system `srid_input`, geometry type `geom_type` and dimensions
        `geom_dim`.

        `srid_output` will be the same as `srid_input` if not specified.

        Created column will be named using the `Session` `default_geom_col` if
        not specified in `col_geom`.
        """
        table = self._table_with_schema(table)

        self.check_table_exists(table)

        self.add_geom_col(
            table, srid_output, geom_type, geom_dim, col_geom, log_query_string
        )
        self.calc_geom_from_xy(
            table, col_x, col_y, srid_input, srid_output, col_geom,
            log_query_string
        )

    def create_spatial_index(
            self, table: Table, col_geom: Optional[IndexColumn] = None,
            suffix=SUFFIX.spatial_index,
            log_query_string=True
    ):
        """
        Creates a spatial index on `table` using the geometry column, which is
        the `Session` `default_geom_col` if not specified in `geom_col`.

        Spatial index has the same name as the table, with `suffix` appended.
        """
        table = self._table_with_schema(table)

        self.check_table_exists(table)
        if col_geom:
            col_geom = self._coalesce_single_col(table, col_geom)
        else:
            col_geom = self.default_geom_col

        self.check_table_has_all_of_cols(table, col_geom)

        part_schema, part_table = self._split_table_identifier(
            table)

        query = self.format_query(
            queries.create_spatial_index, None, dict(
                table=table, col_geom=col_geom,
                name=part_table + suffix
            )
        )
        self.execute_query(query, log_query_string=log_query_string)

    def create_simple_index(
            self, table: Table,
            columns: Optional[OmniColumns],
            first_n_cols: Optional[int] = None,
            suffix=SUFFIX.spatial_index,
            log_query_string=True
    ):
        """
        Creates a simple index on `table` using columns in `columns`.
        Alternatively will create an index on the first `first_n_cols` of the
        table. If neither `columns` or `first_n_cols` is specified, index will
        be create on all of the columns.

        Index has the same name as the table, with `suffix` appended.
        """
        table = self._table_with_schema(table)

        self.check_table_exists(table)

        part_schema, part_table = self._split_table_identifier(
            table
        )

        if columns is None and first_n_cols is None:
            col_names = self.table_col_names(table)
        elif columns is not None:
            col_names = self._coalesce_cols(table, columns)
        else:
            col_names = self._coalesce_cols(
                table,
                list(range(first_n_cols))
            )

        query = self.format_query(
            queries.create_simple_index, None, dict(
                table=table, col_names=col_names,
                name=part_table + suffix
            )
        )
        self.execute_query(query, log_query_string=log_query_string)

    def create_table_by_join_using(
            self,
            output_table: Table,
            tables_to_join: Iterable[Table],
            join_using_columns: OmniColumns,
            join_type=TableJoin.FULL_OUTER_JOIN,
            select_items: Optional[str] = None,
            order_by_using_columns=True,
            temp=False,
            line_template="{S@joiner} {T@table} USING ({cols})",
            from_template="FROM {T@table}"
    ):
        """
        Create `output_table` by joining `tables_to_join` using column(s)
        `join_using_columns`.
        """
        output_table = self._table_with_schema(output_table)

        tables_to_join = [self._table_with_schema(t) for t in tables_to_join]

        if not temp:
            self.check_table_not_exists(output_table)

        coalesced_cols = coalesce_to_list(join_using_columns)
        formatted_cols = parse_iterable_to_sql(coalesced_cols, pgs.Identifier)

        if select_items is None:
            select_block = pgs.SQL(SQL.wildcard)
        else:
            select_block = parse_iterable_to_sql(select_items, pgs.SQL)

        tables = list(tables_to_join)

        if len(tables) < 1:
            raise ValueError("`tables_to_join` must have at least one item")

        lines = [
            pgs.SQL(BLOCK.select).format(select_block),
            self.format_query(from_template, None, dict(table=tables[0])),
            *(
                self.format_query(
                    line_template, None, dict(
                        joiner=join_type,
                        table=table,
                        cols=formatted_cols
                    )
                )
                for i, table in enumerate(tables[1:])
            )
        ]

        # reformat columns as pgs.SQL and insert ORDER BY line
        # if option dictates
        if order_by_using_columns:
            lines.append(sql_block_order_by(
                pgs.Identifier(c) for c in coalesced_cols
            ))

        query = stack_sql_lines(*lines)

        self.create_table_as(output_table, query, temp)

    def list_tables(self, schema: Optional[str]):

        schema = schema or self.default_schema
        tables = self.select(
            "information_schema.tables", "table_name",
            (
                [f""""table_schema" = '{schema}'"""]
                if schema else None
            ),
            result_format=ResultFormat.LIST,
            as_cols=True
        )[0]
        return tables


@contextmanager
def new_session(
        host: str,
        dbname: str,
        user: str,
        password=DEFAULT.password,
        port=DEFAULT.port,
        logger_hub=empty_logger_hub(),
        ensure_table_schema=True,
        connection_kwargs: Optional[KwargsDict] = None,
        session_kwargs: Optional[KwargsDict] = None,
        commit_on_success=False
) -> ContextManager[Session]:
    """
    Wrapper over the `psycopg2.connect` context manager which yields a
    `Session` that is used to make changes to the postgis database.

    Use a context:

        with new_session(my_logger, **my_login_details) as session:
            query = 'SELECT * FROM ' \
                    '(VALUES (1,'one'),(2,'two'),(3,'three')) s("num","name")'

            session.create_table_as(
                'test_table',
                '
            )
    """
    logger = logger_hub.context(f"Session Manager")
    db_path = f"@{host}/{dbname}"
    logger.debug(f"opened connection to {db_path}")
    session = Session(
        host, dbname, user, password, port, logger_hub.context("Session").debug,
        ensure_table_schema, connection_kwargs, **(session_kwargs or {})
    )

    try:
        yield session
        if commit_on_success:
            session.commit()
            logger.debug("committed")
    finally:
        session.close()
        logger.debug(f"closed connection to {db_path}")

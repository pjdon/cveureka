"""
Helper functions used by PostGIS tools
"""

from typing import Callable, Iterable, Tuple, Union, List
import numpy as np
from psycopg2 import sql as pgs

from .config import SQL, DEFAULT, PART, BLOCK
from .xtypes import ColumnConfigDict, ColumnConfigList

wrap_debug_width = 10
wrap_debug_top = "\n" + "∨" * wrap_debug_width
wrap_debug_bottom = "∧" * wrap_debug_width + "\n"


def wrap_query_debug(query: str) -> str:
    """
    Wraps `query` with a debug marker line above and below so it's easier
    to see in the log file
    """
    return f"\n{wrap_debug_top}\n{query}\n{wrap_debug_bottom}"


def is_non_string_iterable(value):
    """Returns whether `value` can be iterated but is not a string"""
    return hasattr(value, '__iter__') and not isinstance(value, str)


def coalesce_to_list(value):
    """
    Returns `value` wrapped into a single-item list if it is not a
    non-string iterable
    """
    if is_non_string_iterable(value):
        return value
    else:
        return [value]


def parse_iter_or_value(func: Callable, value):
    """
    Apply `func` to `value` if it is a non-string iterable or delimiter join
    all items in `value` parsed by `func` otherwise.
    """
    if is_non_string_iterable(value):
        return pgs.SQL(SQL.delim).join(map(func, value))
    else:
        return func(value)


def split_table_identifier_string(
        table_identifier: str, default_schema: str = DEFAULT.schema
) -> Tuple[str, str]:
    """
    Returns a (`schema`,`table`) tuple based on `table_path` split by
    the `.` character.

    If no schema is found in `table_path` then `default_schema` is
    returned as the `schema`.
    """

    tokens = table_identifier.split(SQL.dot)
    if len(tokens) >= 2:
        schema = tokens[0]
        table = SQL.dot.join(tokens[1:])
    else:
        schema = default_schema
        table = table_identifier
    return schema, table


def make_table_identifier(schema: str, table: str) -> pgs.Composable:
    """
    Returns a `psycopg2.sql.Composable` object from `schema` and `table` joined
    by the `.` character.

    If `schema` is None, then `table` is returned as a `psycopg2.sql.Identifier`
    object.
    """

    if schema is None:
        return pgs.Identifier(table)
    else:
        return pgs.SQL("{}{}{}").format(
            pgs.Identifier(schema), pgs.SQL(SQL.dot), pgs.Identifier(table)
        )


def identifier_from_table_path(
        table_path: str, default_schema: str = DEFAULT.schema
) -> pgs.Composable:
    """
    Returns a `psycopg2.sql.Composable' based on `table_path` split by
    the `.` character.

    If no schema is found in `table_path` then `default_schema` is
    returned as the `schema`.
    """
    schema, table = split_table_identifier_string(table_path, default_schema)
    return make_table_identifier(schema, table)


def parse_create_table_cols_config(
        column_configs: Iterable[Tuple[str, str]]
) -> pgs.Composable:
    """Converts a list of `name`,`config` tuples into SQL query component
    used in a CREATE TABLE statement.

    `name` is the column name and `config` is the SQL expression describing
    its type and other parameters."""

    parts = [pgs.SQL("{} {}").format(pgs.Identifier(name), pgs.SQL(config))
             for name, config in column_configs]

    return pgs.SQL(SQL.delim).join(parts)


def parse_iterable_to_sql(
        iterable: Iterable, value_parser=pgs.Literal, delim=SQL.delim
) -> pgs.Composable:
    """
    Convert `iterable` into a `psycopg2.sql.Composed` object delimiter-separated
    list that can be inserted into a query.
    """

    return pgs.SQL(delim).join([value_parser(i) for i in iterable])


def parse_rows_to_sql(
        rows: Iterable[Iterable], value_parser=pgs.Literal
) -> pgs.Composable:
    """
    Convert `rows` into a `psycopg2.sql.Composed` object to be inserted
    into VALUES statement to write data directly through a query.

    `rows` must be an iterable with each item being an iterable representing
    a single row.
    """

    return pgs.SQL(SQL.delim).join(
        pgs.SQL(PART.wrap_bracket).format(
            pgs.SQL(SQL.delim).join(
                list(map(value_parser, row))
            )
        ) for row in rows
    )


def parse_rows_to_sql_values(
        rows: Iterable[Iterable]
) -> pgs.Composable:
    """
    Convert `rows` into a `psycopg2.sql.Composed` VALUES statement that can
    be inserted into a query.

    `rows` must be an iterable with each item being an iterable representing
    a single row.
    """

    return pgs.SQL("( VALUES {})").format(parse_rows_to_sql(rows, pgs.Literal))


def coalesce_indices_to_names(
        names_or_indices: Iterable[Union[str, int]],
        names: Iterable[str]
):
    """
    Return `names_or_indices` with each integer item replaced by the
    corresponding name in `names` if in range. Will raise ValueError if
    the item is not an integer and does not appear in `names`.
    """
    output = []
    for item in names_or_indices:
        if isinstance(item, int):
            try:
                output.append(names[item])
            except IndexError:
                raise IndexError(f"Index {item} is out of range")
        else:
            if item in names_or_indices:
                output.append(item)
            else:
                raise ValueError(f"{item} is not in list")
    return output


def stack_sql_lines(
        *lines: pgs.Composable, delim=SQL.newline
) -> pgs.Composable:
    """
    Stacks `lines` vertically into a single query. Use to combine parts of
    queries in a way which will be readable in the log or debug.
    """

    return pgs.SQL(delim).join(lines)


def union_sql_blocks(
        *blocks: pgs.Composable, delim=BLOCK.union
) -> pgs.Composable:
    """
    Stacks `blocks` vertically separated by the UNION statement.
    """

    return pgs.SQL(delim).join(blocks)


def sql_block_generic(
        block: str,
        items: Iterable[pgs.Composable],
        delim: str
) -> pgs.Composable:
    """
    Returns a single-line SQL block by inserting `items` joined by `delim` into
    `block`.
    """
    return pgs.SQL(block).format(
        pgs.SQL(delim).join(items)
    )


def sql_block_select(column_names: Iterable[str]) -> pgs.Composable:
    """
    Returns a single-line `SELECT ... , ...` block using `items`.
    """
    cols = [pgs.Identifier(name) for name in column_names]
    return sql_block_generic(BLOCK.select, cols, SQL.delim)


def sql_block_where(items: Iterable[pgs.Composable]) -> pgs.Composable:
    """
    Returns a single-line `WHERE ... AND ...` block using `items`.
    """
    return sql_block_generic(BLOCK.where, items, PART.and_sep)


def sql_block_order_by(items: Iterable[pgs.Composable]) -> pgs.Composable:
    """
    Returns a single-line `ORDER BY ... , ...` block using `items`.
    """
    return sql_block_generic(BLOCK.order_by, items, SQL.delim)


def column_config_dict_to_list(
        column_config: ColumnConfigDict
) -> ColumnConfigList:
    return [tuple(p) for p in column_config.items()]

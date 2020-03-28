from collections import namedtuple
from typing import Optional, Iterable
from warnings import warn

from . import Session, ResultFormat
from .xtypes import Table

DiffResult = namedtuple(
    'DiffResult',
    [
        'unique_cols_left', 'unique_cols_right',
        'unique_rows_left', 'unique_rows_right',
        'summary_left', 'summary_right'
    ]
)


def diff_tables(
        table_left: Table,
        table_right: Table,
        session_left: Session,
        session_right: Optional[Session] = None,
        summary_column: Optional[str] = None
):
    """
    Returns non-intersecting columns and rows between two tables
    in two different sessions
    """
    if session_right is None:
        session_right = session_left

    df_left = session_left.select(
        table_left, auto_order_by_first_n_cols=-1,
        result_format=ResultFormat.DATAFRAME,
        convert_array_to_tuple=True
    )

    df_right = session_right.select(
        table_right, auto_order_by_first_n_cols=-1,
        result_format=ResultFormat.DATAFRAME,
        convert_array_to_tuple=True
    )

    unique_cols_left = [
        c for c in df_left.columns
        if c not in df_right.columns
    ]
    unique_cols_right = [
        c for c in df_right.columns
        if c not in df_left.columns
    ]

    if len(unique_cols_left) > 0:
        df_left.drop(unique_cols_left, axis=1, inplace=True)

    if len(unique_cols_right) > 0:
        df_right.drop(unique_cols_right, axis=1, inplace=True)

    merged = df_left.merge(df_right, indicator=True, how='outer')

    unique_rows_left = merged.loc[
        merged['_merge'] == 'left_only',
        merged.columns != '_merge'
    ]

    unique_rows_right = merged.loc[
        merged['_merge'] == 'right_only',
        merged.columns != '_merge'
    ]

    if summary_column:
        summary_left = list(unique_rows_left[summary_column])
        summary_right = list(unique_rows_right[summary_column])
    else:
        summary_left = None
        summary_right = None

    result = DiffResult(
        unique_cols_left, unique_cols_right,
        unique_rows_left, unique_rows_right,
        summary_left, summary_right
    )

    return result


def change_tables_schema(
        session: Session,
        from_schema: str,
        to_schema: str,
        include_table_names: Optional[Iterable[str]],
        exclude_table_names: Optional[Iterable[str]]
):
    """
    Moves tables whose names are in `include_table_names` but not in
    `exclude_table_names` from schema `from_schema` to `to_schema`.
    If `include_table_names` is `None`, then all tables in `from_schema` are
    moved.
    """

    schema_tables = session.list_tables(from_schema)
    if include_table_names:
        include_tables = [t for t in schema_tables if t in include_table_names]
    else:
        include_tables = schema_tables

    if exclude_table_names:
        target_tables = [
            t for t in include_tables if t not in exclude_table_names
        ]
    else:
        target_tables = include_tables

    for table_name in target_tables:
        old_table = f"{from_schema}.{table_name}"
        new_table = f"{to_schema}.{table_name}"

        if not session.table_exists(new_table):
            session.execute_query(
                f"""ALTER TABLE {old_table} SET SCHEMA "{to_schema}" """
            )
        else:
            warn(f"Table {new_table} already existed. Skipped.")



from typing import Dict, List
from collections import defaultdict

from ..postgis import Session, ResultFormat


def get_table_columns(
        session: Session,
        schema: str,
        table_names: List[str],
        order_by=('table_name', 'ordinal_position')
) -> Dict[str, List[str]]:
    """
    Return a `dict` of table names mapping to a list of columns in that table
    drawn from `session` and constrained to tables in `schema` and with
    a name in `table_names`
    """

    formatted_table_names = ', '.join(f"'{t}'" for t in table_names)

    where_items = [
        f"table_schema = '{schema}'",
        f"table_name IN ({formatted_table_names})"
    ]

    rows = session.select(
        'information_schema.columns',
        ['table_name', 'column_name'],
        where=where_items,
        result_format=ResultFormat.LIST,
        order_by=order_by
    )

    table_columns = defaultdict(list)

    for table_name, column_name in rows:
        table_columns[table_name].append(column_name)

    return table_columns


def make_table_columns_decr_md(
        session: Session,
        schema: str,
        table_names: List[str],
        file_path: str,
        format_table_heading="\n## {name}\n",
        format_header="Column | Description | Unit",
        format_subheader="--- | --- | ---",
        format_column="{name} | |",
):
    """
    Create a markdown (md) file at `file_path` with preformatted tables
    containing the columns of tables in `table_names` drawn from `session`
    and constrained to tables in `schema` and with a name in `table_names`
    """

    table_columns = get_table_columns(session, schema, table_names)

    lines = []

    for table in table_names:
        columns = table_columns[table]
        lines.append(format_table_heading.format(name=table))
        lines.append(format_header)
        lines.append(format_subheader)

        for column_name in columns:
            lines.append(format_column.format(name=column_name))

    with open(file_path, 'w') as f:
        f.write('\n'.join(lines) + '\n')
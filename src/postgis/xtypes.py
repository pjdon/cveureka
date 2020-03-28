"""
Extended type hinting objects for PostGIS parameters
"""

from typing import Union, Iterable, Any, Dict, List, Tuple

from psycopg2 import sql as pgs

Query = Union[pgs.Composable, str]
Table = str
Column = str
Columns = Iterable[Column]
IndexColumn = Union[Column, int]
IndexColumns = Iterable[IndexColumn]
OmniColumns = Union[IndexColumn, IndexColumns]
ColumnConfigDict = Dict[str, str]
ColumnConfigList = List[Tuple[str, str]]

table_exists = \
"""\
SELECT EXISTS (
   SELECT 1
   FROM   information_schema.tables
   WHERE  table_schema = {L@schema}
   AND    table_name = {L@table}
   );\
"""

table_cols_info = \
"""\
SELECT *
FROM information_schema.columns
WHERE table_schema = {L@schema}
AND table_name = {L@table}\
"""

add_primary_key = \
"""\
ALTER TABLE {T@table} ADD PRIMARY KEY ({I@col_names})\
"""

drop_table = \
"""\
DROP TABLE {S@exists}{T@table}\
"""

create_table = \
"""\
CREATE {S@temp}TABLE {T@table} ({config})\
"""

create_table_as =\
"""\
CREATE {S@temp}TABLE {T@table} AS ({query})\
"""

select_from_values =\
"""\
SELECT *
FROM ( VALUES {rows} ) s({I@col_names})\
"""

insert = \
"""\
INSERT INTO {T@table} {cols_block} VALUES %s\
"""

add_geom_col = \
"""\
SELECT AddGeometryColumn(
    {L@schema},{L@table},{L@col_geom},{L@srid},{L@geom_type},{L@geom_dim}
)\
"""

calc_geom_col = \
"""\
UPDATE {T@table}
SET {I@col_geom} = ST_SetSRID(
    ST_MakePoint({I@col_x},{I@col_y},{L@srid_input})
)\
"""

calc_geom_col_reproj = \
"""\
UPDATE {T@table}
SET {I@col_geom} = ST_Transform(ST_SetSRID(
    ST_MakePoint({I@col_x}, {I@col_y}), {L@srid_input}),{L@srid_output}
    )\
"""

create_spatial_index = \
"""\
CREATE INDEX IF NOT EXISTS {I@name} ON {T@table} USING GIST({I@col_geom})\
"""

create_simple_index = \
"""\
CREATE INDEX IF NOT EXISTS {I@name} ON {T@table} ({I@col_names})\
"""

select_table = \
"""\
SELECT {columns} FROM {T@table}{clause_where}{clause_order}\
"""
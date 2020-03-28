"""PostGIS Configuration Constants"""


class SQL:
    empty = ''
    dot = '.'
    sep = ','
    space = ' '
    delim = sep + space
    wildcard = '*'
    str_quote = '\''
    idn_quote = '"'
    open_bracket = '('
    close_bracket = ')'
    newline = '\n'


class DEFAULT:
    password = ''
    port = 5432
    schema = "public"
    col_geom = "geom"


class PART:
    temp = "TEMP" + SQL.space
    if_exists = "IF" + SQL.space + "EXISTS" + SQL.space
    wrap_bracket = SQL.open_bracket + "{}" + SQL.close_bracket
    and_sep = SQL.space + "AND" + SQL.space


class BLOCK:
    select = "SELECT {}"
    where = "WHERE {}"
    order_by = "ORDER BY {}"
    union = SQL.newline + "UNION" + SQL.newline


class SUFFIX:
    spatial_index = "_sgix"
    simple_index = "_sidx"

from typing import Dict

import re
from psycopg2 import sql as pgs
from .tools import \
    identifier_from_table_path, \
    parse_iter_or_value


class TemplateQuery:
    """
    Stores `query_string` as a template query with variables which can be
    formatted by arguments using the `format` process. Variables can be
    prepended with forms to apply a `psycopg2.sql` composing function to
    the argument when the query is formatted.


    valid forms:
        S  `psycopg2.pgs.SQL` for sql text
        I  `psycopg2.pgs.Identifier` for identifiers such as columns and tables
        L  `psycopg2.pgs.Literal` for numbers and strings
        P  `psycopg2.pgs.Placeholder` for execution placeholders
        T  splits schema.table strings into a period-joined identifier pair

    example:
    ```
    tquery = TemplateQuery(
        "SELECT {I@col} FROM {T@table}"
        "WHERE {filter_col} {S@comp} {L@threshold}"
    )

    formatted_query = tquery.format(
        col='height', table='myschema.points', comp='>', threshold='30',
        filter_col=psycopg2.pgs.Identifier('value')
    )
    ```

    When `formatted_query` is converted to a string with `as_string` or
    during execution, it will result in the following query:
    ```
    SELECT "height" FROM "myschema"."points"
    WHERE "value" > 30
    ```

    Conversion will only be applied to keyword arguments. Tagged variables with
    no key, such as `{T@}` or `{I@}` will be matched to an empty string key.
    """

    def _qualified_table(self, qualified_table_name: str) -> pgs.Composable:
        kwargs = {}
        if self._default_schema:
            kwargs['default_schema'] = self._default_schema

        return identifier_from_table_path(qualified_table_name, **kwargs)

    # function used to process placeholders with these forms
    # see the TemplateQuery docstring for details
    # changes here have to be reflected in _convert_placeholder_by_form
    _forms = ['S', 'I', 'L', 'P', 'T']

    # NOTE:
    # want to avoid using existing string formatting syntax to prevent confusion
    # and to prevent psycopg2 from rejecting the template input
    _form_marker = '@'

    _base_pattern = \
        r"{{(?P<key>(?P<tag>(?P<form>{forms}){marker})?(?P<subkey>[^{{}}]*))}}"

    _rx_query_forms = re.compile(
        _base_pattern.format(
            forms="|".join(_forms),
            marker=_form_marker
        ), re.I
    )

    def _convert_placeholder_by_form(
            self,
            key: str, value, form_string: str
    ) -> pgs.Composable:

        # NOTE: forms have to be respecified since they have different
        # argument signatures and the T form requires an instance attribute
        if form_string == 'S':
            func = pgs.SQL
        elif form_string == 'I':
            func = pgs.Identifier
        elif form_string == 'L':
            func = pgs.Literal
        elif form_string == 'P':
            func = pgs.Placeholder
        elif form_string == 'T':
            func = lambda v: self._qualified_table(v)
        else:
            raise ValueError(f"Key '{key}' has invalid form '{form_string};")

        return parse_iter_or_value(func, value)

    def _convert_query_kwargs(self, kwargs: Dict[str, any]):
        query_string = self._query_string
        new_kwargs = {}

        for match in self._rx_query_forms.finditer(query_string):
            key = match['key']
            form = match['form']
            subkey = match['subkey']
            tag = match['tag']

            key_error_msg = \
                f"key '{subkey}' missing from format keyword arguments"

            if form is not None and tag is not None:
                try:
                    original_value = kwargs[subkey]
                except KeyError:
                    raise KeyError(key_error_msg)

                new_kwargs[key] = self._convert_placeholder_by_form(
                    key, original_value, form
                )
            else:
                try:
                    new_kwargs[subkey] = kwargs[subkey]
                except KeyError:
                    raise KeyError(key_error_msg)

        return new_kwargs

    def __init__(self, query_string: str, default_schema=None):
        self._query_string = query_string
        self._default_schema = default_schema

    def format(self, *args, **kwargs) -> pgs.Composable:
        new_kwargs = self._convert_query_kwargs(kwargs)
        formatted_query = pgs.SQL(self._query_string).format(*args,
                                                             **new_kwargs)
        return formatted_query

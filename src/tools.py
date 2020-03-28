from typing import Dict, Any
import os
from configparser import SectionProxy


class RelativeConfigPathGetter:
    def __init__(self, parser_section: SectionProxy, relative_path_key: str):
        self.parser_section = parser_section
        if relative_path_key in self.parser_section:
            self.relative_path = self.parser_section[relative_path_key]
        else:
            raise KeyError(
                f"Relative path key '{relative_path_key}' not in parser section"
            )

    def get(self, key: str):
        if key in self.parser_section:
            return os.path.join(
                self.relative_path,
                self.parser_section[key]
            )
        else:
            raise KeyError(
                f"Key '{key}' not in parser section named "
                f"'{self.parser_section.name}'"
            )


class _MetaEchoContainer(type):
    def __getattribute__(self, item):
        actual_value = type.__getattribute__(self, item)
        if actual_value is ...:
            return item
        else:
            return actual_value


class EchoContainer(object, metaclass=_MetaEchoContainer):
    pass


def attrs_to_dict(
        *objects: object,
        exclude_attr_starts_with='_'
) -> Dict[str, Any]:
    """
    Return a dictionary of all attribute:value in `objects` for attributes
    that don't starts with `exclude_attr_starts_with`. Attribute name
    collisions are overwritten by the order in which they appear in `objects`.
    """

    attr_vals = {}
    for obj in objects:
        attr_vals.update(
            {
                name: getattr(obj, name) for name in vars(obj).keys()
                if not name.startswith(exclude_attr_starts_with)
            }
        )
    return attr_vals

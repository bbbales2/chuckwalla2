from typing import List, Tuple, Dict
from sortedcontainers import SortedDict
from pyarrow import int64, string, bool_, float64

import pyarrow
import re


class Schema:
    types: SortedDict[str, str]

    string_type_to_pyarrow_map = {
        "string": string(),
        "bigint": int64(),
        "boolean": bool_(),
        "double": float64()
    }

    def __init__(self, types = Dict[str, str]):
        self.types = SortedDict()

        for name, string_type in types.items():
            if string_type.lower() not in self.string_type_to_pyarrow_map:
                raise TypeError(f"{string_type} is not a supported type")

            self.types[name.lower()] = string_type.lower()

    def __iter__(self):
        yield from self.types

    def __getitem__(self, name : str) -> str:
        return self.types[name]

    @classmethod
    def from_athena_description(cls, description = List[Tuple[str]]):
        regex = re.compile("\\s+")

        string_types = {}
        for line in description:
            if len(line) > 0 and not line.startswith("#"):
                name, string_type = regex.split(line.lower())
                if name in string_types:
                    existing_type = string_types[name]
                    if existing_type != string_type:
                        raise ValueError(f"Invalid description, {name} appears as both {string_type} and {existing_type}")
                string_types[name] = string_type
        return cls(string_types)

    def to_pyarrow_schema(self):
        return pyarrow.schema([(name, self.string_type_to_pyarrow_map[string_type]) for name, string_type in self.types.items()])

    def to_athena_ddl(self):
        return ",".join(f"{name} {dtype}" for name, dtype in self.types.items())

    def assert_equivalent(self, other : 'Schema', excludes = None):
        excludes_lower = list(exclude.lower() for exclude in excludes)

        for name in self.types:
            if name in excludes_lower:
                continue

            if name not in other.types:
                raise KeyError(f"{name} missing from other schema")
            own_type = self.types[name]
            other_type = other.types[name]
            if own_type != other_type:
                raise ValueError(f"{name} is of type {own_type} in one schema and {other_type} in another")

        for name in other.types:
            if name in excludes_lower:
                continue

            if name not in self.types:
                raise KeyError(f"{name} missing from own schema")

from typing import List, Tuple, Dict
from sortedcontainers import SortedDict
from pyarrow import int64, string, bool_, float64

import pyarrow
import re


class Schema:
    types: SortedDict[str, str]
    partitioned_by: List[str]

    string_type_to_pyarrow_map = {
        "string": string(),
        "bigint": int64(),
        "boolean": bool_(),
        "double": float64()
    }

    def __init__(self, types: Dict[str, str], partitioned_by: List[str]):
        self.types = SortedDict()
        self.partitioned_by = list(partition.lower() for partition in partitioned_by)
        # A list might be too general -- consider simplifying this
        assert len(partitioned_by) == 1

        for name_raw, string_type_raw in types.items():
            name = name_raw.lower()
            string_type = string_type_raw.lower()

            if string_type not in self.string_type_to_pyarrow_map:
                raise TypeError(f"{string_type} is not a supported type")

            if name in self.partitioned_by:
                if string_type != "string":
                    raise TypeError(f"Partition {name} is of type {string_type} -- partitions must be of type string")
            else:
                self.types[name] = string_type

    def fields(self):
        yield from self.types

    def __iter__(self):
        for name in self.types:
            yield name

        for name in self.partitioned_by:
            yield name

    def __getitem__(self, name : str) -> str:
        if name in self.partitioned_by:
            return "string"
        else:
            return self.types[name]

    @classmethod
    def from_athena_description(cls, description = List[Tuple[str]]):
        regex = re.compile("\\s+")

        string_types : Dict[str, str] = {}
        partitioned_by : List[str] = []
        partition_line = False
        for line in description:
            comment_line = line.startswith("#")

            # All lines after the first comment correspond to partitions
            if comment_line:
                partition_line = True

            if len(line) > 0 and not comment_line:
                name, string_type = regex.split(line.lower())
                if name in string_types:
                    existing_type = string_types[name]
                    if existing_type != string_type:
                        raise ValueError(f"Invalid description, {name} appears as both {string_type} and {existing_type}")

                if partition_line:
                    partitioned_by.append(name)
                string_types[name] = string_type

        return cls(string_types, partitioned_by)

    def to_pyarrow_schema(self, include_partitions = False):
        field_types = [(name, self.string_type_to_pyarrow_map[string_type]) for name, string_type in self.types.items()]
        partition_types = [(name, self.string_type_to_pyarrow_map["string"]) for name in self.partitioned_by]
        types = field_types + (partition_types if include_partitions else [])
        return pyarrow.schema(types)

    def get_fields_ddl(self):
        return ",".join(f"{name} {dtype}" for name, dtype in self.types.items())

    def get_partitions_ddl(self):
        return ",".join(f"{name} string" for name in self.partitioned_by)

    def assert_equivalent(self, other : 'Schema'):
        for name in self.types:
            if name not in other.types:
                raise KeyError(f"{name} missing from other schema")
            own_type = self.types[name]
            other_type = other.types[name]
            if own_type != other_type:
                raise ValueError(f"{name} is of type {own_type} in one schema and {other_type} in another")

        for name in other.types:
            if name not in self.types:
                raise KeyError(f"{name} missing from own schema")

        for partition in self.partitioned_by:
            if partition not in other.partitioned_by:
                raise KeyError(f"Partition {partition} is missing from other schema")

        for partition in other.partitioned_by:
            if partition not in self.partitioned_by:
                raise KeyError(f"Partition {partition} is missing from own schema")
"""
Schema Registry Loader

Loads table schemas from YAML and provides utilities to:
- Build Spark StructType for Bronze layer ingestion
- Extract field mappings for Silver/Gold transformations
- Validate schema consistency across layers
"""

import os
from typing import Dict, List, Tuple, Any, Optional
import yaml

from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, IntegerType, DecimalType, DateType, TimestampType,
)


class SchemaRegistry:
    """Load and manage schemas from base_schema.yaml and layer configs."""

    def __init__(self, schema_dir: str = None):
        if schema_dir is None:
            schema_dir = os.path.join(
                os.path.dirname(__file__), "..", "schemas"
            )
        self.schema_dir = schema_dir
        self._base_schema: Dict = None
        self._layer_silver: Dict = None
        self._layer_gold: Dict = None

    def _load_yaml(self, filename: str) -> Dict:
        """Load YAML file from schemas directory."""
        path = os.path.join(self.schema_dir, filename)
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}

    @property
    def base_schema(self) -> Dict:
        if self._base_schema is None:
            self._base_schema = self._load_yaml("base_schema.yaml")
        return self._base_schema

    @property
    def layer_silver(self) -> Dict:
        if self._layer_silver is None:
            self._layer_silver = self._load_yaml("layer_silver.yaml")
        return self._layer_silver

    @property
    def layer_gold(self) -> Dict:
        if self._layer_gold is None:
            self._layer_gold = self._load_yaml("layer_gold.yaml")
        return self._layer_gold

    def get_bronze_schema(self, table_name: str) -> StructType:
        """
        Build Spark StructType for Bronze layer.
        All columns are StringType except nested structs (location, metadata).
        """
        base_tables = self.base_schema.get("tables", {})
        if table_name not in base_tables:
            raise ValueError(f"Table '{table_name}' not found in base_schema.yaml")

        table_def = base_tables[table_name]
        columns = table_def.get("columns", {})
        fields = []

        for col_name, col_def in columns.items():
            col_type = col_def.get("type")
            nullable = col_def.get("nullable", True)

            if col_type == "struct":
                nested_fields = self._build_struct_fields(col_def.get("fields", {}))
                field = StructField(col_name, StructType(nested_fields), nullable)
            else:
                # All non-struct are StringType in Bronze (faithful to source)
                field = StructField(col_name, StringType(), nullable)

            fields.append(field)

        return StructType(fields)

    def _build_struct_fields(self, fields_def: Dict) -> List[StructField]:
        """Recursively build nested struct fields."""
        fields = []
        for field_name, field_def in fields_def.items():
            field_type = field_def.get("type")
            nullable = field_def.get("nullable", True)

            if field_type == "string":
                field = StructField(field_name, StringType(), nullable)
            elif field_type == "boolean":
                field = StructField(field_name, BooleanType(), nullable)
            elif field_type == "struct":
                nested = self._build_struct_fields(field_def.get("fields", {}))
                field = StructField(field_name, StructType(nested), nullable)
            else:
                field = StructField(field_name, StringType(), nullable)

            fields.append(field)

        return fields

    def get_silver_field_mappings(self, table_name: str) -> Dict:
        """
        Extract Silver layer field mappings.
        Returns: {target_col: {source, target_type, parser, ...}}
        """
        transformations = self.layer_silver.get("transformations", {})
        if table_name not in transformations:
            raise ValueError(f"Table '{table_name}' not found in layer_silver.yaml")

        return transformations[table_name].get("field_mappings", {})

    def get_silver_deduplication(self, table_name: str) -> Dict:
        """Get deduplication strategy for Silver table."""
        transformations = self.layer_silver.get("transformations", {})
        return transformations.get(table_name, {}).get("deduplication", {})

    def get_silver_null_handling(self, table_name: str) -> Dict:
        """Get null handling rules for Silver table."""
        transformations = self.layer_silver.get("transformations", {})
        return transformations.get(table_name, {}).get("null_handling", {})

    def get_gold_table_def(self, table_name: str) -> Dict:
        """Get full Gold table definition (dim or fact)."""
        tables = self.layer_gold.get("tables", {})
        if table_name not in tables:
            raise ValueError(f"Table '{table_name}' not found in layer_gold.yaml")
        return tables[table_name]

    def get_gold_field_mappings(self, table_name: str) -> Dict:
        """Get Gold layer field mappings for a table."""
        table_def = self.get_gold_table_def(table_name)
        return table_def.get("field_mappings", {})

    def get_base_table_columns(self, table_name: str) -> Dict:
        """Get all column definitions from base schema for a table."""
        base_tables = self.base_schema.get("tables", {})
        if table_name not in base_tables:
            raise ValueError(f"Table '{table_name}' not found in base_schema.yaml")
        return base_tables[table_name].get("columns", {})


def _parse_spark_type(type_str: str) -> type:
    """Convert type string to Spark type."""
    type_str = type_str.lower().strip()

    if type_str == "string":
        return StringType()
    elif type_str == "boolean":
        return BooleanType()
    elif type_str == "integer" or type_str == "int":
        return IntegerType()
    elif type_str == "date":
        return DateType()
    elif type_str == "timestamp":
        return TimestampType()
    elif type_str.startswith("decimal"):
        parts = type_str.split("(")[1].split(")")[0].split(",")
        precision = int(parts[0].strip())
        scale = int(parts[1].strip()) if len(parts) > 1 else 0
        return DecimalType(precision, scale)
    else:
        return StringType()


def build_silver_select_expr(field_mappings: Dict, parser_defs: Dict = None) -> List[Tuple[str, str]]:
    """
    Build list of (expression, alias) tuples for Silver .select() calls.

    Returns list of tuples: (spark_expr_string, alias_name)
    Caller will need to evaluate these with F.col(), F.lit(), etc.
    """
    result = []

    for target_col, mapping in field_mappings.items():
        if isinstance(mapping, str):
            result.append((mapping, target_col))
        elif isinstance(mapping, dict):
            if mapping.get("computed"):
                continue
            source = mapping.get("source", target_col)
            result.append((source, target_col))

    return result


__all__ = ["SchemaRegistry", "build_silver_select_expr"]

from dagster_duckdb import DuckDBResource
from dagster import EnvVar

from .constants import DUCKDB_DATABASE


database_resource = DuckDBResource(
    database=DUCKDB_DATABASE
)
from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
import os

# Point to your existing dbt project
# Default to /app/transformation for Docker, but allow override or fallback for local dev
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", os.path.join(os.getcwd(), "transformation"))

@dbt_assets(
    manifest=os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"),
    dagster_dbt_translator=None, # Use default naming
    name="dbt_transformation",   # Group name
)
def dbt_analytics_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

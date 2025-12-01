from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator
import os

# Point to your existing dbt project
# Default to /app/transformation for Docker, but allow override or fallback for local dev
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", os.path.join(os.getcwd(), "transformation"))

# --- THE BRIDGE ---
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        # For everything (models and sources), use default naming to avoid collisions
        return super().get_asset_key(dbt_resource_props)
    
    def get_group_name(self, dbt_resource_props):
        return "dbt_transformation"

@dbt_assets(
    manifest=os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"),
    # Tell Dagster to use our custom bridge logic
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    name="dbt_transformation",   # Group name
)
def dbt_analytics_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

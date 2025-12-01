from dagster import Definitions, load_assets_from_modules, define_asset_job, AssetSelection
from dagster_dbt import DbtCliResource
from .assets import ingestion, dbt
import os

# Load assets
ingestion_assets = load_assets_from_modules([ingestion])

# Configure Resources
# Default to /app/transformation for Docker, but allow override or fallback for local dev
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", os.path.join(os.getcwd(), "transformation"))

dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR, # Assuming profiles.yml is in the project dir
)

# Define a job that runs ingestion THEN dbt
full_pipeline_job = define_asset_job(
    name="refresh_scouting_platform",
    selection=AssetSelection.groups("ingestion") | AssetSelection.groups("dbt_transformation")
)

defs = Definitions(
    assets=[*ingestion_assets, dbt.dbt_analytics_assets],
    resources={
        "dbt": dbt_resource,
    },
    jobs=[full_pipeline_job],
)

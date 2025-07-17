from dagster import Definitions, load_assets_from_modules
from dagster_aws.s3 import S3Resource
from dagster_dbt import DbtCliResource

from .defs import ingestion
from .defs.dbt_assets import dbt_assets

# Configuration
DBT_PROJECT_DIR = "/Users/estanislao/Educational/qualifyze-data-pipeline/qualifyze_dbt"

# Load all assets
ingestion_assets = load_assets_from_modules([ingestion])
all_assets = ingestion_assets + dbt_assets

# Define resources
resources = {
    "s3": S3Resource(region_name="eu-north-1"),
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROJECT_DIR,
    ),
}

defs = Definitions(assets=all_assets, resources=resources)

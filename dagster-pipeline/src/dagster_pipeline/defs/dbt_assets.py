from dagster import asset, AssetExecutionContext

# Configure dbt project path
DBT_PROJECT_DIR = "/Users/estanislao/Educational/qualifyze-data-pipeline/qualifyze_dbt"

def create_dbt_asset(model_name: str, deps: list, description: str = ""):
    """Helper function to create dbt assets with consistent structure."""
    @asset(
        name=f"dbt_{model_name}",
        deps=deps,
        required_resource_keys={"dbt"},
        description=description or f"Run dbt model: {model_name}"
    )
    def dbt_asset(context: AssetExecutionContext):
        context.log.info(f"Running dbt model: {model_name}")
        
        dbt = context.resources.dbt
        if model_name == "all":
            dbt.cli(["run"])
        else:
            dbt.cli(["run", "--select", model_name])
        
        context.log.info(f"dbt model {model_name} completed!")
        return f"{model_name}_completed"
    
    return dbt_asset

def create_dbt_test_asset(test_name: str, deps: list, description: str = ""):
    """Helper function to create dbt test assets."""
    @asset(
        name=f"dbt_test_{test_name}",
        deps=deps,
        required_resource_keys={"dbt"},
        description=description or f"Run dbt tests: {test_name}"
    )
    def dbt_test_asset(context: AssetExecutionContext):
        context.log.info(f"Running dbt tests: {test_name}")
        
        dbt = context.resources.dbt
        if test_name == "all":
            dbt.cli(["test"])
        else:
            dbt.cli(["test", "--select", test_name])
        
        context.log.info(f"dbt tests {test_name} completed!")
        return f"{test_name}_tests_passed"
    
    return dbt_test_asset



dbt_stg_sites = create_dbt_asset(
    model_name="stg_sites",
    deps=["staged_sites_data"],
    description="Run dbt bronze model - basic data loading from sites_data"
)

dbt_stg_sites_metadata = create_dbt_asset(
    model_name="stg_sites_metadata",
    deps=["staged_sites_metadata"],
    description="Run dbt bronze model - basic data loading from sites_metadata"
)

dbt_sites_unions = create_dbt_asset(
    model_name="sites_unions",
    deps=["dbt_stg_sites", "dbt_stg_sites_metadata"],
    description="Run dbt silver model - combines sites_data and sites_metadata"
)

dbt_sites_enriched = create_dbt_asset(
    model_name="sites_enriched", 
    deps=["dbt_sites_unions"],
    description="Run dbt silver model - heavy lifting with business logic and deduplication"
)

dbt_site_business_insights = create_dbt_asset(
    model_name="site_business_insights",
    deps=["dbt_sites_enriched"],
    description="Run dbt gold model - final business-ready output"
)

# Create dbt test assets - individual tests for immediate feedback
dbt_test_stg_sites = create_dbt_test_asset(
    test_name="stg_sites",
    deps=["dbt_stg_sites"],
    description="Run tests for sites_data bronze model"
)

dbt_test_stg_sites_metadata = create_dbt_test_asset(
    test_name="stg_sites_metadata",
    deps=["dbt_stg_sites_metadata"],
    description="Run tests for sites_metadata bronze model"
)

dbt_test_sites_unions = create_dbt_test_asset(
    test_name="sites_unions",
    deps=["dbt_sites_unions"],
    description="Run tests for sites_unions silver model"
)

dbt_test_sites_enriched = create_dbt_test_asset(
    test_name="sites_enriched",
    deps=["dbt_sites_enriched"],
    description="Run tests for sites_enriched silver model"
)

dbt_test_site_business_insights = create_dbt_test_asset(
    test_name="site_business_insights",
    deps=["dbt_site_business_insights"],
    description="Run tests for site_business_insights gold model"
)



@asset(
    deps=["dbt_test_site_business_insights"],  # Wait for the final model's tests to pass
    required_resource_keys={"dbt"}
)
def generate_dbt_docs(context: AssetExecutionContext):
    """Generate dbt documentation after all models and tests are complete."""
    context.log.info("Generating dbt documentation...")
    
    dbt = context.resources.dbt
    dbt.cli(["docs", "generate"])
    
    context.log.info("dbt documentation generated successfully!")
    return "dbt_docs_generated"

@asset(
    deps=["generate_dbt_docs"],
    required_resource_keys={"dbt", "s3"}
)
def export_business_insights_to_gold(context: AssetExecutionContext):
    """Export the business insights table to S3 gold layer using dbt CLI."""
    import subprocess
    import os
    import boto3
    import tempfile
    
    context.log.info("Exporting business insights to S3 gold layer...")
    
    # Get the dbt project directory
    dbt_project_dir = "/Users/estanislao/Educational/qualifyze-data-pipeline/qualifyze_dbt"
    
    try:
        # Create a temporary parquet file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_parquet_path = temp_file.name
        
        # Use dbt CLI to export the table using our macro
        context.log.info("Running dbt export command...")
        
        # Load environment variables from .env file
        env_vars = os.environ.copy()
        env_file_path = os.path.join(dbt_project_dir, '.env')
        
        if os.path.exists(env_file_path):
            with open(env_file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        env_vars[key] = value
        
        result = subprocess.run(
            ["dbt", "run-operation", "export_table_to_parquet", 
             "--args", f"{{'table_name': 'site_business_insights', 'output_path': '{temp_parquet_path}'}}"],
            capture_output=True,
            text=True,
            cwd=dbt_project_dir,
            env=env_vars
        )
        
        if result.returncode != 0:
            context.log.error(f"dbt export failed: {result.stderr}")
            raise Exception(f"Failed to export table: {result.stderr}")
        
        # Check if the parquet file was created
        if not os.path.exists(temp_parquet_path):
            raise FileNotFoundError(f"Parquet file was not created: {temp_parquet_path}")
        
        # Upload to S3
        s3_client = boto3.client('s3', region_name='eu-north-1')
        
        with open(temp_parquet_path, 'rb') as f:
            s3_client.put_object(
                Bucket="qualifyze-challenge",
                Key="gold/site_business_insights.parquet",
                Body=f.read()
            )
        
        # Clean up temporary file
        os.unlink(temp_parquet_path)
        
        context.log.info("Successfully exported to s3://qualifyze-challenge/gold/site_business_insights.parquet")
        
        return f"s3://qualifyze-challenge/gold/site_business_insights.parquet"
        
    except Exception as e:
        context.log.error(f"Error exporting to S3: {e}")
        raise

# List of all dbt assets
dbt_assets = [
    dbt_stg_sites,
    dbt_stg_sites_metadata,
    dbt_sites_unions,
    dbt_sites_enriched,
    dbt_site_business_insights,
    dbt_test_stg_sites,
    dbt_test_stg_sites_metadata,
    dbt_test_sites_unions,
    dbt_test_sites_enriched,
    dbt_test_site_business_insights,
    generate_dbt_docs,
    export_business_insights_to_gold
] 
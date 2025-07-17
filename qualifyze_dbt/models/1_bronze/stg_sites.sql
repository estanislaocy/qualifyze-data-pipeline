{{ config(materialized='view') }}

/*
BRONZE MODEL: stg_sites
PURPOSE: Basic data loading and minimal transformations from sites_data S3 parquet file
FEATURES:
- Simple data extraction from S3 sources
- Basic field renaming and type casting
- Minimal data cleaning
- No complex business logic or deduplication

NOTE: Using direct S3 paths because DuckDB doesn't automatically create external tables
from source definitions. The source references in stg_sources.yml are for documentation
and lineage tracking purposes.

*/

-- STEP 1: Load sites_data from S3 parquet file with basic transformations
select
    -- Field renaming for consistency across the pipeline
    id as site_id,
    name as site_name,
    address as site_address,
    upper(country_code) as country_code,
    
    -- Add source identifier for lineage tracking
    'sites_data' as source_table

-- STEP 2: Read from S3 parquet file
-- This loads the raw sites_data.parquet file from the silver layer
from read_parquet('s3://qualifyze-challenge/silver/sites_data.parquet')

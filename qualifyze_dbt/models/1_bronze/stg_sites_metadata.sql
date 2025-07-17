{{ config(materialized='view') }}

/*
BRONZE MODEL: stg_sites_metadata
PURPOSE: Basic data loading and minimal transformations from sites_metadata S3 parquet file
FEATURES:
- Simple data extraction from S3 sources
- Basic field renaming and type casting
- Data type conversion for numeric fields
- Source identification for lineage tracking
*/

-- STEP 1: Load sites_metadata from S3 parquet file with basic transformations
select
    -- Field renaming for consistency across the pipeline
    siteKey as site_id,              
    siteName as site_name,            
    siteAddress as site_address, 
    upper(countryCode) as country_code,
    
    -- Data type casting for numeric fields with error handling
    try_cast(revenue as double) as revenue,
    try_cast(employees_count as integer) as employees_count,
    
    -- Metadata fields
    extractedAt as extracted_at, 
    originUri as origin_uri,
    
    -- Add source identifier for lineage tracking
    'sites_metadata' as source_table

-- STEP 2: Read from S3 parquet file
-- This loads the raw sites_metadata.parquet file from the silver layer
from read_parquet('s3://qualifyze-challenge/silver/sites_metadata.parquet')

{{ config(materialized='table') }}

/*
SILVER MODEL: sites_enriched
PURPOSE: Heavy lifting layer - combines and enriches site data with business logic
FEATURES:
- HQ/child classification based on site names
- Simple deduplication: partition by site_id, keep newest by extracted_at
- Keep all records with different IDs (same name/address or different name/address)
- Complete dataset including sites with and without financial data
*/

-- STEP 1: Get all sites with HQ/child classification and simple deduplication
with sites_with_classification as (
    select
        -- Core site information
        site_id,
        site_name,
        site_address,
        country_code,
        
        -- STEP 1.1: Apply HQ/child classification based on site name pattern
        -- If site name contains 'HQ', classify as HQ, otherwise as child company
        case 
            when site_name like '%HQ%' then 'HQ'
            else 'child'
        end as site_type,
        
        -- Financial and metadata fields
        revenue,
        employees_count,
        extracted_at,
        origin_uri,
        source_table
    from {{ ref('sites_unions') }}
),

-- STEP 2: Apply simple deduplication logic
-- Partition by site_id and keep only the newest record by extracted_at timestamp
sites_deduplicated as (
    select
        -- All site fields
        site_id,
        site_name,
        site_address,
        country_code,
        site_type,
        revenue,
        employees_count,
        extracted_at,
        origin_uri,
        source_table,
        
        -- STEP 2.1: Create row numbers for deduplication
        -- Partition by site_id and order by extracted_at DESC to get newest first
        ROW_NUMBER() OVER (
            PARTITION BY site_id 
            ORDER BY extracted_at DESC
        ) as last_updated_record
    from sites_with_classification
)

-- STEP 3: Final output - keep all records with different IDs, deduplicate only by site_id
select
    site_id,
    site_name,
    site_address,
    country_code,
    site_type,
    revenue,
    employees_count,
    extracted_at,
    origin_uri,
    source_table as match_strategy 
from sites_deduplicated
where last_updated_record = 1  -- Keep only the newest record for each site_id
order by 
    site_type desc,  
    site_name        
{{ config(materialized='table') }}

/*
GOLD MODEL: site_business_insights
PURPOSE: Final business-ready output for end users, dashboards, and reporting
FEATURES:
- Complete organizational data including sites with and without financial data
- Integer formatting for revenue and employee_count (0 for missing data)
- Logical ordering for easy consumption
- Ready for BI tools and executive dashboards
*/

-- STEP 1: Transform data from silver layer into business-ready format
select
    -- STEP 1.1: Core site information (no transformations needed)
    site_id,
    site_name,
    site_address,
    country_code,
    site_type,
    
    -- STEP 1.2: Financial data formatting - convert to integers with 0 for missing data
    -- This makes the data more business-friendly and easier to aggregate
    coalesce(revenue, 0) as revenue,          
    coalesce(employees_count, 0) as employee_count,  
    
    -- STEP 1.3: Metadata fields (no transformations needed)
    extracted_at,
    origin_uri

-- STEP 2: Source the data from the silver layer
from {{ ref('sites_enriched') }}

-- STEP 3: Order results for business consumption
order by 
    site_type desc,  
    site_name        

{{ config(materialized='view') }}

/*
SILVER MODEL: sites_unions
PURPOSE: Combines sites_data and sites_metadata from bronze layer using dbt_utils.union_relations
FEATURES:
- Data combination using dbt_utils.union_relations
- Consistent schema across both sources
- Source tracking for lineage
- No business logic or transformations
*/

-- STEP 1: Create a CTE to combine both bronze models
with sites_data as (
    -- Use dbt_utils.union_relations to automatically combine both sources
    -- This function handles schema alignment and union operations
    select *
    from {{ dbt_utils.union_relations(relations=[ref('stg_sites'), ref('stg_sites_metadata')]) }}
)

-- STEP 2: Return the combined dataset
-- This creates a unified view of all site data from both sources
select * 
from sites_data

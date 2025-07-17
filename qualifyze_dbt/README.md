# Qualifyze Data Pipeline - dbt Models Documentation

## Overview

This dbt project transforms site data from S3 parquet files into a comprehensive business intelligence layer. The pipeline follows a modern data architecture with bronze, silver, and gold layers, handling sites data and metadata with intelligent matching, deduplication, and business logic to provide clean, actionable insights.

## Data Interpretation & Thought Process

### Initial Data Analysis
The project began with analyzing the provided CSV and JSON data sources. While the challenge PDF suggested the CSV contained EU sites and JSON contained US sites, our interpretation revealed a different relationship:

- **CSV Data**: Contains the main site information (site IDs, names, addresses, country codes)
- **JSON Data**: Contains metadata for the CSV sites, including:
  - Revenue information
  - Employee counts
  - Site URLs
  - Extraction timestamps

This interpretation suggests that the JSON files are supplementary metadata for the sites defined in the CSV, rather than separate geographical datasets.

### Business Logic Decisions

#### HQ and Child Company Classification
Based on the challenge PDF mentioning HQ companies that could have multiple child companies, we implemented the following logic:

- **HQ Sites**: Identified by searching for 'HQ' substring in the `site_name` field
- **Child Sites**: All other sites are considered child companies
- **Geographical Scope**: For this analysis, we consider child companies within the same country as their HQ, although child companies could exist in other countries

#### Deduplication Strategy
After analyzing the data patterns, we implemented a simple but effective deduplication strategy:

1. **Same ID**: Keep the newest record by `extracted_at` timestamp
2. **Different ID, Same Name, Same Address**: Keep both records (could be different business units)
3. **Different ID, Same Name, Different Address**: Keep both records (could be business relocation or different units)
4. **Different ID, Different Name, Same Address**: Keep both records (could be different companies at same location)

**Rationale**: Records with different IDs but same names and different addresses could represent:
- Different business units located in different places
- Companies that have moved locations (though we can't confirm this without `extracted_at` fields for some records)
- Separate legal entities with similar names

## Data Sources

### S3 Parquet Files
- **`sites_data.parquet`**: Main site information including site IDs, names, addresses, and country codes
- **`sites_metadata.parquet`**: Additional metadata like revenue, employee counts, and extraction timestamps

## Model Architecture

```
S3 Parquet Files → 1_bronze (dbt) → 2_silver (dbt) → 3_gold (dbt)
```

## Model Documentation

### 1. Bronze Models (1_bronze/)

#### `stg_sites` (View)
**Purpose**: Basic data loading and minimal transformations from sites_data S3 parquet file.

**Key Features**:
- **Simple Data Extraction**: Loads site information from sites_data source
- **Basic Field Renaming**: Standardizes field names (id → site_id, name → site_name, etc.)
- **Minimal Data Cleaning**: Uppercase country codes
- **Source Identification**: Adds `source_table` field for lineage tracking

**Output Columns**:
- `site_id`: Unique site identifier
- `site_name`: Site name
- `site_address`: Physical address
- `country_code`: Country code (uppercase)
- `source_table`: Source identifier ('sites_data')

#### `stg_sites_metadata` (View)
**Purpose**: Basic data loading and minimal transformations from sites_metadata S3 parquet file.

**Key Features**:
- **Simple Data Extraction**: Loads metadata information from sites_metadata source
- **Basic Field Renaming**: Standardizes field names (siteKey → site_id, siteName → site_name, etc.)
- **Type Casting**: Converts revenue to double and employees_count to integer
- **Source Identification**: Adds `source_table` field for lineage tracking

**Output Columns**:
- `site_id`: Unique site identifier
- `site_name`: Site name
- `site_address`: Physical address
- `country_code`: Country code (uppercase)
- `revenue`: Revenue amount
- `employees_count`: Employee count
- `extracted_at`: Extraction timestamp
- `origin_uri`: Source URI
- `source_table`: Source identifier ('sites_metadata')

### 2. Silver Models (2_silver/)

#### `sites_unions` (View)
**Purpose**: Combines sites_data and sites_metadata from bronze layer using dbt_utils.union_relations.

**Key Features**:
- **Data Combination**: Uses dbt_utils.union_relations to merge both sources
- **Consistent Schema**: Ensures both sources have the same field structure
- **Source Tracking**: Maintains `source_table` field for lineage

#### `sites_enriched` (Table)
**Purpose**: Heavy lifting layer with business logic and simple deduplication.

**Key Features**:
- **HQ/Child Classification**: Automatically classifies sites as 'HQ' (contains 'HQ' in name) or 'child'
- **Simple Deduplication**: Partition by site_id, keep newest by extracted_at
- **Complete Coverage**: Includes ALL records (with and without financial data) for organizational analysis

**Logic Flow**:
1. **Data Classification**: Apply HQ/child classification based on site name pattern
2. **Simple Deduplication**: Use `ROW_NUMBER()` to keep newest record per site_id
3. **Final Output**: Complete enriched dataset ready for gold layer

**Output Columns**:
- `site_id`: Unique site identifier
- `site_name`: Site name
- `site_address`: Physical address
- `country_code`: Country code
- `site_type`: 'HQ' or 'child'
- `revenue`: Revenue amount (null if not available)
- `employees_count`: Employee count (null if not available)
- `extracted_at`: Latest extraction timestamp
- `origin_uri`: Source URI
- `match_strategy`: Source table identifier ('sites_data' or 'sites_metadata')

### 3. Gold Models (3_gold/)

#### `site_business_insights` (Table)
**Purpose**: Final business-ready output for end users, dashboards, and reporting.

**Key Features**:
- **Complete Organizational Data**: Includes all sites (with and without financial data)
- **Integer Formatting**: Revenue and employee_count as integers (0 for missing data)
- **Business Ready**: Optimized for BI tools and executive dashboards

**Output Columns**:
- `site_id`: Unique site identifier
- `site_name`: Site name
- `site_address`: Physical address
- `country_code`: Country code
- `site_type`: 'HQ' or 'child'
- `revenue`: Revenue as integer (0 for missing data)
- `employee_count`: Employee count as integer (0 for missing data)
- `extracted_at`: Latest extraction timestamp
- `origin_uri`: Source URI

## Data Quality Rules

### Deduplication Strategy
1. **Same ID**: Keep the newest record by `extracted_at`
2. **Different ID, Same Name, Same Address**: Keep both records (different business units)
3. **Different ID, Same Name, Different Address**: Keep both records (relocation or different units)
4. **Different ID, Different Name, Same Address**: Keep both records (different companies at same location)

### Classification Rules
- **HQ Sites**: Site names containing 'HQ' substring
- **Child Sites**: Site names not containing 'HQ' substring

## Testing Strategy

### Model Tests
- **Not Null**: Critical fields like site_id, site_name, country_code


## Dependencies

### Model Dependencies
```
1_bronze/stg_sites → 2_silver/sites_unions → 2_silver/sites_enriched → 3_gold/site_business_insights
```

### External Dependencies
- **S3 Access**: Requires AWS credentials configured in profiles.yml
- **DuckDB**: Local database for development and testing
- **Dagster**: Orchestration framework for pipeline execution

## Configuration

### Environment Variables
This project requires S3 credentials to access the data sources. The credentials are stored in a `.env` file and loaded automatically.

**Required Environment Variables**:
- `S3_REGION`: AWS S3 region (e.g., eu-north-1)
- `S3_ACCESS_KEY_ID`: AWS access key ID
- `S3_SECRET_ACCESS_KEY`: AWS secret access key

**Running dbt with Environment Variables**:

Option 1: Use the provided script (recommended):
```bash
./run_dbt.sh debug
./run_dbt.sh run
./run_dbt.sh test
```

Option 2: Load environment variables manually:
```bash
export $(cat .env | grep -v '^#' | xargs) && dbt debug
```

### Materialization Strategy
- **1_bronze**: Views (data loading)
- **2_silver**: Views and Tables (performance for complex transformations)
- **3_gold**: Tables (final output, optimized for business use)

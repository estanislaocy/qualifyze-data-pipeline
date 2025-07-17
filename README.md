# Qualifyze Data Pipeline - Infrastructure & Deployment

Data pipeline infrastructure built with Dagster and dbt that processes site data from S3 parquet files through a bronze-silver-gold architecture.

## Quick Start

### Prerequisites
- Python 3.8+
- AWS CLI configured with S3 access

### Installation & Setup

```bash
# Clone the repository
git clone https://github.com/estanislaocy/qualifyze-data-pipeline
cd qualifyze-data-pipeline

# Install Dagster pipeline
cd dagster-pipeline
pip install -e .

# Install dbt dependencies
cd ../qualifyze_dbt
dbt deps

# Configure AWS credentials
aws configure
```

### Local Development

```bash
# Start Dagster UI
cd dagster-pipeline/src/dagster_pipeline
dagster dev

# Access Dagster UI at http://localhost:3000
```

### Querying the Data Warehouse

After running the pipeline, you can query the integrated data warehouse using DuckDB:

#### Install DuckDB

**macOS (using Homebrew)**:
```bash
brew install duckdb
```



**Python**:
```bash
pip install duckdb
```

#### Connect to the Data Warehouse

```bash
# Navigate to the DuckDB UI
duckdb --ui

# Paste the path of the warehouse into the connection in the UI
```


## Infrastructure Architecture

### Technology Stack
- **Orchestration**: Dagster (Python-based data orchestrator)
- **Transformation**: dbt (Data Build Tool)
- **Database**: DuckDB (local processing)
- **Storage**: AWS S3
- **Language**: Python, SQL

### System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   S3 Bronze     │    │   S3 Silver     │    │   S3 Gold       │
│   (Raw Data)    │───▶│   (Processed)   │───▶│   (Analytics)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Dagster        │    │  dbt Models     │    │  Business       │
│  Ingestion      │    │  (Bronze→Gold)  │    │  Intelligence   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow Pipeline

1. **Data Ingestion** (Dagster Assets)
   - Read from S3 bronze layer
   - Clean and standardize data
   - Write to S3 silver layer

2. **Data Transformation** (dbt Models)
   - Bronze: Basic data loading
   - Silver: Business logic and enrichment
   - Gold: Analytics-ready insights

3. **Quality Assurance** (dbt Tests)
   - Model validation
   - Documentation generation

4. **Data Export** (Dagster + dbt)
   - Export final results to S3 gold layer
   - Parquet format for analytics

## Project Structure

```
qualifyze-data-pipeline/
├── dagster-pipeline/           # Dagster orchestration
│   ├── src/dagster_pipeline/
│   │   ├── defs/
│   │   │   ├── ingestion.py    # Data ingestion assets
│   │   │   └── dbt_assets.py   # dbt integration assets
│   │   └── definitions.py      # Dagster definitions
│   ├── local_duckdb/           # Local database
│   │   └── warehouse.duckdb    # Persistent DuckDB file
│   └── pyproject.toml          # Python dependencies
├── qualifyze_dbt/              # dbt transformation layer
│   ├── models/                 # SQL models (bronze/silver/gold)
│   ├── macros/                 # Reusable SQL macros
│   ├── dbt_project.yml         # dbt configuration
│   └── profiles.yml            # Database connection
├── logs/                       # Pipeline execution logs
└── README.md                   # This file
```

## Configuration

**DuckDB Setup** (`qualifyze_dbt/profiles.yml`):
```yaml
# AWS Configuration (Use environment variables for production)
qualifyze_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ../dagster-pipeline/local_duckdb/warehouse.duckdb
      threads: 4
      s3_region: "{{ env_var('S3_REGION') }}"
      s3_access_key_id: "{{ env_var('S3_ACCESS_KEY_ID') }}"
      s3_secret_access_key: "{{ env_var('S3_SECRET_ACCESS_KEY') }}"
```

### dbt Configuration

**Project Settings** (`qualifyze_dbt/dbt_project.yml`):
```yaml
name: 'qualifyze_dbt'
version: '1.0.0'
config-version: 2

models:
  qualifyze_dbt:
    materialized: view
    staging:
      materialized: view
    silver:
      materialized: table
    gold:
      materialized: table
```

## Deployment Options

### Local Development
- **Use Case**: Development, testing, small datasets
- **Database**: Local DuckDB file
- **Storage**: S3 for input/output
- **Orchestration**: Dagster dev server


## Monitoring & Observability

### Dagster UI
- **URL**: http://localhost:3000 (local) or Dagster Cloud

### dbt Documentation
```bash
# Generate documentation
cd qualifyze_dbt
dbt docs generate
dbt docs serve
```

## Production

### Production Technology Stack

- **Data Ingestion**: Airbyte (EL platform)
- **Storage**: AWS S3 (bronze, silver, gold layers)
- **Data Warehouse**: Amazon Redshift (analytics database)
- **Transformation**: dbt Cloud (data transformation)
- **Orchestration**: Dagster Cloud (pipeline orchestration)
- **CI/CD**: GitHub Actions (automated deployment)
- **Security**: AWS IAM, VPC, KMS (enterprise security)

### Production Data Flow

#### 1. Data Ingestion (Source → S3)

**Source Systems**: Raw data from various sources (databases, APIs, files) is loaded directly into S3 buckets.

**S3 Storage Structure**:
- **Bronze Bucket**: Raw data from source systems
- **Silver Bucket**: Processed and cleaned data
- **Gold Bucket**: Final analytics-ready data

#### 2. Data Movement (S3 → Redshift)

**Airbyte EL Process**:
- **Extract**: Read data from S3 bronze bucket
- **Load**: Load data into Redshift data warehouse
- **Transform**: Apply deduplication and basic cleaning during load
- **Deduplication**: Use site_id and extracted_at to remove duplicates

**Benefits**:
- **Simple Setup**: Direct S3 to Redshift pipeline
- **Automatic Deduplication**: Built-in duplicate removal
- **Incremental Loading**: Only process new data
- **Error Handling**: Automatic retries and monitoring

#### 3. Data Transformation (Redshift)

**dbt Cloud**:
- **Scheduled Runs**: Automatically run transformations
- **Data Models**: Transform raw data into business-ready tables
- **Testing**: Validate data quality automatically
- **Documentation**: Generate data documentation

#### 4. Orchestration (Dagster Cloud)

**Pipeline Management**:
- **Scheduling**: Coordinate all pipeline steps
- **Monitoring**: Track pipeline execution and errors
- **Alerting**: Notify on failures or issues
- **Dependencies**: Manage data flow between steps

#### 5. Security & CI/CD

**Security**:
- **AWS IAM**: Control access to S3 and Redshift


**CI/CD (GitHub Actions)**:
- **Automated Testing**: Test changes before deployment
- **Deployment**: Deploy to production automatically


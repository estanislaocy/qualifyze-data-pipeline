import io
import json
import pandas as pd
from dagster import asset, MetadataValue

# S3 Configuration
S3_BUCKET = "qualifyze-challenge"
SITES_CSV_KEY = "bronze/sites_data.csv"
SITES_METADATA_PREFIX = "bronze/sites_metadata/"
SITES_DATA_PARQUET_KEY = "silver/sites_data.parquet"
SITES_METADATA_PARQUET_KEY = "silver/sites_metadata.parquet"

def upload_dataframe_to_s3(df: pd.DataFrame, s3_client, bucket: str, key: str, context) -> str:
    """Helper function to upload DataFrame as parquet to S3."""
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue()
    )
    
    context.log.info(f"Uploaded {len(df)} rows to s3://{bucket}/{key}")
    return f"s3://{bucket}/{key}"

def clean_sites_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean sites data."""
    return df.assign(
        id=lambda d: d["id"].str.strip(),
        name=lambda d: d["name"].str.strip(),
        address=lambda d: d["address"].str.strip(),
        country_code=lambda d: d["country_code"].str.upper()
    )

def clean_sites_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Clean sites metadata."""
    return df.assign(
        siteKey=lambda d: d["siteKey"].str.strip(),
        siteName=lambda d: d["siteName"].str.strip(),
        siteAddress=lambda d: d["siteAddress"].str.strip(),
        countryCode=lambda d: d["countryCode"].str.upper(),
        revenue=lambda d: pd.to_numeric(
            d["revenue"].str.replace(",", "").str.replace("$", ""), errors="coerce"
        ),
        employees_count=lambda d: pd.to_numeric(d["employees_count"], errors="coerce"),
    )

@asset(required_resource_keys={"s3"})
def sites_data_csv(context) -> pd.DataFrame:
    """Ingest sites data from bronze S3 layer."""
    s3 = context.resources.s3
    obj = s3.get_object(Bucket=S3_BUCKET, Key=SITES_CSV_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    
    context.add_output_metadata({
        "preview": MetadataValue.md(df.head().to_markdown()),
        "row_count": len(df)
    })
    return df

@asset(required_resource_keys={"s3"})
def sites_metadata_json(context) -> pd.DataFrame:
    """Ingest sites metadata from bronze S3 layer."""
    s3 = context.resources.s3
    paginator = s3.get_paginator("list_objects_v2")
    
    # Collect JSON files
    json_files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=SITES_METADATA_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                json_files.append(obj["Key"])
    
    # Process JSON files
    records = []
    for key in sorted(json_files):
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        raw = json.load(io.BytesIO(obj["Body"].read()))
        payload = raw.get("payload", {})
        
        record = {
            "extractedAt": raw.get("extractedAt"),
            "originUri": raw.get("originUri"),
            **payload,
            "revenue": payload.get("metadata", {}).get("revenue"),
            "employees_count": payload.get("metadata", {}).get("employees_count"),
        }
        records.append(record)
    
    df = pd.DataFrame(records)
    context.add_output_metadata({
        "file_count": len(json_files),
        "row_count": len(df),
        "preview": MetadataValue.md(df.head().to_markdown())
    })
    return df

@asset(deps=[sites_data_csv], required_resource_keys={"s3"})
def staged_sites_data(context, sites_data_csv: pd.DataFrame) -> str:
    """Clean and stage sites data to silver S3 layer."""
    cleaned = clean_sites_data(sites_data_csv)
    return upload_dataframe_to_s3(
        cleaned, context.resources.s3, S3_BUCKET, SITES_DATA_PARQUET_KEY, context
    )

@asset(deps=[sites_metadata_json], required_resource_keys={"s3"})
def staged_sites_metadata(context, sites_metadata_json: pd.DataFrame) -> str:
    """Clean and stage sites metadata to silver S3 layer."""
    cleaned = clean_sites_metadata(sites_metadata_json)
    return upload_dataframe_to_s3(
        cleaned, context.resources.s3, S3_BUCKET, SITES_METADATA_PARQUET_KEY, context
    )

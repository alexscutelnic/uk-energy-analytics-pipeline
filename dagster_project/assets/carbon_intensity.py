import os
from datetime import datetime, timezone

import requests
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

ARCHIVE_URL = "https://api.carbonintensity.org.uk/intensity/{start}/{end}"
TABLE_ID = "{project}.raw_dev.carbon_intensity"

daily_partitions = DailyPartitionsDefinition(start_date="2025-08-01")


@asset(
    partitions_def=daily_partitions,
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def carbon_intensity_raw(context: AssetExecutionContext) -> MaterializeResult:
    """One day of UK national carbon intensity readings, landed in BigQuery raw_dev."""

    # 1. Work out this partition's window
    partition_date = context.partition_key              # e.g. "2025-08-14"
    start = f"{partition_date}T00:00Z"
    end_dt = context.partition_time_window.end          # midnight the next day
    end = end_dt.strftime("%Y-%m-%dT%H:%MZ")

    context.log.info(f"Fetching carbon intensity for {partition_date}")

    # 2. Call the archive endpoint
    response = requests.get(ARCHIVE_URL.format(start=start, end=end), timeout=30)
    response.raise_for_status()
    records = response.json()["data"]

    # 3. Flatten, stamp with metadata, and keep only readings inside this partition
    ingested_at = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "from_time": r["from"],
            "to_time": r["to"],
            "intensity_forecast": r["intensity"].get("forecast"),
            "intensity_actual": r["intensity"].get("actual"),
            "intensity_index": r["intensity"].get("index"),
            "ingested_at": ingested_at,
            "source": "carbon_intensity_api_archive",
            "partition_date": partition_date,
        }
        for r in records
        if r["from"].startswith(partition_date)
    ]

    if not rows:
        raise ValueError(f"No readings returned for {partition_date}")

    # 4. Load into BigQuery
    project_id = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project_id)
    table_id = TABLE_ID.format(project=project_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("from_time", "TIMESTAMP"),
            bigquery.SchemaField("to_time", "TIMESTAMP"),
            bigquery.SchemaField("intensity_forecast", "INTEGER"),
            bigquery.SchemaField("intensity_actual", "INTEGER"),
            bigquery.SchemaField("intensity_index", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("partition_date", "DATE"),
        ],
        write_disposition="WRITE_APPEND",
    )

    load_job = client.load_table_from_json(rows, table_id, job_config=job_config)
    load_job.result()

    context.log.info(f"Loaded {len(rows)} rows for {partition_date}")

    return MaterializeResult(
        metadata={
            "partition": partition_date,
            "rows_loaded": len(rows),
            "table": table_id,
            "preview": MetadataValue.json(rows[0]),
        }
    )
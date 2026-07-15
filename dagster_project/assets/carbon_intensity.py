import os
from datetime import datetime, timezone

import requests
from dagster import asset, MaterializeResult, MetadataValue
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

API_URL = "https://api.carbonintensity.org.uk/intensity"
TABLE_ID = "{project}.raw_dev.carbon_intensity"


@asset
def carbon_intensity_raw() -> MaterializeResult:
    """Fetches current UK grid carbon intensity and lands it in BigQuery raw_dev."""

    # 1. Call the API
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()  # crash loudly on 4xx/5xx instead of silently continuing
    records = response.json()["data"]

    # 2. Flatten the nested JSON + add ingestion metadata
    ingested_at = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "from_time": r["from"],
            "to_time": r["to"],
            "intensity_forecast": r["intensity"]["forecast"],
            "intensity_actual": r["intensity"]["actual"],
            "intensity_index": r["intensity"]["index"],
            "ingested_at": ingested_at,
            "source": API_URL,
        }
        for r in records
    ]

    # 3. Load into BigQuery
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
        ],
        write_disposition="WRITE_APPEND",  # raw layer is append-only, never overwrite
    )

    load_job = client.load_table_from_json(rows, table_id, job_config=job_config)
    load_job.result()  # wait for completion; raises if the load failed

    return MaterializeResult(
        metadata={
            "rows_loaded": len(rows),
            "table": table_id,
            "preview": MetadataValue.json(rows[0]),
        }
    )
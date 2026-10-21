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

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
TABLE_ID = "{project}.raw_dev.weather"

HOURLY_VARIABLES = ["temperature_2m", "wind_speed_10m", "cloud_cover", "precipitation"]

CITIES = [
    {"city": "London", "latitude": 51.5072, "longitude": -0.1276},
    {"city": "Manchester", "latitude": 53.4808, "longitude": -2.2426},
    {"city": "Birmingham", "latitude": 52.4862, "longitude": -1.8904},
    {"city": "Glasgow", "latitude": 55.8642, "longitude": -4.2518},
    {"city": "Brighton", "latitude": 50.8225, "longitude": -0.1372},
]

daily_partitions = DailyPartitionsDefinition(start_date="2025-08-01")


@asset(
    partitions_def=daily_partitions,
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def weather_raw(context: AssetExecutionContext) -> MaterializeResult:
    """One day of hourly weather for five UK cities, landed in BigQuery raw_dev."""

    partition_date = context.partition_key
    ingested_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for city in CITIES:
        context.log.info(f"Fetching {city['city']} for {partition_date}")

        response = requests.get(
            ARCHIVE_URL,
            params={
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "start_date": partition_date,
                "end_date": partition_date,
                "hourly": ",".join(HOURLY_VARIABLES),
                "timezone": "UTC",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        hourly = payload["hourly"]

        # The API returns parallel arrays — zip them back into one row per hour.
        for i, ts in enumerate(hourly["time"]):
            rows.append(
                {
                    "observed_at": f"{ts}Z",  # API returns naive UTC; make it explicit
                    "city": city["city"],
                    "requested_latitude": city["latitude"],
                    "requested_longitude": city["longitude"],
                    "grid_latitude": payload.get("latitude"),
                    "grid_longitude": payload.get("longitude"),
                    "temperature_c": hourly["temperature_2m"][i],
                    "wind_speed_kmh": hourly["wind_speed_10m"][i],
                    "cloud_cover_pct": hourly["cloud_cover"][i],
                    "precipitation_mm": hourly["precipitation"][i],
                    "ingested_at": ingested_at,
                    "source": "open_meteo_archive",
                    "partition_date": partition_date,
                }
            )

    if not rows:
        raise ValueError(f"No weather readings returned for {partition_date}")

    project_id = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project_id)
    table_id = TABLE_ID.format(project=project_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("observed_at", "TIMESTAMP"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("requested_latitude", "FLOAT"),
            bigquery.SchemaField("requested_longitude", "FLOAT"),
            bigquery.SchemaField("grid_latitude", "FLOAT"),
            bigquery.SchemaField("grid_longitude", "FLOAT"),
            bigquery.SchemaField("temperature_c", "FLOAT"),
            bigquery.SchemaField("wind_speed_kmh", "FLOAT"),
            bigquery.SchemaField("cloud_cover_pct", "FLOAT"),
            bigquery.SchemaField("precipitation_mm", "FLOAT"),
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
            "cities": len(CITIES),
            "table": table_id,
            "preview": MetadataValue.json(rows[0]),
        }
    )
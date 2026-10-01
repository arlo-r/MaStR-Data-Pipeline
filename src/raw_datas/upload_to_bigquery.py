from pathlib import Path
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pyarrow.parquet as pq

PROJECT_ID = "energy-flex-analytics"
DATASET_ID = "mastr_raw"

client = bigquery.Client(project=PROJECT_ID)

dataset_ref = client.dataset(DATASET_ID)
dataset = bigquery.Dataset(dataset_ref)
dataset.location = "EU"
client.create_dataset(dataset, exists_ok=True)


def get_local_parquet_row_count(parquet_path: Path) -> int:
    """Reads metadata from local Parquet file to get total row count instantly."""
    metadata = pq.read_metadata(parquet_path)
    return metadata.num_rows


def get_bigquery_table_row_count(table_name: str) -> int | None:
    """Returns row count if table exists in BigQuery, otherwise None."""
    table_ref = dataset_ref.table(table_name)
    try:
        table = client.get_table(table_ref)
        return table.num_rows
    except NotFound:
        return None


def sync_parquet_to_bigquery(
    parquet_path: Path, table_name: str, max_retries: int = 3
):
    table_ref = dataset_ref.table(table_name)

    # 1. Get local and remote row counts
    local_rows = get_local_parquet_row_count(parquet_path)
    remote_rows = get_bigquery_table_row_count(table_name)

    # 2. Check if sync is needed
    if remote_rows is not None and remote_rows == local_rows:
        print(
            f"⏭️ Skipping {table_name}: Already up to date ({local_rows:,} rows)."
        )
        return

    if remote_rows is None:
        print(
            f"🆕 Table {table_name} does not exist. Creating and uploading {local_rows:,} rows..."
        )
    else:
        print(
            f"⚠️ Row count mismatch for {table_name} (Local: {local_rows:,} vs BQ: {remote_rows:,}). Overwriting..."
        )

    # 3. Upload with retry logic (WRITE_TRUNCATE automatically overwrites existing rows)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    for attempt in range(1, max_retries + 1):
        try:
            with open(parquet_path, "rb") as source_file:
                job = client.load_table_from_file(
                    source_file, table_ref, job_config=job_config
                )

            job.result()  # Wait for upload to complete
            table = client.get_table(table_ref)
            print(
                f"✅ Successfully synced {table.num_rows:,} rows into {table_name}.\n"
            )
            break
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for {table_name}: {e}")
            if attempt < max_retries:
                print("Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print(
                    f"❌ Failed to sync {table_name} after {max_retries} attempts.\n"
                )

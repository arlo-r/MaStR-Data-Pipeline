import argparse
from pathlib import Path
from raw_datas.MaStR_downloader import download_mastr
from raw_datas.MaStR_converter import convert_all_mastr_data
from raw_datas.upload_to_bigquery import sync_parquet_to_bigquery


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MaStR ELT Pipeline")
    parser.add_argument(
        "--url",
        type=str,
        required=False,
        help="Direct download URL for the MaStR ZIP file",
    )
    parser.add_argument(
        "--save-path",
        type=str,
        default="mastr_temp.zip",
        help="Path to save the temporary ZIP file",
    )
    parser.add_argument(
        "--extract-path",
        type=str,
        default="./mastr_data",
        help="Directory to extract and process data",
    )

    args = parser.parse_args()

    # If URL is not provided via command line, ask for it interactively
    url = args.url
    if not url:
        url = input("🔗 Please enter the MaStR download ZIP URL: ").strip()
        if not url:
            print("❌ Error: A valid URL is required to run the pipeline.")
            return

    print("🚀 Starting MaStR Data Pipeline...")

    # 1. Download & Extract
    download_mastr(url, args.save_path, args.extract_path)

    # 2. Convert XML to Parquet
    CONVERTED_DIR = Path(args.extract_path) / "converted"
    convert_all_mastr_data(args.extract_path, str(CONVERTED_DIR))

    # 3. Load to BigQuery
    for parquet_file in CONVERTED_DIR.glob("*.parquet"):
        sync_parquet_to_bigquery(parquet_file, parquet_file.stem)

    print("✨ Pipeline finished successfully!")
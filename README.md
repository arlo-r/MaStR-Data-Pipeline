# ⚡ MaStR Data Pipeline (ELT)

An efficient, streaming-based ELT pipeline to download, parse, convert, and load the German Market Master Data Register (**Marktstammdatenregister - MaStR**) XML files into **Google Cloud BigQuery**.

Designed with modern data engineering tools to handle massive XML datasets smoothly without exhausting local RAM.

---

## 🚀 Pipeline Workflow

1. **Download & Extract:** Automatically downloads the latest massive MaStR ZIP archive via streaming and extracts it locally.
2. **Stream Parsing & Parquet Conversion:** Uses `lxml` in streaming mode (`iterparse`) to process massive XML files chunk-by-chunk, converting them into highly compressed **Parquet** files via **Polars** (with schema alignment via diagonal concatenation).
3. **Smart BigQuery Sync:** Compares local Parquet row counts with existing BigQuery tables (`mastr_raw` dataset) to avoid redundant uploads, utilizing smart retry mechanisms and `WRITE_TRUNCATE`.

---

## 🛠️ Tech Stack

* **Python 3.14+**
* **uv:** For ultra-fast dependency management and project running.
* **Polars & PyArrow:** For lightning-fast dataframe operations and Parquet metadata handling.
* **lxml:** For memory-efficient, high-performance XML streaming.
* **Google Cloud BigQuery:** Cloud data warehouse.

---

## 📂 Project Structure

```text
raw-datas/
├── src/
│   └── raw_datas/
│       ├── __init__.py           # Main orchestration entrypoint
│       ├── MaStR_downloader.py   # Handles downloading and extraction
│       ├── MaStR_converter.py    # Stream parses XML and converts to Parquet
│       └── upload_to_bigquery.py # Smart syncs Parquet files to BigQuery
├── mastr_data/                   # Temporary storage for raw & converted data
├── pyproject.toml                # Project configuration & dependencies
└── README.md
```
## ⚙️ Prerequisites & Setup
1. **Google Cloud Authentication**:
Make sure you have authenticated your environment with Google Cloud and have permissions for BigQuery (energy-flex-analytics project):
` gcloud auth application-default login `

2. **Clone & Install Dependencies**:
This project uses [uv](https://github.com/astral-sh/uv) for fast package management.

```
git clone https://github.com/arlo-r/MaStR-Data-Pipeline
cd raw-datas
uv sync
```
## 🏃‍♂️ Usage
To run the entire ELT pipeline from scratch with a single command, use uv:

``` 
uv run raw-datas 
```
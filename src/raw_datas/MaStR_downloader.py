import os
import zipfile
import requests


def download_mastr(url, save_path, extract_to):
    print("Starting download...")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
    }

    with requests.get(url, headers=headers, stream=True) as response:
        response.raise_for_status()

        # Try to extract content length (case-insensitive fallback)
        total_length = response.headers.get("content-length")
        total_bytes = int(total_length) if total_length is not None else 0
        downloaded_bytes = 0

        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB
                if chunk:
                    f.write(chunk)
                    downloaded_bytes += len(chunk)
                    mb_downloaded = downloaded_bytes / (1024**2)

                    if total_bytes > 0:
                        percent = (downloaded_bytes / total_bytes) * 100
                        print(
                            f"\rProgress: {percent:.1f}% ({mb_downloaded:.1f} MB)",
                            end="",
                            flush=True,
                        )
                    else:
                        print(
                            f"\rDownloaded: {mb_downloaded:.1f} MB",
                            end="",
                            flush=True,
                        )

    print("\nDownload complete. Extracting files...")

    try:
        with zipfile.ZipFile(save_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
            print(f"Successfully extracted to: {extract_to}")

        os.remove(save_path)
        print("Temporary zip file removed.")

    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid zip archive.")

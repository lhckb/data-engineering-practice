import requests
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def download_and_extract_file(uri, download_folder, extract_folder):
    """Download and extract a single file from URI"""
    filename = uri.split("/")[-1]

    # Download
    try:
        response = requests.get(uri)
        response.raise_for_status()

        filepath = os.path.join(download_folder, filename)
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"File saved to {filepath}")
    except Exception as e:
        print(f"Error on GET for {filename}: {e}")
        return (filename, False, False)

    # Extract
    try:
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        print(f"Extracted {filename}")
        return (filename, True, True)
    except Exception as e:
        print(f"Error extracting {filename}: {e}")
        return (filename, True, False)


def main():

    download_folder = "downloads"
    extract_folder = "extracted"

    os.makedirs(download_folder, exist_ok=True)
    os.makedirs(extract_folder, exist_ok=True)

    success_filenames = []
    failure_downloads = []
    failure_extractions = []

    # Download and extract files in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(download_and_extract_file, uri, download_folder, extract_folder) for uri in download_uris]

        for future in as_completed(futures):
            filename, download_success, extract_success = future.result()
            if not download_success:
                failure_downloads.append(filename)
            elif not extract_success:
                failure_extractions.append(filename)
            else:
                success_filenames.append(filename)

    print(f"Successfully downloaded and extracted {len(success_filenames)} files")
    print(f"Failed downloading {len(failure_downloads)} files")
    print(f"Failed extracting {len(failure_extractions)} files")
    print(f"Total count of files: {len(download_uris)}")

if __name__ == "__main__":
    main()

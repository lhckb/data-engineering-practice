# IMPORTANT!
# Had to run this one outside of Docker due to
# AWS auth issues.
# Apparently it's now required to be signed in to AWS in your
# machine to access commoncrawl data.
# Was easier to just run locally than configure in Docker.

import boto3
import os
import gzip
import io


def main():
    s3_client = boto3.client('s3', region_name='us-east-1')

    bucket_name = "commoncrawl"
    s3_key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    # Load the file into memory no downloading
    print(f"Downloading {s3_key} into memory...")
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    gzipped_content = response['Body'].read()
    print(f"Downloaded {len(gzipped_content)} bytes")

    # Extract and read the gzip file in memory to get URIs
    with gzip.open(io.BytesIO(gzipped_content), 'rt') as f:
        lines = f.readlines()
        first_uri = lines[0].strip()
        print(f"\nFirst URI found: {first_uri}")

    # Just loading the file from the first URI into memory as well
    print(f"\nDownloading {first_uri} into memory...")
    second_response = s3_client.get_object(Bucket=bucket_name, Key=first_uri)
    second_gzipped_content = second_response['Body'].read()
    print(f"Downloaded {len(second_gzipped_content)} bytes")

    # Stream and print all lines from the downloaded file (without loading into memory)
    print(f"\nStreaming contents:")
    with gzip.open(io.BytesIO(second_gzipped_content), 'rt', encoding='utf-8', errors='ignore') as f:
        for line in f:
            print(line, end='')


if __name__ == "__main__":
    main()

import requests
import pandas
from bs4 import BeautifulSoup


def main():
    base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

    # Fetch the webpage
    response = requests.get(base_url)
    response.raise_for_status()

    # Parse HTML
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract CSV files with their last modified timestamps
    files_data = []
    rows = soup.find_all('tr')

    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 2:
            link = row.find('a')
            if link:
                href = link.get('href')
                if href and href.endswith('.csv'):
                    filename = href
                    last_modified = cells[1].text.strip()
                    files_data.append({
                        'filename': filename,
                        'last_modified': last_modified
                    })

    print(f"Found {len(files_data)} CSV files")

    # Find and download the file with timestamp "2024-01-19 10:27"
    # EDIT feb 6th 2026 no file with this timestamp exist anymore, will just fetch any other
    target_timestamp = "2024-01-19 15:27"
    target_file = None

    for file_info in files_data:
        if file_info['last_modified'] == target_timestamp:
            target_file = file_info['filename']
            break

    if target_file:
        print(f"\nDownloading file with timestamp {target_timestamp}: {target_file}")
        file_url = base_url + target_file

        file_response = requests.get(file_url)
        file_response.raise_for_status()

        with open(target_file, 'wb') as f:
            f.write(file_response.content)

        print(f"Successfully downloaded {target_file}")
    else:
        print(f"\nNo file found with timestamp {target_timestamp}")

    df = pandas.read_csv(target_file)
    df_sorted = df.sort_values(by="HourlyDryBulbTemperature", ascending=False)
    print(df_sorted)

if __name__ == "__main__":
    main()

import json
import csv
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"


def flatten_json(record):
    flat = {}
    for key, value in record.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, list):
                    for i, item in enumerate(sub_value):
                        flat[f"{key}_{sub_key}_{i}"] = item
                else:
                    flat[f"{key}_{sub_key}"] = sub_value
        else:
            flat[key] = value
    return flat


def main():
    json_files = list(DATA_DIR.rglob("*.json"))
    print(f"Found {len(json_files)} JSON files")

    for json_file in json_files:
        with open(json_file) as f:
            data = json.load(f)

        flat = flatten_json(data)

        csv_path = json_file.with_suffix(".csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=flat.keys())
            writer.writeheader()
            writer.writerow(flat)

        print(f"{json_file.name} -> {csv_path.name}")


if __name__ == "__main__":
    main()

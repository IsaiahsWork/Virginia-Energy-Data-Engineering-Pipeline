import json
import os
from pathlib import Path

import requests


# Configuration
API_URL = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"

OUTPUT_PATH = Path("data/raw/va_generation_2020_2025.json")

PAGE_SIZE = 5000


# Validate API key
api_key = os.environ.get("EIA_API_KEY")

if not api_key:
    raise RuntimeError("EIA_API_KEY environment variable is not set.")


# Base API parameters
base_params = {
    "frequency": "monthly",
    "data[0]": "generation",
    "facets[location][]": "VA",
    "start": "2020-01",
    "end": "2025-12",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "length": PAGE_SIZE,
    "api_key": api_key,
}


# Retrieve all records using pagination
all_data = []
offset = 0
total_records = None

print("Requesting Virginia electricity generation data...")

while True:
    params = base_params.copy()
    params["offset"] = offset

    print(f"Requesting records starting at offset {offset}...")

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()

    if "response" not in payload or "data" not in payload["response"]:
        raise RuntimeError("Unexpected EIA API response format.")

    page_data = payload["response"]["data"]

    if total_records is None:
        total_records = int(payload["response"]["total"])
        print(f"Total records reported by EIA: {total_records}")

    all_data.extend(page_data)

    print(f"Retrieved {len(all_data)} / {total_records}")

    if len(all_data) >= total_records or not page_data:
        break

    offset += PAGE_SIZE


# Make sure we received everything
if len(all_data) != total_records:
    raise RuntimeError(
        f"Incomplete download: received {len(all_data)} "
        f"of {total_records} records."
    )


# Preserve the complete raw response
final_payload = {
    "response": {
        "total": total_records,
        "dateFormat": payload["response"].get("dateFormat"),
        "frequency": payload["response"].get("frequency"),
        "description": payload["response"].get("description"),
        "data": all_data,
    }
}

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT_PATH.open("w", encoding="utf-8") as file:
    json.dump(final_payload, file, indent=2)

print()
print("SUCCESS")
print(f"Total records downloaded: {len(all_data)}")
print(f"Raw data saved to: {OUTPUT_PATH}")
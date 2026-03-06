"""
Find NHS Prescribing Data URLs from NHS BSA Open Data Portal.

Queries the API and saves CSV file URLs to prescription_urls.json.
Data source: https://opendata.nhsbsa.net/dataset/english-prescribing-data-epd
"""

import requests
import json
import re

API_URL = "https://opendata.nhsbsa.net/api/3/action/package_show"
DATASET_ID = "english-prescribing-data-epd"
OUTPUT_FILE = "prescription_urls.json"


def get_csv_urls():
    print("Finding NHS Prescribing Data URLs...")
    response = requests.get(API_URL, params={"id": DATASET_ID})
    data = response.json()
    resources = data["result"].get("resources", [])

    csv_files = []
    for r in resources:
        if r.get("format", "").lower() == "csv":
            name = r.get("name", "")
            match = re.match(r"EPD_(\d{6})", name)
            if match:
                period = match.group(1)
                csv_files.append({
                    "period": period,
                    "year": period[:4],
                    "month": period[4:6],
                    "name": name,
                    "url": r.get("url", ""),
                    "size_bytes": int(r.get("size", 0)),
                    "size_mb": int(r.get("size", 0)) / (1024 * 1024),
                })

    csv_files = sorted(csv_files, key=lambda x: x["period"], reverse=True)
    print("Found {} CSV files".format(len(csv_files)))
    if csv_files:
        print("Range: {} to {}".format(csv_files[-1]["period"], csv_files[0]["period"]))
        total_gb = sum(f["size_bytes"] for f in csv_files) / (1024**3)
        print("Total size: {:.2f} GB".format(total_gb))
    return csv_files


if __name__ == "__main__":
    urls = get_csv_urls()
    with open(OUTPUT_FILE, "w") as f:
        json.dump(urls, f, indent=2)
    print("Saved to {}".format(OUTPUT_FILE))
    for u in urls[:5]:
        print("  {}: {:.1f} MB".format(u["period"], u["size_mb"]))

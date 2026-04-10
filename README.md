# Critical Mineral Policy Scraper

A web scraper that extracts critical minerals policy data from [CritMin.org](https://critmin.org/policies/) by country and year, storing results in Google Cloud Storage.

## Overview

This scraper navigates the CritMin policy database, collecting structured data on mineral-related policies including titles, descriptions, and associated products.

## Output

Each policy record contains:

| Field | Description |
|---|---|
| `year` | Policy year |
| `country` | Economy/country code |
| `url` | Source URL |
| `title` | Policy title |
| `description` | Full policy description |
| `products` | Associated mineral products |

Records are saved as JSON files to GCS: `policies/{ECONOMY}_{YEAR}.json`

## Configuration

```python
BASE_URL   = "https://critmin.org"
POLICY_URL = "https://critmin.org/policies/"
ECONOMY    = "USA"              # Change to target country code
YEARS      = range(1970, 2024)  # Adjust year range as needed
```

## Requirements

**Mac/Linux**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

**Windows**
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

## Usage

```bash
python main.py
```

Ensure GCP credentials are configured before running.

## GCS Structure

```
gs://usa-mineral-policies-data/
└── policies/
    ├── USA_1970.json
    ├── USA_1971.json
    └── ...
```

As coverage expands, the bucket and path naming will follow the same `{ECONOMY}_{YEAR}.json` convention across all countries.

## Roadmap

- [ ] Multi-country support (iterate over all available economy codes)
- [ ] CLI arguments for `--country` and `--year-range`
- [ ] Resumable runs (skip already-uploaded blobs)
- [ ] Rate limiting and retry configuration via environment variables

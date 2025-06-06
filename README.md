# Netflix ETL Project

This project extracts, transforms, and loads Netflix title data from a CSV file into a PostgreSQL database. It supports automated scheduling, delta detection (only inserts new titles), and basic logging.

## 🛠 Features
- Load `netflix_titles.csv` into a PostgreSQL database
- Cleans missing and duplicate records
- Transforms duration fields for better analysis
- Supports incremental loads (only new rows are inserted)
- Includes a scheduler to run the ETL job automatically
- Logs activity to `etl_log.txt`

## 🚀 Setup

1. **Clone this repo** or copy the files to a directory:
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

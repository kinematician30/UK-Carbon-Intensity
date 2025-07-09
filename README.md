
# Carbon Intensity Data ETL Pipeline

This Python script automates the process of extracting, transforming, and loading UK regional carbon intensity data into a PostgreSQL database and optionally saving it to a CSV file.

## 📋 Overview

The pipeline performs the following steps:

1. **Extract**: Fetches carbon intensity forecast data for UK regions from the [UK Carbon Intensity API](https://api.carbonintensity.org.uk/).
2. **Transform**: Parses and normalizes the JSON response into a structured tabular format, extracting region-level generation mix and metadata.
3. **Load to PostgreSQL**: Inserts records into a PostgreSQL table named `carbon_intensity`.
4. **Export to CSV**: Saves the transformed data as a CSV file for backup or further analysis.
5. **Logging**: Logs processing steps and errors to `logs/script.log`.

---

## ⚙️ Requirements

* Python 3.x
* PostgreSQL server
* The following Python packages:

```bash
pip install pandas psycopg2 requests pyyaml
```

---

## 🗂️ Project Structure

```
.
├── main_script.py
├── conn.yaml
├── logs/
│   └── script.log
└── output/
    └── carbon_intensity_data.csv
```

---

## 🛠️ Configuration

Before running the script, create a `conn.yaml` file containing your PostgreSQL connection details:

```yaml
host: localhost
port: 5432
user: your_username
password: your_password
database: your_database
```

Make sure the target table exists in your database:

```sql
CREATE TABLE carbon_intensity (
    date DATE,
    "from" TIME,
    day_recorded TEXT,
    month_recorded TEXT,
    dnoregion TEXT,
    region_id INTEGER,
    intensity_forecast INTEGER,
    intensity_index TEXT,
    biomass FLOAT,
    coal FLOAT,
    imports FLOAT,
    gas FLOAT,
    nuclear FLOAT,
    other FLOAT,
    hydro FLOAT,
    solar FLOAT,
    wind FLOAT
);
```

---

## 🚀 How to Run

1. Activate your environment.
2. Run the script:

```bash
python main_script.py
```

---

## 🧩 Script Breakdown

### 1️⃣ Extraction

* **Function:** `extract_data()`
* Sends an HTTP GET request to the UK Carbon Intensity API.
* Retrieves 24-hour forecast data for each region.

### 2️⃣ Transformation

* **Function:** `transform_data()`
* Converts timestamps to human-readable date and time.
* Extracts generation mix percentages per fuel type.
* Produces a list of dictionaries ready for database insertion.

### 3️⃣ Database Loading

* **Functions:**

  * `connectDB()`: Reads database config and connects using `psycopg2`.
  * `load_data_db()`: Inserts records into the `carbon_intensity` table.

### 4️⃣ CSV Export

* **Function:** `load_data_csv()`
* Saves the transformed dataset to `carbon_intensity_data.csv`.

---

## 📝 Logging

Logs are saved to:

```
logs/script.log
```

Each run logs timestamps, info messages, and errors.

---

## 💡 Notes

* **Date Range**: The script fetches data starting from `2024-01-01`.
* **User-Agent**: A custom header is used for API requests.
* **Error Handling**: Minimal—consider extending exception handling for production.
* **Permissions**: Ensure the PostgreSQL user has INSERT privileges.

---

## ✨ Example Output

Sample CSV columns:

| date       | from  | day\_recorded | month\_recorded | dnoregion | region\_id | intensity\_forecast | ... |
| ---------- | ----- | ------------- | --------------- | --------- | ---------- | ------------------- | --- |
| 2024-01-01 | 00:00 | Monday        | January         | London    | 1          | 230                 | ... |

---

## 🙌 Author

Created by \[Your Name].
Feel free to modify or extend this script for your needs!


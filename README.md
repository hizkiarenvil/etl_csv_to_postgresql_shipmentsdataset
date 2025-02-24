Berikut adalah contoh file **README.md** untuk proyek ETL Anda (versi sebelumnya yang belum menggunakan Docker):

---

# ETL Pipeline: CSV to PostgreSQL

This project implements an ETL (Extract, Transform, Load) pipeline in Python to process shipment data from a CSV file and load it into a PostgreSQL database. The project demonstrates how to:

- **Extract:** Read data from a CSV file.
- **Transform:** Clean, validate, and transform the data.
- **Load:** Create a PostgreSQL database (if it doesn't exist), a schema, and a table, and then load the data into it.

## Project Structure

```
├── etl_script.py       # Main ETL script with functions for each ETL step
├── shipments.csv       # Input data file in CSV format
├── .env                # Environment variables for database connection (ignored in Git)
├── requirements.txt    # Python dependencies for the project
└── README.md           # This file
```

## Features

- **Database Setup:** Automatically creates the target database, schema (`warehouse`), and table (`shipments`) if they do not exist.
- **Data Validation:** Checks for required columns and valid values before processing.
- **Data Transformation:** Renames columns to follow PostgreSQL naming conventions, converts numeric columns to appropriate types, and fills missing values.
- **Data Loading:** Inserts data into PostgreSQL with conflict resolution using `ON CONFLICT` clause.
- **Logging:** Utilizes Python’s logging module to capture ETL process details and errors.

## Requirements

- Python 3.x
- PostgreSQL (ensure your PostgreSQL server is running)
- Required Python packages:
  - `pandas`
  - `psycopg2-binary`
  - `python-dotenv`

Install the required packages using:

```bash
pip install -r requirements.txt
```

## Setup Instructions

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yourusername/your-repo-name.git
   cd your-repo-name
   ```

2. **Set up a virtual environment:**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Environment Variables:**

   Create a file named `.env` in the root directory with the following content:

   ```env
   DB_NAME=train
   DB_USER=postgre
   DB_PASSWORD=postgre
   DB_HOST=localhost
   DB_PORT=5432
   ```

   **Note:** Ensure that your `.env` file is listed in your `.gitignore` so that sensitive information is not uploaded to GitHub.

## How to Run the Project

Simply run the ETL script:

```bash
python etl_script.py
```

The script will:
- Create the target database (if it does not exist).
- Create the required schema (`warehouse`) and table (`shipments`).
- Read the shipment data from `shipments.csv`.
- Validate and transform the data.
- Load the transformed data into PostgreSQL.
- Log the progress and errors in a log file (e.g., `etl_log_YYYYMMDD_HHMMSS.log`).

## Code Overview

Below is a brief explanation of key functions in the project:

### `get_connection(database='postgres')`
Establishes a connection to PostgreSQL using credentials from environment variables. When `database` is set to `'postgres'`, it connects to the default PostgreSQL database for creating new databases.

### `create_database()`
Connects to the default `postgres` database and checks if the target database (specified in `.env` as `DB_NAME`) exists. If not, it creates the database.

### `create_schema()`
Connects to the target database and creates a schema named `warehouse` if it doesn't already exist.

### `create_table()`
Creates the `shipments` table within the `warehouse` schema with the necessary columns, data types, and constraints.

### `validate_data(df)`
Validates that the input DataFrame contains all required columns and that specific columns have valid values.

### `transform_data(df)`
Transforms the raw data by:
- Renaming columns to follow PostgreSQL naming conventions.
- Converting numeric columns to proper numeric types.
- Filling missing values where necessary.

### `load_data(df)`
Loads the transformed data into the PostgreSQL table `warehouse.shipments` using a batch insert with conflict handling (updates existing records if the primary key already exists).

### `main()`
Orchestrates the entire ETL process by sequentially:
- Creating the database, schema, and table.
- Extracting data from `shipments.csv`.
- Transforming the data.
- Loading the data into PostgreSQL.
- Logging the process.

## Troubleshooting

- **Database Connection Errors:**  
  Ensure PostgreSQL is running and the connection parameters in the `.env` file are correct.
  
- **Missing or Invalid Data:**  
  The script validates required columns and value ranges. Check your CSV file if validation fails.
  
- **Log Files:**  
  Review the log files generated during the ETL process (e.g., `etl_log_YYYYMMDD_HHMMSS.log`) for detailed error messages.




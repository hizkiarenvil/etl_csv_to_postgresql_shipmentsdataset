import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'etl_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

# Database connection configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'train'),
    'user': os.getenv('DB_USER', 'postgre'), 
    'password': os.getenv('DB_PASSWORD', 'postgre'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def get_connection(database='postgres'):
    """Create database connection with error handling"""
    try:
        if database == 'postgres':
            conn = psycopg2.connect(
                dbname='postgres',
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                host=DB_CONFIG['host']
            )
        else:
            conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise

def create_database():
    """Create the database if it doesn't exist"""
    try:
        conn = get_connection()
        conn.autocommit = True
        cur = conn.cursor()
        
        # Check if database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_CONFIG['dbname'],))
        exists = cur.fetchone()
        
        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(DB_CONFIG['dbname'])
            ))
            logging.info(f"Database {DB_CONFIG['dbname']} created successfully")
        else:
            logging.info(f"Database {DB_CONFIG['dbname']} already exists")
            
    except Exception as e:
        logging.error(f"Error in create_database: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

def create_schema():
    """Create the schema if it doesn't exist"""
    try:
        conn = get_connection(DB_CONFIG['dbname'])
        cur = conn.cursor()
        
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS warehouse;
        """)
        conn.commit()
        logging.info("Schema 'warehouse' created successfully")
        
    except Exception as e:
        logging.error(f"Error in create_schema: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

def create_table():
    """Create the shipments table with appropriate data types"""
    try:
        conn = get_connection(DB_CONFIG['dbname'])
        cur = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS warehouse.shipments (
            id INTEGER PRIMARY KEY,
            warehouse_block VARCHAR(50),
            mode_of_shipment VARCHAR(50),
            customer_care_calls INTEGER,
            customer_rating INTEGER CHECK (customer_rating BETWEEN 1 AND 5),
            cost_of_the_product INTEGER,
            prior_purchases INTEGER,
            product_importance VARCHAR(50),
            gender VARCHAR(50),
            discount_offered INTEGER,
            weight_in_gms INTEGER,
            reached_on_time INTEGER CHECK (reached_on_time IN (0, 1)),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cur.execute(create_table_query)
        conn.commit()
        logging.info("Table 'shipments' created successfully")
        
    except Exception as e:
        logging.error(f"Error in create_table: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

def validate_data(df):
    """Validate data before transformation"""
    assert all(col in df.columns for col in [
        'ID', 'Warehouse_block', 'Mode_of_Shipment', 'Customer_care_calls',
        'Customer_rating', 'Cost_of_the_Product', 'Prior_purchases',
        'Product_importance', 'Gender', 'Discount_offered', 'Weight_in_gms',
        'Reached.on.Time_Y.N'
    ]), "Missing required columns"
    
    assert df['Customer_rating'].between(1, 5).all(), "Invalid customer ratings"
    assert df['Reached.on.Time_Y.N'].isin([0, 1]).all(), "Invalid reached on time values"
    
    return True

def transform_data(df):
    """Transform the data before loading into PostgreSQL"""
    try:
        # Validate data first
        validate_data(df)
        
        # Create a copy to avoid modifying the original dataframe
        df_transformed = df.copy()
        
        # Rename columns to match PostgreSQL naming conventions
        column_mapping = {
            'ID': 'id',
            'Warehouse_block': 'warehouse_block',
            'Mode_of_Shipment': 'mode_of_shipment',
            'Customer_care_calls': 'customer_care_calls',
            'Customer_rating': 'customer_rating',
            'Cost_of_the_Product': 'cost_of_the_product',
            'Prior_purchases': 'prior_purchases',
            'Product_importance': 'product_importance',
            'Gender': 'gender',
            'Discount_offered': 'discount_offered',
            'Weight_in_gms': 'weight_in_gms',
            'Reached.on.Time_Y.N': 'reached_on_time'
        }
        df_transformed.rename(columns=column_mapping, inplace=True)
        
        # Ensure numeric columns are of the correct type
        numeric_columns = [
            'customer_care_calls', 'customer_rating', 'cost_of_the_product',
            'prior_purchases', 'discount_offered', 'weight_in_gms', 
            'reached_on_time'
        ]
        
        for col in numeric_columns:
            df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
            
        # Fill any NaN values
        df_transformed = df_transformed.fillna({
            'customer_care_calls': 0,
            'prior_purchases': 0,
            'discount_offered': 0
        })
        
        logging.info("Data transformation completed successfully")
        return df_transformed
        
    except Exception as e:
        logging.error(f"Error in transform_data: {str(e)}")
        raise

def load_data(df):
    """Load the transformed data into PostgreSQL"""
    try:
        conn = get_connection(DB_CONFIG['dbname'])
        cur = conn.cursor()
        
        # Get column names
        columns = list(df.columns)
        
        # Prepare the INSERT query
        values_placeholder = ','.join(['%s'] * len(columns))
        
        insert_query = f"""
        INSERT INTO warehouse.shipments ({','.join(columns)})
        VALUES ({values_placeholder})
        ON CONFLICT (id) DO UPDATE
        SET {','.join([f"{col}=EXCLUDED.{col}" for col in columns if col != 'id'])};
        """
        
        # Convert DataFrame to list of tuples for insertion
        records = df.values.tolist()
        
        # Execute batch insert
        cur.executemany(insert_query, records)
        conn.commit()
        
        logging.info(f"Successfully loaded {len(records)} records into the database")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error in load_data: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

def main():
    """Main ETL process"""
    try:
        # Create database, schema, and table
        create_database()
        create_schema()
        create_table()
        
        # Extract: Read the CSV file
        df = pd.read_csv('shipments.csv')
        logging.info("Data extracted from CSV successfully")
        
        # Transform: Clean and prepare the data
        df_transformed = transform_data(df)
        
        # Load: Insert data into PostgreSQL
        load_data(df_transformed)
        
        logging.info("ETL process completed successfully")
        
    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
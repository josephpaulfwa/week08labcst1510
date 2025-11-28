import sqlite3
from pathlib import Path
import pandas as pd # <-- NEW: Required for CSV loading

DB_PATH = Path("DATA") / "intelligence_platform.db"

def connect_database(db_path=DB_PATH):
    """Connect to SQLite database."""
    return sqlite3.connect(str(db_path))

def load_csv_to_table(conn, csv_path, table_name):
    """
    Load a CSV file into a database table using pandas.
    
    Args:
        conn: Database connection
        csv_path: Path object to CSV file
        table_name: Name of the target table
        
    Returns:
        int: Number of rows loaded
    """
    if not csv_path.exists():
        print(f"⚠️ CSV File not found: {csv_path}")
        return 0

    try:
        df = pd.read_csv(csv_path)
        
        # Use pandas to_sql method for bulk insertion
        df.to_sql(
            name=table_name, 
            con=conn, 
            if_exists='append', # Add to existing data
            index=False         # Don't save the pandas index column
        )
        row_count = len(df)
        print(f"  ✅ Loaded {row_count} rows into {table_name}")
        return row_count
        
    except Exception as e:
        print(f"❌ Error loading {csv_path.name} into {table_name}: {e}")
        return 0
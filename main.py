import pandas as pd
import sqlite3
from pathlib import Path

# --- CORE IMPORTS ---
# NOTE: load_csv_to_table must be defined in app/data/db.py
from app.data.db import connect_database, load_csv_to_table 
from app.data.schema import create_all_tables
from app.services.user_service import register_user, login_user, migrate_users_from_file
from app.data.incidents import (
    insert_incident, get_all_incidents, 
    update_incident_status, delete_incident, 
    get_incidents_by_type_count, get_high_severity_by_status 
)

# --- GLOBAL CONSTANTS ---
DATA_DIR = Path("DATA")
INCIDENTS_PATH = DATA_DIR / "cyber_incidents.csv"
DATASETS_PATH = DATA_DIR / "datasets_metadata.csv"
TICKETS_PATH = DATA_DIR / "it_tickets.csv"
TABLES_TO_VERIFY = ['users', 'cyber_incidents', 'datasets_metadata', 'it_tickets']


def load_all_csv_data():
    """Wrapper function to load all domain CSV files (Part 6)."""
    conn = connect_database()
    total_rows = 0

    print("  Loading CSV Data...")
    
    # Calls the generic function for each of the three domain files
    total_rows += load_csv_to_table(conn, INCIDENTS_PATH, "cyber_incidents")
    total_rows += load_csv_to_table(conn, DATASETS_PATH, "datasets_metadata")
    total_rows += load_csv_to_table(conn, TICKETS_PATH, "it_tickets")

    conn.close()
    return total_rows

def verify_migration_and_count():
    """Queries the database to verify migrated users and returns row counts (Part 4.3)."""
    conn = connect_database()
    cursor = conn.cursor()
    
    # --- VERIFY MIGRATED USERS ---
    print("\n  🔎 Verifying User Migration...")
    cursor.execute("SELECT id, username, role FROM users")
    users = cursor.fetchall()

    print(f"  Total users found: {len(users)}")
    if users:
        print(f"{'ID':<5} {'Username':<15} {'Role':<10}")
        print("-" * 35)
        for user in users:
            print(f"{user[0]:<5} {user[1]:<15} {user[2]:<10}")
            
    # --- VERIFY TABLE ROW COUNTS ---
    print("\n  🔎 Database Table Counts:")
    print(f"{'Table':<25} {'Row Count':<15}")
    print("-" * 40)
    
    for table in TABLES_TO_VERIFY:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table:<25} {count:<15}")
        except sqlite3.OperationalError:
            print(f"{table:<25} {'[ERROR: Table Missing]':<15}")

    conn.close()


def main():
    print("=" * 60)
    print("🤖 Week 8: Multi-Domain Intelligence Platform Demo")
    print("=" * 60)
    
    # 1. SETUP DATABASE SCHEMA (Part 3)
    print("\n[1] Setting Up Database Schema...")
    conn = connect_database()
    create_all_tables(conn) # Creates all 4 tables
    conn.close()
    print("  ✅ Tables created successfully.")
    
    # 2. DATA PIPELINE: Load CSV Data (Part 6)
    print("\n" + "=" * 60)
    print("[2] Loading CSV Data into Domain Tables (Part 6)...")
    load_all_csv_data()
    
    # 3. MIGRATION: Migrate users from file and verify (Part 4)
    print("\n" + "=" * 60)
    print("[3] Running User Migration and Verification (Part 4)...")
    migrate_users_from_file() 
    verify_migration_and_count()
    
    # 4. TEST AUTHENTICATION (Part 5)
    print("\n" + "=" * 60)
    print("[4] Testing Authentication (Part 5)...")
    
    # Register (Create user entry)
    success, msg = register_user("demouser", "StrongPass123!", "analyst")
    print(f"  Register Status: {msg}")
    
    # Login (Read user entry and check hash)
    success, msg = login_user("demouser", "StrongPass123!")
    print(f"  Login Status:    {msg}")
    
    # 5. TEST CRUD (Part 7: Create, Update, Delete)
    print("\n" + "=" * 60)
    print("[5] Testing Incident CRUD (Part 7)...")
    
    # CREATE (Insert)
    incident_id = insert_incident(
        "2025-11-22", "DDoS Attack", "Critical", "Open", 
        "Large volume of traffic detected.", "demouser"
    )
    print(f"  ✅ Create: Incident #{incident_id} created.")
    
    # UPDATE
    rows_updated = update_incident_status(incident_id, "Resolved")
    print(f"  ✅ Update: Incident #{incident_id} status changed. Rows affected: {rows_updated}")
    
    # DELETE
    rows_deleted = delete_incident(incident_id)
    print(f"  ✅ Delete: Incident #{incident_id} removed. Rows affected: {rows_deleted}")
    
    # 6. TEST ANALYTICAL QUERIES (Part 8)
    print("\n" + "=" * 60)
    print("[6] Testing Analytical Queries (Part 8)...")
    
    print("  Incident Counts by Type:")
    df_type = get_incidents_by_type_count()
    print(df_type.to_markdown(index=False))

    print("\n  High Severity Status Counts:")
    df_high = get_high_severity_by_status()
    print(df_high.to_markdown(index=False))
    
    # 7. FINAL READ Test
    df_all = get_all_incidents() 
    print(f"\n  Total incidents remaining: {len(df_all)}")

    print("\n" + "=" * 60)
    print("✅ Full Demo Execution Complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
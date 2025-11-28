import pandas as pd
from app.data.db import connect_database

def insert_incident(date, incident_type, severity, status, description, reported_by=None):
    """Insert new incident."""
    conn = connect_database()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO cyber_incidents 
        (date, incident_type, severity, status, description, reported_by)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (date, incident_type, severity, status, description, reported_by))
    conn.commit()
    incident_id = cursor.lastrowid
    conn.close()
    return incident_id

def get_all_incidents():
    """Get all incidents as DataFrame."""
    conn = connect_database()
    df = pd.read_sql_query(
        "SELECT * FROM cyber_incidents ORDER BY id DESC",
        conn
    )
    conn.close()
    return df

# In app/data/incidents.py (Add these functions)

def update_incident_status(incident_id, new_status):
    """
    Update the status of an incident using its ID.
    (UPDATE operation)
    """
    conn = connect_database()
    cursor = conn.cursor()
    
    # Use parameterized query to set status where ID matches
    cursor.execute(
        "UPDATE cyber_incidents SET status = ? WHERE id = ?",
        (new_status, incident_id)
    )
    conn.commit()
    rows_affected = cursor.rowcount
    conn.close()
    return rows_affected # Returns 1 if successful, 0 otherwise

def delete_incident(incident_id):
    """
    Delete an incident from the database using its ID.
    (DELETE operation)
    """
    conn = connect_database()
    cursor = conn.cursor()
    
    # Use parameterized query to delete where ID matches
    cursor.execute(
        "DELETE FROM cyber_incidents WHERE id = ?",
        (incident_id,) # Note the comma is needed for a single-item tuple
    )
    conn.commit()
    rows_affected = cursor.rowcount
    conn.close()
    return rows_affected # Returns 1 if successful, 0 otherwise

# In app/data/incidents.py (Add these functions)
# Remember to import pandas if you haven't already: import pandas as pd

def get_incidents_by_type_count():
    """
    Count incidents grouped by their type.
    Uses: SELECT, FROM, GROUP BY, ORDER BY.
    """
    conn = connect_database()
    query = """
    SELECT incident_type, COUNT(*) as count
    FROM cyber_incidents
    GROUP BY incident_type
    ORDER BY count DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_high_severity_by_status():
    """
    Count high severity incidents grouped by their status.
    Uses: SELECT, FROM, WHERE, GROUP BY, ORDER BY.
    """
    conn = connect_database()
    query = """
    SELECT status, COUNT(*) as count
    FROM cyber_incidents
    WHERE severity = 'High'
    GROUP BY status
    ORDER BY count DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_incident_types_with_many_cases(min_count=5):
    """
    Find incident types with a count greater than min_count.
    Uses: SELECT, FROM, GROUP BY, HAVING, ORDER BY.
    """
    conn = connect_database()
    query = """
    SELECT incident_type, COUNT(*) as count
    FROM cyber_incidents
    GROUP BY incident_type
    HAVING COUNT(*) > ?
    ORDER BY count DESC
    """
    # CRITICAL: Pass the parameter (min_count) separately for security (parameterized query)
    df = pd.read_sql_query(query, conn, params=(min_count,))
    conn.close()
    return df
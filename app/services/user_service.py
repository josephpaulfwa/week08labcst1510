import bcrypt
import sqlite3
from pathlib import Path

# --- IMPORTANT: Define Path for Migration ---
# This assumes your DATA folder is in the project root relative to the main execution.
DATA_DIR = Path("DATA") 
USER_FILE_PATH = DATA_DIR / "users.txt"

# --- Imports from Data Layer (Requires correct project structure) ---
# NOTE: These imports rely on you having created the __init__.py files.
from app.data.db import connect_database
from app.data.users import get_user_by_username, insert_user
# from app.data.schema import create_users_table # Not strictly needed here, but often imported

def register_user(username, password, role='user'):
    """
    Registers a new user by hashing the password and inserting the user 
    record into the database.
    """
    # 1. Check if user already exists (using a data layer function)
    if get_user_by_username(username):
        return False, f"Username '{username}' already exists."
    
    # 2. Hash password with bcrypt
    try:
        password_bytes = password.encode('utf-8')
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password_bytes, salt)
        password_hash = hashed.decode('utf-8')
    except Exception as e:
        return False, f"Error hashing password: {e}"

    # 3. Insert into database (using a data layer function)
    insert_user(username, password_hash, role)
    
    return True, f"User '{username}' registered successfully."


def login_user(username, password):
    """
    Authenticates a user by retrieving the stored hash and verifying 
    the provided password using bcrypt.
    """
    # 1. Retrieve user record (using a data layer function)
    user = get_user_by_username(username)
    
    if not user:
        return False, "Username not found."
    
    # 2. Extract stored hash (user[2] is the password_hash column in the users table)
    stored_hash = user[2]
    
    # 3. Verify password
    password_bytes = password.encode('utf-8')
    
    try:
        # Checkpw handles the comparison and salt extraction securely
        if bcrypt.checkpw(password_bytes, stored_hash.encode('utf-8')):
            return True, f"Welcome, {username}! Role: {user[3]}" # user[3] is the role column
        else:
            return False, "Invalid password."
    except ValueError:
        return False, "Invalid hash format detected for user."


def migrate_users_from_file(filepath=USER_FILE_PATH):
    """
    Migrates users from the legacy users.txt file to the database. 
    Uses INSERT OR IGNORE to prevent adding duplicates.
    """
    if not filepath.exists():
        print(f"⚠️  File not found: {filepath}")
        print("   No users to migrate.")
        return 0
    
    # Establish connection internally for the utility function
    conn = connect_database() 
    cursor = conn.cursor()
    migrated_count = 0
    
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            # The format is: username,password_hash,role (role is optional)
            parts = line.split(',')
            
            if len(parts) >= 2:
                username = parts[0]
                password_hash = parts[1]
                # Default role to 'user' if not specified in the text file
                role = parts[2] if len(parts) >= 3 else 'user' 
                
                try:
                    # Use INSERT OR IGNORE and parameterized queries for safety
                    cursor.execute(
                        "INSERT OR IGNORE INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                        (username, password_hash, role)
                    )
                    if cursor.rowcount > 0:
                        migrated_count += 1
                except sqlite3.Error as e:
                    print(f"Error migrating user {username}: {e}")
    
    conn.commit()
    conn.close()
    print(f"✅ Migrated {migrated_count} users from {filepath.name}")
    return migrated_count
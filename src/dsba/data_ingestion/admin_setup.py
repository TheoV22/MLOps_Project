# admin_setup.py - Run this ONCE to set up the admin user
import os
import psycopg2
from dotenv import load_dotenv
import supabase

# Load environment variables
load_dotenv(".env")

# Admin credentials - this is your admin email
ADMIN_EMAIL = "tvaneccelpoel@neuf.fr"
ADMIN_PASSWORD = input("Enter admin password to set up admin role: ")

# Database connection
def get_db_connection():
    # Try primary connection
    try:
        host = os.getenv("PG_HOST", "localhost")
        port = int(os.getenv("PG_PORT", 5432))
        database = os.getenv("PG_DATABASE", "")
        user = os.getenv("PG_USER", "")
        password = os.getenv("PG_PASSWORD", "")
        
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        return psycopg2.connect(conn_string)
    except Exception as e:
        print(f"Primary connection failed: {e}")
        
        # Try pooler connection
        try:
            host_pooler = os.getenv("PG_HOST_POOLER", "localhost")
            user_pooler = os.getenv("PG_USER_POOLER", "")
            
            conn_string = f"postgresql://{user_pooler}:{password}@{host_pooler}:{port}/{database}"
            return psycopg2.connect(conn_string)
        except Exception as e2:
            print(f"Pooler connection failed: {e2}")
            return None

# Set up Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
supabase_client = supabase.create_client(supabase_url, supabase_key)

def main():
    print(f"Setting up admin role for {ADMIN_EMAIL}")
    
    # 1. Authenticate with Supabase to get user ID
    try:
        # Try to authenticate with existing account
        response = supabase_client.auth.sign_in_with_password({
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        
        if not response.user:
            print("Admin authentication failed. Creating new admin account...")
            # Create admin account if it doesn't exist
            response = supabase_client.auth.sign_up({
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            })
            
            if not response.user:
                print("Failed to create admin account")
                return False
        
        admin_id = response.user.id
        print(f"Admin authenticated with ID: {admin_id}")
        
        # 2. Connect to the database
        conn = get_db_connection()
        if not conn:
            print("Failed to connect to database")
            return False
        
        cursor = conn.cursor()
        
        # 3. Check if the roles table exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'roles')")
        if not cursor.fetchone()[0]:
            print("Creating roles table...")
            cursor.execute("CREATE TABLE roles (user_id TEXT PRIMARY KEY, role TEXT NOT NULL)")
        
        # 4. Check if admin exists in roles table
        cursor.execute("SELECT role FROM roles WHERE user_id = %s", [admin_id])
        result = cursor.fetchone()
        
        if result:
            # Admin exists, update role if needed
            if result[0] != "admin":
                cursor.execute("UPDATE roles SET role = 'admin' WHERE user_id = %s", [admin_id])
                print(f"Updated {ADMIN_EMAIL} role to admin")
            else:
                print(f"{ADMIN_EMAIL} already has admin role")
        else:
            # Admin doesn't exist in roles table, add them
            cursor.execute("INSERT INTO roles (user_id, role) VALUES (%s, 'admin')", [admin_id])
            print(f"Added {ADMIN_EMAIL} with admin role")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Admin setup completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error setting up admin: {e}")
        return False

if __name__ == "__main__":
    main()
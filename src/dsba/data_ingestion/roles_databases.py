
# =================================================================================================
#                       TEST : implement authorization access
# =================================================================================================
import os
import psycopg2
from dotenv import load_dotenv
from dataclasses import dataclass
import supabase
import psycopg2
from psycopg2 import sql
import pandas as pd

# Load environment variables from .env file
load_dotenv(".env")

# Initialize Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
supabase_client = supabase.create_client(supabase_url, supabase_key)

@dataclass
class PostgresConfig:
    host: str = os.getenv("PG_HOST", "localhost")
    host_pooler: str = os.getenv("PG_HOST_POOLER", "localhost")
    port: int = int(os.getenv("PG_PORT", 5432)) 
    database: str = os.getenv("PG_DATABASE", "") 
    user: str = os.getenv("PG_USER", "") 
    user_pooler: str = os.getenv("PG_USER_POOLER", "")
    password: str = os.getenv("PG_PASSWORD", "")
    schema: str | None = None

    def get_connection(self, use_pooler=False):
        """Helper function to get a connection to the PostgreSQL database."""
        connection_string = (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        
        # If pooler is enabled, use the pooler connection string
        if use_pooler:
            connection_string = (
                f"postgresql://{self.user_pooler}:{self.password}@"
                f"{self.host_pooler}:{self.port}/{self.database}"
            )
        
        try:
            conn = psycopg2.connect(connection_string)
            return conn
        except Exception as e:
            return None


def get_user_id_from_supabase(email: str, password: str):
    """Authenticate user with Supabase and fetch user_id."""
    # Authenticate with Supabase
    response = supabase_client.auth.sign_in_with_password({
        "email": email,
        "password": password
    })
    
    if response.user:
        return response.user.id  # User ID from Supabase
    return None


def get_user_role(config: PostgresConfig, user_id: str):
    """Fetch user role from the 'roles' table using user_id."""
    conn = None
    try:
        # Connect to PostgreSQL using pooler as backup
        conn = config.get_connection(use_pooler=False)

        if not conn:
            # If the first connection attempt fails, use pooler
            conn = config.get_connection(use_pooler=True)

        if not conn:
            return "Connection failed"  # Return error if connection fails

        cursor = conn.cursor()

        # Use parameterized queries to avoid SQL injection
        query = sql.SQL("SELECT role FROM roles WHERE user_id = %s;")
        cursor.execute(query, [user_id])

        result = cursor.fetchone()

        if result:
            return result[0]  # Role from the 'roles' table
        return "unauthorized"  # Default role if not found

    except Exception as e:
        print(f"Error fetching user role: {e}")
        return "Query execution failed"

    finally:
        if conn:
            cursor.close()
            conn.close()

def register_new_user(email: str, password: str, default_role: str = "read_access"):
    """Register a new user in Supabase and assign them a default role."""
    try:
        # Register user in Supabase
        signup_response = supabase_client.auth.sign_up({
            "email": email,
            "password": password
        })
        
        if signup_response.user:
            user_id = signup_response.user.id
            
            # Create PostgreSQL config
            config = PostgresConfig()
            conn = config.get_connection()
            if not conn:
                conn = config.get_connection(use_pooler=True)
            if not conn:
                return None, "Failed to connect to database"
                
            try:
                cursor = conn.cursor()
                
                # Use simple INSERT without ON CONFLICT clause
                roles_query = sql.SQL("""
                    INSERT INTO roles (user_id, role) 
                    VALUES (%s, %s);
                """)
                
                cursor.execute(roles_query, [user_id, default_role])
                conn.commit()
                
                return user_id, f"New user registered with {default_role} role. Please check your email to confirm your account."
            except Exception as db_error:
                print(f"Error updating user role: {db_error}")
                return user_id, f"User registered in Supabase, but database update failed: {str(db_error)}"
            finally:
                if conn:
                    cursor.close()
                    conn.close()
        else:
            return None, "Registration failed: No user returned"
    except Exception as e:
        return None, f"Registration failed: {str(e)}"
    
def add_or_update_user(config: PostgresConfig, user_id: str, role: str, requester_role: str = None):
    """Add a new user or update an existing user's role in the roles table."""
    # Check permissions if requester_role is provided
    if requester_role is not None and requester_role != "read_write_access":
        return "Access denied: Only users with read_write_access role can update roles"
    
    # Validate role input
    if role not in ["read_access", "read_write_access", "unauthorized"]:
        return "Invalid role. Allowed values are: read_access, read_write_access, unauthorized"
    
    conn = None
    try:
        conn = config.get_connection(use_pooler=False)
        if not conn:
            conn = config.get_connection(use_pooler=True)
        if not conn:
            return "Connection failed"

        cursor = conn.cursor()
        
        # Check if the user already exists
        check_query = sql.SQL("SELECT user_id FROM roles WHERE user_id = %s")
        cursor.execute(check_query, [user_id])
        user_exists = cursor.fetchone() is not None
        
        if user_exists:
            # Update existing user
            update_query = sql.SQL("UPDATE roles SET role = %s WHERE user_id = %s")
            cursor.execute(update_query, [role, user_id])
        else:
            # Insert new user
            insert_query = sql.SQL("INSERT INTO roles (user_id, role) VALUES (%s, %s)")
            cursor.execute(insert_query, [user_id, role])
        
        conn.commit()
        return f"User {user_id} assigned role: {role}"
        
    except Exception as e:
        print(f"Error updating user role: {e}")
        return f"Failed to update user: {str(e)}"
    finally:
        if conn:
            cursor.close()
            conn.close()

def perform_query(config: PostgresConfig, query: str, user_role: str):
    """Perform a query on the database based on user role and return the result as a pandas DataFrame."""
    
    # Handle unauthorized users
    if user_role == "unauthorized":
        return "Access denied: Unauthorized role"
    
    # Handle read-only access: deny any write operations (INSERT, UPDATE, DELETE)
    if user_role == "read_access" and ('INSERT' in query or 'UPDATE' in query or 'DELETE' in query):
        return "Access denied: Read-only role cannot modify data"
    
    conn = None
    try:
        # Connect to PostgreSQL using pooler as backup
        conn = config.get_connection(use_pooler=False)

        if not conn:
            # If the first connection attempt fails, use pooler
            conn = config.get_connection(use_pooler=True)

        if not conn:
            return "Connection failed"  # Return error if connection fails

        cursor = conn.cursor()

        # Check if the query is a SELECT query
        if 'SELECT' in query:
            # Execute the SELECT query
            cursor.execute(query)

            # Fetch the result
            result = cursor.fetchall()

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Convert the result into a pandas DataFrame
            df = pd.DataFrame(result, columns=columns)

            return df  # Return the DataFrame

        else:
            # For non-SELECT queries (INSERT, UPDATE, DELETE), execute without fetching
            cursor.execute(query)
            conn.commit()  # Commit the transaction for changes

            return "Query executed successfully"  # Return success message for write operations

    except Exception as e:
        print(f"Error performing query: {e}")
        return "Query execution failed"

    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    config = PostgresConfig()

    print("\n----- Database Access System -----\n")
    action = input("Do you want to (1) Login or (2) Register? Enter 1 or 2: ")
    
    if action == "2":
        print("\n----- User Registration -----")
        email = input("Email: ")
        password = input("Password: ")
        
        user_uuid, message = register_new_user(email, password)
        
        print(f"\n{message}")
        if not user_uuid:
            exit()
            
        print("\nPlease check your email to confirm your account before logging in.")
        exit()
    else:
        print("\n----- User Login -----")
        email = input("Email: ")
        password = input("Password: ")
        
        user_uuid = get_user_id_from_supabase(email, password)

        if user_uuid:
            print(f"\nUser authenticated! User ID: {user_uuid}")

            role = get_user_role(config, user_uuid)
            print(f"User role: {role}")
        else:
            print("Authentication failed!")
            exit()

    # Continue with actions based on role
    while True:
        print("\n----- Available Actions -----")
        print("1: Query data (read_access and read_write_access)")
        print("2: Insert data (read_write_access only)")
        
        # Only show admin options for read_write_access role
        if role == "read_write_access":
            print("3: Add/Update user role (read_write_access only)")
            print("4: List all user roles (read_write_access only)")
        
        print("0: Exit")
        
        choice = input("\nSelect an action (0-4): ")
        
        if choice == "0":
            print("Exiting. Goodbye!")
            break
            
        elif choice == "1":
            print("\n--- Query Data ---")
            if role not in ["read_access", "read_write_access"]:
                print("Access denied: Your role doesn't have read permissions")
                continue
                
            query = "SELECT * FROM classifier_data LIMIT 10;"
            print(f"Executing: {query}")
            result = perform_query(config, query, role)
            if isinstance(result, pd.DataFrame):
                print(f"Query results:\n{result}")
            else:
                print(f"Result: {result}")
                
        elif choice == "2":
            print("\n--- Insert Data ---")
            if role != "read_write_access":
                print("Access denied: Your role doesn't have write permissions")
                continue
                
            try:
                is_deficient = input("Is deficient (True/False): ").lower() == "true"
                usage = int(input("Usage per month: "))
                year = int(input("Construction year: "))
                
                query = f"INSERT INTO classifier_data (is_deficient, usage_per_month, construction_year) VALUES ({is_deficient}, {usage}, {year});"
                print(f"Executing: {query}")
                result = perform_query(config, query, role)
                print(f"Result: {result}")
            except ValueError:
                print("Invalid input. Please enter numeric values where required.")
                
        elif choice == "3" and role == "read_write_access":
            print("\n--- Add/Update User Role ---")
            target_user_id = input("Enter user ID: ")
            new_role = input("Enter role (read_access, read_write_access, unauthorized): ")
            
            # Validation happens inside the function
            result = add_or_update_user(config, target_user_id, new_role, requester_role=role)
            print(f"Result: {result}")
            
        elif choice == "4" and role == "read_write_access":
            print("\n--- List All User Roles ---")
            query = "SELECT user_id, role FROM roles;"
            result = perform_query(config, query, role)
            if isinstance(result, pd.DataFrame):
                print(f"Users and roles:\n{result}")
            else:
                print(f"Result: {result}")
                
        else:
            print("Invalid choice or insufficient permissions. Please try again.")

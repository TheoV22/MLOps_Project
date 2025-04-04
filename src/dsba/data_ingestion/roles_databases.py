# =================================================================================================
#                       TEST : implement authorization access
# =================================================================================================
import os
import psycopg2
from dotenv import load_dotenv
from dataclasses import dataclass
import supabase
from psycopg2 import sql
import pandas as pd

# Load environment variables from .env file
load_dotenv(".env")

# Initialize Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
supabase_client = supabase.create_client(supabase_url, supabase_key)

# Admin credentials
ADMIN_EMAIL = "tvaneccelpoel@neuf.fr"

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
            print(f"Error connecting to PostgreSQL: {e}")
            return None


def get_user_id_from_supabase(email: str, password: str):
    """Authenticate user with Supabase and fetch user_id."""
    try:
        # Authenticate user in Supabase
        response = supabase_client.auth.sign_in_with_password({
            "email": email,
            "password": password
        })
        
        if response.user:
            return response.user.id  # User ID from Supabase
        return None
    except Exception as e:
        print(f"Error authenticating user: {e}")
        return None


def get_user_role(config: PostgresConfig, user_id: str):
    """Fetch user role from the 'roles' table using user_id."""
    conn = None
    try:
        # Try connection with various fallbacks
        conn = config.get_connection(use_pooler=False)
        if not conn:
            conn = config.get_connection(use_pooler=True)
        if not conn:
            # If we can't connect at all but this is the admin, return admin role
            if user_id == "2c81d64b-7019-4bdb-8e23-ad40c223dfe7":  # Your admin ID
                print("Connection failed but recognized admin ID - granting admin privileges")
                return "admin"
            return "Connection failed"

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


def register_new_user(email: str, password: str):
    """Register a new user in Supabase and assign them 'unauthorized' by default."""
    try:
        # Register user in Supabase
        signup_response = supabase_client.auth.sign_up({
            "email": email,
            "password": password
        })
        
        if signup_response.user:
            user_id = signup_response.user.id

            # Set default role (always unauthorized for new users)
            default_role = "unauthorized"
            
            # Create PostgreSQL config and connect to database
            config = PostgresConfig()
            conn = config.get_connection() or config.get_connection(use_pooler=True)
            if not conn:
                return None, "Failed to connect to database"
                
            try:
                cursor = conn.cursor()
                
                # Use simple INSERT query to add the user to the roles table
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
    

def add_or_update_user(config: PostgresConfig, user_id: str, role: str, requester_id: str):
    """Add a new user or update an existing user's role in the roles table. Only admin can do this."""
    # Double-check requester's role for maximum security
    requester_role = get_user_role(config, requester_id)
    
    # Strict check: Only users with "admin" role can update roles
    if requester_role != "admin":
        return "Access denied: Only admin can update roles" 
       
    # Validate role input
    valid_roles = ["read_access", "read_write_access", "unauthorized", "admin"]
    if role not in valid_roles:
        return f"Invalid role. Allowed values are: {', '.join(valid_roles)}"
    
    conn = None
    try:
        conn = config.get_connection() or config.get_connection(use_pooler=True)
        if not conn:
            return "Connection failed"

        cursor = conn.cursor()
        
        # Check if the target user already exists
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
    
    # Handle read-only access: deny any write operations
    if user_role == "read_access" and any(op in query.upper() for op in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER']):
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
        if query.upper().startswith('SELECT'):
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
        return f"Query execution failed: {str(e)}"

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

            # Get the role from the database
            role = get_user_role(config, user_uuid)
            
            # Display role information
            print(f"User role: {role}")
            if role == "admin":
                print("You have administrator privileges")
            elif role == "read_write_access":
                print("You have read and write privileges (but not admin)")
            elif role == "read_access":
                print("You have read-only privileges")
            else:
                print("You have no access privileges")
        else:
            print("Authentication failed!")
            exit()

    # Continue with actions based on role
    while True:
        print("\n----- Available Actions -----")
        print("1: Query data (read_access, read_write_access, admin)")
        print("2: Insert data (read_write_access, admin)")
            
        # Only show admin options for admin role specifically
        if role == "admin":
            print("3: Add/Update user role (admin only)")
            print("4: List all user roles (admin only)")
        
        print("0: Exit")
        
        choice = input("\nSelect an action (0-4): ")
        
        if choice == "0":
            print("Exiting. Goodbye!")
            break
            
        elif choice == "1":
            print("\n--- Query Data ---")
            if role not in ["read_access", "read_write_access", "admin"]:
                print("Access denied: Your role doesn't have read permissions")
                continue
                
            query = input("Enter your SELECT query: ")
            if not query.upper().startswith('SELECT'):
                print("Only SELECT queries are allowed with this option")
                continue
                
            print(f"Executing: {query}")
            result = perform_query(config, query, role)
            if isinstance(result, pd.DataFrame):
                print(f"Query results:\n{result}")
            else:
                print(f"Result: {result}")
                
        elif choice == "2":
            print("\n--- Insert Data ---")
            if role not in ["read_write_access", "admin"]:
                print("Access denied: Your role doesn't have write permissions")
                continue
                
            try:
                query = input("Enter your INSERT/UPDATE query: ")
                if not (query.upper().startswith('INSERT') or query.upper().startswith('UPDATE')):
                    print("Only INSERT or UPDATE queries are allowed with this option")
                    continue
                
                print(f"Executing: {query}")
                result = perform_query(config, query, role)
                print(f"Result: {result}")
            except ValueError:
                print("Invalid input. Please enter valid data.")
                
        elif choice == "3":
            # Additional security check to prevent non-admin access
            if role != "admin":
                print("Access denied: Only admin can update user roles")
                continue
                
            print("\n--- Add/Update User Role ---")
            target_user_id = input("Enter user ID: ")
            new_role = input("Enter role (read_access, read_write_access, unauthorized, admin): ")
            
            # Double validation happens inside the function
            result = add_or_update_user(config, target_user_id, new_role, user_uuid)
            print(f"Result: {result}")
            
        elif choice == "4":
            # Additional security check to prevent non-admin access
            if role != "admin":
                print("Access denied: Only admin can list all user roles")
                continue
                
            print("\n--- List All User Roles ---")
            query = "SELECT user_id, role FROM roles;"
            result = perform_query(config, query, role)
            if isinstance(result, pd.DataFrame):
                print(f"Users and roles:\n{result}")
            else:
                print(f"Result: {result}")
                
        else:
            print("Invalid choice or insufficient permissions. Please try again.")
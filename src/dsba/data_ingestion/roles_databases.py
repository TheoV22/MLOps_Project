
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


import pandas as pd
from psycopg2 import sql

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
    config = PostgresConfig()  # Initialize PostgresConfig with environment variables

    # Input: Email and Password (these would typically come from user input)
    # read_write access:
    #email = "tvaneccelpoel@neuf.fr"
    #password = "MLOps_1"
    # read only access:
    #email = "b00810891@essec.edu"
    #password = "MLOps_2"
    # unauthorized access:
    email = "theo.van-eccelpoel@student-cs.fr"
    password = "MLOps_3"

    # Step 1: Authenticate user and get UUID
    user_uuid = get_user_id_from_supabase(email, password)

    if user_uuid:
        print(f"User authenticated! User ID: {user_uuid}")

        # Step 2: Get role using the UUID
        role = get_user_role(config, user_uuid)
        print(f"User role: {role}")

        # Step 3: Perform a query based on role
        query = "SELECT * FROM classifier_data;" # read query
        # query = "INSERT INTO classifier_data (is_deficient, usage_per_month, construction_year) VALUES (True, 120, 2018);" # write query

        result_df = perform_query(config, query, role)
        if isinstance(result_df, pd.DataFrame):
            print(f"Query result (as DataFrame):\n{result_df}")
        else:
            print(f"Query failed: {result_df}")
    else:
        print("Authentication failed! Invalid email or password.")

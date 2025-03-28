# =================================================================================================
#                       See 'roles_databases.py' for improved version
# =================================================================================================

import os
from dataclasses import dataclass
from typing import Any
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(".env")

@dataclass
class PostgresConfig:
    host: str = os.getenv("PG_HOST", "localhost")
    host_pooler : str = os.getenv("PG_HOST_POOLER", "localhost")
    port: int = int(os.getenv("PG_PORT", 5432)) 
    database: str = os.getenv("PG_DATABASE", "") 
    user: str = os.getenv("PG_USER", "") 
    user_pooler: str = os.getenv("PG_USER_POOLER", "")
    password: str = os.getenv("PG_PASSWORD", "")
    schema: str | None = None


def query_postgres(config: PostgresConfig, query: str, **pandas_kwargs: Any) -> pd.DataFrame:
    connection_string = (
        f"postgresql://{config.user}:{config.password}@"
        f"{config.host}:{config.port}/{config.database}"
    )
    
    connection_string_pooler  = (
        f"postgresql://{config.user_pooler}:{config.password}@"
        f"{config.host_pooler}:{config.port}/{config.database}"
    )    

    try:
        # Attempt to connect to the PostgreSQL database using the provided connection string
        print('Connecting to PostgreSQL database...')
        engine = sqlalchemy.create_engine(connection_string)
        with engine.connect() as connection:
            return pd.read_sql(query, connection, **pandas_kwargs)
    
    except:
        # If the first connection fails, try the connection with the pooler (IPv4 compatible)
        engine = sqlalchemy.create_engine(connection_string_pooler)
        with engine.connect() as connection:
            return pd.read_sql(query, connection, **pandas_kwargs)


if __name__ == "__main__":
    config = PostgresConfig()
    query = "SELECT * FROM classifier_data;"
    df = query_postgres(config, query)
    print(df)
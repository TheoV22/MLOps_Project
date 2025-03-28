from dataclasses import dataclass
from typing import Any
import pandas as pd
import sqlalchemy


@dataclass
class PostgresConfig:
    host: str
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    schema: str | None = None


def query_postgres(
    config: PostgresConfig, query: str, **pandas_kwargs: Any
) -> pd.DataFrame:
    connection_string = (
        f"postgresql://{config.user}:{config.password}@"
        f"{config.host}:{config.port}/{config.database}"
    )

    engine = sqlalchemy.create_engine(connection_string)

    with engine.connect() as connection:
        return pd.read_sql(query, connection, **pandas_kwargs)

# =================================================================================================
#                       TEST : implement authentification accessing
# =================================================================================================

import os
from dataclasses import dataclass
from typing import Any
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(".env")  # Change to ".creds" if needed

@dataclass
class PostgresConfig:
    host: str = os.getenv("PG_HOST", "localhost") # db.fmevbuggpndlcsznsjdr.supabase.co
    port: int = int(os.getenv("PG_PORT", 5432)) # 5432
    database: str = os.getenv("PG_DATABASE", "") # postgres
    user: str = os.getenv("PG_USER", "") # postgres
    password: str = os.getenv("PG_PASSWORD", "") # MLOps_1
    schema: str | None = None


def query_postgres(config: PostgresConfig, query: str, **pandas_kwargs: Any) -> pd.DataFrame:
    connection_string = (
        f"postgresql://{config.user}:{config.password}@"
        f"{config.host}:{config.port}/{config.database}"
    )

    engine = sqlalchemy.create_engine(connection_string)

    with engine.connect() as connection:
        return pd.read_sql(query, connection, **pandas_kwargs)


# Example usage
if __name__ == "__main__":
    config = PostgresConfig()
    query = "SELECT * FROM your_table LIMIT 10;"
    df = query_postgres(config, query)
    print(df)


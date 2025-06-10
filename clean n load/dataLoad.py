import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Get DB config from environment
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
SCHEMA_NAME = os.getenv("SCHEMA_NAME")

# Create SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Ensure schema exists
with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"))

# Load JSON files


def load_json_to_df(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data)


album_df = load_json_to_df(
    "/home/naman/Desktop/FEval/extraction/output/album_data.json")
artist_df = load_json_to_df(
    "/home/naman/Desktop/FEval/extraction/output/artist_data.json")
track_df = load_json_to_df(
    "/home/naman/Desktop/FEval/extraction/output/track_data.json")

# Flatten list columns


def flatten_columns(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: ', '.join(
                x) if isinstance(x, list) else x)
    return df


album_df = flatten_columns(album_df)
artist_df = flatten_columns(artist_df)
track_df = flatten_columns(track_df)

# Upload function


def upload_df_to_postgres(df, table_name):
    df.to_sql(
        name=table_name,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists="replace",
        index=False
    )
    print(f"Uploaded {table_name} to schema '{SCHEMA_NAME}'")


# Upload all three
upload_df_to_postgres(album_df, "albums")
upload_df_to_postgres(artist_df, "artists")
upload_df_to_postgres(track_df, "tracks")

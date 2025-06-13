# import os
# import json
# import pandas as pd
# from sqlalchemy import create_engine, text
# from dotenv import load_dotenv

# load_dotenv()

# DB_USER = os.getenv("DB_USER")
# DB_PASS = os.getenv("DB_PASS")
# DB_HOST = os.getenv("DB_HOST")
# DB_PORT = os.getenv("DB_PORT")
# DB_NAME = os.getenv("DB_NAME")
# SCHEMA_NAME = os.getenv("SCHEMA_NAME")

# # engine
# engine = create_engine(
#     f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# # connection setup
# with engine.connect() as conn:
#     conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"))

# # json laoder function


# def load_json_to_df(file_path):
#     with open(file_path, 'r') as f:
#         data = json.load(f)
#     return pd.DataFrame(data)


# album_df = load_json_to_df(
#     "/home/naman/Desktop/FEval/extraction/output/album_data.json")
# artist_df = load_json_to_df(
#     "/home/naman/Desktop/FEval/extraction/output/artist_data.json")
# track_df = load_json_to_df(
#     "/home/naman/Desktop/FEval/extraction/output/track_data.json")

# # flattening


# def flatten_columns(df):
#     for col in df.columns:
#         if df[col].apply(lambda x: isinstance(x, list)).any():
#             df[col] = df[col].apply(lambda x: ', '.join(
#                 x) if isinstance(x, list) else x)
#     return df


# album_df = flatten_columns(album_df)
# artist_df = flatten_columns(artist_df)
# track_df = flatten_columns(track_df)

# # laoder


# def upload_df_to_postgres(df, table_name):
#     df.to_sql(
#         name=table_name,
#         con=engine,
#         schema=SCHEMA_NAME,
#         if_exists="replace",
#         index=False
#     )
#     print(f"Uploaded {table_name} to schema '{SCHEMA_NAME}'")


# upload_df_to_postgres(album_df, "albums")
# upload_df_to_postgres(artist_df, "artists")
# upload_df_to_postgres(track_df, "tracks")
import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
SCHEMA_NAME = os.getenv("SCHEMA_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"))


def load_json(path):
    with open(path, 'r') as f:
        return json.load(f)


def upload_df(df, table_name):
    df.to_sql(
        name=table_name,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists='replace',
        index=False
    )
    print(f"✅ Uploaded: {table_name}")


def process_albums(file_path):
    raw_data = load_json(file_path)
    records = []

    for item in raw_data:
        artist_names = ', '.join([a['name'] for a in item.get('artists', [])])
        artist_ids = ', '.join([a['id'] for a in item.get('artists', [])])

        records.append({
            "id": item["id"],
            "name": item["name"],
            "release_date": item["release_date"],
            "total_tracks": item["total_tracks"],
            "popularity": item["popularity"],
            "artist_names": artist_names,
            "artist_ids": artist_ids,
            "extraction_datetime": item["extraction_datetime"],
            "source": item["source"],
            "extractor": item.get("extractor", "unknown"),
            "data_version": item["data_version"],
            "timezone": item["timezone"]
        })

    df = pd.DataFrame(records)
    upload_df(df, "albums")


def process_artists(file_path):
    raw_data = load_json(file_path)
    records = []

    for item in raw_data:
        genres = ', '.join(item.get("genres", []))
        records.append({
            "id": item["id"],
            "name": item["name"],
            "genres": genres,
            "popularity": item["popularity"],
            "extraction_datetime": item["extraction_datetime"],
            "source": item["source"],
            "extractor": item.get("extractor", "unknown"),
            "data_version": item["data_version"],
            "timezone": item["timezone"]
        })

    df = pd.DataFrame(records)
    upload_df(df, "artists")


def process_tracks(file_path):
    raw_data = load_json(file_path)
    records = []

    for item in raw_data:
        artist_names = ', '.join([a['name'] for a in item.get('artist', [])])
        artist_ids = ', '.join([a['id'] for a in item.get('artist', [])])
        album_id = item["album"]["id"]
        album_name = item["album"]["name"]

        records.append({
            "id": item["id"],
            "name": item["name"],
            "artist_names": artist_names,
            "artist_ids": artist_ids,
            "album_id": album_id,
            "album_name": album_name,
            "duration_ms": item["duration_ms"],
            "explicit": item["explicit"],
            "popularity": item["popularity"],
            "extraction_datetime": item["extraction_datetime"],
            "source": item["source"],
            "extractor": item.get("extractor", "unknown"),
            "data_version": item["data_version"],
            "timezone": item["timezone"]
        })

    df = pd.DataFrame(records)
    upload_df(df, "tracks")


process_albums("/home/naman/Desktop/FEval/extraction/albums.json")
process_artists("/home/naman/Desktop/FEval/extraction/artists.json")
process_tracks("/home/naman/Desktop/FEval/extraction/tracks.json")

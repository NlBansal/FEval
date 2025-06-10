import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import datetime
import json
import os
import asyncio
from typing import List

load_dotenv()

# === Spotify Authentication ===
client_id = os.getenv("SPOTIPY_CLIENT_ID")
client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")

# # Check if .env is loading correctly
# print(f"SPOTIPY_CLIENT_ID: {client_id}")
# print(f"SPOTIPY_CLIENT_SECRET: {client_secret}")

client_credentials_manager = SpotifyClientCredentials(
    client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# === Semaphore to limit concurrent API calls (Spotify recommends <10/sec) ===
semaphore = asyncio.Semaphore(3)

# === Metadata Helper ===


def get_metadata():
    now = datetime.datetime.now()
    return {
        "extraction_datetime": now.isoformat(),
        "source": "Spotify API v1",
        "extractor": "spotipy",
        "data_version": "1.0",
        "timezone": str(datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo)
    }

# === Album Fetch ===


async def fetch_album(album_id):
    async with semaphore:
        album = await asyncio.to_thread(sp.album, album_id)
        metadata = get_metadata()
        return {
            "id": album['id'],
            "name": album['name'],
            "release_date": album['release_date'],
            "total_tracks": album['total_tracks'],
            "popularity": album['popularity'],
            "artists": [artist['name'] for artist in album['artists']],
            "tracks": [track['name'] for track in album['tracks']['items']],
            **metadata
        }

# === Track Fetch ===


async def fetch_track(track_id):
    async with semaphore:
        track = await asyncio.to_thread(sp.track, track_id)
        await asyncio.sleep(0.3)
        metadata = get_metadata()
        return {
            "id": track['id'],
            "name": track['name'],
            "album": track['album']['name'],
            "artist": [artist['name'] for artist in track['artists']],
            "duration_ms": track['duration_ms'],
            "explicit": track['explicit'],
            "popularity": track['popularity'],
            **metadata
        }

# === Artist Fetch ===


async def fetch_artist(artist_id):
    async with semaphore:
        artist = await asyncio.to_thread(sp.artist, artist_id)
        await asyncio.sleep(0.3)
        metadata = get_metadata()
        return {
            "id": artist['id'],
            "name": artist['name'],
            "genres": artist['genres'],
            "popularity": artist['popularity'],
            **metadata
        }

# === All Albums of Artist ===


async def fetch_all_albums(artist_id):
    async with semaphore:
        albums = await asyncio.to_thread(sp.artist_albums, artist_id, album_type='album')
        await asyncio.sleep(0.3)
        return [album['id'] for album in albums['items']]

# === All Tracks of Album ===


async def fetch_all_tracks(album_id):
    async with semaphore:
        tracks = await asyncio.to_thread(sp.album_tracks, album_id)
        await asyncio.sleep(0.3)
        return [track['id'] for track in tracks['items']]

# === Save JSON ===


def save_json(data: List[dict], filename: str):
    os.makedirs("output", exist_ok=True)
    filepath = os.path.join("output", filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print(f"Saved {filename} with {len(data)} records.")

# === Controller Function ===


async def extract_everything(artist_ids: List[str]):
    all_albums, all_tracks, all_artists = [], [], []

    for artist_id in artist_ids:
        all_artists.append(await fetch_artist(artist_id))

        album_ids = await fetch_all_albums(artist_id)
        album_tasks = [fetch_album(album_id) for album_id in album_ids]
        album_data = await asyncio.gather(*album_tasks)
        all_albums.extend(album_data)

        for album_id in album_ids:
            track_ids = await fetch_all_tracks(album_id)
            track_tasks = [fetch_track(track_id) for track_id in track_ids]
            track_data = await asyncio.gather(*track_tasks)
            all_tracks.extend(track_data)

    save_json(all_albums, "album_data.json")
    save_json(all_artists, "artist_data.json")
    save_json(all_tracks, "track_data.json")

# === Run Example ===
if __name__ == "__main__":
    # Example artist list (The Weeknd, Ariana Grande,arijit singh,ed sheeran)
    artist_ids = [
        "1Xyo4u8uXC1ZmMpatF05PJ",
        "66CXWjxzNUsdJxJ2JdwvnR",
        "4YRxDV8wJFPHPTeXepOstw",
        "6eUKZXaKkcviH0Ku9w2n3V"
    ]
    asyncio.run(extract_everything(artist_ids))

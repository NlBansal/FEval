import os
import json
import asyncio
import aiohttp
import random
import datetime
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy import Spotify
from aiohttp import ClientSession, ClientTimeout, TCPConnector


load_dotenv()
CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")

sp_sync = Spotify(auth_manager=SpotifyClientCredentials(
    client_id=CLIENT_ID, client_secret=CLIENT_SECRET))


ARTISTS, ALBUMS, TRACKS = [], [], []

# Semaphore
semaphore = asyncio.Semaphore(4)


def get_metadata():
    now = datetime.datetime.now()
    return {
        "extraction_datetime": now.isoformat(),
        "source": "Spotify API v1",
        "extractor": "spotipy",
        "data_version": "1.0",
        "timezone": str(datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo)
    }


async def fetch(session, url, headers, max_retries=3):
    for attempt in range(max_retries):
        async with semaphore:
            await asyncio.sleep(2 ** attempt + random.uniform(0.1, 0.5))
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 429:
                        retry_after = int(
                            response.headers.get("Retry-After", "1"))
                        print(f"[429] Retrying after {retry_after}s: {url}")
                        await asyncio.sleep(retry_after)
                        continue
                    elif response.status != 200:
                        print(f"[{response.status}] Failed: {url}")
                        return None
                    return await response.json()
            except aiohttp.ClientError as e:
                print(f"[ClientError] {e} on {url}")
            except Exception as e:
                print(f"[Unexpected Error] {e} on {url}")
    return None


async def fetch_artist_details(session, artist_id, headers):
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    return await fetch(session, url, headers)


async def fetch_artist_albums(session, artist_id, headers):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?limit=5&include_groups=album"
    return await fetch(session, url, headers)


async def fetch_album(session, album_id, headers):
    url = f"https://api.spotify.com/v1/albums/{album_id}"
    return await fetch(session, url, headers)


async def fetch_track(session, track_id, headers):
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    return await fetch(session, url, headers)


async def process_artist(session, artist_id, headers, metadata):
    try:
        artist_data = await fetch_artist_details(session, artist_id, headers)
        if not artist_data or artist_data.get("popularity", 0) <= 80:
            return

        await asyncio.sleep(random.uniform(0.2, 0.5))
        ARTISTS.append({**{
            "id": artist_data["id"],
            "name": artist_data["name"],
            "genres": artist_data["genres"],
            "popularity": artist_data["popularity"]
        }, **metadata})

        albums_data = await fetch_artist_albums(session, artist_id, headers)
        if not albums_data:
            return

        await asyncio.sleep(random.uniform(0.2, 0.5))
        album_ids = set()

        for album in albums_data.get("items", []):
            album_id = album["id"]
            if album_id in album_ids:
                continue
            album_ids.add(album_id)

            full_album = await fetch_album(session, album_id, headers)
            if not full_album:
                continue

            await asyncio.sleep(random.uniform(0.2, 0.5))
            ALBUMS.append({**{
                "id": full_album["id"],
                "name": full_album["name"],
                "release_date": full_album["release_date"],
                "total_tracks": full_album["total_tracks"],
                "popularity": full_album["popularity"],
                "artists": [{"id": a["id"], "name": a["name"]} for a in full_album["artists"]]
            }, **metadata})

            for track in full_album.get("tracks", {}).get("items", []):
                full_track = await fetch_track(session, track["id"], headers)
                if not full_track:
                    continue

                await asyncio.sleep(random.uniform(0.1, 0.3))
                TRACKS.append({**{
                    "id": full_track["id"],
                    "name": full_track["name"],
                    "artist": [{"id": a["id"], "name": a["name"]} for a in full_track["artists"]],
                    "album": {"id": full_album["id"], "name": full_album["name"]},
                    "duration_ms": full_track["duration_ms"],
                    "explicit": full_track["explicit"],
                    "popularity": full_track["popularity"]
                }, **metadata})

        await asyncio.sleep(random.uniform(0.3, 0.6))

    except Exception as e:
        print(f"[ERROR] Failed to process artist {artist_id}: {e}")


async def main():
    try:
        auth_manager = SpotifyClientCredentials(
            client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        token = auth_manager.get_access_token(as_dict=False)
        headers = {"Authorization": f"Bearer {token}"}

        connector = TCPConnector(limit=10)
        timeout = ClientTimeout(total=60)
        metadata = get_metadata()

        async with ClientSession(connector=connector, timeout=timeout) as session:
            artist_ids = []
            seen_ids = set()
            search_terms = ["a", "e", "i", "m", "pop", "rock"]

            for term in search_terms:
                results = sp_sync.search(q=term, type="artist", limit=50)
                for artist in results["artists"]["items"]:
                    if artist["popularity"] > 80 and artist["id"] not in seen_ids:
                        artist_ids.append(artist["id"])
                        seen_ids.add(artist["id"])

            print(f"Found {len(artist_ids)} unique popular artists...")

            tasks = [process_artist(session, artist_id, headers, metadata)
                     for artist_id in artist_ids]
            await asyncio.gather(*tasks)

        with open("artists.json", "w") as f:
            json.dump(ARTISTS, f, indent=2)

        with open("albums.json", "w") as f:
            json.dump(ALBUMS, f, indent=2)

        with open("tracks.json", "w") as f:
            json.dump(TRACKS, f, indent=2)

        print("Data with metadata saved to JSON files.")

    except Exception as e:
        print(f"[FATAL ERROR] {e}")

if __name__ == "__main__":
    asyncio.run(main())

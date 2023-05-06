import asyncio
import requests
import time
from db import Database

async def update_difficulty_rating():
    db = Database()

    await db.execute_query("CREATE TABLE IF NOT EXISTS beatmaps_eyup (beatmap_id INTEGER PRIMARY KEY, difficultyrating FLOAT)")

    start_time = time.time()

    query = "SELECT beatmap_id FROM beatmaps WHERE approved BETWEEN 1 AND 2 AND mode = 0 AND beatmap_id NOT IN (SELECT beatmap_id from beatmaps_eyup)"
    beatmaps = await db.execute_query(query)

    for i, b in enumerate(beatmaps):
        response = requests.get(f"https://osu.respektive.pw/b/{b['beatmap_id']}")
        data = response.json()
        eyup_stars = data["beatmap"]["eyup_star_rating"]
        await db.execute_query(f"INSERT INTO beatmaps_eyup VALUES ({b['beatmap_id']}, {eyup_stars})")
        print(i, "/", len(beatmaps), "beatmap_id:", b["beatmap_id"])

    end_time = time.time()

    print(f"Elapsed time: {end_time - start_time} seconds.")


asyncio.run(update_difficulty_rating())
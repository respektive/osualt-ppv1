import asyncio
import time
from db import Database

async def update_ss_ratio():
    db = Database()

    await db.execute_query("CREATE TABLE IF NOT EXISTS beatmap_ss_ratio (beatmap_id INTEGER PRIMARY KEY, ratio FLOAT)")

    start_time = time.time()

    print("Deleting from table...")

    await db.execute_query("DELETE FROM beatmap_ss_ratio")

    print("Inserting into table...")

    await db.execute_query("""
        WITH ss_counts AS (
            SELECT beatmap_id,
                COUNT(CASE WHEN rank IN ('X', 'XH') THEN 1 END) AS ss_count,
                COUNT(CASE WHEN rank IN ('SH', 'S') THEN 1 END) AS s_count
            FROM scores_top
            GROUP BY beatmap_id
        )
        INSERT INTO beatmap_ss_ratio (beatmap_id, ratio)
        SELECT beatmap_id, COALESCE(ss_count::float / NULLIF(s_count, 0), 0)
        FROM ss_counts
    """)

    end_time = time.time()

    print("Done.")

    print(f"Elapsed time: {end_time - start_time} seconds.")


asyncio.run(update_ss_ratio())

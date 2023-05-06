import asyncio
import time
from db import Database

async def update_scores_top():
    db = Database()

    await db.execute_query("""
    CREATE TABLE IF NOT EXISTS scores_top (
        user_id integer NOT NULL,
        beatmap_id integer NOT NULL,
        score integer,
        count300 integer,
        count100 integer,
        count50 integer,
        countmiss integer,
        combo integer,
        perfect smallint,
        enabled_mods character varying,
        date_played timestamp without time zone,
        rank character varying(3),
        pp numeric(9,4),
        replay_available smallint,
        is_hd boolean,
        is_hr boolean,
        is_dt boolean,
        is_fl boolean,
        is_ht boolean,
        is_ez boolean,
        is_nf boolean,
        is_nc boolean,
        is_td boolean,
        is_so boolean,
        is_sd boolean,
        is_pf boolean,
        accuracy numeric(5,2),
        pos integer,
        PRIMARY KEY (user_id, beatmap_id)
        )
    """)

    start_time = time.time()

    await db.execute_query("DELETE FROM scores_top")

    await db.execute_query("""
        INSERT INTO scores_top
        SELECT *
        FROM (
          SELECT *,
                 row_number() OVER (PARTITION BY beatmap_id ORDER BY score DESC) AS pos
          FROM scores
        ) t
        WHERE t.pos <= 500
        ON CONFLICT DO NOTHING
    """)

    end_time = time.time()

    print(f"Elapsed time: {end_time - start_time} seconds.")

asyncio.run(update_scores_top())

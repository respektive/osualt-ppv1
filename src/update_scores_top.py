import asyncio
import time
from db import Database

async def update_scores_top():
    db = Database()

    start_time = time.time()

    print("Dropping old table...")

    await db.execute_query("DROP TABLE scores_top")

    print("Creating new table...")

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
        pp numeric(10,3) DEFAULT NULL,
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

    print("Creating indexes...")

    await db.execute_query("CREATE INDEX idx_scores_top_score ON scores_top (score)")
    await db.execute_query("CREATE INDEX idx_scores_top_user_id ON scores_top (user_id)")
    await db.execute_query("CREATE INDEX idx_scores_top_beatmap_id ON scores_top (beatmap_id)")
    await db.execute_query("GRANT SELECT ON scores_top TO bot")

    print("Inserting scores...")

    await db.execute_query("""
        INSERT INTO scores_top (user_id, beatmap_id, score, count300, count100, count50, countmiss, combo, perfect, enabled_mods, date_played, rank, pp, replay_available, is_hd, is_hr, is_dt, is_fl, is_ht, is_ez, is_nf, is_nc, is_td, is_so, is_sd, is_pf, accuracy, pos)
        SELECT user_id, beatmap_id, score, count300, count100, count50, countmiss, combo, perfect, enabled_mods, date_played, rank, NULL AS pp, replay_available, is_hd, is_hr, is_dt, is_fl, is_ht, is_ez, is_nf, is_nc, is_td, is_so, is_sd, is_pf, accuracy, pos
        FROM (
        SELECT *,
                row_number() OVER (PARTITION BY beatmap_id ORDER BY score DESC) AS pos
        FROM scores
        ) t
        WHERE t.pos <= 500
        ON CONFLICT DO NOTHING
    """)

    print("Done.")

    end_time = time.time()

    print(f"Elapsed time: {end_time - start_time} seconds.")

asyncio.run(update_scores_top())

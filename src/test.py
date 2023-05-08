import asyncio
import time
import math
import decimal
from db import Database

async def test():
    db = Database()

    start_time = time.time()

    scores = await db.execute_query("SELECT * FROM scores_top WHERE user_id = 1023489 AND pp > 0.001 order by pp desc")
    print(len(scores))
    this_scores = []
    for s in scores:
        this_scores.append(float(s["pp"]))

    rank_score = 0

    if len(this_scores) > 0:

        j = 1.0
        rank_score = 0
        for s in this_scores:
            rank_score += s * j
            j *= 0.994

        
        rank_score = max(0, math.log(rank_score + 1) * 400)

    print(f"{rank_score:.0f}pp")

    end_time = time.time()

    print(f"Elapsed time: {end_time - start_time} seconds.")


asyncio.run(test())
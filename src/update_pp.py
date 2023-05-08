import math
import time
import datetime
import asyncio
import sys
from db import Database

CUTOFF_SCORE = 0.001

async def update_pp():
    db = Database()

    args = sys.argv

    print("Gathering beatmap info...")

    result = await db.execute_query("""
    WITH max_playcount AS (
        SELECT set_id, mode, MAX(playcount) AS max_playcount
        FROM beatmaps
        GROUP BY set_id, mode
    )
    SELECT b.beatmap_id, e.difficultyrating, b.mode,
        b.playcount::float / mp.max_playcount::float AS relativeplaycount,
        b.playcount, b.passcount, bss.ratio AS ss_ratio
    FROM beatmaps AS b
    INNER JOIN beatmaps_eyup AS e ON b.beatmap_id = e.beatmap_id
    INNER JOIN max_playcount AS mp ON b.set_id = mp.set_id AND b.mode = mp.mode
    INNER JOIN beatmap_ss_ratio AS bss ON b.beatmap_id = bss.beatmap_id
    WHERE b.approved BETWEEN 1 AND 2 AND b.mode = 0 AND mp.max_playcount > 0
    """)

    # Convert results to dictionary with beatmap_id keys
    beatmap_info = {}
    for row in result:
        beatmap_info[row['beatmap_id']] = row

    TOP_SCORE_TABLE = "scores_top"

    print(len(beatmap_info), "found")
    print("Gathering users...")
    
    arg = None
    if len(args) > 1:
        arg = args[1]
    if arg and arg.isdigit() and int(arg) > 0:
        users = await db.execute_query(f"SELECT user_id, username, ranked_score, pp FROM users2 WHERE user_id = {arg}")
    elif arg and arg == "full":
        users = await db.execute_query("SELECT distinct user_id FROM scores_top ORDER BY user_id ASC")
    else:
        users = await db.execute_query("SELECT user_id, username, ranked_score, pp FROM users2 WHERE ranked_score > 0 AND user_id IN (SELECT user_id FROM priorityuser) ORDER BY ranked_score DESC")

    total_users = len(users)
    print(total_users, "found")
    print("Calculating per-user scores...")

    start_time = time.time()

    for i, u in enumerate(users):
        this_scores = []
        this_accuracies = []
        update_queries = []

        count = await db.execute_query(f"SELECT COUNT(*) as c FROM {TOP_SCORE_TABLE} t WHERE user_id = {u['user_id']}")
        if count[0]["c"] <= 0:
            continue
        
        scores = await db.execute_query(f"""SELECT * FROM {TOP_SCORE_TABLE} WHERE user_id = {u['user_id']}""")

        for s in scores:
            b_info = beatmap_info.get(s["beatmap_id"])

            if b_info is None:
                continue

            this_score = math.pow(b_info["difficultyrating"], 4) / math.pow(s["pos"], 0.8)

            #older scores aren't worth as much 
            this_score *= max(0, 1 - 0.01 * ((time.time() - s["date_played"].replace(tzinfo=datetime.timezone.utc).timestamp()) / 3600 / 24 / 10))

            #bonus for FC
            if s["rank"] == 'X' or s["rank"] == 'XH':
                this_score *= 1.36
            elif s["perfect"]:
                this_score *= 1.2

            #bonus for "skill"-based mods
            enabled_mods = int(s["enabled_mods"])
            if (enabled_mods & 16) > 0: #hard rock
                this_score *= 1.1
            if (enabled_mods & 64) > 0: #doble time
                this_score *= 1.1
            if (enabled_mods & 2) > 0 or (enabled_mods & 256) > 0: #easy, half time
                this_score *= 0.2

            #adjust based on playcount of map
            this_score *= math.pow(b_info["playcount"], 0.4) * 3.6

            if this_score <= CUTOFF_SCORE:
                continue

            #debuffs    
            this_score *= 1 - min(1, 3 * float(b_info["ss_ratio"]))

            # if b_info["relativedifficulty"] < 0.98:
            #     this_score *= 0.2

            if b_info["relativeplaycount"] < 0.98:
                this_score *= 0.24

            if (b_info["passcount"] / b_info["playcount"]) > 0.3:
                this_score *= 0.2

            #adjust based on accuracy
            this_accuracy = calculate_accuracy(s)
            this_score *= math.pow(this_accuracy/100, 15)
            
            this_score = max(0, this_score)

            if this_score > CUTOFF_SCORE:
                this_scores.append(this_score)
                this_accuracies.append(this_accuracy)

            # add score to list of scores to update
            update = False
            if len(args) > 2:
                update = True
            if update:
                update_queries.append(f"({u['user_id']}, {s['beatmap_id']}, {this_score})")

        # generate single update query to update all scores
        if update_queries:
            update_query = f"UPDATE {TOP_SCORE_TABLE} SET pp = new_values.pp FROM (VALUES {','.join(update_queries)}) AS new_values(user_id, beatmap_id, pp) WHERE {TOP_SCORE_TABLE}.user_id = new_values.user_id AND {TOP_SCORE_TABLE}.beatmap_id = new_values.beatmap_id"
            await db.execute_query(update_query)

        rank_score = 0
        accuracy = 0
        accuracy_total = 0

        if len(this_scores) > 0:
            this_scores, this_accuracies = zip(*sorted(zip(this_scores, this_accuracies), reverse=True))
            
            j = 1
            rank_score = 0
            for s in this_scores:
                rank_score += s * j
                j *= 0.994
            
            j = 1
            accuracy = 0
            accuracy_total = 0
            for a in this_accuracies:
                accuracy += a * j
                accuracy_total += j
                j *= 0.996
            
            accuracy /= accuracy_total
            
            rank_score = max(0, math.log(rank_score + 1) * 400)

        if i % 100 == 0:
            print(f"{i+1}/{total_users} {u['user_id']} {u['username'] if 'username' in u else ''} ({i/total_users*100:.2f}%): {(u['pp'] if 'pp' in u else 0):.0f}pp -> {rank_score:.0f}pp")
            
        await db.execute_query(f"INSERT INTO users_ppv1 VALUES ({u['user_id']}, {rank_score}, {accuracy}) ON CONFLICT (user_id) DO UPDATE SET ppv1 = EXCLUDED.ppv1, accuracyv1 = EXCLUDED.accuracyv1")

    end_time = time.time()

    print(f"Elapsed time: {end_time - start_time} seconds.")


def calculate_accuracy(score):
    totalHits = ((score['count50'] + score['count100'] + score['count300'] + score['countmiss']) * 300)
    if totalHits > 0:
        accuracy = round(((score['count50'] * 50 + score['count100'] * 100 + score['count300'] * 300)/totalHits) * 100, 2)
    return accuracy


asyncio.run(update_pp())
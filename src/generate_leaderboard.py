import asyncio
import time
from db import Database
from jinja2 import Template

async def generate_leaderboard():
    db = Database()
    start_time = time.time()

    results = await db.execute_query(f"select p.user_id, username, round(ppv1::numeric, 2) as ppv1, round(accuracyv1::numeric, 2) as accv1 from users_ppv1 p left join users2 using (user_id) order by ppv1 desc")

    template = Template('''
    <html>
    <head>
        <title>Leaderboard</title>
        <style>
        body {
        margin: 0;
        padding: 0;
        background-color: #f2f2f2;
        font-family: Arial, sans-serif;
        }

        .leaderboard {
        max-width: 800px;
        margin: 0 auto;
        background-color: white;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }

        .leaderboard h1 {
        text-align: center;
        font-size: 36px;
        font-weight: bold;
        color: #333333;
        padding: 20px;
        margin: 0;
        }

        .leaderboard table {
        width: 100%;
        border-collapse: collapse;
        margin: 0px;
        }

        .leaderboard table th {
        background-color: #eeeeee;
        font-size: 20px;
        font-weight: bold;
        text-align: center;
        padding: 10px;
        border: 1px solid #cccccc;
        }

        .leaderboard table td {
        font-size: 16px;
        text-align: center;
        padding: 10px;
        border: 1px solid #cccccc;
        }

        .leaderboard table tr:nth-child(even) {
        background-color: #f9f9f9;
        }

        </style>
    </head>
    <body>
        <div class="leaderboard">
        <h1>ppv1</h1>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>User ID</th>
                    <th>Username</th>
                    <th>pp</th>
                    <th>Acc</th>
                </tr>
            </thead>
            <tbody>
                {% for row in results %}
                <tr>
                    <td>{{ loop.index }}</td>
                    <td>{{ row["user_id"] }}</td>
                    <td>{{ row["username"] }}</td>
                    <td>{{ row["ppv1"] }}</td>
                    <td>{{ row["accv1"] }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        </div>
    </body>
    </html>
    ''')

    html = template.render(results=results)

    # Write the HTML to a file
    with open('ppv1leaderboard.html', 'w') as f:
        f.write(html)

    end_time = time.time()

    print(f"Elapsed time: {end_time - start_time} seconds.")

asyncio.run(generate_leaderboard())
import random
import pandas as pd
import os
from game import PygameWindow
from Python.helperfunctions import get_database_connection


conn = get_database_connection()

#add query to get matches + team, something like this????
query_matches = """
SELECT m.match_id, t_home.team_name AS home_team_name, t_away.team_name AS away_team_name, m.home_team_id, m.away_team_id
FROM matches m
JOIN teams t_home ON m.home_team_id = t_home.team_id
JOIN teams t_away ON m.away_team_id = t_away.team_id
"""

# Create DataFrame
matches_df = pd.read_sql_query(query_matches, conn)
print(matches_df)

if __name__ == "__main__":
    #campus
    game = PygameWindow(conn, title="Maximized Pygame Window", fullscreen=False)
    game.run(matches_df)

#CHECK HELPERFUNCTIONS AND ANIMATION TOOL
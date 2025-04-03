import random
import pandas as pd
import os
from game import PygameWindow
from Python.helperfunctions import get_database_connection


"""
!!!!!IMPORTANT must read!!!!

excuse my bad english (I cannot think anymore, because my mind is fried).
BEHOLD, code I write when I should be asleep, I am running on 1 braincell so don't expect high quaity code.
It is 23:50 right now so please have mercy on my spaghetti code.


How to run
1. go in your terminal and make sure you open it in this folder and then write: pip install -r requiremtns.txt or: pip install -m requiremtns.txt (I forgot, oops).
2. run main.py (this file)
3. enjoy

Basically I remade it completly.... but this time in a pygame so it is easily interactable (idfk if that is a word).
There are plenty of comments that should help you understand what everything does (except in graphs.py, I copied it from other file but there are some comments that could help).
basically when you launch it it shows a list of buttons an graphs next to it, depending on what you click it shows the entire match (still a WIP), I also want to add buttons or something that let's you see specific points in a match.
The graphs button redirects you to the graphs page (duh). There you can find 2 web graphs (that Denis made, thanksss <3). idk how to style that part soooooo. good luck to whoever decides to do that (it will probably be me :( ).

all the graphs that you can use are available in the graphs.py file.
game.py is basically what you see on the application, for example the buttons, text, colors and stuff. logic is also handled there (idk if that is the correct way of doing it but hey, idk).
WATCH OUT (fr), there is a 'while True' in the run function of game.py so DON'T. DO. QUERIES. IN. THAT. LOOP!!!! if you need to do queries look at the function 'fetch_data_once' that shows how you can only fetch it once and store it in sort of a cache (it is easily expandable I think, also that function is untested)
this main.py file is the very beginning, it initialises the pygame and it gives all the matches with it (thanks riad and his group for the inspiration), so you normally don't have to add anything here.
feel free to make extra classes, files, whatever you want.


DO NOT CHANGE THE BACKGROUND COLOR!!!!!!
blue is my favorite color :D (I think the one I chose is really beatifull).


things to do:
-remove all hard coded values and implement database connection (IMPORTANT!!!)
-implement the soccer_animation tool on the match page (also important)
-make buttons (or a list) of timestamps and noteable actions so the user can quickly see those in action.
-style the graphs and make them more appealing to look at (no you cannot mat it scrollable, so work with buttons or smthng).
-try to fit a model in somehow???? (read summary and look at the info folder).
-calculate the average pass length and average pass speed of a team (or player) in a match.
-probably a lot more sadly :(


But if we LOCK THE FUCK IN for 1 day and crank things up to the MAX, we will have something presentable.
I belive in whoever reads this (probably just me...)
And remeber: If I don't respond within 5 minutes, I am probably having a heart attack of the amount of caffeine I ingested.  
Remeber for real: my advise for if you ever need to lock in: Hardstyle at 110% volume, A LOT of energy drinks and of course stay positive!! 
"""


conn = get_database_connection()

#add query to get matches + team, something like this????
query_matches = """
SELECT m.match_id, t_home.team_name AS home_team_name, t_away.team_name AS away_team_name
FROM matches m
JOIN teams t_home ON m.home_team_id = t_home.team_id
JOIN teams t_away ON m.away_team_id = t_away.team_id
"""

# Create DataFrame
matches_df = pd.read_sql_query(query_matches, conn)


# Display DataFrame
print(matches_df)

if __name__ == "__main__":
    #campus
    game = PygameWindow(conn, title="Maximized Pygame Window", fullscreen=False)
    game.run(matches_df)

#CHECK HELPERFUNCTIONS AND ANIMATION TOOL
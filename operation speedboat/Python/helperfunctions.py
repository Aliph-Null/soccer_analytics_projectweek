import pandas as pd
import psycopg2
import dotenv
import os
import matplotlib.pyplot as plt
from mplsoccer import Pitch
import matplotlib as mpl
from IPython.display import clear_output


def get_database_connection():
    """
    Establish and return a connection to the PostgreSQL database.

    Returns:
        psycopg2.extensions.connection: A connection object to the database.
    """
    dotenv.load_dotenv()

    PG_PASSWORD = os.getenv("PG_PASSWORD")
    PG_USER = os.getenv("PG_USER")
    PG_HOST = os.getenv("PG_HOST")
    PG_PORT = os.getenv("PG_PORT")
    PG_DATABASE = os.getenv("PG_DB")

    # Establish and return the database connection
    return psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
        sslmode="require",
    )

def fetch_tracking_data(game_id, conn):
    """
    Fetch tracking data for a specific game from the database.

    Args:
        game_id (str): The ID of the game to fetch tracking data for.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        pd.DataFrame: A DataFrame containing the tracking data.
    """
    # Ensure the connection is passed as a parameter
    if conn is None:
        raise ValueError("Database connection 'conn' must be provided.")

    try:
        # Query to fetch tracking data
        query = f"""
        SELECT pt.frame_id, pt.timestamp, pt.player_id, pt.x, pt.y, p.jersey_number, p.player_name, p.team_id
        FROM player_tracking pt
        JOIN players p ON pt.player_id = p.player_id
        JOIN teams t ON p.team_id = t.team_id
        WHERE pt.game_id = '{game_id}';
        """
        # Execute query and load data into a DataFrame
        tracking_df = pd.read_sql_query(query, conn)
        return tracking_df
    finally:
        # Close the connection
        # Ensure the caller handles connection closure
        pass

def fetch_match_events(match_id, conn):
    """
    Fetch match events for a specific match from the database.

    Args:
        match_id (str): The ID of the match to fetch events for.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        pd.DataFrame: A DataFrame containing the match events.
    """
    # Ensure the connection is passed as a parameter
    if conn is None:
        raise ValueError("Database connection 'conn' must be provided.")

    # Query to fetch match events
    query = f"""
    SELECT me.match_id, me.event_id, me.eventtype_id, et.name AS eventtype_name, me.result, me.success, me.period_id, 
            me.timestamp, me.end_timestamp, me.ball_state, me.ball_owning_team, 
            me.team_id, me.player_id, me.x, me.y, me.end_coordinates_x, 
            me.end_coordinates_y, me.receiver_player_id, rp.team_id AS receiver_team_id
    FROM matchevents me
    LEFT JOIN players rp ON me.receiver_player_id = rp.player_id
    LEFT JOIN eventtypes et ON me.eventtype_id = et.eventtype_id
    WHERE me.match_id = '{match_id}'
    ORDER BY me.period_id ASC, me.timestamp ASC;
    """
    # Execute query and load data into a DataFrame
    events_df = pd.read_sql_query(query, conn)
    return events_df

def fetch_team_matches(team_name, conn):
    """
    Fetch all matches for a team where the team name contains the specified string.
    The result includes a 'home' column indicating whether the team is the home team (1 for home, 0 otherwise).

    Args:
        team_name (str): The substring to search for in team names.
        conn (psycopg2.extensions.connection): The database connection object.

    Returns:
        pd.DataFrame: A DataFrame containing the matches for the team, including a 'home' column.
    """
    if conn is None:
        raise ValueError("Database connection 'conn' must be provided.")

    try:
        # Query to fetch matches for the team
        query = f"""
        SELECT m.match_id, m.match_date, m.home_team_id, ht.team_name AS home_team_name, 
                m.away_team_id, at.team_name AS away_team_name,
                CASE 
                    WHEN ht.team_name ILIKE '%{team_name}%' THEN 1
                    ELSE 0
                END AS home
        FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        WHERE ht.team_name ILIKE '%{team_name}%' OR at.team_name ILIKE '%{team_name}%';
        """
        # Execute query and load data into a DataFrame
        matches_df = pd.read_sql_query(query, conn)
        return matches_df
    finally:
        # Ensure the caller handles connection closure
        pass

def calculate_ball_possession(match_id, conn, team_id):
    """
    Calculate ball possession changes for a specific team during a match.
    This function processes match events to identify changes in ball possession
    and calculates whether the specified team is in possession of the ball at
    each change point.
    Args:
        match_id (int): The unique identifier for the match.
        conn (object): A database connection object used to fetch match events.
        team_id (int): The unique identifier for the team whose possession is being calculated.
    Returns:
        pandas.DataFrame: A DataFrame containing the following columns:
            - match_id (int): The match identifier.
            - team_id (int): The team in possession of the ball at each change point.
            - timestamp (datetime): The timestamp of each possession change.
            - ball_possession (int): A binary column indicating whether the specified team
              is in possession of the ball (1 if in possession, 0 otherwise).
    Notes:
        - The function assumes that the `helperfunctions.fetch_match_events` function
          is available and returns a DataFrame with columns `ball_owning_team`,
          `match_id`, and `timestamp`.
        - The input DataFrame is expected to be sorted by timestamp.
    Example:
        >>> changes = calculate_ball_possession(match_id=123, conn=db_conn, team_id=456)
        >>> print(changes.head())
    """

    # Fetch match events for the given match_id
    match_events = fetch_match_events(match_id, conn)

    
    # Initialize an empty list to store changes
    changes_list = []

    # Start with the first row
    previous_team = match_events.iloc[0]['ball_owning_team']
    previous_match_id = match_events.iloc[0]['match_id']
    previous_timestamp = match_events.iloc[0]['timestamp']

    # Add the first row as the starting point
    changes_list.append({
        'match_id': previous_match_id,
        'team_id': previous_team,
        'timestamp': previous_timestamp
    })

    # Iterate over the rows of match_events
    for _, row in match_events.iterrows():
        current_team = row['ball_owning_team']
        # Unused variable removed
        current_timestamp = row['timestamp']

        # Check if the ball_owning_team has changed
        if current_team != previous_team:
            # Append the change to the list
            changes_list.append({
                'match_id': previous_match_id,
                'team_id': current_team,
                'timestamp': current_timestamp
            })
            # Update the previous team
            previous_team = current_team

    # Create the changes dataframe
    changes = pd.DataFrame(changes_list)

    # Add ball_possession column
    changes['ball_possession'] = (changes['team_id'] == team_id).astype(int)

    # Add end_time column as the timestamp of the next row
    changes['end_time'] = changes['timestamp'].shift(-1)

    # Ensure timestamp and end_time are in datetime format, coercing errors to NaT
    changes['timestamp'] = pd.to_timedelta(changes['timestamp'])
    changes['end_time'] = pd.to_timedelta(changes['end_time'])

    # Calculate the time difference between timestamp and end_time
    changes['time_difference'] = changes['end_time'] - changes['timestamp']

    return changes

# def get_spadl_data(match_id, conn):
#     Query_spadl_data = f""" 
#     SELECT spa.game_id, period_id, seconds, spa.player_id, p.player_name, spa.team_id, t.team_name, start_x, end_x, start_y, end_y, action_type, result FROM spadl_actions spa
#     JOIN players p ON spa.player_id = p.player_id
#     JOIN teams t ON spa.team_id = t.team_id
#     WHERE spa.game_id ILIKE '%{match_id}%' AND spa.period_id = 1
#     ORDER BY seconds ASC;
#     """

#     spadl_data = pd.read_sql_query(Query_spadl_data, conn)

#     spadl_data_formatted = spadl_data.rename(columns={"action_type": "type_id"})

#     spadl_data_formatted["type_id"] = spadl_data_formatted["type_id"].astype(int)
#     spadl_data_formatted["result_id"] = spadl_data_formatted["result"].astype(int)
#     spadl_data_formatted["bodypart_id"] = spadl_data_formatted["bodypart"].astype(int)

#     spadl_data_df = spadl.add_names(spadl_data_formatted)

#     return spadl_data_df

def fetch_player_teams(team_id, conn):
    query = f'''
    SELECT p.player_id FROM players p
    WHERE p.team_id = '{team_id}' AND p.player_name != 'ball';
    '''
    home_players = pd.read_sql_query(query, conn)
    return home_players

def seconds_to_hms(seconds):
    try:
        total_seconds = int(float(seconds))
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        secs = total_seconds % 60
        return f"{hours:02}:{minutes:02}:{secs:02}"
    except (TypeError, ValueError):
        return "00:00:00"
    
def fetch_transitions(match_id, team_id, conn):
    query = f"""
    WITH action_changes AS (
        SELECT
            a.*,
            LAG(a.team_id) OVER (ORDER BY a.period_id, a.seconds, a.id) AS prev_team_id,
            LEAD(a.team_id) OVER (ORDER BY a.period_id, a.seconds, a.id) AS next_team_id
        FROM
            spadl_actions a
        WHERE
            a.game_id = '{match_id}'
    ),
    possession_markers AS (
        SELECT
            *,
            CASE WHEN prev_team_id IS NULL OR team_id != prev_team_id THEN 1 ELSE 0 END AS is_new_possession
        FROM
            action_changes
    ),
    possession_sequences AS (
        SELECT
            *,
            SUM(is_new_possession) OVER (ORDER BY period_id, seconds, id) AS possession_group
        FROM
            possession_markers
    ),
    possession_stats AS (
        SELECT
            possession_group,
            team_id,
            COUNT(*) AS action_count,
            MAX(id) AS last_action_id,
            MIN(id) AS first_action_id
        FROM
            possession_sequences
        GROUP BY
            possession_group, team_id
    )
    SELECT
        a.id AS action_id,
        a.game_id,
        a.period_id,
        a.seconds AS time_seconds,
        a.team_id AS team_losing_possession,
        a.next_team_id AS team_gaining_possession,
        a.action_type AS type_name,
        a.result AS result_name,
        ps.action_count AS consecutive_team_actions,
        a.start_x,
        a.start_y,
        a.end_x,
        a.end_y,
        a.id AS original_event_id,
        start_a.period_id AS start_period_id,
        start_a.seconds AS start_seconds
    FROM
        possession_sequences a
    JOIN
        possession_stats ps ON a.possession_group = ps.possession_group 
        AND a.team_id = ps.team_id
        AND a.id = ps.last_action_id
    JOIN
        spadl_actions start_a ON start_a.id = ps.first_action_id
    WHERE
        ps.action_count >= 3
        AND a.team_id != a.next_team_id
        AND a.next_team_id IS NOT NULL
        AND a.start_x < 50
        AND a.end_x > 50
        AND a.next_team_id = '{team_id}'
    ORDER BY
        a.period_id,
        a.seconds,
        a.id;
    """
    df = pd.read_sql_query(query, conn)

    transitions = []

    for index, row in df.iterrows():
        period = row['period_id']
        timestamp1 = seconds_to_hms(row['time_seconds'])
        timestamp2 = seconds_to_hms(row['start_seconds'])

        transitions.append({
            'period': period,
            'start_timestamp' : timestamp2,
            'end_timestamp' : timestamp1
        })
    return transitions
        


def visualise_important_moments(match_id, conn):
    query = f'''
        SELECT spa.* , m.home_team_id, m.away_team_id, me.ball_owning_team, t.team_name, p.player_name
        FROM spadl_actions spa
        JOIN matches m on spa.game_id = m.match_id
        JOIN matchevents me on m.match_id = me.match_id
        JOIN players p on spa.player_id = p.player_id
        JOIN teams t on p.team_id = t.team_id
        WHERE game_id = '{match_id}' AND start_x < 45 AND spa.period_id = 1
        ORDER BY seconds ASC
    '''
    
    important_moments_df= pd.read_sql_query(query, conn)
    important_moments_df = important_moments_df.drop_duplicates()
        
    correct_types = important_moments_df[important_moments_df["action_type"].isin(["0","1","21","11"])]
    correct_team = correct_types[correct_types["away_team_id"] == correct_types["team_id"]]
    fail = correct_team[correct_team["result"] == "0"]

    times_array = fail["seconds"]

    df_time = pd.DataFrame(times_array)

    df_time = df_time.sort_values('seconds', ascending=True)

    query_tracking = f"""
    SELECT pt.* , p.player_name, p.team_id, p.jersey_number, m.match_id FROM player_tracking pt
    JOIN players p on pt.player_id = p.player_id
    JOIN matches m on pt.game_id = m.match_id
    WHERE pt.game_id ILIKE '%{match_id}%' AND pt.period_id = 1
    ORDER BY timestamp ASC
    """

    df_tracking = pd.read_sql_query(query_tracking, conn)
    df_tracking['timestamp'] = pd.to_timedelta(df_tracking['timestamp']).dt.total_seconds()
    df_tracking['timestamp'] = df_tracking['timestamp'].astype('float64')


    ball_df = df_tracking[df_tracking['player_name'] == 'Ball']

    valid_times = []

    # Loop through important times (when opponent loses ball)
    for time in df_time['seconds']:
        initial_ball_pos = ball_df[ball_df['timestamp'] == time]

        if not initial_ball_pos.empty and initial_ball_pos.iloc[0]['x'] <= 50:
            ball_movement = ball_df[(ball_df['timestamp'] > time) & (ball_df['timestamp'] <= time + 10)]

            # Check if the ball ever crosses x = 50
            if not ball_movement[ball_movement['x'] > 50].empty:
                valid_times.append(time)

    # Create a filtered DataFrame with only the valid times
    filtered_df_time = df_time[df_time['seconds'].isin(valid_times)]
    filtered_df_time = filtered_df_time.drop_duplicates()

    time = filtered_df_time.iloc[0]['seconds']
    subset = df_tracking[((df_tracking['timestamp'] >= time -5) & (df_tracking['timestamp'] < time + 18))]
    subset

    colors = ["orange", "blue"]

    def plot_tracking_data(tracking_data):
        clear_output(wait=True)
        pitch = Pitch(pitch_color='grass', line_color='white', pitch_type='opta', pitch_length=105, pitch_width=68)
        fig, ax = pitch.draw(figsize=(12, 8))

        timestamp = tracking_data['timestamp'].iloc[0]
        team_colors = {team: colors[i % len(colors)] for i, team in enumerate(sorted(tracking_data['team_id'].unique()))}

        for _, row in tracking_data.iterrows():
            x, y = row['x'], row['y']
            player_name = row['player_name']
            jersey_no = row['jersey_number']

            if player_name == 'Ball':
                pitch.scatter(x, y, s=90, color='yellow', ax=ax, label='Ball')
            else:
                pitch.scatter(x, y, s=100, color=team_colors[row['team_id']], ax=ax, label=row['team_id'])
                ax.text(x + 2, y + 2, f"{player_name} ({jersey_no})", fontsize=8)

        ax.set_title(f'Player Positions at Event Timestamp: {timestamp}', fontsize=16)
        plt.tight_layout()
        plt.show()
    unique_frame_ids = subset['frame_id'].unique()
    
    for frame_id in unique_frame_ids:
        filtered_tracking_df = subset[subset['frame_id'] == frame_id]
        plot_tracking_data(filtered_tracking_df)

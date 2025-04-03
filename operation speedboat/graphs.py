import matplotlib as mpl
import pygame
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib.pyplot as plt
from mplsoccer import Pitch
from IPython.display import clear_output

#function to transform matplotlib to pygame comaptible stuff (basically magic)
def matplotlib_to_pygame_surface(fig):
    canvas = FigureCanvas(fig)
    canvas.draw()
    width, height = fig.get_size_inches() * fig.get_dpi()
    image = pygame.image.fromstring(canvas.tostring_argb(), (int(width), int(height)), 'ARGB')

    plt.close()
    return image

#denis graph 2 (No comments available as IDK wth this code is)
def SpiderChart_2T(ChartTitle, TeamNames, labels, t1Values, t2Values, value_range):
    num_vars = len(labels)
    angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()

    t1Values = t1Values + t1Values[:1]
    t2Values = t2Values + t2Values[:1]
    angles = angles + angles[:1]

    team1_color = "#4C4CBF"
    team2_color = "#BF4C4C"

    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True), facecolor='none')
    ax.set_facecolor('none')

    ax.plot(angles, t1Values, color=team1_color, linewidth=2, label=TeamNames[0])
    ax.fill(angles, t1Values, color=team1_color, alpha=0.25)

    ax.plot(angles, t2Values, color=team2_color, linewidth=2, label=TeamNames[1])
    ax.fill(angles, t2Values, color=team2_color, alpha=0.25)

    ax.set_thetagrids(np.degrees(angles[:-1]), labels)
    for label in ax.get_xticklabels():
        label.set_color("white")
    for label in ax.get_yticklabels():
        label.set_color("white")
    
    ax.grid(color='white', linestyle='--', linewidth=0.5, alpha=0.7)
    for spine in ax.spines.values():
        spine.set_color('white')
    
    ax.set_ylim(value_range[0], value_range[1])
    ax.set_title(ChartTitle, y=1.08, color='white')

    leg = ax.legend(loc='upper right', bbox_to_anchor=(0.1, 0.1), frameon=False)
    for text in leg.get_texts():
        text.set_color("white")

    return matplotlib_to_pygame_surface(fig)

#denis graph 1 (No comments available as IDK wth this code is)
def SpiderChart_1T(ChartTitle, TeamName, labels, t1Values, value_range, teamColor):
    num_vars = len(labels)
    angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()

    t1Values = t1Values + t1Values[:1]
    angles = angles + angles[:1]

    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True), facecolor='none')
    ax.set_facecolor('none')

    ax.plot(angles, t1Values, color=teamColor, linewidth=2, label=TeamName)
    ax.fill(angles, t1Values, color=teamColor, alpha=0.25)

    ax.set_thetagrids(np.degrees(angles[:-1]), labels)
    for label in ax.get_xticklabels():
        label.set_color("white")
    for label in ax.get_yticklabels():
        label.set_color("white")
    
    ax.grid(color='white', linestyle='--', linewidth=0.5, alpha=0.7)
    for spine in ax.spines.values():
        spine.set_color('white')
    
    ax.set_ylim(value_range[0], value_range[1])
    ax.set_title(ChartTitle, y=1.08, color='white')

    leg = ax.legend(loc='upper right', bbox_to_anchor=(0.1, 0.1), frameon=False)
    for text in leg.get_texts():
        text.set_color("white")
    
    return matplotlib_to_pygame_surface(fig)

#IDK if this is the correct one, but I copied it from denis his file, for more info please talk to him :D
def voronoi_graph(tracking_data):
        # Define pitch dimensions and colors
    pitch = Pitch(pitch_color='grass', line_color='white', pitch_type='opta')
    fig, ax = pitch.draw(figsize=(13, 8))
    
    # Extract timestamp and team names
    timestamp = tracking_data['timestamp'].iloc[0]
    team_names = tracking_data['team_id'].unique()
    colors = mpl.colors.TABLEAU_COLORS
    color_map = {team: color for team, color in zip(team_names, colors.values())}
    
    points = []
    colors = []
    
    # Plot player positions
    for _, row in tracking_data.iterrows():
        x = row['x']
        y = row['y']
        player_name = row['player_name']
        team_name = row['team_id']
        jersey_no = row['jersey_number']
        
        # Plot the ball
        if player_name == 'Ball':
            pitch.scatter(x, y, s=90, color='yellow', ax=ax, label='Ball')
        else:
            # Plot players
            pitch.scatter(x, y, s=100, color=color_map[team_name], ax=ax, label=team_name)
            points.append([x, y])
            colors.append(color_map[team_name])
        
        # Add player names (excluding the ball)
        if player_name != 'Ball':
            ax.text(x + 2, y + 2, f"{player_name} ({jersey_no})", fontsize=8)


    

    team1, team2 = pitch.voronoi(points, colors)

    t1 = pitch.polygon(team1, ax=ax, fc='#c34c45', ec='white', lw=3, alpha=0.4)
    t2 = pitch.polygon(team2, ax=ax, fc='#6f63c5', ec='white', lw=3, alpha=0.4)
    
    # Set title
    #ax.set_title(f'Player Positions and Voronoi Diagram at Event Timestamp: {timestamp}', fontsize=16)
    return matplotlib_to_pygame_surface(fig)

#draw the pitch on a specific timeStep (frame_ID), tbf I don;t even know
def pitch_graph(tracking_data):
    colors = ["red", "black"]
    clear_output(wait=True)

    # Define pitch dimensions and colors
    pitch = Pitch(pitch_color='grass', line_color='white', pitch_type='opta',
                  pitch_length=105, pitch_width=68)
    fig, ax = pitch.draw(figsize=(12, 8))

    # Extract timestamp
    timestamp = tracking_data['timestamp'].iloc[0]
    
    # Assign colors to teams based on sorted order
    team_names = sorted(tracking_data['team_id'].unique())  # Sort to maintain consistency
    team_colors = {team: colors[i % len(colors)] for i, team in enumerate(team_names)}

    # Plot player positions
    for _, row in tracking_data.iterrows():
        x, y = row['x'], row['y']
        player_name = row['player_name']
        team_name = row['team_id']
        jersey_no = row['jersey_number']

        # Plot the ball
        if player_name == 'Ball':
            pitch.scatter(x, y, s=90, color='yellow', ax=ax, label='Ball')
        else:
            # Plot players with consistent team colors
            pitch.scatter(x, y, s=100, color=team_colors[team_name], ax=ax, label=team_name)

        # Add player names (excluding the ball)
        if player_name != 'Ball':
            ax.text(x + 2, y + 2, f"{player_name} ({jersey_no})", fontsize=8)

    # Set title
    ax.set_title(f'Player Positions at Event Timestamp: {timestamp}', fontsize=16)
    plt.tight_layout()
    return matplotlib_to_pygame_surface(fig)


#idk how to call this function
def spadl_graph():
    return None

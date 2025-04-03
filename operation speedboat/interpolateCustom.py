import pandas as pd

def add_frames(added_frames, dataframe):
    added_time = 1 / (added_frames - 1)
    subset_copy = dataframe.copy()

    new_rows = []

    for i in subset_copy['timestamp'].unique():

        x1_list = subset_copy[subset_copy['timestamp'] == i][['x']].values
        if i + 1 in subset_copy['timestamp'].values:
            x2_list = subset_copy[subset_copy['timestamp'] == i + 1][['x']].values
        else:
            continue

        y1_list = subset_copy[subset_copy['timestamp'] == i][['y']].values
        if i + 1 in subset_copy['timestamp'].values:
            y2_list = subset_copy[subset_copy['timestamp'] == i + 1][['y']].values
        else:
            continue

        if i != subset_copy['timestamp'].unique()[-1]:
            for index in range(len(subset_copy['player_id'].unique())):
                x1 = x1_list[index].item()
                x2 = x2_list[index].item()

                y1 = y1_list[index].item()
                y2 = y2_list[index].item()

                deltax = (x2 - x1) / added_frames
                deltay = (y2 - y1) / added_frames

                current_time = i

                for j in range(added_frames - 1):

                    current_time = current_time + added_time
                    x1 = x1 + deltax
                    y1 = y1 + deltay

                    current_player_id = subset_copy['player_id'].unique()[index]
                    current_player = subset_copy[subset_copy['player_id'] == current_player_id]

                    current_frame = current_player['frame_id'].unique()[0]
                    current_name = current_player['player_name'].unique()[0]
                    current_num = current_player['jersey_number'].unique()[0]
                    current_team = current_player['team_id'].unique()[0]

                    new_rows.append([current_frame, current_time, current_player_id, x1, y1, current_num, current_name, current_team])
                    subset_copy.index = subset_copy.index + 1


    new_rows_df = pd.DataFrame(new_rows, columns=list(subset_copy.columns.values))
    subset_copy = pd.concat([subset_copy, new_rows_df], ignore_index=True)

    subset_copy.sort_values(by='timestamp', inplace=True)

    return subset_copy

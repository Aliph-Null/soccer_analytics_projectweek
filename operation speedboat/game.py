import pandas as pd
import pygame
from graphs import SpiderChart_1T, SpiderChart_2T, pitch_graph, voronoi_graph
from Python.helperfunctions import fetch_match_events, fetch_tracking_data, fetch_home_players

class PygameWindow:
    # init the window, just basic ass values
    def __init__(self, connect, title="speedboat", fullscreen=False):
        pygame.init()
        self.title = title
        self.connection = connect
        
        info = pygame.display.Info()
        self.width, self.height = info.current_w, info.current_h
        flags = pygame.FULLSCREEN if fullscreen else pygame.RESIZABLE
        
        self.screen = pygame.display.set_mode((info.current_w, info.current_h), flags)
        pygame.display.set_caption(self.title)
        
        self.running = True
        self.clock = pygame.time.Clock()
        self.font = pygame.font.Font(None, 22)

        # State variable to track if we are in the main menu, match view, or graph view
        self.view = "main"  # It can be "main", "graph", or "match"
        self.selected_match = None  # To store the currently selected match
        self.cached_data = {} #So I don't crash the DB X)

    def draw_button(self, text, x, y, width, height, color, hover_color, action=None):
        mouse = pygame.mouse.get_pos()
        click = pygame.mouse.get_pressed()
        
        button_rect = pygame.Rect(x, y, width, height)

        # change to hover_color when hovering over        
        if button_rect.collidepoint(mouse):
            pygame.draw.rect(self.screen, hover_color, button_rect)
            if click[0] and action:
                action()

        # normal color otherwise
        else:
            pygame.draw.rect(self.screen, color, button_rect)
        
        # colors and stuff
        text_surface = self.font.render(text, True, (0, 0, 0))
        text_rect = text_surface.get_rect(center=(x + width // 2, y + height // 2))
        self.screen.blit(text_surface, text_rect)

    # draws text on x and y coords must be given with function
    def draw_text(self, x, y, text, color=(255, 255, 255)):
        text_surface = self.font.render(text, True, color)
        text_rect = text_surface.get_rect(center=(x, y))
        self.screen.blit(text_surface, text_rect)

    # everything that needs to be shown on match thingy majyg (I am tired af :( )
    def display_graph(self, match_id, home_team, away_team):
        """ Display the new frame with teams' names """
        self.screen.fill((168, 213, 241))
        self.draw_text(self.width // 2, self.height // 9, f"Match id: {match_id}")
        self.draw_text(self.width // 2, self.height // 9 + 50, f"Home Team: {home_team}")
        self.draw_text(self.width // 2, self.height // 9 + 100, f"Away Team: {away_team}", color=(0, 255, 0))  # You can change the color if desired

        # campus
        # df_match_events = fetch_match_events(match_id, self.connection)
        # df_tracking = fetch_tracking_data(match_id, self.connection)
        
        # Ask denis for extra info on how to implement this because IDFK anymore man, I want to sleep but I CAN'T, and I WON'T!!!
        # values (we have to get those from db)
        labels = ["Short Passes %", "Medium Passes %", "Long Passes %", "Pass success rate %", "Time to first Pass (s)"]
        t1Values = [10, 70, 20, 58, 20]
        t2Values = [30, 20, 50, 53, 3]

        # generate the graphs(we need a better way to give the values to the graph functions because IDK what all of this means)
        image1 = SpiderChart_2T("Passes comparison", ["Belgium", "France"], labels, t1Values, t2Values, [0, 100])
        image2 = SpiderChart_1T("Passes", "Belgium", labels, t1Values, [0, 100], "#4CEF4C")
        # voronoi = voronoi_graph(tracking_data) #check this how I am goin to do that at campus (my enlgish is slowly fading...), mr dtark, i don't feel so goosd.

        # sizes of the graphs
        image1_rect = image1.get_rect(center=(self.width // 3, self.height // 3))
        image2_rect = image2.get_rect(center=(2 * self.width // 3, self.height // 3))

        # Blit images to the screen
        self.screen.blit(image2, image2_rect)
        self.screen.blit(image1, image1_rect)

        # Create a button to go back to the main screen
        button_width, button_height = 150, 60
        button_x = (self.width - button_width) // 2
        button_y = self.height - 100
        self.draw_button("Back", button_x, button_y, button_width, button_height, (200, 0, 0), (255, 0, 0), self.return_to_main)

        pygame.display.flip()
        
    def display_match(self, match_id):
        tracking_df = self.fetch_data_once(match_id).get('tracking_data')
        
        # frame_id1 = tracking_df['frame_id'].unique()[0] 
        # filtered_tracking_df1 = tracking_df[tracking_df['frame_id'] == frame_id1]
        # pitch = pitch_graph(filtered_tracking_df1)
        
        # image1_rect = pitch.get_rect(center=(self.width // 3, self.height // 3))
        # self.screen.blit(pitch, image1_rect)
        
        df_ball = tracking_df[tracking_df['player_id'] == 'ball']
        
        
        print(self.fetch_data_once(match_id).get('home_players_id'))
        #exit button
        button_width, button_height = 150, 60
        button_x = (self.width - button_width) // 2
        button_y = self.height - 100
        self.draw_button("Back", button_x, button_y, button_width, button_height, (200, 0, 0), (255, 0, 0), self.return_to_main)

        pygame.display.flip()
        
        
    # def generate_video_once():
        
        
        
    #Idk why I said please in the comment below, Just know I am running on 1 brainncell
    def fetch_data_once(self, match_id, thing):
        if match_id not in self.cached_data:
            
            if thing == 'events':
                # remove None and uncomment this please
                match_events = fetch_match_events(match_id, self.connection)
            tracking_data = fetch_tracking_data(match_id, self.connection)
            home_players_id = fetch_home_players(match_id, self.connection)
            #away_id = fetch_away_player(match_id, self.connection)    
            
            
            self.cached_data[match_id] = {
                'match_events': match_events,
                'tracking_data': tracking_data,
                'home_players': home_players_id
            }
        return self.cached_data[match_id]

    # return to the main function (I mean name says it all)
    def return_to_main(self):
        """ Function to return to the main menu """
        self.view = "main"
        self.selected_match = None  # Reset the selected match

    # toggles between match view, graph view, and main menu, also resets the selected match
    def toggle_views(self, match_id=None, home_team=None, away_team=None, view_type="main"):
        """ Toggle between the main menu, graph view, and match view """
        if view_type == "main":
            self.view = "main"
        #select match view en set selected match to the selected one
        elif view_type == "match":
            self.view = "match"
            self.selected_match = (match_id, home_team, away_team)
        
        #select graph view en set selected match to the selected one
        elif view_type == "graph":
            self.view = "graph"
            self.selected_match = (match_id, home_team, away_team)

    # main loop
    def run(self, games):
        # initial values for buttons
        button_width, button_height = 150, 60

        # if you want a quit button use these values, now it is just on escape button
        button_x = (self.width - button_width) // 2
        button_y = (self.height - 100)

        # initial values for match buttons
        match_button_h = 50
        vertical_spacing = 75
        match_button_w = 300
        match_pos_y = self.height // 2
        match_pos_x = (self.width - match_button_w) // 2

        # initial values for graph buttons
        graph_button_w = 100
        graph_pos_x = (self.width - graph_button_w) // 2 + 375
        
        while self.running:
            self.screen.fill((168, 213, 241))

            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    self.running = False
                # Check if the Escape key is pressed
                if event.type == pygame.KEYDOWN:
                    if event.key == pygame.K_ESCAPE:  # Escape key
                        self.running = False
            
            if self.view == "main":
                # Draw the main menu
                self.draw_text(self.width // 2, 100, "Please select a match to analyze!, watching the match takes time to load.")
                
                # loops for all matches
                for i, match_id in enumerate(games["match_id"]):
                    home_team = games[games["match_id"] == match_id]['home_team_name'].values[0]
                    away_team = games[games["match_id"] == match_id]['away_team_name'].values[0]
                    match_string = f"{home_team} vs {away_team}"

                    # Calculate the vertical position for each match
                    match_pos_y = 200 + i * vertical_spacing  # Adjust the y-position for each match 

                    # When a match button is clicked, show the match details
                    self.draw_button(match_string, match_pos_x, match_pos_y, match_button_w, match_button_h, (168, 177, 241), (156, 166, 235), 
                                     lambda match_id=match_id, home_team=home_team, away_team=away_team: self.toggle_views(match_id, home_team, away_team, view_type="match"))
                    
                    self.draw_button('graphs', graph_pos_x, match_pos_y, graph_button_w, match_button_h, (168, 177, 241), (156, 166, 235), 
                                     lambda match_id=match_id, home_team=home_team, away_team=away_team: self.toggle_views(match_id, home_team, away_team, view_type="graph"))

            # only shows when a graph is selected
            elif self.view == "graph" and self.selected_match:
                match_id, home_team, away_team = self.selected_match
                self.display_graph(match_id, home_team, away_team)
            
            elif self.view == "match" and self.selected_match:
                match_id, home_team, away_team = self.selected_match
                self.display_match(match_id)
            
            pygame.display.flip()
            self.clock.tick(60)

    def quit_game(self):
        self.running = False

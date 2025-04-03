import math
import pandas as pd
from Python.VisualisationTools import soccer_animation
import pygame

from Python.helperfunctions import calculate_ball_possession, fetch_match_events, fetch_tracking_data, fetch_player_teams, visualise_important_moments

from graphs import SpiderChart_1T, SpiderChart_2T, pitch_graph, voronoi_graph
from interpolateCustom import add_frames

class PygameWindow:
    def __init__(self, connect, title="speedboat", fullscreen=True):
        self.time = 20
        pygame.init()
        self.title = title
        self.connection = connect
        
        info = pygame.display.Info()
        self.width, self.height = info.current_w, info.current_h
        flags = pygame.FULLSCREEN if fullscreen else pygame.RESIZABLE
        
        self.screen = pygame.display.set_mode((self.width, self.height), flags)
        pygame.display.set_caption(self.title)
        
        self.running = True
        self.clock = pygame.time.Clock()
        # Default font is kept, but now we allow custom font in draw_text method
        self.font = pygame.font.Font(None, 22)

        # State variables for view and pagination
        self.view = "main"  # "main", "graph", or "match"
        self.selected_match = None  
        self.cached_data = {}
        self.current_page = 0
        self.items_per_page = 6
        
        self.frame = 0
        
        # Load and scale the background ball image to cover the entire screen
        try:
            original_ball_img = pygame.image.load("ball.png").convert_alpha()
            orig_rect = original_ball_img.get_rect()
            scale_factor = max(self.width / orig_rect.width, self.height / orig_rect.height)
            new_width = int(orig_rect.width * scale_factor)
            new_height = int(orig_rect.height * scale_factor)
            self.ball_img = pygame.transform.smoothscale(original_ball_img, (new_width, new_height))
        except Exception as e:
            print(f"Error loading ball.png: {e}")
            self.ball_img = None

    def scale_image_to_fit(self, image, max_width, max_height):
        width, height = image.get_size()
        scale_factor = min(max_width / width, max_height / height)
        if scale_factor < 1:  # Only scale down if needed
            new_width = int(width * scale_factor)
            new_height = int(height * scale_factor)
            return pygame.transform.smoothscale(image, (new_width, new_height))
        return image

    def set_fullscreen(self):
        """Explicitly set the display to full-screen mode using the screen's native resolution."""
        info = pygame.display.Info()
        self.width, self.height = info.current_w, info.current_h
        self.screen = pygame.display.set_mode((self.width, self.height), pygame.FULLSCREEN)

    def draw_button(self, text, x, y, width, height, color, hover_color, events, action=None):
        button_rect = pygame.Rect(x, y, width, height)
        mouse = pygame.mouse.get_pos()
        current_color = hover_color if button_rect.collidepoint(mouse) else color
        pygame.draw.rect(self.screen, current_color, button_rect)
        
        # Using the new draw_text method with custom font options if needed.
        self.draw_text(x + width // 2, y + height // 2, text, font_size=22, bold=False, color=(0, 0, 0))
        
        # Check for MOUSEBUTTONUP events over this button
        for event in events:
            if event.type == pygame.MOUSEBUTTONUP and button_rect.collidepoint(event.pos):
                if action:
                    action()

    def draw_text(self, x, y, text, font_size=22, bold=False, color=(255, 255, 255)):
        """
        Draw text on the screen at (x, y) with the option to specify font size and bold style.
        """
        # Create a new font with the specified size
        font = pygame.font.Font(None, font_size)
        font.set_bold(bold)
        text_surface = font.render(text, True, color)
        text_rect = text_surface.get_rect(center=(x, y))
        self.screen.blit(text_surface, text_rect)

    def display_graph(self, match_id, home_team, away_team, home_team_id, away_team_id, events):
        self.screen.fill((168, 213, 241))
        self.draw_text(self.width // 2, self.height // 9, f"Match id: {match_id}", font_size=28, bold=True, color=(16, 16, 16))
        self.draw_text(self.width // 2, self.height // 9 + 50, f"Home Team: {home_team}", font_size=28, bold=True, color=(77, 169, 77))
        self.draw_text(self.width // 2, self.height // 9 + 100, f"Away Team: {away_team}", font_size=28, bold=True, color=(169, 77, 77))

        # Use cached data once
        data = self.fetch_data_once(match_id)
        tracking_df = data.get('tracking_data')
        events_df = data.get('match_events')

        labels = ["Short Passes %", "Medium Passes %", "Long Passes %", "Pass success rate %", "Time to first Pass (s)"]
        t1Values = [10, 70, 20, 58, 20]
        t2Values = [30, 20, 50, 53, 3]

        image1 = SpiderChart_2T("Passes comparison", [home_team, away_team], labels, t1Values, t2Values, [0, 100])
        image2 = SpiderChart_1T("Passes", home_team, labels, t1Values, [0, 100], "#4CEF4C")

        # Define maximum dimensions for each graph (e.g. half the screen width minus a margin, and half the screen height)
        max_width = (self.width // 2 - 150) * 2
        max_height = (self.height // 2 - 150) * 2

        # Scale images if they exceed these dimensions
        image1 = self.scale_image_to_fit(image1, max_width, max_height)
        image2 = self.scale_image_to_fit(image2, max_width, max_height)

        image1_rect = image1.get_rect(center=(self.width // 4, self.height // 2))
        image2_rect = image2.get_rect(center=(self.width - self.width // 4, self.height // 2))

        self.screen.blit(image2, image2_rect)
        self.screen.blit(image1, image1_rect)

        # Back button for graph view
        button_width, button_height = 150, 60
        button_x = (self.width - button_width) // 2
        button_y = self.height - 100
        self.draw_button("Back", button_x, button_y, button_width, button_height, (200, 0, 0), (255, 0, 0), events, self.return_to_main)

        pygame.display.flip()
        
    def display_match(self, match_id, home_team_id, away_team_id, events):
        data = self.fetch_data_once(match_id)
        tracking_df = data.get('tracking_data')

        home_players = self.fetch_player_from_team(home_team_id)['player_id'].tolist()
        away_players = self.fetch_player_from_team(away_team_id)['player_id'].tolist()
            
        # df_ball = tracking_df[tracking_df['player_id'] == 'ball']
        # df_home = tracking_df[tracking_df['player_id'].isin(home_players)]
        # df_away = tracking_df[tracking_df['player_id'].isin(away_players)]

        if tracking_df["timestamp"].unique()[self.frame] < tracking_df["timestamp"].unique()[-1]:
            plot = pitch_graph(tracking_df[tracking_df['timestamp'] == tracking_df["timestamp"].unique()[self.frame]])
        
        max_width = (self.width // 2 - 150) * 2
        max_height = (self.height // 2 - 150) * 2
        image1 = self.scale_image_to_fit(plot, max_width, max_height)
        image1_rect = image1.get_rect(center=(self.width // 4, self.height // 2))
        
        self.screen.blit(image1, image1_rect)
        
        # Exit/back button
        button_width, button_height = 150, 60
        button_x = (self.width - button_width) // 2
        button_y = self.height - 100
        self.draw_button("Back", button_x, button_y, button_width, button_height, (200, 0, 0), (255, 0, 0), events, self.return_to_main)

        pygame.display.flip()
        
    def fetch_data_once(self, match_id):
        if match_id not in self.cached_data:

            # remove None and uncomment this please
            match_events = fetch_match_events(match_id, self.connection)
            tracking_data = fetch_tracking_data(match_id, self.connection)
            tracking_data['timestamp'] = pd.to_timedelta(tracking_data['timestamp']).dt.total_seconds()
            #tracking_data = tracking_data[((tracking_data['timestamp'] >= self.time -1) & (tracking_data['timestamp'] < self.time + 30))]
            #tracking_data = add_frames(10, tracking_data)

            self.cached_data[match_id] = {
                'match_events': match_events,
                'tracking_data': tracking_data,
            }
        return self.cached_data[match_id]

    def fetch_player_from_team(self, team_id):
        return fetch_player_teams(team_id, self.connection)
        
    def return_to_main(self):
        self.view = "main"
        self.selected_match = None

    def toggle_views(self, match_id=None, home_team=None, away_team=None, home_team_id=None, away_team_id=None, view_type="main"):
        if view_type == "main":
            self.view = "main"
        elif view_type == "match":
            self.view = "match"
            self.selected_match = (match_id, home_team, away_team, home_team_id, away_team_id)
        elif view_type == "graph":
            self.view = "graph"
            self.selected_match = (match_id, home_team, away_team, home_team_id, away_team_id)

    def run(self, games):
        button_width, button_height = 150, 60
        match_button_h = 50
        vertical_spacing = 75
        match_button_w = 300
        graph_button_w = 100

        self.set_fullscreen()

        while self.running:
            events = pygame.event.get()
            for event in events:
                if event.type == pygame.QUIT:
                    self.running = False
                if event.type == pygame.KEYDOWN and event.key == pygame.K_ESCAPE:
                    self.running = False
                # if event.type == pygame.VIDEORESIZE:
                #     # Reset to full-screen mode with original dimensions
                #     self.screen = pygame.display.set_mode((self.width, self.height), pygame.FULLSCREEN)
            
            self.screen.fill((168, 213, 241))
            
            if self.view == "main":
                # Draw the background ball image scaled to cover the screen
                if self.ball_img:
                    ball_rect = self.ball_img.get_rect(center=(0, self.height // 2))
                    self.screen.blit(self.ball_img, ball_rect)
                
                self.draw_text(self.width // 2, 100, "Please select a match to analyze! getting all the info takes a while", font_size=30, bold=True, color=(16, 16, 16))
                
                total_matches = len(games["match_id"])
                total_pages = math.ceil(total_matches / self.items_per_page)
                start_index = self.current_page * self.items_per_page
                end_index = start_index + self.items_per_page
                current_matches = games.iloc[start_index:end_index]

                match_pos_x = (self.width - match_button_w) // 2
                graph_pos_x = match_pos_x + match_button_w + 20

                for idx, row in enumerate(current_matches.itertuples()):
                    match_id = row.match_id
                    home_team = row.home_team_name
                    away_team = row.away_team_name
                    home_team_id = row.home_team_id
                    away_team_id = row.away_team_id
                    match_string = f"{home_team} vs {away_team}"
                    
                    match_pos_y = 200 + idx * vertical_spacing
                    
                    self.draw_button(
                        match_string, match_pos_x, match_pos_y, match_button_w, match_button_h, 
                        (168, 177, 241), (156, 166, 235), events, 
                        lambda m_id=match_id, h=home_team, a=away_team, hi=home_team_id, ai=away_team_id: self.toggle_views(m_id, h, a, hi, ai, view_type="match")
                    )
                    
                    self.draw_button(
                        "Graphs", graph_pos_x, match_pos_y, graph_button_w, match_button_h, 
                        (168, 177, 241), (156, 166, 235), events, 
                        lambda m_id=match_id, h=home_team, a=away_team, hi=home_team_id, ai=away_team_id: self.toggle_views(m_id, h, a, hi, ai, view_type="graph")
                    )
                
                # Pagination buttons
                pagination_y = self.height - 50
                if self.current_page > 0:
                    self.draw_button("Prev", 50, pagination_y, 100, 40, (200, 200, 200), (150, 150, 150), events, 
                                     lambda: self.change_page(-1))
                if self.current_page < total_pages - 1:
                    self.draw_button("Next", self.width - 150, pagination_y, 100, 40, (200, 200, 200), (150, 150, 150), events, 
                                     lambda: self.change_page(1))
            
            elif self.view == "graph" and self.selected_match:
                match_id, home_team, away_team, home_team_id, away_team_id = self.selected_match
                self.display_graph(match_id, home_team, away_team, home_team_id, away_team_id, events)
            
            elif self.view == "match" and self.selected_match:
                self.frame += 1
                match_id, home_team, away_team, home_team_id, away_team_id = self.selected_match
                self.display_match(match_id, home_team_id, away_team_id, events)
            
            pygame.display.flip()
            self.clock.tick(60)

    def change_page(self, delta):
        self.current_page += delta
        if self.current_page < 0:
            self.current_page = 0

    def quit_game(self):
        self.running = False

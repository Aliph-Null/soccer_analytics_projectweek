import pygame


class player_pos():
    def __init__(self, x, y, player_id, team, playarea, playsize, player_name="#na", size=9):
        self.x = x
        self.y = y
        self.team = team
        self.player_id = player_id
        self.size = size
        self.player_name = player_name
        self.scrn_pos = playarea
        self.scrn_size = playsize
        
    
    def draw(self, window, x_offset=0, y_offset=0):
        chunk = (self.scrn_size[0] / 100, self.scrn_size[1] / 100)
        color = (0, 0, 255)
        if self.team == 1:
            color = (255, 0, 0)
        elif self.team == 2:
            color = (255, 255, 0)
        pygame.draw.circle(window, color, (self.scrn_pos[0] + chunk[0] * (self.x + x_offset), self.scrn_pos[1] + chunk[1] * (self.y + y_offset)), self.size)
    
    
    def __str__(self):
        return f"Player {self.player_id} at ({self.x, self.y}) team {self.team}"
-- Foreign Key Constraints
-- Added: 2026-02-06
-- Pattern: NOT VALID first (instant), then VALIDATE (scans table but no lock)

-- FK: core.drives -> core.games
ALTER TABLE core.drives
  ADD CONSTRAINT fk_drives_game
  FOREIGN KEY (game_id) REFERENCES core.games(id) NOT VALID;
ALTER TABLE core.drives VALIDATE CONSTRAINT fk_drives_game;

-- FK: betting.lines -> core.games
ALTER TABLE betting.lines
  ADD CONSTRAINT fk_lines_game
  FOREIGN KEY (game_id) REFERENCES core.games(id) NOT VALID;
ALTER TABLE betting.lines VALIDATE CONSTRAINT fk_lines_game;

-- FK: metrics.pregame_win_probability -> core.games
ALTER TABLE metrics.pregame_win_probability
  ADD CONSTRAINT fk_pregame_wp_game
  FOREIGN KEY (game_id) REFERENCES core.games(id) NOT VALID;
ALTER TABLE metrics.pregame_win_probability VALIDATE CONSTRAINT fk_pregame_wp_game;

-- FK: stats.game_havoc -> core.games
ALTER TABLE stats.game_havoc
  ADD CONSTRAINT fk_game_havoc_game
  FOREIGN KEY (game_id) REFERENCES core.games(id) NOT VALID;
ALTER TABLE stats.game_havoc VALIDATE CONSTRAINT fk_game_havoc_game;

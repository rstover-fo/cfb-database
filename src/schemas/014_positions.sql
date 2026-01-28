-- Position reference table for grouping and analysis
-- Maps position codes to human-readable names and categorizations

CREATE TABLE IF NOT EXISTS ref.positions (
    id text PRIMARY KEY,
    name text NOT NULL,
    side text NOT NULL CHECK (side IN ('offense', 'defense', 'special_teams')),
    position_group text NOT NULL
);

COMMENT ON TABLE ref.positions IS 'Position reference table for categorization and grouping';
COMMENT ON COLUMN ref.positions.id IS 'Position abbreviation (e.g., QB, RB, WR)';
COMMENT ON COLUMN ref.positions.side IS 'Side of the ball: offense, defense, or special_teams';
COMMENT ON COLUMN ref.positions.position_group IS 'Position grouping for aggregate analysis';

INSERT INTO ref.positions (id, name, side, position_group) VALUES
    -- Offense
    ('QB', 'Quarterback', 'offense', 'passer'),
    ('RB', 'Running Back', 'offense', 'rusher'),
    ('FB', 'Fullback', 'offense', 'rusher'),
    ('WR', 'Wide Receiver', 'offense', 'receiver'),
    ('TE', 'Tight End', 'offense', 'receiver'),
    ('OT', 'Offensive Tackle', 'offense', 'lineman'),
    ('OG', 'Offensive Guard', 'offense', 'lineman'),
    ('OC', 'Center', 'offense', 'lineman'),
    ('OL', 'Offensive Line', 'offense', 'lineman'),
    ('C', 'Center', 'offense', 'lineman'),

    -- Defense
    ('DE', 'Defensive End', 'defense', 'dline'),
    ('DT', 'Defensive Tackle', 'defense', 'dline'),
    ('NT', 'Nose Tackle', 'defense', 'dline'),
    ('DL', 'Defensive Line', 'defense', 'dline'),
    ('EDGE', 'Edge Rusher', 'defense', 'dline'),
    ('ILB', 'Inside Linebacker', 'defense', 'linebacker'),
    ('OLB', 'Outside Linebacker', 'defense', 'linebacker'),
    ('MLB', 'Middle Linebacker', 'defense', 'linebacker'),
    ('LB', 'Linebacker', 'defense', 'linebacker'),
    ('CB', 'Cornerback', 'defense', 'db'),
    ('FS', 'Free Safety', 'defense', 'db'),
    ('SS', 'Strong Safety', 'defense', 'db'),
    ('S', 'Safety', 'defense', 'db'),
    ('DB', 'Defensive Back', 'defense', 'db'),

    -- Special Teams
    ('K', 'Kicker', 'special_teams', 'specialist'),
    ('P', 'Punter', 'special_teams', 'specialist'),
    ('LS', 'Long Snapper', 'special_teams', 'specialist'),
    ('PR', 'Punt Returner', 'special_teams', 'returner'),
    ('KR', 'Kick Returner', 'special_teams', 'returner'),

    -- Multi-position
    ('ATH', 'Athlete', 'offense', 'athlete'),
    ('APB', 'All-Purpose Back', 'offense', 'rusher'),
    ('H', 'H-Back', 'offense', 'receiver')
ON CONFLICT (id) DO NOTHING;

-- Index for position group queries
CREATE INDEX IF NOT EXISTS idx_positions_side ON ref.positions (side);
CREATE INDEX IF NOT EXISTS idx_positions_group ON ref.positions (position_group);

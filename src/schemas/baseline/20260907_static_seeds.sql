-- Static repository-owned seeds for an empty warehouse. No production row export.

-- Source: src/schemas/014_positions.sql
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

-- Source: src/schemas/017_era_reference.sql
INSERT INTO ref.eras (era_code, era_name, start_year, end_year, description) VALUES
    ('BCS', 'BCS Era', 2004, 2013, 'Bowl Championship Series, pre-playoff'),
    ('PLAYOFF_V1', 'Playoff V1', 2014, 2023, '4-team playoff, conference championship emphasis'),
    ('PORTAL_NIL', 'Portal/NIL Era', 2021, NULL, 'Transfer portal explosion, NIL deals reshape rosters'),
    ('PLAYOFF_V2', 'Playoff V2', 2024, NULL, '12-team playoff, expanded access');

-- Source: src/schemas/migrations/061_pff_tables.sql
INSERT INTO pff.team_map (pff_team_name, cfbd_school) VALUES
    ('AIR FORCE', 'Air Force'),
    ('AKRON', 'Akron'),
    ('ALABAMA', 'Alabama'),
    ('APP STATE', 'App State'),
    ('ARIZONA', 'Arizona'),
    ('ARIZONA ST', 'Arizona State'),
    ('ARK STATE', 'Arkansas State'),
    ('ARKANSAS', 'Arkansas'),
    ('ARMY', 'Army'),
    ('AUBURN', 'Auburn'),
    ('BALL ST', 'Ball State'),
    ('BAYLOR', 'Baylor'),
    ('BOISE ST', 'Boise State'),
    ('BOSTON COL', 'Boston College'),
    ('BOWL GREEN', 'Bowling Green'),
    ('BUFFALO', 'Buffalo'),
    ('BYU', 'BYU'),
    ('C MICHIGAN', 'Central Michigan'),
    ('CAL', 'California'),
    ('CHARLOTTE', 'Charlotte'),
    ('CINCINNATI', 'Cincinnati'),
    ('CLEMSON', 'Clemson'),
    ('COAST CAR', 'Coastal Carolina'),
    ('COLO STATE', 'Colorado State'),
    ('COLORADO', 'Colorado'),
    ('DELAWARE', 'Delaware'),
    ('DOMINION', 'Old Dominion'),
    ('DUKE', 'Duke'),
    ('E CAROLINA', 'East Carolina'),
    ('E MICHIGAN', 'Eastern Michigan'),
    ('FAU', 'Florida Atlantic'),
    ('FIU', 'Florida International'),
    ('FLORIDA', 'Florida'),
    ('FLORIDA ST', 'Florida State'),
    ('FRESNO ST', 'Fresno State'),
    ('GA SOUTHRN', 'Georgia Southern'),
    ('GA STATE', 'Georgia State'),
    ('GA TECH', 'Georgia Tech'),
    ('GEORGIA', 'Georgia'),
    ('HAWAII', 'Hawai''i'),
    ('HOUSTON', 'Houston'),
    ('ILLINOIS', 'Illinois'),
    ('INDIANA', 'Indiana'),
    ('IOWA', 'Iowa'),
    ('IOWA STATE', 'Iowa State'),
    ('JAMES MAD', 'James Madison'),
    ('JVILLE ST', 'Jacksonville State'),
    ('KANSAS', 'Kansas'),
    ('KANSAS ST', 'Kansas State'),
    ('KENNESAW', 'Kennesaw State'),
    ('KENT STATE', 'Kent State'),
    ('KENTUCKY', 'Kentucky'),
    ('LA LAFAYET', 'Louisiana'),
    ('LA MONROE', 'UL Monroe'),
    ('LA TECH', 'Louisiana Tech'),
    ('LIBERTY', 'Liberty'),
    ('LOUISVILLE', 'Louisville'),
    ('LSU', 'LSU'),
    ('MARSHALL', 'Marshall'),
    ('MARYLAND', 'Maryland'),
    ('MEMPHIS', 'Memphis'),
    ('MIAMI FL', 'Miami'),
    ('MIAMI OH', 'Miami (OH)'),
    ('MICH STATE', 'Michigan State'),
    ('MICHIGAN', 'Michigan'),
    ('MIDDLE TN', 'Middle Tennessee'),
    ('MINNESOTA', 'Minnesota'),
    ('MISS STATE', 'Mississippi State'),
    ('MISSOURI', 'Missouri'),
    ('MO STATE', 'Missouri State'),
    ('N CAROLINA', 'North Carolina'),
    ('N ILLINOIS', 'Northern Illinois'),
    ('N TEXAS', 'North Texas'),
    ('NAVY', 'Navy'),
    ('NC STATE', 'NC State'),
    ('NEBRASKA', 'Nebraska'),
    ('NEVADA', 'Nevada'),
    ('NEW MEX ST', 'New Mexico State'),
    ('NEW MEXICO', 'New Mexico'),
    ('NOTRE DAME', 'Notre Dame'),
    ('NWESTERN', 'Northwestern'),
    ('OHIO', 'Ohio'),
    ('OHIO STATE', 'Ohio State'),
    ('OKLA STATE', 'Oklahoma State'),
    ('OKLAHOMA', 'Oklahoma'),
    ('OLE MISS', 'Ole Miss'),
    ('OREGON', 'Oregon'),
    ('OREGON ST', 'Oregon State'),
    ('PENN STATE', 'Penn State'),
    ('PITTSBURGH', 'Pittsburgh'),
    ('PURDUE', 'Purdue'),
    ('RICE', 'Rice'),
    ('RUTGERS', 'Rutgers'),
    ('S ALABAMA', 'South Alabama'),
    ('S CAROLINA', 'South Carolina'),
    ('S DIEGO ST', 'San Diego State'),
    ('S JOSE ST', 'San José State'),
    ('SM HOUSTON', 'Sam Houston'),
    ('SMU', 'SMU'),
    ('SO MISS', 'Southern Miss'),
    ('STANFORD', 'Stanford'),
    ('SYRACUSE', 'Syracuse'),
    ('TCU', 'TCU'),
    ('TEMPLE', 'Temple'),
    ('TENNESSEE', 'Tennessee'),
    ('TEXAS', 'Texas'),
    ('TEXAS A&M', 'Texas A&M'),
    ('TEXAS ST', 'Texas State'),
    ('TEXAS TECH', 'Texas Tech'),
    ('TOLEDO', 'Toledo'),
    ('TROY', 'Troy'),
    ('TULANE', 'Tulane'),
    ('TULSA', 'Tulsa'),
    ('UAB', 'UAB'),
    ('UCF', 'UCF'),
    ('UCLA', 'UCLA'),
    ('UCONN', 'UConn'),
    ('UMASS', 'Massachusetts'),
    ('UNLV', 'UNLV'),
    ('USC', 'USC'),
    ('USF', 'South Florida'),
    ('UTAH', 'Utah'),
    ('UTAH ST', 'Utah State'),
    ('UTEP', 'UTEP'),
    ('UTSA', 'UTSA'),
    ('VA TECH', 'Virginia Tech'),
    ('VANDERBILT', 'Vanderbilt'),
    ('VIRGINIA', 'Virginia'),
    ('W KENTUCKY', 'Western Kentucky'),
    ('W MICHIGAN', 'Western Michigan'),
    ('W VIRGINIA', 'West Virginia'),
    ('WAKE', 'Wake Forest'),
    ('WASH STATE', 'Washington State'),
    ('WASHINGTON', 'Washington'),
    ('WISCONSIN', 'Wisconsin'),
    ('WYOMING', 'Wyoming'),
    -- Not part of the 136-row 2025-membership JSON: every real 2023 export
    -- (all five families) also carries W GEORGIA -- West Georgia, a
    -- Division II program in 2023 that PFF graded anyway. Verified against
    -- the live warehouse: CFBD's exact school string is 'West Georgia'
    -- (they appear in core.games as an FCS opponent from 2024). Without
    -- this row the whole 2023 backfill fails on UnmappedNamesError.
    ('W GEORGIA', 'West Georgia')
ON CONFLICT (pff_team_name) DO UPDATE SET cfbd_school = EXCLUDED.cfbd_school;

-- Source: src/schemas/migrations/seed/team_name_xwalk_seed_massey.sql
-- Generated by scripts/seed_team_xwalk.py at 2026-08-30T17:33:17.450157
-- Source: massey
-- Total source names: 131
-- Min confidence threshold: 0.85
-- REVIEW: inspect confidence scores and unmatched entries below before applying.

INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Air Force', 'Air Force') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Akron', 'Akron') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Alabama', 'Alabama') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Arizona', 'Arizona') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Arkansas', 'Arkansas') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Army', 'Army') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Auburn', 'Auburn') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'BYU', 'BYU') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Baylor', 'Baylor') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Boston College', 'Boston College') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Bowling Green', 'Bowling Green') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Buffalo', 'Buffalo') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'California', 'California') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Charlotte', 'Charlotte') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Cincinnati', 'Cincinnati') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Clemson', 'Clemson') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Colorado', 'Colorado') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Duke', 'Duke') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'East Carolina', 'East Carolina') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Florida', 'Florida') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Georgia', 'Georgia') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Georgia Tech', 'Georgia Tech') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Houston', 'Houston') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Illinois', 'Illinois') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Indiana', 'Indiana') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Iowa', 'Iowa') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'James Madison', 'James Madison') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Kansas', 'Kansas') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Kentucky', 'Kentucky') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'LSU', 'LSU') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Liberty', 'Liberty') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Louisiana', 'Louisiana') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Louisiana Tech', 'Louisiana Tech') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Louisville', 'Louisville') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Marshall', 'Marshall') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Maryland', 'Maryland') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Massachusetts', 'Massachusetts') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Memphis', 'Memphis') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Michigan', 'Michigan') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Minnesota', 'Minnesota') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Missouri', 'Missouri') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'NC State', 'NC State') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Navy', 'Navy') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Nebraska', 'Nebraska') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Nevada', 'Nevada') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'New Mexico', 'New Mexico') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'North Carolina', 'North Carolina') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'North Texas', 'North Texas') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Northwestern', 'Northwestern') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Notre Dame', 'Notre Dame') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Ohio', 'Ohio') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Oklahoma', 'Oklahoma') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Old Dominion', 'Old Dominion') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Oregon', 'Oregon') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Pittsburgh', 'Pittsburgh') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Purdue', 'Purdue') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Rice', 'Rice') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Rutgers', 'Rutgers') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'SMU', 'SMU') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'South Alabama', 'South Alabama') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'South Carolina', 'South Carolina') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'South Florida', 'South Florida') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Southern Miss', 'Southern Miss') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Stanford', 'Stanford') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Syracuse', 'Syracuse') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'TCU', 'TCU') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Temple', 'Temple') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Tennessee', 'Tennessee') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Texas', 'Texas') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Texas A&M', 'Texas A&M') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Texas Tech', 'Texas Tech') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Toledo', 'Toledo') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Troy', 'Troy') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Tulane', 'Tulane') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Tulsa', 'Tulsa') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'UAB', 'UAB') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'UCF', 'UCF') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'UCLA', 'UCLA') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'UNLV', 'UNLV') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'USC', 'USC') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'UTEP', 'UTEP') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Utah', 'Utah') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Vanderbilt', 'Vanderbilt') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Virginia', 'Virginia') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Virginia Tech', 'Virginia Tech') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Wake Forest', 'Wake Forest') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Washington', 'Washington') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'West Virginia', 'West Virginia') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Wisconsin', 'Wisconsin') ON CONFLICT (source, source_name) DO NOTHING;
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Wyoming', 'Wyoming') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Arizona St', 'Arizona State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Arkansas St', 'Arkansas State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Ball St', 'Ball State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Boise St', 'Boise State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Colorado St', 'Colorado State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Florida St', 'Florida State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Fresno St', 'Fresno State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Georgia St', 'Georgia State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.92 (fuzzy)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Hawaii', 'Hawai''i') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Iowa St', 'Iowa State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Kansas St', 'Kansas State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Miami OH', 'Miami (OH)') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Michigan St', 'Michigan State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Mississippi St', 'Mississippi State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'New Mexico St', 'New Mexico State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Ohio St', 'Ohio State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Oklahoma St', 'Oklahoma State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Oregon St', 'Oregon State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Penn St', 'Penn State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'San Diego St', 'San Diego State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.93 (fuzzy)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'San Jose St', 'San José State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Texas St', 'Texas State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Utah St', 'Utah State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEW: confidence 0.95 (abbrev)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Washington St', 'Washington State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Appalachian St', 'App State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'C Michigan', 'Central Michigan') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Coastal Car', 'Coastal Carolina') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Connecticut', 'UConn') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'E Michigan', 'Eastern Michigan') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'FL Atlantic', 'Florida Atlantic') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Florida Intl', 'Florida International') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Ga Southern', 'Georgia Southern') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Kent', 'Kent State') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'MTSU', 'Middle Tennessee') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
-- CFBD's plain 'Miami' is the Florida school; 'Miami (OH)' is the RedHawks.
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Miami FL', 'Miami') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'Mississippi', 'Ole Miss') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'N Illinois', 'Northern Illinois') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'ULM', 'UL Monroe') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'UT San Antonio', 'UTSA') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'W Michigan', 'Western Michigan') ON CONFLICT (source, source_name) DO NOTHING;
-- REVIEWED 2026-08-30: manual correction (fuzzy matcher missed; human-verified)
INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('massey', 'WKU', 'Western Kentucky') ON CONFLICT (source, source_name) DO NOTHING;
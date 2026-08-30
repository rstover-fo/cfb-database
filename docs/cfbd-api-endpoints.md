# CFBD API Endpoints Reference

**Total: 79 endpoints across 19 API categories**

Base URL: `https://api.collegefootballdata.com`
Auth: API Key required (Bearer token)
Docs: https://api.collegefootballdata.com/

## API Categories

### AdjustedMetricsApi (4 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/wepa/players/passing` | Opponent-adjusted player passing stats |
| GET | `/wepa/players/rushing` | Opponent-adjusted player rushing stats |
| GET | `/wepa/team/season` | Opponent-adjusted team season stats |
| GET | `/wepa/players/kicking` | Kicker PAAR (Points Added Above Replacement) |

### BettingApi (1 endpoint)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/lines` | Betting lines and spreads |

### CoachesApi (4 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/coaches` | Coach information and history |
| GET | `/coaches/profile` | Coach profile with canonical identity and career totals |
| GET | `/coaches/seasons` | Coach-season records with attributed results and team context |
| GET | `/coaches/tenures` | Continuous head-coaching tenures and their attributed records |

### ConferencesApi (3 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/conferences` | Conference information |
| GET | `/conferences/affiliations` | Historical team conference affiliations |
| GET | `/conferences/changes` | Team conference changes by season |

### DraftApi (3 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/draft/picks` | NFL draft picks |
| GET | `/draft/positions` | Draft position mappings |
| GET | `/draft/teams` | NFL teams for draft data |

### DrivesApi (1 endpoint)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/drives` | Drive-level game data |

### GamesApi (9 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/games` | Game schedule and results |
| GET | `/games/players` | Player stats by game |
| GET | `/games/teams` | Team stats by game |
| GET | `/games/media` | TV/streaming info |
| GET | `/games/weather` | Game weather conditions |
| GET | `/game/box/advanced` | Advanced box score |
| GET | `/calendar` | Season calendar/weeks |
| GET | `/records` | Team win/loss records |
| GET | `/scoreboard` | Live scoreboard |

### InfoApi (2 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/info` | Authenticated user's Patreon tier and remaining API calls |
| GET | `/info/usage` | Recent usage against the shared CFB/CBB call pool |

### MetricsApi (8 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/ppa/predicted` | Predicted points model |
| GET | `/ppa/games` | PPA by game |
| GET | `/ppa/players/games` | Player PPA by game |
| GET | `/ppa/players/season` | Player PPA by season |
| GET | `/ppa/teams` | Team PPA |
| GET | `/metrics/fg/ep` | Field goal expected points |
| GET | `/metrics/wp/pregame` | Pregame win probability |
| GET | `/metrics/wp` | In-game win probability |

### PassingApi (5 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/passing/plays` | Play-grain pass charting: air yards, pass depth/direction/location, YAC |
| GET | `/passing/players/games` | Player-game passing aggregates: attempts, air yards, aDOT, YAC |
| GET | `/passing/teams/games` | Team-game passing aggregates (offense/defense) |
| GET | `/passing/players/season` | Player season passing aggregates |
| GET | `/passing/teams/season` | Team season passing aggregates (offense/defense) |

### PlayersApi (5 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/player/search` | Search players by name |
| GET | `/player/usage` | Player usage rates |
| GET | `/player/returning` | Returning production |
| GET | `/player/portal` | Transfer portal entries |
| GET | `/player/season/overview` | Player season overview: box score, usage, PPA |

### PlayoffsApi (3 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/playoffs/cfp` | Complete College Football Playoff bracket for a season |
| GET | `/playoffs/cfp/games` | College Football Playoff matchups for a season |
| GET | `/playoffs/cfp/participants` | College Football Playoff participants for a season |

### PlaysApi (5 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/plays` | Play-by-play data |
| GET | `/plays/types` | Play type classifications |
| GET | `/plays/stats` | Player-play associations |
| GET | `/plays/stats/types` | Play stat type definitions |
| GET | `/live/plays` | Live play-by-play |

### RankingsApi (1 endpoint)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/rankings` | Poll rankings (AP, Coaches, CFP) |

### RatingsApi (7 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/ratings/sp` | SP+ ratings |
| GET | `/ratings/sp/conferences` | Conference SP+ |
| GET | `/ratings/srs` | Simple Rating System |
| GET | `/ratings/srs/expanded` | Expanded SRS ratings, including FCS teams |
| GET | `/ratings/elo` | Elo ratings |
| GET | `/ratings/fpi` | ESPN FPI ratings |
| GET | `/ratings/core` | CORE (Context & Opponent-Relative Efficiency) ratings, 2016+ |

### RecruitingApi (3 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/recruiting/players` | Player recruiting rankings |
| GET | `/recruiting/teams` | Team recruiting rankings |
| GET | `/recruiting/groups` | Recruiting by position group |

### StatsApi (8 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/stats/season` | Team season stats |
| GET | `/stats/season/advanced` | Advanced team season stats |
| GET | `/stats/player/season` | Player season stats |
| GET | `/stats/player/success` | Player passing/rushing success rates by season |
| GET | `/stats/player/success/game` | Player passing/rushing success rates by game |
| GET | `/stats/game/advanced` | Advanced game stats |
| GET | `/stats/game/havoc` | Havoc stats by game |
| GET | `/stats/categories` | Available stat categories |

### TeamsApi (6 endpoints)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/teams` | Team info and conference history |
| GET | `/teams/fbs` | FBS teams only |
| GET | `/teams/matchup` | Historical matchup records |
| GET | `/teams/ats` | Against-the-spread records |
| GET | `/roster` | Team rosters |
| GET | `/talent` | 247 Talent Composite |

### VenuesApi (1 endpoint)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/venues` | Stadium information |

## Data Availability Notes

- **Historical depth varies by endpoint**:
  - Games/scores: back to 1869
  - Play-by-play: ~2004+
  - Recruiting: ~2000+
  - Advanced metrics: ~2014+
  - Passing charting (air yards, aDOT, depth/direction/location, YAC): 2025+

- **API rate limits**: account tier and monthly call budget are configured in
  `.dlt/config.toml`, not hardcoded here -- access policies change. See that
  file for the current budget this warehouse runs against.

## Key Entities for Schema Design

1. **Reference/Dimension Tables**: teams, conferences, venues, coaches, draft_positions, draft_teams, play_types, stat_categories
2. **Fact Tables**: games, plays, drives, player_stats, team_stats, betting_lines, recruiting
3. **Metrics/Ratings**: sp_ratings, elo, fpi, srs, ppa_metrics

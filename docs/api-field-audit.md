# CFBD API Field Audit

> Sprint 0.2 — Resolved PK ambiguities by calling live API endpoints

## transfer_portal (`/player/portal`)

**API response fields**: `season`, `firstName`, `lastName`, `position`, `origin`, `destination`, `transferDate`, `rating`, `stars`, `eligibility`

**No `player_id` field.** The API does not return a player ID for transfer portal entries.

**Correct PK**: `["season", "first_name", "last_name"]` (matching code in `recruiting.py:101`)

**Action**: Update `endpoints.py:170` from `["player_id", "season"]` to `["season", "first_name", "last_name"]`

**Risk**: Name collisions (two players with same name in same season). Low probability but possible. Could add `origin` to PK as tiebreaker.

---

## draft_picks (`/draft/picks`)

**API response fields**: `collegeAthleteId`, `nflAthleteId`, `collegeId`, `collegeTeam`, `collegeConference`, `nflTeamId`, `nflTeam`, `year`, `overall`, `round`, `pick`, `name`, `position`, `height`, `weight`, `preDraftRanking`, `preDraftPositionRanking`, `preDraftGrade`, `hometownInfo`

**Both `collegeAthleteId` and `overall` are present.** `collegeAthleteId` is the unique player identifier. `overall` is the draft pick number (1-based, unique per year).

**Best PK**: `["college_athlete_id", "year"]` — stable identifier, survives if pick numbers get corrected.

**Alternative PK**: `["year", "overall"]` — also unique, simpler but less semantically meaningful.

**Action**: Update code in `draft.py` to use `["college_athlete_id", "year"]` (matching config). The field comes through as `college_athlete_id` after dlt snake_case normalization.

---

## lines (`/lines`)

**API response structure**: Nested — top-level has game metadata (`id`, `season`, `homeTeam`, etc.) with a `lines` array containing provider-specific odds.

**Top-level fields**: `id`, `season`, `seasonType`, `week`, `startDate`, `homeTeamId`, `homeTeam`, `homeConference`, `homeClassification`, `homeScore`, `awayTeamId`, `awayTeam`, `awayConference`, `awayClassification`, `awayScore`, `lines[]`

**Each `lines[]` item**: `provider`, `spread`, `formattedSpread`, `spreadOpen`, `overUnder`, `overUnderOpen`, `homeMoneyline`, `awayMoneyline`

**Natural key after flattening**: `["id", "provider"]` — game `id` + provider name uniquely identifies each line.

**Current code** (`betting.py`): Flattens nested structure and uses `["game_id", "provider"]`.

**Current config** (`endpoints.py`): Uses `["id"]` — wrong, because flattening creates multiple rows per game.

**Action**: Update `endpoints.py:177` to `["id", "provider"]`. Also verify code maps `id` correctly (the top-level `id` is the game ID).

---

## player_season_stats (`/stats/player/season`)

**API response fields**: `season`, `playerId`, `player`, `position`, `team`, `conference`, `category`, `statType`, `stat`

**Both `category` and `statType` are present.** `category` is the broad category (e.g., "passing"), `statType` is the specific stat (e.g., "YDS", "TD", "INT").

**Correct PK**: `["player_id", "season", "category", "stat_type"]` — a player has multiple statTypes within each category.

**Current code** (`stats.py:71`): Uses `["player_id", "season", "category"]` — **WRONG**, missing `stat_type`. A QB will have passing YDS, TD, INT, COMP, ATT all with category="passing". Without `stat_type` in the PK, only the last stat per category survives the merge.

**Current config** (`endpoints.py:99`): Uses `["player_id", "season", "stat_type"]` — **ALSO WRONG**, missing `category`. Different categories can have the same statType name.

**Action**: Update both to `["player_id", "season", "category", "stat_type"]`.

**Data quality impact**: Existing `player_season_stats` table likely has only ~1 row per player/season/category instead of ~5-10 (one per statType). Need to reload after fixing PK.

---

## coaches (existing bug — no API call needed)

**Config** (`endpoints.py:44`): `["first_name", "last_name", "seasons"]` — `seasons` is a JSONB array, cannot be PK.

**Code** (`reference.py:76`): `["first_name", "last_name"]` — correct.

**Action**: Update config to `["first_name", "last_name"]`.

**Risk**: Two coaches with identical first+last name. Low probability in practice. Could add a tiebreaker but the API doesn't provide a coach ID.

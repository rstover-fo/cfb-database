# Ratings Backfill Design

**Goal:** Expand ratings coverage from 2020-2025 to 2004-2025 for all rating types.

## Current State

| Table | Rows | Coverage |
|-------|------|----------|
| sp_ratings | 800 | 2020-2025 |
| elo_ratings | 791 | 2020-2025 |
| fpi_ratings | 791 | 2020-2025 |
| srs_ratings | 1,258 | 2020-2025 |
| sp_conference_ratings | 0 | (table missing) |

## API Availability

Tested API endpoints for historical data:

| Rating | Earliest Year | Notes |
|--------|---------------|-------|
| SP+ | 2004 | Full coverage |
| Elo | 2004 | Full coverage |
| SRS | 2004 | Full coverage |
| FPI | 2005 | No data for 2004 |
| SP+ Conference | 2004 | Not yet tested |

## Implementation

### Step 1: Update Year Range Config

File: `src/pipelines/config/years.py`

```python
# Change from:
"ratings": YearRange(start=2015, end=2026),

# To:
"ratings": YearRange(start=2004, end=2026),
```

### Step 2: Run Backfill

```bash
cd /Users/robstover/Development/personal/cfb-database
python -m src.pipelines.run --source ratings --mode backfill --years 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019
```

### Step 3: Verify Coverage

```sql
SELECT 'sp_ratings' as tbl, COUNT(*), MIN(year), MAX(year) FROM ratings.sp_ratings
UNION ALL SELECT 'elo_ratings', COUNT(*), MIN(year), MAX(year) FROM ratings.elo_ratings
UNION ALL SELECT 'srs_ratings', COUNT(*), MIN(year), MAX(year) FROM ratings.srs_ratings
UNION ALL SELECT 'fpi_ratings', COUNT(*), MIN(year), MAX(year) FROM ratings.fpi_ratings
UNION ALL SELECT 'sp_conference_ratings', COUNT(*), MIN(year), MAX(year) FROM ratings.sp_conference_ratings;
```

## Expected Outcome

| Table | Est. Rows | Coverage |
|-------|-----------|----------|
| sp_ratings | ~2,800 | 2004-2025 |
| elo_ratings | ~2,800 | 2004-2025 |
| fpi_ratings | ~2,600 | 2005-2025 |
| srs_ratings | ~4,000 | 2004-2025 |
| sp_conference_ratings | ~200 | 2004-2025 |

## API Cost

- 4 rating endpoints × 16 years = 64 calls
- Plus sp_conference_ratings: +16 calls
- **Total: ~80 API calls**

## Error Handling

- FPI 2004 returns empty - code handles gracefully (yields nothing)
- Existing merge disposition ensures idempotent loads
- No schema changes needed - dlt handles table creation

## Rollback

If issues occur, data is additive (merge disposition). No rollback needed - existing 2020-2025 data remains intact.

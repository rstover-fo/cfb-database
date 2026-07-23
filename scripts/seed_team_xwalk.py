#!/usr/bin/env python3
"""Generate reviewable seed SQL for ref.team_name_xwalk (T8).

External data sources spell team names their own way ("Ohio St", "Miami OH",
"ULM"); the warehouse uses exact CFBD full names ("Ohio State", "Miami (OH)",
"Louisiana Monroe"). This script generates SQL seed data mapping source names to
CFBD canonical names, reviewable before application to the database.

The script never applies anything to the DB; it only writes SQL to a file and
prints a summary. Matching uses three tiers:
  1. Exact match after normalize_name → confidence 1.0
  2. Exact match after normalize_name + expand_abbrevs → confidence 0.95
  3. Best difflib.SequenceMatcher.ratio() → its ratio as confidence

Outputs are sorted by source name (deterministic), with confidence comments on
fuzzy matches and commented-out INSERTs for unmatched names below the threshold.

Usage:
    python scripts/seed_team_xwalk.py --source massey --from-fixture \\
        --teams-file canonical_names.txt --out seed.sql

    python scripts/seed_team_xwalk.py --source sbr --names-file raw_names.txt \\
        --teams-file canonical_names.txt --min-confidence 0.90

    python scripts/seed_team_xwalk.py --source massey --from-fixture \\
        (no --teams-file: tries live DB)
"""

import argparse
import csv
import difflib
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures" / "flatfiles"


def normalize_name(name: str) -> str:
    """Canonicalize a source spelling: trim, collapse whitespace, lowercase.

    Reused from src.pipelines.utils.team_xwalk.normalize_name.
    """
    return " ".join(name.strip().split()).casefold()


ABBREV_RULES = [
    # Order matters: check dotted version first, then undotted
    (" st.", " state"),
    (" st", " state"),
    (" univ.", " university"),
    (" univ", " university"),
    ("&amp;", "&"),
]


def expand_abbrevs(name: str) -> str:
    """Apply additional abbreviation expansions after normalization.

    Handles:
      - " st" / " st." → " state"
      - " univ" / " univ." → " university"
      - "&amp;" → "&"
      - Parenthetical state qualifiers: "miami oh"/"miami-ohio" → "miami (oh)"

    Pure function; operates on already-normalized names.
    """
    expanded = name
    # Apply simple abbreviation rules.
    # Strategy: replace abbreviations in a careful order to avoid double-replacement.
    # For abbreviations ending in punctuation (like " st."), replace first.
    # For abbreviations without punctuation, use negative lookahead to avoid
    # matching partial words.

    # Handle "&amp;" specially (not a word-boundary issue)
    expanded = expanded.replace("&amp;", "&")

    # Handle " st." before " st" to avoid double-replacement
    expanded = re.sub(r" st\.$", " state", expanded)  # at end of string
    expanded = re.sub(r" st\. ", " state ", expanded)  # in middle

    # Handle " st" (not preceded by a letter to avoid matching " state")
    expanded = re.sub(r" st$", " state", expanded)  # at end of string
    expanded = re.sub(r" st([^a-z])", r" state\1", expanded)  # followed by non-letter

    # Handle " univ." and " univ" similarly
    expanded = re.sub(r" univ\.$", " university", expanded)
    expanded = re.sub(r" univ\. ", " university ", expanded)
    expanded = re.sub(r" univ$", " university", expanded)
    expanded = re.sub(r" univ([^a-z])", r" university\1", expanded)

    # Handle parenthetical state qualifiers: miami oh/miami-ohio → miami (oh)
    # State qualifier mappings: full name → 2-letter abbreviation
    state_qualifiers = {
        "alabama": "al",
        "alaska": "ak",
        "arizona": "az",
        "arkansas": "ar",
        "california": "ca",
        "colorado": "co",
        "connecticut": "ct",
        "delaware": "de",
        "florida": "fl",
        "georgia": "ga",
        "hawaii": "hi",
        "idaho": "id",
        "illinois": "il",
        "indiana": "in",
        "iowa": "ia",
        "kansas": "ks",
        "kentucky": "ky",
        "louisiana": "la",
        "maine": "me",
        "maryland": "md",
        "massachusetts": "ma",
        "michigan": "mi",
        "minnesota": "mn",
        "mississippi": "ms",
        "missouri": "mo",
        "montana": "mt",
        "nebraska": "ne",
        "nevada": "nv",
        "new hampshire": "nh",
        "new jersey": "nj",
        "new mexico": "nm",
        "new york": "ny",
        "north carolina": "nc",
        "north dakota": "nd",
        "ohio": "oh",
        "oklahoma": "ok",
        "oregon": "or",
        "pennsylvania": "pa",
        "rhode island": "ri",
        "south carolina": "sc",
        "south dakota": "sd",
        "tennessee": "tn",
        "texas": "tx",
        "utah": "ut",
        "vermont": "vt",
        "virginia": "va",
        "washington": "wa",
        "west virginia": "wv",
        "wisconsin": "wi",
        "wyoming": "wy",
    }

    # Also check for 2-letter abbreviations
    state_abbrev_map = {v: v for v in state_qualifiers.values()}

    # Combine both forms for checking
    all_forms = {**{k: v for k, v in state_qualifiers.items()}, **state_abbrev_map}

    for form, abbrev in sorted(all_forms.items(), key=lambda x: -len(x[0])):
        # Match patterns like " ohio" or "-ohio" followed by end of string
        # Convert to " (oh)"
        # Try space separator first
        pattern_space = r" " + re.escape(form) + r"$"
        if re.search(pattern_space, expanded):
            expanded = re.sub(pattern_space, f" ({abbrev})", expanded)
            continue

        # Try dash separator
        pattern_dash = r"-" + re.escape(form) + r"$"
        if re.search(pattern_dash, expanded):
            expanded = re.sub(pattern_dash, f" ({abbrev})", expanded)

    return expanded


def match_team(
    source_name: str, canonical_names: list[str], min_confidence: float = 0.85
) -> tuple[str | None, float, str]:
    """Match a source team name to a canonical CFBD name.

    Returns (cfbd_name, confidence, match_type) where:
      - cfbd_name: the matched canonical name, or None if below threshold
      - confidence: 1.0 (exact), 0.95 (abbrev), or difflib ratio (fuzzy)
      - match_type: "exact" | "abbrev" | "fuzzy" | "unmatched"

    Tries three tiers:
      1. Exact match after normalize_name on both sides → 1.0
      2. Exact match after normalize_name + expand_abbrevs → 0.95
      3. Best difflib.SequenceMatcher.ratio() → its ratio (usually < 0.9)

    Returns only the best match at the highest tier.
    """
    norm_source = normalize_name(source_name)

    # Tier 1: exact match after basic normalization
    for cfbd_name in canonical_names:
        if normalize_name(cfbd_name) == norm_source:
            return (cfbd_name, 1.0, "exact")

    # Tier 2: exact match after additional abbreviation expansion
    expanded_source = expand_abbrevs(norm_source)
    for cfbd_name in canonical_names:
        expanded_cfbd = expand_abbrevs(normalize_name(cfbd_name))
        if expanded_cfbd == expanded_source:
            return (cfbd_name, 0.95, "abbrev")

    # Tier 3: best difflib.SequenceMatcher ratio
    best_match = None
    best_ratio = 0.0
    for cfbd_name in canonical_names:
        ratio = difflib.SequenceMatcher(None, expanded_source, expand_abbrevs(normalize_name(cfbd_name))).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = cfbd_name

    if best_match and best_ratio >= min_confidence:
        return (best_match, best_ratio, "fuzzy")

    if best_match and best_ratio > 0:
        # Return the match even if below threshold, so we can comment it out
        return (best_match, best_ratio, "unmatched")

    return (None, 0.0, "unmatched")


def escape_sql(s: str) -> str:
    """Escape single quotes in a SQL string literal."""
    return s.replace("'", "''")


def load_names_from_massey_fixture() -> list[str]:
    """Extract team names from the Massey fixture CSV.

    Parses the fixture leniently, skipping the preamble and systems legend,
    extracting team names from the matrix section.
    """
    fixture_path = FIXTURES_DIR / "massey_compare_sample.csv"
    if not fixture_path.exists():
        raise FileNotFoundError(f"Massey fixture not found: {fixture_path}")

    names = []
    with open(fixture_path) as f:
        lines = f.readlines()

    # The fixture header is at line 17 (0-indexed line 16), data starts at line 19 (0-indexed 18)
    # We look for the line that starts with "Team, Conf" to find the header
    header_line_idx = None
    for i, line in enumerate(lines):
        if line.startswith("Team,"):
            header_line_idx = i
            break

    if header_line_idx is None:
        raise ValueError("Could not find Massey fixture header row")

    # Data starts right after the blank line following the header
    data_start = header_line_idx + 2
    for line in lines[data_start:]:
        line = line.strip()
        if not line:
            continue
        # First field is the team name (padded to 18 chars in the fixture)
        parts = line.split(",")
        if parts:
            team_name = parts[0].strip()
            if team_name and team_name != "Team":  # Skip header if it repeats
                names.append(team_name)

    return names


def load_names_from_sbr_fixture() -> list[str]:
    """Extract team names from the SBR fixture XLSX.

    Uses openpyxl to read the Team column (column D, 0-indexed 3).
    """
    try:
        import openpyxl
    except ImportError:
        raise ImportError("openpyxl is required for --from-fixture with --source sbr")

    fixture_path = FIXTURES_DIR / "sbr_sample_synthetic.xlsx"
    if not fixture_path.exists():
        raise FileNotFoundError(f"SBR fixture not found: {fixture_path}")

    names = []
    wb = openpyxl.load_workbook(fixture_path)
    ws = wb.active

    # Skip header row, iterate from row 2
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row and len(row) > 3:
            team_name = row[3]  # Team column is index 3 (0-indexed)
            if team_name:
                names.append(str(team_name))

    return names


def load_names_from_file(path: str) -> list[str]:
    """Load team names from a text file (one per line)."""
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


def load_canonical_names_from_db() -> list[str]:
    """Load canonical CFBD team names from the database.

    Requires DB credentials from dlt secrets or env vars (same as compute_house_elo.py).
    """
    try:
        from src.pipelines.utils.load_ledger import get_db_url
    except ImportError:
        raise ImportError("Cannot import get_db_url from src.pipelines.utils.load_ledger")

    import psycopg2

    db_url = get_db_url()
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT school FROM ref.teams ORDER BY school")
            rows = cur.fetchall()
            return [row[0] for row in rows]
    finally:
        conn.close()


def generate_seed_sql(
    source: str,
    source_names: list[str],
    canonical_names: list[str],
    min_confidence: float = 0.85,
) -> tuple[str, int, int, int]:
    """Generate SQL INSERT statements for the crosswalk.

    Returns (sql_text, exact_count, fuzzy_count, unmatched_count).
    """
    lines = []

    # Header comment
    now = datetime.now().isoformat()
    lines.append(f"-- Generated by scripts/seed_team_xwalk.py at {now}")
    lines.append(f"-- Source: {source}")
    lines.append(f"-- Total source names: {len(set(source_names))}")
    lines.append(f"-- Min confidence threshold: {min_confidence}")
    lines.append("-- REVIEW: inspect confidence scores and unmatched entries below before applying.")
    lines.append("")

    # Collect results by match type
    exact = []
    fuzzy = []
    unmatched = []

    for source_name in sorted(set(source_names)):
        cfbd_name, confidence, match_type = match_team(
            source_name, canonical_names, min_confidence
        )

        if match_type == "exact":
            exact.append((source_name, cfbd_name, confidence))
        elif match_type == "fuzzy" or (match_type == "abbrev"):
            fuzzy.append((source_name, cfbd_name, confidence))
        else:  # unmatched
            unmatched.append((source_name, cfbd_name, confidence))

    # Output exact matches
    for source_name, cfbd_name, confidence in sorted(exact):
        source_escaped = escape_sql(source_name)
        cfbd_escaped = escape_sql(cfbd_name)
        lines.append(
            f"INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('{source}', '{source_escaped}', '{cfbd_escaped}') ON CONFLICT (source, source_name) DO NOTHING;"
        )

    # Output fuzzy matches with confidence comments
    for source_name, cfbd_name, confidence in sorted(fuzzy):
        source_escaped = escape_sql(source_name)
        cfbd_escaped = escape_sql(cfbd_name)
        conf_str = f"{confidence:.2f}"
        match_type_str = "abbrev" if confidence == 0.95 else "fuzzy"
        lines.append(f"-- REVIEW: confidence {conf_str} ({match_type_str})")
        lines.append(
            f"INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('{source}', '{source_escaped}', '{cfbd_escaped}') ON CONFLICT (source, source_name) DO NOTHING;"
        )

    # Output unmatched as commented-out INSERTs
    for source_name, cfbd_name, confidence in sorted(unmatched):
        source_escaped = escape_sql(source_name)
        cfbd_escaped = escape_sql(cfbd_name) if cfbd_name else "??UNKNOWN??"
        conf_str = f"{confidence:.2f}" if cfbd_name else "no candidate"
        lines.append(f"-- UNMATCHED (best: {cfbd_name} @ {conf_str})")
        lines.append(
            f"-- INSERT INTO ref.team_name_xwalk (source, source_name, cfbd_name) VALUES ('{source}', '{source_escaped}', '{cfbd_escaped}') ON CONFLICT (source, source_name) DO NOTHING;"
        )

    sql = "\n".join(lines)
    return sql, len(exact), len(fuzzy), len(unmatched)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate seed SQL for ref.team_name_xwalk (manual review required)"
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=["massey", "sbr"],
        help="Data source name",
    )
    parser.add_argument(
        "--names-file",
        type=str,
        help="Text file with source team names (one per line)",
    )
    parser.add_argument(
        "--from-fixture",
        action="store_true",
        help="Extract source names from the fixture for this source",
    )
    parser.add_argument(
        "--teams-file",
        type=str,
        help="Text file with canonical CFBD team names (one per line)",
    )
    parser.add_argument(
        "--out",
        type=str,
        help="Output SQL file path (default: src/schemas/migrations/seed/team_name_xwalk_seed_<source>.sql)",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.85,
        help="Minimum confidence threshold for fuzzy matches (default: 0.85)",
    )

    args = parser.parse_args()

    # Load source names
    if args.from_fixture:
        logger.info(f"Loading source names from {args.source} fixture...")
        if args.source == "massey":
            source_names = load_names_from_massey_fixture()
        elif args.source == "sbr":
            source_names = load_names_from_sbr_fixture()
        else:
            raise ValueError(f"Unknown source: {args.source}")
    elif args.names_file:
        logger.info(f"Loading source names from {args.names_file}...")
        source_names = load_names_from_file(args.names_file)
    else:
        parser.error("Must provide either --from-fixture or --names-file")

    logger.info(f"Loaded {len(set(source_names))} distinct source names")

    # Load canonical names
    if args.teams_file:
        logger.info(f"Loading canonical names from {args.teams_file}...")
        canonical_names = load_names_from_file(args.teams_file)
    else:
        logger.info("Loading canonical names from database...")
        try:
            canonical_names = load_canonical_names_from_db()
        except Exception as e:
            parser.error(
                f"Cannot load canonical names from DB and --teams-file not provided: {e}"
            )

    logger.info(f"Loaded {len(canonical_names)} canonical team names")

    # Generate SQL
    logger.info("Matching source names to canonical names...")
    sql, exact_count, fuzzy_count, unmatched_count = generate_seed_sql(
        args.source, source_names, canonical_names, args.min_confidence
    )

    # Write output
    if args.out:
        out_path = Path(args.out)
    else:
        out_path = (
            PROJECT_ROOT
            / "src"
            / "schemas"
            / "migrations"
            / "seed"
            / f"team_name_xwalk_seed_{args.source}.sql"
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(sql)
    logger.info(f"Wrote seed SQL to {out_path}")

    # Print summary line
    print(
        f"XWALK_SEED source={args.source} names={len(set(source_names))} exact={exact_count} fuzzy={fuzzy_count} unmatched={unmatched_count} out={out_path}"
    )


if __name__ == "__main__":
    main()

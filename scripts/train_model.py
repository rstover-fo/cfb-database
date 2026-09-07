#!/usr/bin/env python3
"""Train the ``fitted_v1`` walk-forward margin + win-prob model.

``fitted_v1`` (docs/brainstorms/2026-07-21-team-week-feature-design.md, section
2) is a ridge linear **margin** model (normal equations, intercept unpenalized)
plus a logistic **win-prob** model (IRLS) with **Platt** calibration, fit on the
15-feature home-minus-away diff vector (+ intercept) built from
``features.team_week``. Targets: ``y_margin = home_points - away_points`` and
``y_win = 1[home_points > away_points]``.

Architecture mirrors the other compute_*.py scripts: everything above
``# --- I/O layer ---`` is pure math -- plain numpy arrays / dicts, no DB -- so
it is fully unit-testable without a database (see tests/test_fitted_model.py).
scripts/score_fitted.py imports the vectorization + transform helpers
(``build_feature_vector``, ``standardize``, ``sigmoid``, ``platt_transform``,
the ``FEATURE_NAMES``/``TEAM_WEEK_SOURCE_COLUMNS`` constants) from here so train
and score share one implementation of the feature contract.

Walk-forward protocol (design section 3): expanding window, minimum 3 train
seasons. For each score season ``S`` in ``--seasons START END`` (default
2018..2025) we train on seasons ``2015..S-1`` and append one immutable candidate
to ``features.training_fits``. Training changes the selected deployment only
when ``--promote`` is supplied with a nonempty ``--promotion-reason``.
The imputation means (section 2b) and z-score stats (section 2c) are computed on
the TRAIN window only and frozen in the metadata row -- scoring never recomputes
them, which is what makes the NULL-imputation leak-free.

Usage:
    python scripts/train_model.py                     # walk-forward 2018..2025
    python scripts/train_model.py --seasons 2020 2025 # score-season range

Each fit prints a machine-readable gate line:
    FITTED_GATE train_through={s} n_train={n} margin_train_mae={a:.3f} \
        winprob_train_brier={b:.4f} platt_a={pa:.4f} platt_b={pb:.4f}
"""

import argparse
import inspect
import logging
import math
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import numpy as np

from scripts.training_manifest import (
    build_training_manifest,
    manifests_match_for_freshness,
    source_text_fingerprint,
)
from scripts.training_registry import (
    import_legacy_fits,
    insert_fit,
    promote_fit,
    selected_fits,
)
from scripts.training_registry import (
    load_fit as load_registry_fit,
)
from src.pipelines.game_identity import eligible_game_sql
from src.pipelines.season_lifecycle import season_is_final

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MODEL_VERSION = "fitted_v1"

# Feature availability starts 2015 (design section 3); the expanding train
# window always begins here, so the earliest score season 2018 trains on
# {2015, 2016, 2017}.
TRAIN_START_SEASON = 2015
MIN_TRAIN_SEASONS = 3
DEFAULT_SCORE_START = 2018
DEFAULT_SCORE_END = 2025

# --- Hyperparameters (documented tunables; frozen per fit in model_metadata) ---
# Margin ridge penalty (design section 2, alpha ~= 5-10). Applied to every
# penalized column; the intercept is exempt. Features are z-scored so this is a
# scale-free shrinkage. Recorded as model_metadata.ridge_alpha.
RIDGE_ALPHA = 7.0
# IRLS Hessian ridge stabilizer for the logistic win-prob fit. Small on purpose:
# the design is standardized (unit-scale), so a light penalty barely biases the
# coefficients while keeping (X^T W X + alpha*P) well-conditioned. Chosen >=
# 0.001 so it survives model_metadata.winprob_ridge_alpha's NUMERIC(8,3)
# precision (1e-4 would round to 0.000). Recorded as winprob_ridge_alpha.
WINPROB_ALPHA = 1e-3

# Numerical-stability constants for the logistic core.
_MAX_LOGIT = 35.0  # clip |logit| so exp() never overflows; keeps p off exact 0/1
_W_MIN = 1e-6  # floor on IRLS weights p*(1-p) so the Hessian stays PD


# =============================================================================
# Feature contract -- ordered vector, source columns, masks (design section 2a).
# =============================================================================

INTERCEPT = "intercept"
NEUTRAL_SITE = "neutral_site"

# The 13 home-minus-away diff features, in design section 2a order (design-
# matrix indices 2..14), each mapped to its single features.team_week source
# column. adj_epa_net and the other transparency-only team_week columns are
# deliberately NOT here (net is an exact off-def combo -- design section 2a).
DIFF_FEATURE_COLUMNS: list[tuple[str, str]] = [
    ("d_elo", "elo_pregame"),
    ("d_adj_epa_off", "adj_epa_off"),
    ("d_adj_epa_def", "adj_epa_def"),
    ("d_off_epa_per_play", "off_epa_per_play"),
    ("d_def_epa_per_play_allowed", "def_epa_per_play_allowed"),
    ("d_off_success_rate", "off_success_rate"),
    ("d_def_success_rate_allowed", "def_success_rate_allowed"),
    ("d_off_explosiveness_rate", "off_explosiveness_rate"),
    ("d_off_plays_per_game", "off_plays_per_game"),
    ("d_havoc_rate_defense", "havoc_rate_defense"),
    ("d_havoc_rate_offense_allowed", "havoc_rate_offense_allowed"),
    ("d_returning_ppa_pct", "returning_ppa_pct"),
    ("d_preseason_sp_rating", "preseason_sp_rating"),
    # Migration 042 -- preseason columns that cleared the section 2.5 screen
    # (plus blue_chip_pipeline, a recorded override at +0.0782 against an 0.08
    # floor). These are the only features in the vector that carry information
    # in WEEK 1: design doc section 1i leaves all seven season-to-date columns
    # and both havoc columns NULL before a team has played, so their diffs are
    # exactly zero and the week-1 prediction previously collapsed to Elo,
    # prior SP+ and returning production alone.
    ("d_recruiting_points_3yr", "recruiting_points_3yr"),
    ("d_blue_chip_pipeline", "blue_chip_pipeline"),
    # Migration 046 -- REPLACES d_hc_first_year, keeping the vector at 20.
    # The first-year penalty is not a penalty for changing coaches: split by the
    # incoming coach's career record it sits entirely on unproven hires
    # (-0.1844), while proven hires screen as a powered null (+0.0096, p=0.73).
    # The flat binary's -0.1548 was those two averaged together. Both halves of
    # a split-window re-run agree. hc_first_year stays populated in
    # features.team_week; it just no longer enters the fit.
    ("d_hc_first_year_unproven", "hc_first_year_unproven"),
    # Migration 047 -- opposite signs, so they are two terms and not one net.
    # draft_picks_3yr +0.0834 (picks PRODUCED S-1..S-3, survives the recruiting
    # control), draft_departures -0.0925 (picks LOST in year S). Both rejections
    # before the 2000-2019 backfill were measured on fabricated zeros.
    ("d_draft_picks_3yr", "draft_picks_3yr"),
    ("d_draft_departures", "draft_departures"),
    ("d_prior_def_line_yards", "prior_def_line_yards"),
    ("d_prior_def_stuff_rate", "prior_def_stuff_rate"),
    # Migration 048 -- starter-pack plan U1-U4 (screened, then gated). Season-
    # to-date offensive points per drive, the sole survivor of the six
    # 2026-07-28 drive/trajectory candidates: v1 screen +0.0700 (below the 0.08
    # floor), v2 model-margin-control screen +0.0901 (p=5.7e-12, floor pass).
    # U4's isolated walk-forward gate (workflow run 30413381476, 2018-2025,
    # n=21,815 held-out games) then PASSED against the production baseline:
    # margin_mae 14.6600 vs 14.9078 (improves), brier 0.173669 vs 0.180879
    # (improves), ats_hit_rate 0.5029 vs 0.4899 (improves). Adopted per KTD2 --
    # every train_through_season vintage is retrained by the next
    # --refit-if-stale run.
    ("d_off_ppd", "off_ppd"),
]


# Fixed design-matrix column order (index = position): intercept, neutral_site,
# then the 21 diffs. 23 features + unpenalized intercept (was 15 before
# migration 042, 20 before 047, 21 before 048's off_ppd -- see
# docs/brainstorms/2026-07-21-team-week-feature-design.md for the full
# migration history and each column's screened/gated numbers).
#
# CHANGING THIS LIST INVALIDATES EVERY STORED FIT. score_fitted.load_fit builds
# its coefficient vector by name lookup over FEATURE_NAMES, so a fit written
# before a column was added raises KeyError rather than degrading gracefully.
# Every train_through_season vintage has to be retrained in the same deploy that
# ships the column -- there is no partial-rollout path.
FEATURE_NAMES: list[str] = [INTERCEPT, NEUTRAL_SITE] + [name for name, _ in DIFF_FEATURE_COLUMNS]

INTERCEPT_IDX = 0
NEUTRAL_SITE_IDX = 1

# The features.team_week source columns a diff feature reads -- also the columns
# that get a frozen imputation mean (design section 2b). score_fitted imports
# this to build its SELECT column list so the two never drift.
TEAM_WEEK_SOURCE_COLUMNS: list[str] = [col for _, col in DIFF_FEATURE_COLUMNS]


def penalty_mask() -> np.ndarray:
    """Ridge penalty mask over FEATURE_NAMES: 1.0 for every penalized column,
    0.0 for the unpenalized intercept (design section 2a). ``neutral_site`` IS
    penalized (only the intercept is exempt)."""
    mask = np.ones(len(FEATURE_NAMES), dtype=np.float64)
    mask[INTERCEPT_IDX] = 0.0
    return mask


# =============================================================================
# Pure math core -- no I/O, no DB, unit-tested directly.
# =============================================================================


def sigmoid(z: np.ndarray | float) -> np.ndarray | float:
    """Numerically safe logistic. Clips the argument to +/-_MAX_LOGIT so exp()
    cannot overflow and probabilities never reach exactly 0 or 1."""
    z = np.clip(z, -_MAX_LOGIT, _MAX_LOGIT)
    return 1.0 / (1.0 + np.exp(-z))


def platt_transform(logit: float, a: float, b: float) -> float:
    """Calibrated probability from a raw logit: sigmoid(a*logit + b)."""
    return float(sigmoid(a * logit + b))


def _impute_value(value, mean_c) -> float:
    """One side's value for a source column, imputed to the frozen train-window
    league mean when NULL (design section 2b), then coerced to float. If the
    column was entirely NULL over the train window (mean_c is None) fall back to
    0.0 -- the standardized-diff mean-centering makes 0 the neutral choice."""
    if value is not None:
        return float(value)
    if mean_c is not None:
        return float(mean_c)
    return 0.0


def build_feature_vector(
    game_row: dict, home_tw: dict, away_tw: dict, feature_means: dict
) -> np.ndarray:
    """Raw (pre-standardization) design row for one game, in FEATURE_NAMES order.

    ``game_row`` supplies the game-level ``neutral_site``. ``home_tw`` / ``away_tw``
    are the home/away ``features.team_week`` source-column dicts. ``feature_means``
    is ``{team_week_column: mean_c}`` (design section 2b): a NULL home- or away-
    side value is imputed to ``mean_c`` **before** differencing, i.e. the missing
    side is treated as a league-average team. Returns a length-``len(FEATURE_NAMES)``
    numpy array; standardization (section 2c) is applied separately by
    ``standardize`` so the imputation/diff step stays inspectable on its own.
    """
    x = np.empty(len(FEATURE_NAMES), dtype=np.float64)
    x[INTERCEPT_IDX] = 1.0
    x[NEUTRAL_SITE_IDX] = 1.0 if game_row.get("neutral_site") else 0.0
    for offset, (_feat_name, col) in enumerate(DIFF_FEATURE_COLUMNS):
        mean_c = feature_means.get(col)
        home_val = _impute_value(home_tw.get(col), mean_c)
        away_val = _impute_value(away_tw.get(col), mean_c)
        x[2 + offset] = home_val - away_val
    return x


def standardize(X: np.ndarray, diff_means: dict, diff_stds: dict) -> np.ndarray:
    """Z-score the standardized diff features of ``X`` using frozen train-window
    stats (design section 2c). ``intercept`` and ``neutral_site`` pass through
    unchanged. A zero (or missing) std maps that column to 0.0 -- a constant
    feature carries no signal. Accepts a single row (1-D) or a design matrix
    (2-D) and returns the same shape; the input is never mutated.
    """
    arr = np.asarray(X, dtype=np.float64)
    single = arr.ndim == 1
    Z = np.atleast_2d(arr).astype(np.float64, copy=True)
    for i, feat_name in enumerate(FEATURE_NAMES):
        if feat_name in (INTERCEPT, NEUTRAL_SITE):
            continue
        mean = float(diff_means[feat_name])
        std = float(diff_stds[feat_name])
        if std > 0.0:
            Z[:, i] = (Z[:, i] - mean) / std
        else:
            Z[:, i] = 0.0
    return Z[0] if single else Z


def compute_feature_means(team_week_rows: list[dict]) -> dict:
    """Frozen imputation means (design section 2b step 1): the mean of each
    ``TEAM_WEEK_SOURCE_COLUMNS`` value over the given team-week rows (both home
    and away sides of the TRAIN games), ignoring NULLs. A column that is NULL in
    every row maps to None (``_impute_value`` then falls back to 0.0)."""
    means: dict = {}
    for col in TEAM_WEEK_SOURCE_COLUMNS:
        vals = [float(r[col]) for r in team_week_rows if r.get(col) is not None]
        means[col] = (sum(vals) / len(vals)) if vals else None
    return means


def compute_diff_stats(X_raw: np.ndarray) -> tuple[dict, dict]:
    """Per-column mean/std over the imputed (pre-standardization) TRAIN design
    matrix, for the standardized diff features only (design section 2c). Uses
    population std (ddof=0), so re-standardizing this same matrix reproduces
    unit variance exactly. Returns ``(diff_means, diff_stds)`` keyed by
    ``feature_name``."""
    X_raw = np.asarray(X_raw, dtype=np.float64)
    diff_means: dict = {}
    diff_stds: dict = {}
    for i, feat_name in enumerate(FEATURE_NAMES):
        if feat_name in (INTERCEPT, NEUTRAL_SITE):
            continue
        col = X_raw[:, i]
        diff_means[feat_name] = float(col.mean())
        diff_stds[feat_name] = float(col.std())
    return diff_means, diff_stds


def ridge_fit(X: np.ndarray, y: np.ndarray, alpha: float, penalize_mask: np.ndarray) -> np.ndarray:
    """Ridge regression via the normal equations
    ``(X^T X + alpha * diag(penalize_mask)) beta = X^T y``.

    ``penalize_mask`` is 1.0 for penalized columns and 0.0 for the intercept, so
    the intercept is fit unpenalized (design section 2a). Solved with
    ``numpy.linalg.solve``; ``alpha > 0`` on the penalized columns keeps the
    system positive definite even when X's columns are collinear.
    """
    X = np.asarray(X, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    penalize_mask = np.asarray(penalize_mask, dtype=np.float64)
    xtx = X.T @ X
    xty = X.T @ y
    return np.linalg.solve(xtx + alpha * np.diag(penalize_mask), xty)


def _penalized_nll(
    X: np.ndarray, y: np.ndarray, beta: np.ndarray, alpha: float, penalize_mask: np.ndarray
) -> float:
    """Ridge-penalized logistic negative log-likelihood (the IRLS objective)."""
    p = np.clip(sigmoid(X @ beta), _W_MIN, 1.0 - _W_MIN)
    ll = float(np.sum(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))
    penalty = 0.5 * alpha * float(np.sum(penalize_mask * beta * beta))
    return -ll + penalty


def irls_logistic(
    X: np.ndarray,
    y: np.ndarray,
    alpha: float,
    penalize_mask: np.ndarray,
    max_iter: int = 25,
    tol: float = 1e-8,
) -> np.ndarray:
    """Ridge-penalized logistic regression by Newton/IRLS.

    Minimizes the penalized NLL with gradient ``X^T (p - y) + alpha*P*beta`` and
    Hessian ``X^T W X + alpha*P`` (``W = diag(p*(1-p))``, ``P = diag(penalize_mask)``).
    The IRLS weights are floored at ``_W_MIN`` so the Hessian stays positive
    definite (with the ridge term) even for near-separable data; probabilities
    are clipped for stability. Converges on ``max|delta beta| < tol`` and logs
    iterations. On non-convergence returns the lowest-NLL beta seen, with a
    warning, rather than a diverged final step.
    """
    X = np.asarray(X, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    penalize_mask = np.asarray(penalize_mask, dtype=np.float64)
    P = np.diag(penalize_mask)

    beta = np.zeros(X.shape[1], dtype=np.float64)
    best_beta = beta.copy()
    best_nll = _penalized_nll(X, y, beta, alpha, penalize_mask)
    converged = False

    for iteration in range(1, max_iter + 1):
        p = sigmoid(X @ beta)
        w = np.clip(p * (1.0 - p), _W_MIN, None)
        grad = X.T @ (p - y) + alpha * (penalize_mask * beta)
        hess = (X.T * w) @ X + alpha * P
        try:
            delta = np.linalg.solve(hess, grad)
        except np.linalg.LinAlgError:
            logger.warning(
                "irls_logistic: singular Hessian at iter %d; returning best beta", iteration
            )
            return best_beta

        beta = beta - delta
        nll = _penalized_nll(X, y, beta, alpha, penalize_mask)
        if nll < best_nll:
            best_nll = nll
            best_beta = beta.copy()

        max_step = float(np.max(np.abs(delta)))
        logger.debug("irls_logistic iter=%d max|delta|=%.3e nll=%.6f", iteration, max_step, nll)
        if max_step < tol:
            converged = True
            break

    if not converged:
        logger.warning(
            "irls_logistic did not converge in %d iterations; returning best beta", max_iter
        )
        return best_beta
    return beta


def platt_fit(logits: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    """Platt scaling: a 1-D logistic regression of ``y`` on the raw ``logits``,
    fit by the same IRLS on a 2-column design ``[1, logit]`` (design section 3
    step 7). Returns ``(a, b)`` so the calibrated probability is
    ``sigmoid(a*logit + b)``. The fit is unpenalized (alpha=0) -- Platt is a
    2-parameter recalibration, not something we want shrunk."""
    logits = np.asarray(logits, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    design = np.column_stack([np.ones_like(logits), logits])
    beta = irls_logistic(design, y, alpha=0.0, penalize_mask=np.zeros(2, dtype=np.float64))
    b = float(beta[0])
    a = float(beta[1])
    return a, b


def build_design(
    games: list[dict], feature_means: dict
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Vectorize game dicts into ``(X_raw, y_margin, y_win)`` (pre-standardization).

    Each game dict needs ``neutral_site``, ``home_points``, ``away_points`` and
    the ``home_tw`` / ``away_tw`` team-week source-column dicts. Pure (numpy +
    dicts only), so the whole train/score path is testable without a DB.
    """
    x_rows = []
    y_margin = []
    y_win = []
    for g in games:
        x_rows.append(build_feature_vector(g, g["home_tw"], g["away_tw"], feature_means))
        margin = float(g["home_points"] - g["away_points"])
        y_margin.append(margin)
        y_win.append(1.0 if g["home_points"] > g["away_points"] else 0.0)
    return (
        np.array(x_rows, dtype=np.float64),
        np.array(y_margin, dtype=np.float64),
        np.array(y_win, dtype=np.float64),
    )


def collect_team_week_rows(games: list[dict]) -> list[dict]:
    """Every team-week row referenced by ``games`` (both sides), for the frozen
    imputation-mean pass (design section 2b, "both home and away sides")."""
    rows: list[dict] = []
    for g in games:
        rows.append(g["home_tw"])
        rows.append(g["away_tw"])
    return rows


# =============================================================================
# --- I/O layer --- (thin: fetch team_week+games, drive the math, persist fits)
# =============================================================================


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Copied from scripts/compute_predictions.py's get_db_url pattern (each
    compute_*.py script keeps its own copy rather than importing across scripts
    for this one utility).
    """
    import os

    import dlt

    url = None
    try:
        creds = dlt.secrets.get("destination.postgres.credentials")
        if creds:
            url = str(creds)
    except Exception:
        pass

    if not url:
        url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")

    if not url:
        raise RuntimeError(
            "No database URL found. Set destination.postgres.credentials in "
            ".dlt/secrets.toml or SUPABASE_DB_URL environment variable."
        )

    if "options=" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}options=-c%20statement_timeout%3D0"

    return url


def _train_games_query() -> str:
    """SELECT completed games in the given seasons joined to both team-week
    sides. Column list is built from TEAM_WEEK_SOURCE_COLUMNS so it never drifts
    from the feature contract."""
    home_cols = ",\n           ".join(f"h.{c} AS home_{c}" for c in TEAM_WEEK_SOURCE_COLUMNS)
    away_cols = ",\n           ".join(f"a.{c} AS away_{c}" for c in TEAM_WEEK_SOURCE_COLUMNS)
    return f"""
        SELECT g.id AS game_id, g.season, g.season_type, g.week,
               g.neutral_site, g.home_points, g.away_points,
           {home_cols},
           {away_cols}
        FROM core.games g
        JOIN features.team_week h
          ON h.game_id = g.id AND h.team = g.home_team
        JOIN features.team_week a
          ON a.game_id = g.id AND a.team = g.away_team
        WHERE g.season = ANY(%s)
          AND {eligible_game_sql("g.id")}
          AND COALESCE(g.completed, false)
          AND g.home_points IS NOT NULL
          AND g.away_points IS NOT NULL
        ORDER BY g.season, g.week, g.id
    """


def fetch_games(conn, seasons: list[int]) -> list[dict]:
    """Completed games for the seasons, each with both team-week source-column
    dicts split out into ``home_tw`` / ``away_tw`` (design section 3 step 2)."""
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_train_games_query(), (list(seasons),))
        raw = cur.fetchall()

    games: list[dict] = []
    for r in raw:
        games.append(
            {
                "game_id": r["game_id"],
                "season": r["season"],
                "season_type": r["season_type"],
                "week": r["week"],
                "neutral_site": r["neutral_site"],
                "home_points": r["home_points"],
                "away_points": r["away_points"],
                "home_tw": {c: r[f"home_{c}"] for c in TEAM_WEEK_SOURCE_COLUMNS},
                "away_tw": {c: r[f"away_{c}"] for c in TEAM_WEEK_SOURCE_COLUMNS},
            }
        )
    return games


FIT_PARAMETER_KEYS = frozenset(
    {
        "feature_names",
        "feature_means",
        "diff_means",
        "diff_stds",
        "beta_margin",
        "beta_winprob",
        "platt_a",
        "platt_b",
    }
)


def _numeric6(value: float) -> float:
    """Match the legacy NUMERIC(12, 6) storage precision."""
    numeric = Decimal(str(float(value))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    if abs(numeric) >= Decimal("1000000"):
        raise OverflowError(f"value {value!r} exceeds legacy NUMERIC(12, 6)")
    return float(numeric)


def build_fit_parameters(
    feature_means: dict,
    diff_means: dict,
    diff_stds: dict,
    beta_margin: np.ndarray,
    beta_winprob: np.ndarray,
    platt_a: float,
    platt_b: float,
) -> dict:
    """Freeze scoring parameters under the immutable registry contract."""
    return {
        "feature_names": list(FEATURE_NAMES),
        "feature_means": {
            name: None if value is None else float(value) for name, value in feature_means.items()
        },
        "diff_means": {name: float(value) for name, value in diff_means.items()},
        "diff_stds": {name: float(value) for name, value in diff_stds.items()},
        "beta_margin": [_numeric6(value) for value in beta_margin],
        "beta_winprob": [_numeric6(value) for value in beta_winprob],
        "platt_a": _numeric6(platt_a),
        "platt_b": _numeric6(platt_b),
    }


def fit_parameters_are_compatible(parameters: object) -> bool:
    """Validate the scoring shape of a registry candidate or selected fit."""
    if not isinstance(parameters, dict) or set(parameters) != FIT_PARAMETER_KEYS:
        return False
    if parameters["feature_names"] != FEATURE_NAMES:
        return False
    mappings = ("feature_means", "diff_means", "diff_stds")
    if any(not isinstance(parameters[name], dict) for name in mappings):
        return False
    if set(parameters["feature_means"]) != set(TEAM_WEEK_SOURCE_COLUMNS):
        return False
    diff_names = set(FEATURE_NAMES) - {INTERCEPT, NEUTRAL_SITE}
    if set(parameters["diff_means"]) != diff_names:
        return False
    if set(parameters["diff_stds"]) != diff_names:
        return False
    vectors = ("beta_margin", "beta_winprob")
    if any(not isinstance(parameters[name], list) for name in vectors):
        return False
    if not all(len(parameters[name]) == len(FEATURE_NAMES) for name in vectors):
        return False

    def number(value: object) -> float | None:
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        return numeric if math.isfinite(numeric) else None

    if any(
        value is not None and number(value) is None
        for value in parameters["feature_means"].values()
    ):
        return False
    if any(
        number(value) is None
        for name in ("diff_means", "diff_stds")
        for value in parameters[name].values()
    ):
        return False
    if any(number(value) is None for name in vectors for value in parameters[name]):
        return False
    if number(parameters["platt_a"]) is None or number(parameters["platt_b"]) is None:
        return False
    return all(float(value) >= 0 for value in parameters["diff_stds"].values())


def current_implementation_contract() -> dict:
    """Fingerprint the exact training and upstream feature transformations."""
    from scripts import (
        build_features,
        compute_adjusted_epa,
        compute_adjusted_epa_week,
        compute_house_elo,
        compute_predictions,
    )

    training_math = {
        obj.__name__: inspect.getsource(obj)
        for obj in (
            penalty_mask,
            sigmoid,
            platt_transform,
            _impute_value,
            build_feature_vector,
            compute_feature_means,
            compute_diff_stats,
            standardize,
            ridge_fit,
            _penalized_nll,
            irls_logistic,
            platt_fit,
            build_design,
            collect_team_week_rows,
            fetch_games,
            _numeric6,
            build_fit_parameters,
            fit_one,
        )
    }
    training_math["numerical_constants"] = repr(
        {
            "max_logit": _MAX_LOGIT,
            "w_min": _W_MIN,
            "intercept_idx": INTERCEPT_IDX,
            "neutral_site_idx": NEUTRAL_SITE_IDX,
        }
    )
    repository = Path(__file__).resolve().parents[1]
    upstream_sql_paths = (
        "src/schemas/marts/010_play_epa.sql",
        "src/schemas/marts/031_returning_production.sql",
    )
    return {
        "training_math": source_text_fingerprint(training_math),
        "training_query": source_text_fingerprint({"query": _train_games_query()}),
        "feature_builder": source_text_fingerprint(
            {"scripts/build_features.py": inspect.getsource(build_features)}
        ),
        "upstream_feature_python": source_text_fingerprint(
            {
                "scripts/compute_predictions.py": inspect.getsource(compute_predictions),
                "scripts/compute_house_elo.py": inspect.getsource(compute_house_elo),
                "scripts/compute_adjusted_epa.py": inspect.getsource(compute_adjusted_epa),
                "scripts/compute_adjusted_epa_week.py": inspect.getsource(
                    compute_adjusted_epa_week
                ),
            }
        ),
        "upstream_feature_sql": source_text_fingerprint(
            {
                "FEATURE_ROWS_QUERY": build_features.FEATURE_ROWS_QUERY,
                **{
                    path: (repository / path).read_text(encoding="utf-8")
                    for path in upstream_sql_paths
                },
            }
        ),
    }


def current_training_manifest(
    train_seasons: list[int],
    games: list[dict],
    *,
    implementation: dict | None = None,
) -> dict:
    return build_training_manifest(
        feature_names=FEATURE_NAMES,
        team_week_source_columns=TEAM_WEEK_SOURCE_COLUMNS,
        diff_feature_columns=DIFF_FEATURE_COLUMNS,
        implementation=implementation or current_implementation_contract(),
        train_seasons=train_seasons,
        games=games,
        ridge_alpha=RIDGE_ALPHA,
        winprob_ridge_alpha=WINPROB_ALPHA,
    )


def selected_fit_is_fresh(selected: dict, current_manifest: dict) -> bool:
    """Require known current lineage and a valid frozen scoring contract."""
    fit_id = selected.get("training_fit_id")
    return (
        isinstance(fit_id, str)
        and len(fit_id) == 64
        and all(character in "0123456789abcdef" for character in fit_id)
        and manifests_match_for_freshness(selected.get("manifest", {}), current_manifest)
        and fit_parameters_are_compatible(selected.get("parameters"))
    )


def fit_one(
    conn,
    train_through: int,
    train_seasons: list[int],
    *,
    games: list[dict] | None = None,
    implementation: dict | None = None,
    promote: bool = False,
    promotion_reason: str | None = None,
) -> str | None:
    """Train and persist a single walk-forward fit for ``train_through_season``.

    Runs design section 3 steps 3-8: impute means, vectorize, scale, fit ridge
    margin, fit IRLS win-prob, Platt-calibrate the train logits, persist, and
    print the FITTED_GATE line.
    """
    games = games if games is not None else fetch_games(conn, train_seasons)
    if not games:
        logger.warning(
            "train_through=%d: no completed games with team_week features for seasons %s; "
            "skipping (has build_features.py run?)",
            train_through,
            train_seasons,
        )
        return None

    # Step 3: frozen imputation means over the TRAIN team-week rows, then
    # vectorize (imputation applied inside build_feature_vector).
    feature_means = compute_feature_means(collect_team_week_rows(games))
    X_raw, y_margin, y_win = build_design(games, feature_means)

    # Step 4: z-score stats over the imputed TRAIN design, then apply.
    diff_means, diff_stds = compute_diff_stats(X_raw)
    X_std = standardize(X_raw, diff_means, diff_stds)

    mask = penalty_mask()
    # Step 5: ridge margin.
    beta_margin = ridge_fit(X_std, y_margin, RIDGE_ALPHA, mask)
    # Step 6: IRLS win-prob.
    beta_winprob = irls_logistic(X_std, y_win, WINPROB_ALPHA, mask)
    # Step 7: Platt-calibrate the TRAIN logits.
    train_logits = X_std @ beta_winprob
    platt_a, platt_b = platt_fit(train_logits, y_win)

    # Train-window diagnostics for the gate line.
    margin_pred = X_std @ beta_margin
    margin_mae = float(np.mean(np.abs(margin_pred - y_margin)))
    calibrated = np.array([platt_transform(z, platt_a, platt_b) for z in train_logits])
    winprob_brier = float(np.mean((calibrated - y_win) ** 2))

    # Step 8: append the immutable candidate. Preserve the old database's
    # NUMERIC(12, 6) coefficient/calibration precision in the new JSON payload.
    manifest = current_training_manifest(
        train_seasons,
        games,
        implementation=implementation,
    )
    parameters = build_fit_parameters(
        feature_means,
        diff_means,
        diff_stds,
        beta_margin,
        beta_winprob,
        platt_a,
        platt_b,
    )
    try:
        training_fit_id = insert_fit(
            conn,
            MODEL_VERSION,
            train_through,
            manifest,
            parameters,
        )
        if promote:
            promote_fit(conn, training_fit_id, promotion_reason or "")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    print(
        f"FITTED_GATE train_through={train_through} n_train={len(games)} "
        f"training_fit_id={training_fit_id} promoted={str(promote).lower()} "
        f"margin_train_mae={margin_mae:.3f} winprob_train_brier={winprob_brier:.4f} "
        f"platt_a={platt_a:.4f} platt_b={platt_b:.4f}"
    )
    return training_fit_id


def train_score_seasons(
    conn,
    score_seasons: list[int],
    *,
    games: list[dict] | None = None,
    promote: bool = False,
    promotion_reason: str | None = None,
) -> list[str]:
    """Train exactly the requested score-season vintages, reusing one fetch."""
    if not score_seasons:
        return []
    score_seasons = sorted(set(score_seasons))
    earliest = TRAIN_START_SEASON + MIN_TRAIN_SEASONS
    if score_seasons[0] < earliest:
        raise ValueError(f"earliest score season is {earliest}")

    all_games = games
    if all_games is None:
        all_games = fetch_games(
            conn,
            list(range(TRAIN_START_SEASON, score_seasons[-1])),
        )
    implementation = current_implementation_contract()
    fit_ids = []
    for score_season in score_seasons:
        train_through = score_season - 1
        train_seasons = list(range(TRAIN_START_SEASON, score_season))
        train_games = [game for game in all_games if int(game["season"]) <= train_through]
        logger.info(
            "Fitting fitted_v1 for score season %d: train_through=%d, train_seasons=%s",
            score_season,
            train_through,
            train_seasons,
        )
        fit_id = fit_one(
            conn,
            train_through,
            train_seasons,
            games=train_games,
            implementation=implementation,
            promote=promote,
            promotion_reason=promotion_reason,
        )
        if fit_id is not None:
            fit_ids.append(fit_id)
    return fit_ids


def train_walk_forward(
    conn,
    score_start: int,
    score_end: int,
    *,
    promote: bool = False,
    promotion_reason: str | None = None,
) -> list[str]:
    """Expanding-window walk-forward over score seasons ``score_start..score_end``
    (design section 3): each season ``S`` trains on ``2015..S-1`` and persists a
    fit keyed ``train_through_season=S-1``."""
    return train_score_seasons(
        conn,
        list(range(score_start, score_end + 1)),
        promote=promote,
        promotion_reason=promotion_reason,
    )


def stale_score_seasons(
    latest_finished_season: int,
    existing_train_through: list[int],
    default_start: int = DEFAULT_SCORE_START,
) -> list[int]:
    """Score seasons still needed so a fit exists at
    ``train_through_season = latest_finished_season``.

    Walk-forward keying (design section 2d): score season ``S`` produces the fit
    ``train_through_season = S-1``. So covering train-through values up through
    ``latest_finished_season`` means running score seasons up through
    ``latest_finished_season + 1``.

    The caller MUST pass the last *fully finished* season (see
    fetch_refit_state). Passing a season that is merely in progress would train
    a fit on partial data and then score the rest of that same season in-sample.

    Returns the set difference across every expected vintage, so an interior
    hole is trained without retraining the enclosing range. Returns ``[]`` only
    when all expected selected vintages are current.

    Exists because the annual refit was a manual chore and was missed: the 2025
    season ended in January 2026 and the newest fit was still
    ``train_through_season=2024`` in July, so 2026 scoring would have silently
    used a two-season-stale fit.
    """
    expected = set(range(default_start - 1, latest_finished_season + 1))
    existing = {int(season) for season in existing_train_through}
    return [season + 1 for season in sorted(expected - existing)]


def safe_training_frontier(conn) -> int | None:
    """Last contiguous, schedule-aware final season beginning in 2015."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT season FROM core.games WHERE season >= %s ORDER BY season DESC",
            (TRAIN_START_SEASON,),
        )
        candidates = [int(row[0]) for row in cur.fetchall()]

    present = set(candidates)
    finality: dict[int, bool] = {}

    def is_final(season: int) -> bool:
        if season not in finality:
            finality[season] = season in present and season_is_final(conn, season)
        return finality[season]

    latest_finished = next((season for season in candidates if is_final(season)), None)
    if latest_finished is not None:
        for season in range(TRAIN_START_SEASON, latest_finished + 1):
            if not is_final(season):
                logger.warning(
                    "Automatic refit cannot include season=%d: missing or unfinished within "
                    "the expanding training window through %d",
                    season,
                    latest_finished,
                )
                latest_finished = season - 1 if season > TRAIN_START_SEASON else None
                break

    return latest_finished


def _fresh_selected_seasons(
    selected: list[dict],
    latest_finished: int,
    games: list[dict],
) -> list[int]:
    """Return selected vintages matching current spec and current input rows."""
    if not selected:
        return []
    implementation = current_implementation_contract()
    expected = range(DEFAULT_SCORE_START - 1, latest_finished + 1)
    selected_by_season = {
        int(fit["train_through_season"]): fit
        for fit in selected
        if int(fit["train_through_season"]) <= latest_finished
    }
    fresh = []
    for train_through in expected:
        fit = selected_by_season.get(train_through)
        if fit is None:
            continue
        train_seasons = list(range(TRAIN_START_SEASON, train_through + 1))
        train_games = [game for game in games if int(game["season"]) <= train_through]
        current = current_training_manifest(
            train_seasons,
            train_games,
            implementation=implementation,
        )
        if selected_fit_is_fresh(fit, current):
            fresh.append(train_through)
    return fresh


def fetch_refit_plan(conn) -> tuple[int | None, list[int], list[dict]]:
    """Return safe frontier, fresh selected vintages, and once-fetched rows."""
    latest_finished = safe_training_frontier(conn)
    if latest_finished is None:
        return None, [], []
    games = fetch_games(conn, list(range(TRAIN_START_SEASON, latest_finished + 1)))
    selected = selected_fits(conn, MODEL_VERSION)
    return latest_finished, _fresh_selected_seasons(selected, latest_finished, games), games


def fetch_refit_state(conn) -> tuple[int | None, list[int]]:
    """Return safe frontier and selected vintages fresh for current inputs."""
    latest_finished, fresh, _games = fetch_refit_plan(conn)
    return latest_finished, fresh


def validate_promotion_fit(conn, fit: dict, *, current_required: bool) -> None:
    """Require a safe closed vintage and, normally, current known lineage."""
    if fit.get("model_version") != MODEL_VERSION:
        raise ValueError(f"cannot promote model_version={fit.get('model_version')!r}")
    train_through = int(fit["train_through_season"])
    earliest = TRAIN_START_SEASON + MIN_TRAIN_SEASONS - 1
    frontier = safe_training_frontier(conn)
    if frontier is None or not earliest <= train_through <= frontier:
        raise ValueError(
            f"train_through={train_through} is outside safe closed frontier {earliest}..{frontier}"
        )
    if not fit_parameters_are_compatible(fit.get("parameters")):
        raise ValueError("fit parameters do not match the current scoring contract")
    if current_required:
        games = fetch_games(conn, list(range(TRAIN_START_SEASON, train_through + 1)))
        current = current_training_manifest(
            list(range(TRAIN_START_SEASON, train_through + 1)), games
        )
        if not selected_fit_is_fresh(fit, current):
            raise ValueError("fit manifest or training inputs are stale for current code/data")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train fitted_v1 walk-forward ridge-margin + IRLS/Platt win-prob "
        "candidates into the immutable training registry"
    )
    parser.add_argument(
        "--seasons",
        nargs=2,
        type=int,
        metavar=("START", "END"),
        default=[DEFAULT_SCORE_START, DEFAULT_SCORE_END],
        help="Walk-forward SCORE-season range (inclusive); each season S trains "
        f"through S-1 on {TRAIN_START_SEASON}..S-1. Default: "
        f"{DEFAULT_SCORE_START} {DEFAULT_SCORE_END}.",
    )
    parser.add_argument(
        "--refit-if-stale",
        action="store_true",
        help="Train only missing or stale selected vintages through the safe closed frontier.",
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        help="Select newly trained/imported fits after safe-finality validation.",
    )
    parser.add_argument(
        "--promotion-reason",
        help="Required nonempty audit reason for --promote or --promote-fit.",
    )
    parser.add_argument(
        "--promote-fit",
        metavar="TRAINING_FIT_ID",
        help="Select one existing known-lineage candidate after current-input validation.",
    )
    parser.add_argument(
        "--import-legacy",
        action="store_true",
        help="Snapshot legacy coefficient tables; never promotes unless --promote is explicit.",
    )
    args = parser.parse_args()
    start, end = args.seasons
    if start > end:
        parser.error(f"--seasons start {start} is after end {end}")

    reason = (args.promotion_reason or "").strip()
    if (args.promote or args.promote_fit) and not reason:
        parser.error("--promote and --promote-fit require a nonempty --promotion-reason")
    if reason and not (args.promote or args.promote_fit):
        parser.error("--promotion-reason requires --promote or --promote-fit")
    if args.promote_fit and args.promote:
        parser.error("--promote-fit cannot be combined with --promote")
    command_modes = sum(
        bool(value) for value in (args.refit_if_stale, args.promote_fit, args.import_legacy)
    )
    if command_modes > 1:
        parser.error("--refit-if-stale, --promote-fit, and --import-legacy are mutually exclusive")
    if "--seasons" in sys.argv[1:] and (args.promote_fit or args.import_legacy):
        parser.error("--seasons is not used with --promote-fit or --import-legacy")

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        if args.promote_fit:
            fit = load_registry_fit(conn, args.promote_fit)
            validate_promotion_fit(conn, fit, current_required=True)
            promote_fit(conn, args.promote_fit, reason)
            conn.commit()
            print(
                f"FITTED_PROMOTION training_fit_id={args.promote_fit} "
                f"train_through={fit['train_through_season']}"
            )
            return

        if args.import_legacy:
            fit_ids = import_legacy_fits(conn, MODEL_VERSION)
            if args.promote:
                for training_fit_id in fit_ids:
                    fit = load_registry_fit(conn, training_fit_id)
                    # Import records the historical values exactly but cannot
                    # claim current input lineage. Explicit import promotion is
                    # limited to safely closed vintages; freshness still treats
                    # these deployments as stale and schedules known-lineage fits.
                    validate_promotion_fit(conn, fit, current_required=False)
                    promote_fit(conn, training_fit_id, reason)
            conn.commit()
            for training_fit_id in fit_ids:
                print(
                    f"FITTED_LEGACY_IMPORT training_fit_id={training_fit_id} "
                    f"promoted={str(args.promote).lower()}"
                )
            return

        if args.refit_if_stale:
            latest_finished, existing, games = fetch_refit_plan(conn)
            if latest_finished is None:
                logger.info("No finished season in core.games yet; nothing to refit")
                return
            needed = stale_score_seasons(latest_finished, existing)
            if not needed:
                logger.info(
                    "fitted_v1 is current: all selected vintages through the latest "
                    "FINISHED season (%d) match current code and inputs; nothing to refit",
                    latest_finished,
                )
                return
            logger.info(
                "fitted_v1 has missing/stale selected vintages through latest FINISHED "
                "season %d; training score season(s) %s",
                latest_finished,
                needed,
            )
            train_score_seasons(
                conn,
                needed,
                games=games,
                promote=args.promote,
                promotion_reason=reason or None,
            )
            return

        if args.promote:
            frontier = safe_training_frontier(conn)
            requested_train_through = list(range(start - 1, end))
            if frontier is None or any(season > frontier for season in requested_train_through):
                parser.error(
                    f"--promote range includes a vintage beyond safe closed frontier {frontier}"
                )
        train_walk_forward(
            conn,
            start,
            end,
            promote=args.promote,
            promotion_reason=reason or None,
        )
    except Exception:
        conn.rollback()
        logger.exception("fitted_v1 training failed")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

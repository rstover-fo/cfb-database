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
2018..2025) we train on seasons ``2015..S-1`` and persist ONE frozen fit keyed
``(model_version='fitted_v1', train_through_season=S-1)`` into
``features.model_coefficients`` + ``features.model_metadata`` (migration 028).
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
import logging
import sys

import numpy as np

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
]


def feature_names_for(diff_feature_columns: list[tuple[str, str]]) -> list[str]:
    """``[intercept, neutral_site, *diff names]`` for an arbitrary diff-column
    list. Factored out so the U4 candidate-evaluation harness
    (``evaluate_candidate_diff_columns``) can build the same positional
    contract for a feature list that differs from the production
    ``DIFF_FEATURE_COLUMNS``, without duplicating this line."""
    return [INTERCEPT, NEUTRAL_SITE] + [name for name, _ in diff_feature_columns]


# Fixed design-matrix column order (index = position): intercept, neutral_site,
# then the 20 diffs. 22 features + unpenalized intercept (was 15 before
# migration 042, 20 before 047; migration 048's off_ppd is populated in
# features.team_week but not in DIFF_FEATURE_COLUMNS -- it is evaluated
# in-memory via CANDIDATE_DIFF_FEATURE_COLUMNS (U4) and enters this list only
# on a gate pass, see docs/brainstorms/2026-07-21-team-week-feature-design.md).
#
# CHANGING THIS LIST INVALIDATES EVERY STORED FIT. score_fitted.load_fit builds
# its coefficient vector by name lookup over FEATURE_NAMES, so a fit written
# before a column was added raises KeyError rather than degrading gracefully.
# Every train_through_season vintage has to be retrained in the same deploy that
# ships the column -- there is no partial-rollout path.
FEATURE_NAMES: list[str] = feature_names_for(DIFF_FEATURE_COLUMNS)

INTERCEPT_IDX = 0
NEUTRAL_SITE_IDX = 1

# The features.team_week source columns a diff feature reads -- also the columns
# that get a frozen imputation mean (design section 2b). score_fitted imports
# this to build its SELECT column list so the two never drift.
TEAM_WEEK_SOURCE_COLUMNS: list[str] = [col for _, col in DIFF_FEATURE_COLUMNS]


def penalty_mask(feature_names: list[str] | None = None) -> np.ndarray:
    """Ridge penalty mask over ``feature_names`` (default ``FEATURE_NAMES``):
    1.0 for every penalized column, 0.0 for the unpenalized intercept (design
    section 2a). ``neutral_site`` IS penalized (only the intercept is exempt).
    ``INTERCEPT_IDX`` (0) is a fixed positional constant regardless of the diff
    list, so this holds for any candidate feature-name list too (U4)."""
    names = FEATURE_NAMES if feature_names is None else feature_names
    mask = np.ones(len(names), dtype=np.float64)
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
    game_row: dict,
    home_tw: dict,
    away_tw: dict,
    feature_means: dict,
    diff_feature_columns: list[tuple[str, str]] | None = None,
) -> np.ndarray:
    """Raw (pre-standardization) design row for one game, in FEATURE_NAMES order.

    ``game_row`` supplies the game-level ``neutral_site``. ``home_tw`` / ``away_tw``
    are the home/away ``features.team_week`` source-column dicts. ``feature_means``
    is ``{team_week_column: mean_c}`` (design section 2b): a NULL home- or away-
    side value is imputed to ``mean_c`` **before** differencing, i.e. the missing
    side is treated as a league-average team. ``diff_feature_columns`` defaults to
    the production ``DIFF_FEATURE_COLUMNS``; the U4 candidate-evaluation harness
    passes a candidate list so the same vectorization runs for a feature set that
    is not yet (or never) in the shipped vector. Returns a length-``2 +
    len(diff_feature_columns)`` numpy array; standardization (section 2c) is
    applied separately by ``standardize`` so the imputation/diff step stays
    inspectable on its own.
    """
    columns = DIFF_FEATURE_COLUMNS if diff_feature_columns is None else diff_feature_columns
    x = np.empty(2 + len(columns), dtype=np.float64)
    x[INTERCEPT_IDX] = 1.0
    x[NEUTRAL_SITE_IDX] = 1.0 if game_row.get("neutral_site") else 0.0
    for offset, (_feat_name, col) in enumerate(columns):
        mean_c = feature_means.get(col)
        home_val = _impute_value(home_tw.get(col), mean_c)
        away_val = _impute_value(away_tw.get(col), mean_c)
        x[2 + offset] = home_val - away_val
    return x


def standardize(
    X: np.ndarray,
    diff_means: dict,
    diff_stds: dict,
    feature_names: list[str] | None = None,
) -> np.ndarray:
    """Z-score the standardized diff features of ``X`` using frozen train-window
    stats (design section 2c). ``intercept`` and ``neutral_site`` pass through
    unchanged. A zero (or missing) std maps that column to 0.0 -- a constant
    feature carries no signal. Accepts a single row (1-D) or a design matrix
    (2-D) and returns the same shape; the input is never mutated.
    ``feature_names`` defaults to ``FEATURE_NAMES`` (U4 passes a candidate name
    list built by ``feature_names_for``).
    """
    names = FEATURE_NAMES if feature_names is None else feature_names
    arr = np.asarray(X, dtype=np.float64)
    single = arr.ndim == 1
    Z = np.atleast_2d(arr).astype(np.float64, copy=True)
    for i, feat_name in enumerate(names):
        if feat_name in (INTERCEPT, NEUTRAL_SITE):
            continue
        mean = float(diff_means[feat_name])
        std = float(diff_stds[feat_name])
        if std > 0.0:
            Z[:, i] = (Z[:, i] - mean) / std
        else:
            Z[:, i] = 0.0
    return Z[0] if single else Z


def compute_feature_means(
    team_week_rows: list[dict], source_columns: list[str] | None = None
) -> dict:
    """Frozen imputation means (design section 2b step 1): the mean of each
    source column's value over the given team-week rows (both home and away
    sides of the TRAIN games), ignoring NULLs. A column that is NULL in every
    row maps to None (``_impute_value`` then falls back to 0.0).
    ``source_columns`` defaults to ``TEAM_WEEK_SOURCE_COLUMNS`` (U4 passes a
    candidate source-column list)."""
    columns = TEAM_WEEK_SOURCE_COLUMNS if source_columns is None else source_columns
    means: dict = {}
    for col in columns:
        vals = [float(r[col]) for r in team_week_rows if r.get(col) is not None]
        means[col] = (sum(vals) / len(vals)) if vals else None
    return means


def compute_diff_stats(
    X_raw: np.ndarray, feature_names: list[str] | None = None
) -> tuple[dict, dict]:
    """Per-column mean/std over the imputed (pre-standardization) TRAIN design
    matrix, for the standardized diff features only (design section 2c). Uses
    population std (ddof=0), so re-standardizing this same matrix reproduces
    unit variance exactly. Returns ``(diff_means, diff_stds)`` keyed by
    ``feature_name``. ``feature_names`` defaults to ``FEATURE_NAMES`` (U4 passes
    a candidate name list)."""
    names = FEATURE_NAMES if feature_names is None else feature_names
    X_raw = np.asarray(X_raw, dtype=np.float64)
    diff_means: dict = {}
    diff_stds: dict = {}
    for i, feat_name in enumerate(names):
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
    games: list[dict],
    feature_means: dict,
    diff_feature_columns: list[tuple[str, str]] | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Vectorize game dicts into ``(X_raw, y_margin, y_win)`` (pre-standardization).

    Each game dict needs ``neutral_site``, ``home_points``, ``away_points`` and
    the ``home_tw`` / ``away_tw`` team-week source-column dicts. Pure (numpy +
    dicts only), so the whole train/score path is testable without a DB.
    ``diff_feature_columns`` defaults to the production ``DIFF_FEATURE_COLUMNS``
    (U4 passes a candidate list through to ``build_feature_vector``).
    """
    x_rows = []
    y_margin = []
    y_win = []
    for g in games:
        x_rows.append(
            build_feature_vector(g, g["home_tw"], g["away_tw"], feature_means, diff_feature_columns)
        )
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


def _train_games_query(source_columns: list[str] | None = None) -> str:
    """SELECT completed games in the given seasons joined to both team-week
    sides. Column list is built from ``source_columns`` (default
    ``TEAM_WEEK_SOURCE_COLUMNS``) so it never drifts from the feature contract;
    U4 passes a candidate source-column list to pull an extra team_week column
    the production query does not need."""
    columns = TEAM_WEEK_SOURCE_COLUMNS if source_columns is None else source_columns
    home_cols = ",\n           ".join(f"h.{c} AS home_{c}" for c in columns)
    away_cols = ",\n           ".join(f"a.{c} AS away_{c}" for c in columns)
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
          AND COALESCE(g.completed, false)
          AND g.home_points IS NOT NULL
          AND g.away_points IS NOT NULL
        ORDER BY g.season, g.week, g.id
    """


def fetch_games(conn, seasons: list[int], source_columns: list[str] | None = None) -> list[dict]:
    """Completed games for the seasons, each with both team-week source-column
    dicts split out into ``home_tw`` / ``away_tw`` (design section 3 step 2).
    ``source_columns`` defaults to ``TEAM_WEEK_SOURCE_COLUMNS`` (U4 passes a
    candidate source-column list so the query pulls the extra column)."""
    import psycopg2.extras

    columns = TEAM_WEEK_SOURCE_COLUMNS if source_columns is None else source_columns
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_train_games_query(columns), (list(seasons),))
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
                "home_tw": {c: r[f"home_{c}"] for c in columns},
                "away_tw": {c: r[f"away_{c}"] for c in columns},
            }
        )
    return games


def _coef_rows(train_through: int, component: str, beta: np.ndarray) -> list[tuple]:
    return [
        (MODEL_VERSION, train_through, component, i, FEATURE_NAMES[i], float(beta[i]))
        for i in range(len(FEATURE_NAMES))
    ]


def persist_fit(
    conn,
    train_through: int,
    train_seasons: list[int],
    n_train: int,
    feature_means: dict,
    diff_means: dict,
    diff_stds: dict,
    beta_margin: np.ndarray,
    beta_winprob: np.ndarray,
    platt_a: float,
    platt_b: float,
) -> None:
    """DELETE+INSERT the frozen fit into features.model_coefficients (both
    components) and features.model_metadata under
    ``(model_version='fitted_v1', train_through_season)``. One commit per fit."""
    from psycopg2.extras import Json, execute_values

    coef_rows = _coef_rows(train_through, "margin", beta_margin) + _coef_rows(
        train_through, "winprob", beta_winprob
    )
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM features.model_coefficients "
            "WHERE model_version = %s AND train_through_season = %s",
            (MODEL_VERSION, train_through),
        )
        execute_values(
            cur,
            """
            INSERT INTO features.model_coefficients
                (model_version, train_through_season, model_component,
                 feature_order, feature_name, coefficient)
            VALUES %s
            """,
            coef_rows,
        )

        cur.execute(
            "DELETE FROM features.model_metadata "
            "WHERE model_version = %s AND train_through_season = %s",
            (MODEL_VERSION, train_through),
        )
        cur.execute(
            """
            INSERT INTO features.model_metadata
                (model_version, train_through_season, ridge_alpha,
                 winprob_ridge_alpha, platt_a, platt_b, train_seasons,
                 n_train_games, feature_means, feature_diff_means,
                 feature_diff_stds)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                MODEL_VERSION,
                train_through,
                RIDGE_ALPHA,
                WINPROB_ALPHA,
                platt_a,
                platt_b,
                list(train_seasons),
                n_train,
                Json(feature_means),
                Json(diff_means),
                Json(diff_stds),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def fit_one(conn, train_through: int, train_seasons: list[int]) -> None:
    """Train and persist a single walk-forward fit for ``train_through_season``.

    Runs design section 3 steps 3-8: impute means, vectorize, scale, fit ridge
    margin, fit IRLS win-prob, Platt-calibrate the train logits, persist, and
    print the FITTED_GATE line.
    """
    games = fetch_games(conn, train_seasons)
    if not games:
        logger.warning(
            "train_through=%d: no completed games with team_week features for seasons %s; "
            "skipping (has build_features.py run?)",
            train_through,
            train_seasons,
        )
        return

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

    # Step 8: persist the frozen fit.
    persist_fit(
        conn,
        train_through,
        train_seasons,
        len(games),
        feature_means,
        diff_means,
        diff_stds,
        beta_margin,
        beta_winprob,
        platt_a,
        platt_b,
    )

    print(
        f"FITTED_GATE train_through={train_through} n_train={len(games)} "
        f"margin_train_mae={margin_mae:.3f} winprob_train_brier={winprob_brier:.4f} "
        f"platt_a={platt_a:.4f} platt_b={platt_b:.4f}"
    )


def train_walk_forward(conn, score_start: int, score_end: int) -> None:
    """Expanding-window walk-forward over score seasons ``score_start..score_end``
    (design section 3): each season ``S`` trains on ``2015..S-1`` and persists a
    fit keyed ``train_through_season=S-1``."""
    for score_season in range(score_start, score_end + 1):
        train_through = score_season - 1
        train_seasons = list(range(TRAIN_START_SEASON, score_season))
        assert len(train_seasons) >= MIN_TRAIN_SEASONS, (
            f"score season {score_season} has only {len(train_seasons)} train season(s); "
            f"need >= {MIN_TRAIN_SEASONS} (earliest score season is "
            f"{TRAIN_START_SEASON + MIN_TRAIN_SEASONS})"
        )
        logger.info(
            "Fitting fitted_v1 for score season %d: train_through=%d, train_seasons=%s",
            score_season,
            train_through,
            train_seasons,
        )
        fit_one(conn, train_through, train_seasons)


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

    Returns ``[]`` when the newest fit is already current -- the no-op the daily
    workflow hits on 364 days a year. Pure, so the staleness rule is testable
    without a DB.

    Exists because the annual refit was a manual chore and was missed: the 2025
    season ended in January 2026 and the newest fit was still
    ``train_through_season=2024`` in July, so 2026 scoring would have silently
    used a two-season-stale fit.
    """
    target = latest_finished_season
    if not existing_train_through:
        return list(range(default_start, target + 2))
    max_existing = max(existing_train_through)
    if max_existing >= target:
        return []
    return list(range(max_existing + 2, target + 2))


# A season counts as FINISHED once at least this share of its games are
# complete. Not 100%: a cancelled or never-reconciled game would otherwise keep
# a season "unfinished" forever and permanently freeze the refit. 99% clears a
# handful of stragglers on a ~1,600-game season while still requiring the
# season to be genuinely over.
SEASON_COMPLETE_THRESHOLD = 0.99

# Below this many games a season is too small to judge complete by ratio alone
# (an early season with 3 of 3 games played would look 100% finished).
MIN_GAMES_FOR_FINISHED_SEASON = 100


def fetch_refit_state(conn) -> tuple[int | None, list[int]]:
    """``(latest FINISHED season, existing fitted_v1 train_through values)``.

    "Finished" deliberately means *the whole season is over*, not "has at least
    one completed game". Using ``MAX(season) WHERE completed`` would flip to the
    new season the moment its first game ends, and the consequences compound:

      1. ``stale_score_seasons`` would train a fit labelled
         ``train_through_season = S`` on a season with a handful of games played,
      2. ``score_fitted --upcoming`` selects ``MAX(train_through_season)`` and
         would adopt it,
      3. the remaining games of season S would then be scored **in-sample**, and
      4. the staleness check would never fire again, because the S key now
         exists.

    That is a leak that inflates apparent accuracy rather than an outage, so it
    would not announce itself. Requiring a finished season keeps every fit
    strictly walk-forward.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(season) FROM (
                SELECT season
                FROM core.games
                GROUP BY season
                HAVING COUNT(*) >= %s
                   AND AVG(CASE WHEN COALESCE(completed, false) THEN 1.0 ELSE 0.0 END) >= %s
            ) finished
            """,
            (MIN_GAMES_FOR_FINISHED_SEASON, SEASON_COMPLETE_THRESHOLD),
        )
        row = cur.fetchone()[0]
        latest_finished = int(row) if row is not None else None

        # A fit counts as EXISTING only if it was written under the CURRENT
        # feature contract (PR #56 review, P1).
        #
        # Season recency alone is not staleness. Changing DIFF_FEATURE_COLUMNS
        # invalidates every stored fit -- score_fitted.load_fit builds its
        # coefficient vector by name lookup, so a fit written before a column
        # was added raises KeyError rather than degrading. But a fit for the
        # latest finished season already EXISTS by season, so --refit-if-stale
        # returned [] and the daily workflow no-opped straight into that
        # KeyError. Predictions and season simulation both stop, and the
        # staleness check can never recover on its own because the season key
        # is present.
        #
        # This file's own header says "there is no partial-rollout path", and
        # until now that invariant was enforced by whoever remembered to run
        # the retrain by hand. Comparing the frozen feature_means key set to
        # TEAM_WEEK_SOURCE_COLUMNS makes it self-enforcing: a vector change
        # drops every mismatched fit out of `existing`, so stale_score_seasons
        # rebuilds the whole walk-forward ladder.
        #
        # Set EQUALITY, not containment -- migration 046 REMOVED hc_first_year
        # from the vector, and a fit carrying a column the model no longer uses
        # is just as wrong as one missing a column it does.
        cur.execute(
            "SELECT train_through_season, feature_means FROM features.model_metadata "
            "WHERE model_version = %s",
            (MODEL_VERSION,),
        )
        expected = set(TEAM_WEEK_SOURCE_COLUMNS)
        existing = []
        stale_by_contract = []
        for season, means in cur.fetchall():
            if isinstance(means, dict) and set(means) == expected:
                existing.append(int(season))
            else:
                stale_by_contract.append(int(season))
        if stale_by_contract:
            logger.warning(
                "%d stored fit(s) predate the current %d-feature contract and will be "
                "retrained: train_through=%s",
                len(stale_by_contract),
                len(TEAM_WEEK_SOURCE_COLUMNS),
                sorted(stale_by_contract),
            )
    return latest_finished, existing


# =============================================================================
# U4 -- isolated walk-forward candidate evaluation (starter-pack plan,
# docs/plans/2026-07-28-001-feat-starter-pack-model-features-plan.md, KTD2).
#
# Reuses every fitting function above with a CANDIDATE diff-feature list in
# place of the production DIFF_FEATURE_COLUMNS -- a mode, not a duplicate
# fitting harness. The candidate fit is held ENTIRELY in memory: it is never
# passed to persist_fit, so features.model_coefficients / model_metadata are
# untouched and every stored production fit stays exactly as it was.
# =============================================================================

# The sole U1/U2 survivor (screened-and-shipped-to-evaluation per the design
# doc's 2026-07-28 v2 confirmation entry): off_ppd, offensive points per drive.
# The other five 2026-07-28 candidates were rejected by the screen and never
# reach this evaluation step.
OFF_PPD_FEATURE = ("d_off_ppd", "off_ppd")
CANDIDATE_DIFF_FEATURE_COLUMNS: list[tuple[str, str]] = [*DIFF_FEATURE_COLUMNS, OFF_PPD_FEATURE]


def fit_candidate(
    conn,
    train_through: int,
    train_seasons: list[int],
    diff_feature_columns: list[tuple[str, str]],
) -> dict | None:
    """In-memory walk-forward fit for a candidate diff-feature list -- mirrors
    ``fit_one`` step for step (impute, vectorize, scale, ridge, IRLS, Platt)
    but returns the frozen fit as a dict instead of persisting it. Returns
    None when there are no completed games with team_week rows for
    ``train_seasons`` (mirrors ``fit_one``'s warning-and-skip)."""
    source_columns = [col for _, col in diff_feature_columns]
    games = fetch_games(conn, train_seasons, source_columns)
    if not games:
        return None

    feature_means = compute_feature_means(collect_team_week_rows(games), source_columns)
    X_raw, y_margin, y_win = build_design(games, feature_means, diff_feature_columns)

    feature_names = feature_names_for(diff_feature_columns)
    diff_means, diff_stds = compute_diff_stats(X_raw, feature_names)
    X_std = standardize(X_raw, diff_means, diff_stds, feature_names)

    mask = penalty_mask(feature_names)
    beta_margin = ridge_fit(X_std, y_margin, RIDGE_ALPHA, mask)
    beta_winprob = irls_logistic(X_std, y_win, WINPROB_ALPHA, mask)
    platt_a, platt_b = platt_fit(X_std @ beta_winprob, y_win)

    return {
        "train_through": train_through,
        "n_train": len(games),
        "feature_means": feature_means,
        "diff_means": diff_means,
        "diff_stds": diff_stds,
        "beta_margin": beta_margin,
        "beta_winprob": beta_winprob,
        "platt_a": platt_a,
        "platt_b": platt_b,
    }


def score_candidate_game(
    game: dict, fit: dict, diff_feature_columns: list[tuple[str, str]]
) -> tuple[float, float]:
    """Frozen-candidate-fit prediction for one game: ``(expected_home_margin,
    home_win_prob)``. Same math as ``score_fitted.score_game``, generalized to
    an arbitrary diff-feature list; kept here rather than in score_fitted.py so
    the evaluation harness never imports the production scoring/write path."""
    feature_names = feature_names_for(diff_feature_columns)
    x_raw = build_feature_vector(
        game, game["home_tw"], game["away_tw"], fit["feature_means"], diff_feature_columns
    )
    x_std = standardize(x_raw, fit["diff_means"], fit["diff_stds"], feature_names)
    expected_margin = float(x_std @ fit["beta_margin"])
    logit = float(x_std @ fit["beta_winprob"])
    win_prob = platt_transform(logit, fit["platt_a"], fit["platt_b"])
    return expected_margin, win_prob


def evaluate_candidate_diff_columns(
    conn,
    diff_feature_columns: list[tuple[str, str]],
    score_start: int = DEFAULT_SCORE_START,
    score_end: int = DEFAULT_SCORE_END,
) -> list[dict]:
    """Walk-forward isolated evaluation (U4): for each score season S in
    ``score_start..score_end``, fit a candidate feature list in-memory on
    ``TRAIN_START_SEASON..S-1`` and score S's completed games with THAT
    season's own frozen candidate fit -- never the production fit, and never
    persisted (KTD2). Returns one dict per scored game; the caller aggregates
    and compares against the production baseline (see
    ``aggregate_candidate_metrics`` / ``fetch_production_baseline`` /
    ``evaluate_gate``).
    """
    source_columns = [col for _, col in diff_feature_columns]
    scored: list[dict] = []
    for score_season in range(score_start, score_end + 1):
        train_through = score_season - 1
        train_seasons = list(range(TRAIN_START_SEASON, score_season))
        if len(train_seasons) < MIN_TRAIN_SEASONS:
            continue
        fit = fit_candidate(conn, train_through, train_seasons, diff_feature_columns)
        if fit is None:
            logger.warning(
                "evaluate_candidate_diff_columns: no train games for train_through=%d; "
                "skipping score season %d",
                train_through,
                score_season,
            )
            continue

        test_games = fetch_games(conn, [score_season], source_columns)
        for g in test_games:
            expected_margin, win_prob = score_candidate_game(g, fit, diff_feature_columns)
            scored.append(
                {
                    "game_id": g["game_id"],
                    "season": score_season,
                    "actual_margin": float(g["home_points"] - g["away_points"]),
                    "actual_win": 1.0 if g["home_points"] > g["away_points"] else 0.0,
                    "expected_margin": expected_margin,
                    "win_prob": win_prob,
                }
            )
    return scored


def aggregate_candidate_metrics(scored_games: list[dict], market_by_game: dict[int, dict]) -> dict:
    """Margin MAE / Brier / ATS hit rate over a scored-game list, matching
    ``check_backtest.py``'s ``edge_threshold=0`` convention: MAE and Brier are
    computed over every scored game; ATS is computed only over games with a
    market spread (pushes excluded from the hit-rate denominator, exactly like
    the mart). Pure -- ``market_by_game`` is ``{game_id: {"spread": float |
    None}}`` (the shape ``compute_predictions.fetch_market_from_lines``
    returns), so this is testable without a DB.
    """
    from scripts.compute_predictions import compute_edge

    n = len(scored_games)
    if n == 0:
        return {
            "n_games": 0,
            "margin_mae": None,
            "brier": None,
            "ats_wins": 0,
            "ats_losses": 0,
            "ats_pushes": 0,
            "ats_hit_rate": None,
        }

    margin_mae = sum(abs(g["expected_margin"] - g["actual_margin"]) for g in scored_games) / n
    brier = sum((g["win_prob"] - g["actual_win"]) ** 2 for g in scored_games) / n

    wins = losses = pushes = 0
    for g in scored_games:
        market = market_by_game.get(g["game_id"]) or {}
        spread = market.get("spread")
        _edge, edge_pick = compute_edge(g["expected_margin"], spread)
        if edge_pick is None:
            continue
        actual_cover = g["actual_margin"] + spread
        if actual_cover == 0:
            pushes += 1
        elif (edge_pick == "home" and actual_cover > 0) or (
            edge_pick == "away" and actual_cover < 0
        ):
            wins += 1
        else:
            losses += 1

    return {
        "n_games": n,
        "margin_mae": margin_mae,
        "brier": brier,
        "ats_wins": wins,
        "ats_losses": losses,
        "ats_pushes": pushes,
        "ats_hit_rate": (wins / (wins + losses)) if (wins + losses) else None,
    }


# Production baseline aggregate: the same n-weighted formulas
# check_backtest.py's AGGREGATE_QUERY uses, restricted to edge_threshold=0 (the
# unconditional "every scored game" row) and to the candidate's own scored
# season range, so the two sides of the comparison cover identical games.
_BASELINE_QUERY = """
    SELECT
        SUM(n_games) AS n_games,
        SUM(ats_wins) AS ats_wins,
        SUM(ats_losses) AS ats_losses,
        SUM(margin_mae * n_games) / NULLIF(SUM(n_games), 0) AS margin_mae,
        SUM(n_scored_win_prob) AS n_scored_win_prob,
        SUM(brier * n_scored_win_prob) / NULLIF(SUM(n_scored_win_prob), 0) AS brier
    FROM marts.prediction_accuracy
    WHERE model_version = %(model_version)s AND edge_threshold = 0
      AND season BETWEEN %(start)s AND %(end)s
"""


def fetch_production_baseline(conn, score_start: int, score_end: int) -> dict:
    """The production ``fitted_v1`` per-game accuracy aggregate over the same
    season range a candidate was scored on, read from
    ``marts.prediction_accuracy`` (the same surface ``check_backtest.py``
    reports). Hard error if the mart has no rows for the range -- the gate
    must never compare a candidate against an empty baseline."""
    with conn.cursor() as cur:
        cur.execute(
            _BASELINE_QUERY,
            {"model_version": MODEL_VERSION, "start": score_start, "end": score_end},
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(
            f"No marts.prediction_accuracy rows for model_version={MODEL_VERSION!r} "
            f"edge_threshold=0 seasons {score_start}-{score_end}; run "
            "scripts/score_fitted.py --backfill and refresh the mart before evaluating"
        )
    n_games, ats_wins, ats_losses, margin_mae, n_scored_win_prob, brier = row
    ats_wins = int(ats_wins or 0)
    ats_losses = int(ats_losses or 0)
    return {
        "n_games": int(n_games),
        "margin_mae": float(margin_mae) if margin_mae is not None else None,
        "brier": float(brier) if brier is not None else None,
        "ats_wins": ats_wins,
        "ats_losses": ats_losses,
        "ats_hit_rate": (ats_wins / (ats_wins + ats_losses)) if (ats_wins + ats_losses) else None,
    }


def evaluate_gate(candidate: dict, baseline: dict) -> dict:
    """The R5 adoption rule (KTD5), stated mechanically over a candidate vs.
    baseline metrics dict: held-out MAE must IMPROVE (strictly lower) and
    neither Brier nor ATS hit rate may DEGRADE (candidate at least as good).
    This is the decision rule the printed report states explicitly -- the gate
    itself stays a human call applied to that report, exactly like
    ``check_backtest.py`` never fails on its own numbers.
    """
    mae_delta = candidate["margin_mae"] - baseline["margin_mae"]
    brier_delta = candidate["brier"] - baseline["brier"]
    ats_delta = candidate["ats_hit_rate"] - baseline["ats_hit_rate"]
    mae_improves = mae_delta < 0.0
    brier_holds = brier_delta <= 0.0
    ats_holds = ats_delta >= 0.0
    return {
        "mae_delta": mae_delta,
        "brier_delta": brier_delta,
        "ats_delta": ats_delta,
        "mae_improves": mae_improves,
        "brier_holds": brier_holds,
        "ats_holds": ats_holds,
        "verdict": "PASS" if (mae_improves and brier_holds and ats_holds) else "FAIL",
    }


def run_candidate_evaluation(
    conn, score_start: int = DEFAULT_SCORE_START, score_end: int = DEFAULT_SCORE_END
) -> dict:
    """Drive U4 end to end: fit+score the candidate walk-forward, aggregate its
    metrics, fetch the production baseline over the identical season range,
    apply the gate, and print the comparison report. Never writes
    features.model_coefficients / features.model_metadata. Returns the full
    report dict for callers that want it (e.g. tests, or a future --json mode)."""
    from scripts.compute_predictions import fetch_market_from_lines

    scored = evaluate_candidate_diff_columns(
        conn, CANDIDATE_DIFF_FEATURE_COLUMNS, score_start, score_end
    )
    game_ids = [g["game_id"] for g in scored]
    market_by_game = fetch_market_from_lines(conn, game_ids)
    candidate_metrics = aggregate_candidate_metrics(scored, market_by_game)
    baseline_metrics = fetch_production_baseline(conn, score_start, score_end)
    gate = evaluate_gate(candidate_metrics, baseline_metrics)

    print(
        f"CANDIDATE_EVAL_GATE candidates={','.join(name for name, _ in (OFF_PPD_FEATURE,))} "
        f"seasons={score_start}-{score_end} "
        f"n_candidate={candidate_metrics['n_games']} n_baseline={baseline_metrics['n_games']}"
    )
    print(
        f"  margin_mae candidate={candidate_metrics['margin_mae']:.4f} "
        f"baseline={baseline_metrics['margin_mae']:.4f} "
        f"delta={gate['mae_delta']:+.4f} improves={gate['mae_improves']}"
    )
    print(
        f"  brier      candidate={candidate_metrics['brier']:.6f} "
        f"baseline={baseline_metrics['brier']:.6f} "
        f"delta={gate['brier_delta']:+.6f} holds={gate['brier_holds']}"
    )
    print(
        f"  ats_hit_rate candidate={candidate_metrics['ats_hit_rate']:.4f} "
        f"baseline={baseline_metrics['ats_hit_rate']:.4f} "
        f"delta={gate['ats_delta']:+.4f} holds={gate['ats_holds']}"
    )
    print(f"CANDIDATE_EVAL_VERDICT {gate['verdict']}")

    return {
        "candidate_metrics": candidate_metrics,
        "baseline_metrics": baseline_metrics,
        "gate": gate,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train fitted_v1 walk-forward ridge-margin + IRLS/Platt win-prob "
        "fits into features.model_coefficients / features.model_metadata"
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
        help="Train only the fits missing relative to the latest completed season, "
        "then exit. A clean no-op when the newest fit is already current -- safe "
        "to run daily. What the daily workflow runs.",
    )
    parser.add_argument(
        "--evaluate-candidates",
        action="store_true",
        help="U4 (starter-pack plan): isolated walk-forward evaluation of "
        "CANDIDATE_DIFF_FEATURE_COLUMNS (currently off_ppd) against the production "
        "fitted_v1 baseline in marts.prediction_accuracy. Never writes "
        "features.model_coefficients / features.model_metadata; prints the "
        "MAE/Brier/ATS comparison and the R5 gate verdict, then exits.",
    )
    args = parser.parse_args()
    start, end = args.seasons
    if start > end:
        logger.error("--seasons start %d is after end %d", start, end)
        sys.exit(1)

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        if args.evaluate_candidates:
            run_candidate_evaluation(conn, start, end)
            return

        if args.refit_if_stale:
            latest_finished, existing = fetch_refit_state(conn)
            if latest_finished is None:
                logger.info("No finished season in core.games yet; nothing to refit")
                return
            needed = stale_score_seasons(latest_finished, existing)
            if not needed:
                logger.info(
                    "fitted_v1 is current: newest fit train_through=%d covers the "
                    "latest FINISHED season (%d); nothing to refit",
                    max(existing),
                    latest_finished,
                )
                return
            logger.info(
                "fitted_v1 is stale: newest fit train_through=%s vs latest FINISHED "
                "season %d; training score season(s) %s",
                max(existing) if existing else "none",
                latest_finished,
                needed,
            )
            train_walk_forward(conn, needed[0], needed[-1])
            return

        train_walk_forward(conn, start, end)
    except Exception:
        conn.rollback()
        logger.exception("fitted_v1 training failed")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

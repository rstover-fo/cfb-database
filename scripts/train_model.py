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
]

# Fixed design-matrix column order (index = position): intercept, neutral_site,
# then the 13 diffs. 15 features + unpenalized intercept.
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
    args = parser.parse_args()
    start, end = args.seasons
    if start > end:
        logger.error("--seasons start %d is after end %d", start, end)
        sys.exit(1)

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        train_walk_forward(conn, start, end)
    except Exception:
        conn.rollback()
        logger.exception("fitted_v1 training failed")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

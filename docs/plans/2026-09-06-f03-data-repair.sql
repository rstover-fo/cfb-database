-- Explicit-file recovery only; not an automatic migration.
-- Run transactionally via run_migrations.py after reviewing F03 recovery evidence.
-- Retain exact pre-repair rows, including dlt metadata, in an owner-only journal.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';
CREATE SCHEMA IF NOT EXISTS recovery;
REVOKE ALL ON SCHEMA recovery FROM PUBLIC, anon, authenticated, analyst_ro;
CREATE TABLE IF NOT EXISTS recovery.f03_row_archive (
    source_table text NOT NULL,
    row_id bigint NOT NULL,
    payload jsonb NOT NULL,
    archived_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_table,row_id)
);
REVOKE ALL ON recovery.f03_row_archive FROM PUBLIC, anon, authenticated, analyst_ro;

-- Short table locks prevent concurrent ingestion or same-day prediction upserts
-- from changing a row between its audit snapshot and repair/deletion.
LOCK TABLE core.games, predictions.game_predictions IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE features.model_metadata,features.model_coefficients IN SHARE MODE;
-- Preserve the first pre-rebuild fit snapshot so postflight can prove equality.
INSERT INTO recovery.f03_row_archive(source_table,row_id,payload)
SELECT 'features.model_metadata',0,COALESCE(jsonb_agg(to_jsonb(m)), '[]'::jsonb)
FROM features.model_metadata m ON CONFLICT DO NOTHING;
INSERT INTO recovery.f03_row_archive(source_table,row_id,payload)
SELECT 'features.model_coefficients',0,COALESCE(jsonb_agg(to_jsonb(c)), '[]'::jsonb)
FROM features.model_coefficients c ON CONFLICT DO NOTHING;
DO $repair$
DECLARE expected record; actual core.games%ROWTYPE; n bigint;
BEGIN
 FOR expected IN SELECT * FROM (VALUES
   -- Official Taylor game log, November 2, 2024:
   -- https://taylor2023.prestosports.com/sports/fball/2024-25/bios/loveless_carter_825c
   (401677463::bigint,2024,620,190,63,12),
   -- Official Sul Ross box score, October 25, 2025 (October 26 UTC):
   -- https://srlobos.com/sports/football/stats/2025/angelo-state/boxscore/3842
   (401773541::bigint,2025,2834,2025,0,62)
 ) AS r(id,season,home_id,away_id,home_points,away_points)
 LOOP
   SELECT * INTO STRICT actual FROM core.games WHERE id=expected.id;
   IF actual.season IS DISTINCT FROM expected.season
      OR actual.home_id IS DISTINCT FROM expected.home_id
      OR actual.away_id IS DISTINCT FROM expected.away_id THEN
     RAISE EXCEPTION 'Identity mismatch for game %',expected.id;
   END IF;
   IF (actual.home_points IS NOT NULL AND actual.home_points<>expected.home_points)
      OR (actual.away_points IS NOT NULL AND actual.away_points<>expected.away_points) THEN
     RAISE EXCEPTION 'Conflicting score for game %',expected.id;
   END IF;
   IF actual.completed IS TRUE AND actual.home_points=expected.home_points
      AND actual.away_points=expected.away_points THEN CONTINUE; END IF;
   INSERT INTO recovery.f03_row_archive(source_table,row_id,payload)
   VALUES ('core.games',actual.id,to_jsonb(actual)) ON CONFLICT DO NOTHING;
   IF NOT EXISTS (SELECT 1 FROM recovery.f03_row_archive
       WHERE source_table='core.games' AND row_id=actual.id AND payload=to_jsonb(actual)) THEN
     RAISE EXCEPTION 'Original game archive mismatch for %',actual.id;
   END IF;
   UPDATE core.games SET completed=true,home_points=expected.home_points,
      away_points=expected.away_points WHERE id=expected.id;
   RAISE NOTICE 'Restored reviewed result for game %',expected.id;
 END LOOP;

 INSERT INTO recovery.f03_row_archive(source_table,row_id,payload)
 SELECT 'predictions.game_predictions',p.prediction_id,to_jsonb(p)
 FROM predictions.game_predictions p WHERE game_id=401866625
 ON CONFLICT DO NOTHING;
 IF EXISTS (SELECT 1 FROM predictions.game_predictions p
     LEFT JOIN recovery.f03_row_archive a ON a.source_table='predictions.game_predictions'
       AND a.row_id=p.prediction_id
     WHERE p.game_id=401866625 AND a.payload IS DISTINCT FROM to_jsonb(p)) THEN
   RAISE EXCEPTION 'Prediction archive mismatch; no deletion permitted';
 END IF;
 DELETE FROM predictions.game_predictions p USING recovery.f03_row_archive a
 WHERE p.game_id=401866625 AND a.source_table='predictions.game_predictions'
   AND a.row_id=p.prediction_id AND a.payload=to_jsonb(p);
 GET DIAGNOSTICS n=ROW_COUNT;
 RAISE NOTICE 'Archived and removed % superseded-event prediction snapshots',n;
END $repair$;

-- Read-only catalog/aggregate preflight for the authorized F04 cutover.
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '60s';
DO $preflight$
DECLARE r record; n bigint;
BEGIN
    SELECT count(*) INTO n FROM pg_stat_activity
    WHERE pid <> pg_backend_pid() AND state <> 'idle'
      AND query ~* '(insert into|update|delete from).*predictions\.';
    IF n <> 0 THEN RAISE EXCEPTION 'Other active prediction writers: %', n; END IF;
    SELECT count(*) INTO n FROM predictions.game_predictions;
    RAISE NOTICE 'F04 ledger rows before cutover: %', n;
    FOR r IN
        WITH RECURSIVE closure(oid) AS (
            SELECT oid FROM pg_class WHERE oid IN
                ('marts.scored_matchup_edges'::regclass, 'marts.prediction_accuracy'::regclass)
            UNION
            SELECT rw.ev_class FROM closure c
            JOIN pg_depend d ON d.refclassid='pg_class'::regclass AND d.refobjid=c.oid
            JOIN pg_rewrite rw ON d.classid='pg_rewrite'::regclass AND rw.oid=d.objid
            WHERE rw.ev_class <> c.oid
        )
        SELECT c.oid::regclass AS relation, p.relkind, pg_get_userbyid(p.relowner) AS owner,
               p.relacl, p.reloptions
        FROM closure c JOIN pg_class p ON p.oid=c.oid ORDER BY 1
    LOOP
        RAISE NOTICE 'F04 dependency: %', row_to_json(r);
        IF r.relation::text NOT IN ('marts.scored_matchup_edges','marts.prediction_accuracy',
                                   'api.scored_matchup_edges','api.prediction_accuracy') THEN
            RAISE EXCEPTION 'Additional dependency requires preservation: %', r.relation;
        END IF;
    END LOOP;
    FOR r IN SELECT model_version,count(*) AS rows,min(prediction_id) AS first_id,
                    max(prediction_id) AS last_id
             FROM predictions.game_predictions GROUP BY model_version ORDER BY model_version
    LOOP RAISE NOTICE 'F04 legacy population: %', row_to_json(r); END LOOP;
    RAISE NOTICE 'F04 read-only preflight complete';
END
$preflight$;

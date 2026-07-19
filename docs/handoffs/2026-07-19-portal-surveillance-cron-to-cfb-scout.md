# Handoff: Portal Surveillance Cron → cfb-scout

**Date:** 2026-07-19
**Removed from this repo:** `src/schemas/migrations/017_portal_surveillance_cron.sql` (introduced in commit `593a16c`, 2026-02-26)

## Why this was removed from cfb-database

The migration defines `scouting.fn_evaluate_portal_value()` and schedules it via
pg_cron, but every table it touches — `scouting.players`, `scouting.transfer_events`,
`scouting.pff_grades`, `scouting.alerts`, `scouting.alert_history` — belongs to the
`scouting` schema, which `docs/SCHEMA_CONTRACT.md` assigns to **cfb-scout**. This repo
never creates those tables, so:

- Applying the file against a database without the scouting schema **fails outright**.
- It was wired into no runner (`scripts/run_migrations.py` does not reference
  `src/schemas/migrations/`), so it only ever ran if applied by hand.
- Its `017` number collides with `src/schemas/017_era_reference.sql`.

Per the schema-ownership contract, the function and its cron schedule should live in
cfb-scout's migration chain instead.

## Action for cfb-scout

1. Adopt the SQL below into cfb-scout's migrations (renumber to fit its chain).
2. pg_cron must be enabled on the Supabase project (Dashboard → Database → Extensions)
   if `create extension` lacks permission.
3. Check whether the job is already live before re-applying:
   `SELECT jobname, schedule FROM cron.job;`
   - Live-database state as of this handoff: see note at the bottom of this doc.
   - If `daily-portal-surveillance` already exists, the `cron.schedule` call in the SQL
     would create a duplicate under some pg_cron versions — prefer
     `cron.alter_job`/`cron.unschedule` first.

## Original SQL (verbatim)

```sql
-- src/schemas/migrations/017_portal_surveillance_cron.sql
-- Goal: Automated Transfer Portal Surveillance via pg_cron

-- 1. Enable pg_cron (if permissions allow, otherwise requires Supabase Dashboard toggle)
create extension if not exists pg_cron;

-- 2. Scouter Function: Calculate and Alert on Portal Value
create or replace function scouting.fn_evaluate_portal_value()
returns table (
    player_id int,
    player_name text,
    value_score decimal,
    alert_fired boolean
) as $$
declare
    entry record;
    v_score decimal;
    v_alert_id int;
    v_threshold decimal := 80.0;
begin
    -- Iterate through players who entered in the last 24h
    for entry in 
        select distinct p.id, p.name, p.composite_grade, te.from_team
        from scouting.players p
        join scouting.transfer_events te on p.id = te.player_id
        where te.event_type = 'entered'
          and te.created_at >= now() - interval '24 hours'
    loop
        -- Calculate Score: (Composite Grade * 50%) + (PFF * 50%) 
        -- Note: Scaled sentiment can be added if weighted appropriately in SQL
        select 
            round(
                (coalesce(entry.composite_grade, 0) * 0.5) + 
                (coalesce(
                    (select overall_grade 
                     from scouting.pff_grades 
                     where player_id = entry.id 
                     order by season desc, week desc nulls first limit 1
                    ), 0) * 0.5),
            1) into v_score;

        -- Fire Alert if threshold hit
        if v_score >= v_threshold then
            -- Find or create the alert definition for this player
            insert into scouting.alerts (user_id, name, alert_type, player_id, threshold)
            values ('system', 'portal-value-' || entry.id, 'portal_entry', entry.id, jsonb_build_object('min_score', v_threshold))
            on conflict (user_id, name) do update set last_checked_at = now()
            returning id into v_alert_id;

            -- Record the fire event
            insert into scouting.alert_history (alert_id, trigger_data, message)
            values (
                v_alert_id, 
                jsonb_build_object(
                    'score', v_score, 
                    'composite', entry.composite_grade,
                    'from_team', entry.from_team
                ),
                'High-value portal entrant: ' || entry.name || ' (Score: ' || v_score || ')'
            );
            
            player_id := entry.id;
            player_name := entry.name;
            value_score := v_score;
            alert_fired := true;
            return next;
        end if;
    end loop;
end;
$$ language plpgsql security definer;

-- 3. Schedule the cron job
-- Run at 9:00 PM UTC every day
select cron.schedule(
    'daily-portal-surveillance',
    '0 21 * * *',
    'select scouting.fn_evaluate_portal_value();'
);

comment on function scouting.fn_evaluate_portal_value() is 'Automated scouter that identifies high-value portal entrants and fires system alerts.';
```

## Live database state

_To be recorded during verification: `SELECT jobname, schedule FROM cron.job;`_

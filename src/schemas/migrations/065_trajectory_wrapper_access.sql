-- F06 executed-role checks found that this public invoker-rights wrapper was
-- granted to consumers while its underlying mart was not. Restore the read
-- path without changing view security mode or exposing private schemas.
-- Managed forward migration; production application requires separate approval.
GRANT SELECT ON TABLE marts.team_season_trajectory TO anon, authenticated;

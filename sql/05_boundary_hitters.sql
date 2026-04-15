SELECT
    player_name,
    competition,
    season_label,
    innings,
    total_runs,
    total_fours,
    total_sixes,
    boundary_pct,
    sixes_per_innings
FROM analytics_player_batting_summary
WHERE innings >= 6
ORDER BY boundary_pct DESC, sixes_per_innings DESC, total_runs DESC
LIMIT 30;

SELECT
    player_name,
    competition,
    season_label,
    innings,
    total_runs,
    strike_rate,
    sixes_per_innings,
    boundary_pct
FROM analytics_player_batting_summary
WHERE innings >= 6
  AND strike_rate >= 150
ORDER BY strike_rate DESC, sixes_per_innings DESC, total_runs DESC
LIMIT 30;

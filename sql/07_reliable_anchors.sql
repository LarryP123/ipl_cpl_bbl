SELECT
    player_name,
    competition,
    season_label,
    innings,
    total_runs,
    avg_runs,
    strike_rate,
    consistency_score
FROM analytics_player_batting_summary
WHERE innings >= 8
  AND avg_runs >= 35
  AND strike_rate BETWEEN 120 AND 150
ORDER BY consistency_score DESC, avg_runs DESC
LIMIT 25;

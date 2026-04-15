SELECT
    player_name,
    competition,
    season_label,
    innings,
    total_runs,
    avg_runs,
    strike_rate,
    batting_index,
    ROUND(avg_runs * strike_rate, 2) AS efficiency_score
FROM analytics_player_batting_summary
WHERE innings >= 8
  AND avg_runs >= 25
  AND strike_rate >= 140
ORDER BY efficiency_score DESC, total_runs DESC
LIMIT 20;

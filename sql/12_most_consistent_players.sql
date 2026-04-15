SELECT
    player_name,
    competition,
    season_label,
    innings,
    total_runs,
    avg_runs,
    strike_rate,
    consistency_score,
    batting_index
FROM analytics_player_batting_summary
WHERE innings >= 8
ORDER BY consistency_score DESC, avg_runs DESC
LIMIT 25;

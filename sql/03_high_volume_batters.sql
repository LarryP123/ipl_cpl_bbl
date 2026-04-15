SELECT
    player_name,
    competition,
    season_label,
    innings,
    balls_faced,
    total_runs,
    avg_runs,
    strike_rate
FROM analytics_player_batting_summary
WHERE innings >= 8
ORDER BY total_runs DESC, batting_index DESC
LIMIT 40;

SELECT
    player_name,
    competition,
    season_label,
    innings,
    balls_faced,
    total_runs,
    avg_runs,
    strike_rate,
    batting_index
FROM analytics_player_batting_summary
WHERE innings >= 8
ORDER BY batting_index DESC, total_runs DESC
LIMIT 20;

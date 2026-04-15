SELECT
    competition,
    season_label,
    player_name,
    innings,
    total_runs,
    avg_runs,
    strike_rate,
    batting_index
FROM analytics_player_batting_summary
WHERE innings >= 6
ORDER BY competition, batting_index DESC, total_runs DESC;

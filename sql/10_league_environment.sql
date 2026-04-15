SELECT
    competition,
    season_label,
    COUNT(*) AS players,
    ROUND(AVG(avg_runs), 2) AS avg_runs,
    ROUND(AVG(strike_rate), 2) AS avg_strike_rate,
    ROUND(AVG(boundary_pct), 2) AS avg_boundary_pct,
    ROUND(AVG(sixes_per_innings), 2) AS avg_sixes_per_innings,
    ROUND(AVG(batting_index), 2) AS avg_batting_index
FROM analytics_player_batting_summary
WHERE innings >= 4
GROUP BY competition, season_label
ORDER BY avg_batting_index DESC, avg_strike_rate DESC;

SELECT
    player_name,
    COUNT(DISTINCT competition) AS leagues_played,
    SUM(innings) AS total_innings,
    SUM(total_runs) AS total_runs,
    ROUND(AVG(avg_runs), 2) AS avg_runs,
    ROUND(AVG(strike_rate), 2) AS strike_rate,
    ROUND(AVG(batting_index), 2) AS batting_index
FROM analytics_player_batting_summary
GROUP BY player_name
HAVING COUNT(DISTINCT competition) >= 2
   AND SUM(innings) >= 8
ORDER BY leagues_played DESC, batting_index DESC, total_runs DESC
LIMIT 30;

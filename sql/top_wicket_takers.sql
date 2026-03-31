SELECT
    player_name,
    team,
    SUM(wickets) AS total_wickets
FROM bowling_scorecard
GROUP BY player_name, team
HAVING SUM(wickets) IS NOT NULL
ORDER BY total_wickets DESC
LIMIT 10;
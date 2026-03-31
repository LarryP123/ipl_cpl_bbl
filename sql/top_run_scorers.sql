SELECT
    player_name,
    team,
    SUM(runs) AS total_runs
FROM batting_scorecard
GROUP BY player_name, team
HAVING SUM(runs) IS NOT NULL
ORDER BY total_runs DESC
LIMIT 10;
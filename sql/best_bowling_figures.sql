SELECT
    player_name,
    team,
    wickets,
    runs_conceded,
    overs,
    economy
FROM bowling_scorecard
WHERE wickets IS NOT NULL
ORDER BY wickets DESC, runs_conceded ASC
LIMIT 10;
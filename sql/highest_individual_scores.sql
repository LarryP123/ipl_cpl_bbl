SELECT
    player_name,
    team,
    runs,
    balls,
    strike_rate
FROM batting_scorecard
WHERE runs IS NOT NULL
ORDER BY runs DESC, strike_rate DESC
LIMIT 10;
WITH ranked_innings AS (
    SELECT
        player_name,
        team,
        competition,
        season_label,
        match_date,
        runs,
        balls,
        fours,
        sixes,
        match_id,
        ROW_NUMBER() OVER (
            PARTITION BY player_name, competition
            ORDER BY match_date DESC, match_id DESC
        ) AS row_num
    FROM analytics_batting_innings
),
last_five AS (
    SELECT *
    FROM ranked_innings
    WHERE row_num <= 5
)
SELECT
    player_name,
    competition,
    season_label,
    COUNT(*) AS recent_innings,
    SUM(runs) AS recent_runs,
    ROUND(AVG(runs), 2) AS recent_avg,
    ROUND(
        CASE
            WHEN SUM(balls) > 0 THEN SUM(runs) * 100.0 / SUM(balls)
            ELSE 0
        END,
        2
    ) AS recent_strike_rate
FROM last_five
GROUP BY player_name, competition, season_label
HAVING COUNT(*) = 5
ORDER BY recent_runs DESC, recent_strike_rate DESC
LIMIT 30;

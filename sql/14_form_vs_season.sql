WITH ranked_innings AS (
    SELECT
        player_name,
        competition,
        season_label,
        match_date,
        match_id,
        runs,
        balls,
        ROW_NUMBER() OVER (
            PARTITION BY player_name, competition
            ORDER BY match_date DESC, match_id DESC
        ) AS row_num
    FROM analytics_batting_innings
),
last_five AS (
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
    FROM ranked_innings
    WHERE row_num <= 5
    GROUP BY player_name, competition, season_label
)
SELECT
    season.player_name,
    season.competition,
    season.season_label,
    season.innings,
    season.avg_runs AS season_avg,
    recent.recent_avg,
    ROUND(recent.recent_avg - season.avg_runs, 2) AS avg_form_delta,
    season.strike_rate AS season_strike_rate,
    recent.recent_strike_rate,
    ROUND(recent.recent_strike_rate - season.strike_rate, 2) AS strike_rate_form_delta
FROM analytics_player_batting_summary AS season
JOIN last_five AS recent
  ON season.player_name = recent.player_name
 AND season.competition = recent.competition
WHERE season.innings >= 8
  AND recent.recent_innings = 5
ORDER BY avg_form_delta DESC, strike_rate_form_delta DESC
LIMIT 30;

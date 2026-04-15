DROP VIEW IF EXISTS player_metrics;
DROP VIEW IF EXISTS analytics_player_batting_summary;
DROP VIEW IF EXISTS analytics_batting_innings;

CREATE VIEW analytics_batting_innings AS
SELECT
    b.batting_id,
    b.match_id,
    b.innings_number,
    b.player_name,
    b.team,
    b.runs,
    b.balls,
    b.fours,
    b.sixes,
    b.strike_rate,
    b.dismissal,
    m.competition,
    m.season_label,
    m.match_date
FROM batting_scorecard AS b
LEFT JOIN matches AS m
    ON m.match_id = b.match_id
WHERE b.player_name IS NOT NULL;

CREATE VIEW analytics_player_batting_summary AS
WITH base AS (
    SELECT *
    FROM analytics_batting_innings
    WHERE competition IS NOT NULL
)
SELECT
    player_name,
    team,
    competition,
    season_label,
    COUNT(*) AS innings,
    SUM(runs) AS total_runs,
    SUM(balls) AS balls_faced,
    ROUND(AVG(runs), 2) AS avg_runs,
    ROUND(
        CASE
            WHEN SUM(balls) > 0 THEN SUM(runs) * 100.0 / SUM(balls)
            ELSE 0
        END,
        2
    ) AS strike_rate,
    SUM(fours) AS total_fours,
    SUM(sixes) AS total_sixes,
    ROUND(
        CASE
            WHEN COUNT(*) > 0 THEN SUM(sixes) * 1.0 / COUNT(*)
            ELSE 0
        END,
        2
    ) AS sixes_per_innings,
    ROUND(
        CASE
            WHEN SUM(runs) > 0 THEN ((SUM(fours) * 4.0) + (SUM(sixes) * 6.0)) * 100.0 / SUM(runs)
            ELSE 0
        END,
        2
    ) AS boundary_pct,
    ROUND(
        AVG(runs) *
        CASE
            WHEN SUM(balls) > 0 THEN SUM(runs) * 100.0 / SUM(balls)
            ELSE 0
        END,
        2
    ) AS batting_index,
    ROUND(AVG(runs) * LOG10(COUNT(*) + 1), 2) AS consistency_score
FROM base
GROUP BY
    player_name,
    team,
    competition,
    season_label;

CREATE VIEW player_metrics AS
SELECT *
FROM analytics_player_batting_summary;

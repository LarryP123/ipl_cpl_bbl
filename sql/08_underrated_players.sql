WITH elite AS (
    SELECT player_name
    FROM analytics_player_batting_summary
    WHERE innings >= 8
    ORDER BY batting_index DESC
    LIMIT 20
)
SELECT
    player_name,
    competition,
    season_label,
    innings,
    total_runs,
    avg_runs,
    strike_rate,
    batting_index,
    consistency_score
FROM analytics_player_batting_summary
WHERE innings >= 8
  AND total_runs >= 250
  AND avg_runs >= 30
  AND strike_rate BETWEEN 130 AND 155
  AND player_name NOT IN (SELECT player_name FROM elite)
ORDER BY batting_index DESC, consistency_score DESC
LIMIT 25;

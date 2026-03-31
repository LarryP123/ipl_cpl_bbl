SELECT
    competition,
    season_label,
    COUNT(*) AS match_count
FROM matches
GROUP BY competition, season_label
ORDER BY match_count DESC;
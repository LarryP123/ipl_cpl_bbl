from src.extract import (
    get_series,
    filter_target_series,
    get_matches_from_series_info,
    get_match_scorecard,
)
from src.load import (
    create_tables,
    load_matches,
    load_innings,
    load_batting_scorecard,
    load_bowling_scorecard,
)
from src.transform import get_competition_metadata, parse_scorecard


def run():
    searches = [
        "Indian Premier League",
        "Big Bash League",
        "Caribbean Premier League",
    ]

    all_series = []

    for term in searches:
        results = get_series(term)
        all_series.extend(results)

    filtered_series = filter_target_series(all_series)

    print("\nFetching matches for selected series...\n")

    match_rows = []
    innings_rows = []
    batting_rows = []
    bowling_rows = []

    for series in filtered_series:
        series_id = series.get("id")
        series_name = series.get("name", "")

        print(f"Fetching: {series_name}")

        matches = get_matches_from_series_info(series_id)
        print(f" → Found {len(matches)} matches")

        metadata = get_competition_metadata(series_name)

        for match in matches:
            match_id = match.get("id")
            teams = match.get("teams", [])
            team_1 = teams[0] if len(teams) > 0 else None
            team_2 = teams[1] if len(teams) > 1 else None

            match_rows.append(
                {
                    "match_id": match_id,
                    "raw_series_name": series_name,
                    "competition": metadata["competition"],
                    "season_label": metadata["season_label"],
                    "match_date": match.get("date"),
                    "match_type": match.get("matchType"),
                    "team_1": team_1,
                    "team_2": team_2,
                    "venue": match.get("venue"),
                    "result_text": match.get("status"),
                    "status": match.get("status"),
                }
            )

            try:
                scorecard_data = get_match_scorecard(match_id)
                parsed_innings, parsed_batting, parsed_bowling = parse_scorecard(
                    match_id,
                    scorecard_data,
                    team_1,
                    team_2,                   
                )

                innings_rows.extend(parsed_innings)
                batting_rows.extend(parsed_batting)
                bowling_rows.extend(parsed_bowling)

                print(
                    f"   → Scorecard parsed: "
                    f"{len(parsed_innings)} innings, "
                    f"{len(parsed_batting)} batting rows, "
                    f"{len(parsed_bowling)} bowling rows"
                )
            except Exception as exc:
                print(f"   → Skipped scorecard for {match_id}: {exc}")

    print(f"\nTotal matches collected: {len(match_rows)}")
    print(f"Total innings collected: {len(innings_rows)}")
    print(f"Total batting rows collected: {len(batting_rows)}")
    print(f"Total bowling rows collected: {len(bowling_rows)}")

    create_tables()
    load_matches(match_rows)
    load_innings(innings_rows)
    load_batting_scorecard(batting_rows)
    load_bowling_scorecard(bowling_rows)

    print("All data loaded into SQLite.")


if __name__ == "__main__":
    run()
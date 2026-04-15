from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src.config import get_settings


st.set_page_config(
    page_title="Cricket Franchise Batting Analytics",
    layout="wide",
)

settings = get_settings()
DATA_DIR = settings.exports_dir
FILE_MAP = settings.dashboard_files


@st.cache_data
def load_csv(filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


@st.cache_data
def load_data() -> dict[str, pd.DataFrame]:
    return {key: load_csv(filename) for key, filename in FILE_MAP.items()}


def format_numeric_table(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    formatted = dataframe.copy()
    for column in formatted.columns:
        if pd.api.types.is_float_dtype(formatted[column]):
            formatted[column] = formatted[column].round(2)
    return formatted


def apply_filters(
    dataframe: pd.DataFrame,
    competitions: list[str],
    min_innings: int,
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    filtered = dataframe.copy()
    if "competition" in filtered.columns and competitions:
        filtered = filtered[filtered["competition"].isin(competitions)]
    if "innings" in filtered.columns:
        filtered = filtered[filtered["innings"] >= min_innings]
    return filtered.copy()


def top_n(dataframe: pd.DataFrame, sort_column: str, n_rows: int, ascending: bool = False) -> pd.DataFrame:
    if dataframe.empty or sort_column not in dataframe.columns:
        return dataframe
    return dataframe.sort_values(sort_column, ascending=ascending).head(n_rows).copy()


def render_empty_state(message: str) -> None:
    st.info(message)


def render_table(title: str, dataframe: pd.DataFrame) -> None:
    st.markdown(f"### {title}")
    if dataframe.empty:
        render_empty_state("No rows available for the current filters.")
        return
    st.dataframe(format_numeric_table(dataframe), use_container_width=True, hide_index=True)


def build_player_snapshot(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    roles = data.get("roles", pd.DataFrame())
    if roles.empty:
        return pd.DataFrame()

    base = roles[
        ["player_name", "competition", "season_label", "innings", "avg_runs", "strike_rate", "role"]
    ].drop_duplicates()

    enrichments = {
        "consistent": ["batting_index", "consistency_score", "total_runs"],
        "efficiency": ["efficiency_score"],
        "boundary": ["boundary_pct", "sixes_per_innings"],
    }

    for key, columns in enrichments.items():
        dataframe = data.get(key, pd.DataFrame())
        if dataframe.empty:
            continue
        keep_columns = [
            column
            for column in ["player_name", "competition", "season_label", *columns]
            if column in dataframe.columns
        ]
        base = base.merge(
            dataframe[keep_columns].drop_duplicates(),
            on=["player_name", "competition", "season_label"],
            how="left",
        )

    for flag_name, dataset_name in (
        ("is_explosive", "explosive"),
        ("is_anchor", "anchors"),
        ("is_underrated", "underrated"),
    ):
        dataframe = data.get(dataset_name, pd.DataFrame())
        if dataframe.empty:
            continue
        flags = dataframe[["player_name", "competition", "season_label"]].drop_duplicates().copy()
        flags[flag_name] = "Yes"
        base = base.merge(
            flags,
            on=["player_name", "competition", "season_label"],
            how="left",
        )

    for column in ["is_explosive", "is_anchor", "is_underrated"]:
        if column in base.columns:
            base[column] = base[column].fillna("No")

    return base


def build_insight_cards(data: dict[str, pd.DataFrame]) -> list[tuple[str, str]]:
    cards: list[tuple[str, str]] = []

    overall = data.get("overall", pd.DataFrame())
    league_environment = data.get("league_environment", pd.DataFrame())
    form_vs_season = data.get("form_vs_season", pd.DataFrame())
    multi_league = data.get("multi_league", pd.DataFrame())

    if not overall.empty:
        top_player = overall.sort_values("batting_index", ascending=False).iloc[0]
        cards.append(
            (
                "Best all-round T20 batting profile",
                f"{top_player['player_name']} leads the filtered sample with a batting index of {top_player['batting_index']:.2f} across {int(top_player['innings'])} innings.",
            )
        )

        fastest = overall.sort_values("strike_rate", ascending=False).iloc[0]
        cards.append(
            (
                "Fastest scorer in view",
                f"{fastest['player_name']} is scoring at {fastest['strike_rate']:.2f}, showing the upper edge of this batting cohort.",
            )
        )

    if not league_environment.empty:
        hardest = league_environment.sort_values("avg_batting_index", ascending=True).iloc[0]
        easiest = league_environment.sort_values("avg_batting_index", ascending=False).iloc[0]
        cards.append(
            (
                "League context matters",
                f"{easiest['competition']} looks like the friendliest batting environment, while {hardest['competition']} appears toughest on this dataset.",
            )
        )

    if not form_vs_season.empty:
        riser = form_vs_season.sort_values("avg_form_delta", ascending=False).iloc[0]
        cards.append(
            (
                "Recent form riser",
                f"{riser['player_name']} is outperforming season baseline by {riser['avg_form_delta']:.2f} runs per innings over the latest five knocks.",
            )
        )

    if not multi_league.empty:
        traveller = multi_league.sort_values(["leagues_played", "batting_index"], ascending=[False, False]).iloc[0]
        cards.append(
            (
                "Portable performance",
                f"{traveller['player_name']} stands out across {int(traveller['leagues_played'])} leagues, which is useful when scouting durable T20 output.",
            )
        )

    return cards[:4]


data = load_data()
available_frames = [frame for frame in data.values() if not frame.empty]

all_competitions = sorted(
    pd.concat(
        [
            frame["competition"]
            for frame in available_frames
            if "competition" in frame.columns
        ],
        ignore_index=True,
    ).dropna().unique().tolist()
) if available_frames else []

st.title("Cricket Franchise Batting Analytics")
st.caption(
    "A recruiter-friendly analytics layer across major T20 leagues, built from API ingestion through SQLite, SQL exports, and interactive reporting."
)

with st.sidebar:
    st.header("Filters")
    selected_competitions = st.multiselect(
        "Competitions",
        options=all_competitions,
        default=all_competitions,
    )
    top_n_players = st.slider("Leaderboard size", min_value=5, max_value=25, value=10)
    min_innings = st.slider("Minimum innings", min_value=1, max_value=15, value=6)
    st.caption(f"Source exports: {Path(DATA_DIR).resolve()}")

filtered_data = {
    name: apply_filters(dataframe, selected_competitions, min_innings)
    for name, dataframe in data.items()
}

player_snapshot = build_player_snapshot(filtered_data)
insight_cards = build_insight_cards(filtered_data)

summary_columns = st.columns(4)
overall = filtered_data.get("overall", pd.DataFrame())
recent_form = filtered_data.get("recent_form", pd.DataFrame())

summary_columns[0].metric(
    "Players in scope",
    int(overall["player_name"].nunique()) if not overall.empty else 0,
)
summary_columns[1].metric(
    "Competitions covered",
    len(selected_competitions),
)
summary_columns[2].metric(
    "Top batting index",
    f"{overall['batting_index'].max():.2f}" if not overall.empty else "N/A",
)
summary_columns[3].metric(
    "Recent form samples",
    int(len(recent_form)) if not recent_form.empty else 0,
)

if insight_cards:
    st.markdown("### Key Takeaways")
    insight_columns = st.columns(len(insight_cards))
    for column, (title, body) in zip(insight_columns, insight_cards):
        column.markdown(
            f"""
            <div style="padding: 1rem; border: 1px solid rgba(49, 51, 63, 0.18); border-radius: 0.75rem; height: 100%;">
                <p style="font-size: 0.85rem; font-weight: 600; margin-bottom: 0.4rem;">{title}</p>
                <p style="margin: 0; color: #4a4a4a;">{body}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

tab_summary, tab_specialists, tab_leagues, tab_players, tab_form, tab_exports = st.tabs(
    [
        "Executive Summary",
        "Specialists",
        "League Context",
        "Player Explorer",
        "Form Watch",
        "Exports",
    ]
)

with tab_summary:
    st.subheader("Best Batting Profiles")
    if overall.empty:
        render_empty_state("Run the export step first, or widen your filters to populate the overview.")
    else:
        top_overall = top_n(overall, "batting_index", top_n_players)
        chart = px.bar(
            top_overall.sort_values("batting_index"),
            x="batting_index",
            y="player_name",
            color="competition",
            orientation="h",
            hover_data=["innings", "total_runs", "avg_runs", "strike_rate"],
            title="Top players by batting index",
        )
        st.plotly_chart(chart, use_container_width=True)

        volume_column, consistency_column = st.columns(2)
        with volume_column:
            volume = top_n(filtered_data.get("high_volume", pd.DataFrame()), "total_runs", top_n_players)
            if volume.empty:
                render_empty_state("No high-volume batting records match the current filters.")
            else:
                volume_chart = px.bar(
                    volume.sort_values("total_runs"),
                    x="total_runs",
                    y="player_name",
                    color="competition",
                    orientation="h",
                    title="Run volume leaders",
                )
                st.plotly_chart(volume_chart, use_container_width=True)
        with consistency_column:
            consistency = top_n(filtered_data.get("consistent", pd.DataFrame()), "consistency_score", top_n_players)
            if consistency.empty:
                render_empty_state("No consistency results match the current filters.")
            else:
                consistency_chart = px.bar(
                    consistency.sort_values("consistency_score"),
                    x="consistency_score",
                    y="player_name",
                    color="competition",
                    orientation="h",
                    title="Most consistent batters",
                )
                st.plotly_chart(consistency_chart, use_container_width=True)

        render_table("Overall leaderboard", top_overall)

with tab_specialists:
    st.subheader("Specialist Segments")
    specialist_options = {
        "Explosive Hitters": ("explosive", "strike_rate"),
        "Reliable Anchors": ("anchors", "consistency_score"),
        "Boundary Hitters": ("boundary", "boundary_pct"),
        "Efficiency + Aggression": ("efficiency", "efficiency_score"),
        "Hidden Gems": ("underrated", "batting_index"),
    }
    selected_view = st.radio(
        "Focus area",
        options=list(specialist_options.keys()),
        horizontal=True,
    )
    dataset_name, sort_column = specialist_options[selected_view]
    specialist_df = filtered_data.get(dataset_name, pd.DataFrame())
    if specialist_df.empty:
        render_empty_state("No specialist rows available for the current filters.")
    else:
        ranked = top_n(specialist_df, sort_column, top_n_players)
        column_left, column_right = st.columns([3, 2])
        with column_left:
            specialist_chart = px.bar(
                ranked.sort_values(sort_column),
                x=sort_column,
                y="player_name",
                color="competition",
                orientation="h",
                title=selected_view,
            )
            st.plotly_chart(specialist_chart, use_container_width=True)
        with column_right:
            render_table(f"{selected_view} table", ranked)

with tab_leagues:
    st.subheader("League-Level Context")
    league_environment = filtered_data.get("league_environment", pd.DataFrame())
    by_league = filtered_data.get("by_league", pd.DataFrame())

    if league_environment.empty:
        render_empty_state("League comparison exports are missing or filtered out.")
    else:
        league_columns = st.columns(2)
        with league_columns[0]:
            strike_rate_chart = px.bar(
                league_environment.sort_values("avg_strike_rate"),
                x="competition",
                y="avg_strike_rate",
                color="competition",
                title="Average strike rate by league",
            )
            st.plotly_chart(strike_rate_chart, use_container_width=True)
        with league_columns[1]:
            batting_index_chart = px.bar(
                league_environment.sort_values("avg_batting_index"),
                x="competition",
                y="avg_batting_index",
                color="competition",
                title="Average batting index by league",
            )
            st.plotly_chart(batting_index_chart, use_container_width=True)

        render_table("League environment", league_environment)
        render_table("Best players by league", by_league.head(100))

with tab_players:
    st.subheader("Player Explorer")
    if player_snapshot.empty:
        render_empty_state("Player explorer is unavailable because the role breakdown export is empty.")
    else:
        player_names = sorted(player_snapshot["player_name"].dropna().unique().tolist())
        selected_player = st.selectbox("Select a player", player_names)
        player_rows = player_snapshot[player_snapshot["player_name"] == selected_player].copy()

        spotlight = player_rows.sort_values("batting_index", ascending=False).head(1)
        if not spotlight.empty:
            spotlight_row = spotlight.iloc[0]
            summary = st.columns(5)
            summary[0].metric("Competition", spotlight_row["competition"])
            summary[1].metric("Role", spotlight_row["role"])
            summary[2].metric("Average", f"{spotlight_row['avg_runs']:.2f}")
            summary[3].metric("Strike rate", f"{spotlight_row['strike_rate']:.2f}")
            summary[4].metric(
                "Consistency",
                f"{spotlight_row['consistency_score']:.2f}" if pd.notna(spotlight_row.get("consistency_score")) else "N/A",
            )

        render_table(f"{selected_player} profile", player_rows.sort_values("batting_index", ascending=False))

with tab_form:
    st.subheader("Recent Form Watch")
    recent_form_df = filtered_data.get("recent_form", pd.DataFrame())
    form_delta_df = filtered_data.get("form_vs_season", pd.DataFrame())

    left, right = st.columns(2)
    with left:
        if recent_form_df.empty:
            render_empty_state("No recent-form export is available yet.")
        else:
            recent_chart = px.bar(
                top_n(recent_form_df, "recent_runs", top_n_players).sort_values("recent_runs"),
                x="recent_runs",
                y="player_name",
                color="competition",
                orientation="h",
                title="Most productive last five innings",
            )
            st.plotly_chart(recent_chart, use_container_width=True)
    with right:
        if form_delta_df.empty:
            render_empty_state("No form-vs-season export is available yet.")
        else:
            delta_chart = px.bar(
                top_n(form_delta_df, "avg_form_delta", top_n_players).sort_values("avg_form_delta"),
                x="avg_form_delta",
                y="player_name",
                color="competition",
                orientation="h",
                title="Players outperforming season baseline",
            )
            st.plotly_chart(delta_chart, use_container_width=True)

    render_table("Recent form table", recent_form_df.head(50))
    render_table("Form versus season table", form_delta_df.head(50))

with tab_exports:
    st.subheader("Available Export Artifacts")
    export_inventory = pd.DataFrame(
        [
            {
                "dataset": dataset_name,
                "file": filename,
                "rows": int(len(dataframe)),
                "available": "Yes" if not dataframe.empty else "No",
            }
            for dataset_name, filename in FILE_MAP.items()
            for dataframe in [data.get(dataset_name, pd.DataFrame())]
        ]
    )
    render_table("Export inventory", export_inventory)
    st.caption(
        "If a dataset shows as unavailable, rerun `python export_results.py` after refreshing the SQLite database."
    )

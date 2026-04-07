import os
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(
    page_title="Cricket Batting Analytics Dashboard",
    layout="wide"
)

st.title("Cricket Batting Analytics Dashboard")
st.caption("Cross-league batting analysis across T20 competitions")

DATA_DIR = "exports"

FILE_MAP = {
    "overall": "01_best_overall_batters.csv",
    "by_league": "02_best_by_league.csv",
    "high_volume": "03_high_volume_batters.csv",
    "consistency_aggression": "04_efficiency_plus_aggression.csv",
    "efficiency_aggression": "04_efficiency_plus_aggression.csv",
    "boundary": "05_boundary_hitters.csv",
    "explosive": "06_explosive_hitters.csv",
    "anchors": "07_reliable_anchors.csv",
    "underrated": "08_underrated_players.csv",
    "multi_league": "09_multi_league_performers.csv",

    # Fix these mismatches
    "league_env": "10_league_environment.csv",
    "roles": "11_role_breakdown.csv",
    "consistent": "12_most_consistent_players.csv",
}



@st.cache_data
def load_csv(filename: str) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, filename)
    return pd.read_csv(path)


@st.cache_data
def load_data():
    return {key: load_csv(filename) for key, filename in FILE_MAP.items()}


def format_numeric_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_float_dtype(out[col]):
            out[col] = out[col].round(2)
    return out


def apply_competition_filter(df: pd.DataFrame, competitions):
    if "competition" in df.columns and competitions:
        return df[df["competition"].isin(competitions)].copy()
    return df.copy()


def top_n(df: pd.DataFrame, sort_col: str, n: int, ascending: bool = False):
    if sort_col not in df.columns or df.empty:
        return df
    return df.sort_values(sort_col, ascending=ascending).head(n).copy()


def build_player_base(
    roles_df: pd.DataFrame,
    consistent_df: pd.DataFrame,
    efficiency_df: pd.DataFrame,
    underrated_df: pd.DataFrame,
    explosive_df: pd.DataFrame,
    anchors_df: pd.DataFrame,
    boundary_df: pd.DataFrame,
) -> pd.DataFrame:
    base = roles_df.copy()

    base = base[
        ["player_name", "competition", "innings", "avg_runs", "strike_rate", "role"]
    ].drop_duplicates()

    if not consistent_df.empty:
        keep = ["player_name", "competition"]
        if "batting_index" in consistent_df.columns:
            keep.append("batting_index")
        if "consistency_score" in consistent_df.columns:
            keep.append("consistency_score")
        if "total_runs" in consistent_df.columns:
            keep.append("total_runs")

        base = base.merge(
            consistent_df[keep].drop_duplicates(),
            on=["player_name", "competition"],
            how="left"
        )

    if not efficiency_df.empty and "efficiency_score" in efficiency_df.columns:
        base = base.merge(
            efficiency_df[
                ["player_name", "competition", "efficiency_score"]
            ].drop_duplicates(),
            on=["player_name", "competition"],
            how="left"
        )

    if not boundary_df.empty:
        boundary_keep = ["player_name", "competition"]
        if "boundary_pct" in boundary_df.columns:
            boundary_keep.append("boundary_pct")
        if "sixes_per_innings" in boundary_df.columns:
            boundary_keep.append("sixes_per_innings")

        base = base.merge(
            boundary_df[boundary_keep].drop_duplicates(),
            on=["player_name", "competition"],
            how="left"
        )

    if not explosive_df.empty:
        explosive_flag = explosive_df[
            ["player_name", "competition"]
        ].drop_duplicates().copy()
        explosive_flag["is_explosive"] = "Yes"
        base = base.merge(
            explosive_flag,
            on=["player_name", "competition"],
            how="left"
        )

    if not anchors_df.empty:
        anchor_flag = anchors_df[
            ["player_name", "competition"]
        ].drop_duplicates().copy()
        anchor_flag["is_anchor"] = "Yes"
        base = base.merge(
            anchor_flag,
            on=["player_name", "competition"],
            how="left"
        )

    if not underrated_df.empty:
        underrated_flag = underrated_df[
            ["player_name", "competition"]
        ].drop_duplicates().copy()
        underrated_flag["is_underrated"] = "Yes"
        base = base.merge(
            underrated_flag,
            on=["player_name", "competition"],
            how="left"
        )

    for col in ["is_explosive", "is_anchor", "is_underrated"]:
        if col in base.columns:
            base[col] = base[col].fillna("No")

    return base


def add_insight_card(title: str, text: str):
    st.markdown(
        f"""
        <div style="
            padding: 1rem 1.2rem;
            border: 1px solid rgba(128,128,128,0.25);
            border-radius: 10px;
            margin-bottom: 0.75rem;
            background-color: rgba(255,255,255,0.02);
        ">
            <h4 style="margin: 0 0 0.35rem 0;">{title}</h4>
            <p style="margin: 0;">{text}</p>
        </div>
        """,
        unsafe_allow_html=True
    )


data = load_data()

overall_df = data["overall"]
by_league_df = data["by_league"]
high_volume_df = data["high_volume"]
consistency_aggression_df = data["consistency_aggression"]
efficiency_aggression_df = data["efficiency_aggression"]
boundary_df = data["boundary"]
explosive_df = data["explosive"]
anchors_df = data["anchors"]
underrated_df = data["underrated"]
multi_league_df = data["multi_league"]
league_env_df = data["league_env"]
roles_df = data["roles"]
consistent_df = data["consistent"]

all_competitions = sorted(
    pd.concat(
        [
            overall_df["competition"] if "competition" in overall_df.columns else pd.Series(dtype=str),
            by_league_df["competition"] if "competition" in by_league_df.columns else pd.Series(dtype=str),
            high_volume_df["competition"] if "competition" in high_volume_df.columns else pd.Series(dtype=str),
            efficiency_aggression_df["competition"] if "competition" in efficiency_aggression_df.columns else pd.Series(dtype=str),
            boundary_df["competition"] if "competition" in boundary_df.columns else pd.Series(dtype=str),
            explosive_df["competition"] if "competition" in explosive_df.columns else pd.Series(dtype=str),
            anchors_df["competition"] if "competition" in anchors_df.columns else pd.Series(dtype=str),
            underrated_df["competition"] if "competition" in underrated_df.columns else pd.Series(dtype=str),
            league_env_df["competition"] if "competition" in league_env_df.columns else pd.Series(dtype=str),
            roles_df["competition"] if "competition" in roles_df.columns else pd.Series(dtype=str),
            consistent_df["competition"] if "competition" in consistent_df.columns else pd.Series(dtype=str),
        ],
        ignore_index=True
    ).dropna().unique().tolist()
)

st.sidebar.header("Filters")

selected_competitions = st.sidebar.multiselect(
    "Competitions",
    options=all_competitions,
    default=all_competitions
)

top_n_players = st.sidebar.slider("Top N players/charts", min_value=5, max_value=25, value=10)
min_innings = st.sidebar.slider("Minimum innings", min_value=1, max_value=15, value=6)

overall_filtered = apply_competition_filter(overall_df, selected_competitions)
by_league_filtered = apply_competition_filter(by_league_df, selected_competitions)
high_volume_filtered = apply_competition_filter(high_volume_df, selected_competitions)
efficiency_filtered = apply_competition_filter(efficiency_aggression_df, selected_competitions)
boundary_filtered = apply_competition_filter(boundary_df, selected_competitions)
explosive_filtered = apply_competition_filter(explosive_df, selected_competitions)
anchors_filtered = apply_competition_filter(anchors_df, selected_competitions)
underrated_filtered = apply_competition_filter(underrated_df, selected_competitions)
league_env_filtered = apply_competition_filter(league_env_df, selected_competitions)
roles_filtered = apply_competition_filter(roles_df, selected_competitions)
consistent_filtered = apply_competition_filter(consistent_df, selected_competitions)

filtered_names = [
    "overall_filtered",
    "by_league_filtered",
    "high_volume_filtered",
    "efficiency_filtered",
    "boundary_filtered",
    "explosive_filtered",
    "anchors_filtered",
    "underrated_filtered",
    "roles_filtered",
    "consistent_filtered",
]

for df_name in filtered_names:
    df = locals()[df_name]
    if "innings" in df.columns:
        locals()[df_name] = df[df["innings"] >= min_innings].copy()

overall_filtered = locals()["overall_filtered"]
by_league_filtered = locals()["by_league_filtered"]
high_volume_filtered = locals()["high_volume_filtered"]
efficiency_filtered = locals()["efficiency_filtered"]
boundary_filtered = locals()["boundary_filtered"]
explosive_filtered = locals()["explosive_filtered"]
anchors_filtered = locals()["anchors_filtered"]
underrated_filtered = locals()["underrated_filtered"]
roles_filtered = locals()["roles_filtered"]
consistent_filtered = locals()["consistent_filtered"]

player_base = build_player_base(
    roles_filtered,
    consistent_filtered,
    efficiency_filtered,
    underrated_filtered,
    explosive_filtered,
    anchors_filtered,
    boundary_filtered
)

player_names = sorted(player_base["player_name"].dropna().unique().tolist()) if not player_base.empty else []

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs(
    [
        "Overview",
        "Player Archetypes",
        "Hidden Gems",
        "League Comparison",
        "Specialists",
        "Player Search",
        "Compare Players",
        "Key Insights",
    ]
)

with tab1:
    st.subheader("Overview")

    if overall_filtered.empty:
        st.warning("No data available for the selected filters.")
    else:
        c1, c2, c3, c4 = st.columns(4)

        c1.metric("Players in view", int(overall_filtered["player_name"].nunique()))
        c2.metric("Competitions", int(overall_filtered["competition"].nunique()))
        c3.metric("Highest Batting Index", round(overall_filtered["batting_index"].max(), 2))
        c4.metric("Highest Strike Rate", round(overall_filtered["strike_rate"].max(), 2))

        top_overall = top_n(overall_filtered, "batting_index", top_n_players)

        fig_overall = px.bar(
            top_overall.sort_values("batting_index"),
            x="batting_index",
            y="player_name",
            color="competition",
            orientation="h",
            hover_data=["innings", "total_runs", "avg_runs", "strike_rate"],
            title="Top Batters by Batting Index"
        )
        st.plotly_chart(fig_overall, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            high_runs = top_n(high_volume_filtered, "total_runs", top_n_players)
            if not high_runs.empty:
                fig_runs = px.bar(
                    high_runs.sort_values("total_runs"),
                    x="total_runs",
                    y="player_name",
                    color="competition",
                    orientation="h",
                    hover_data=["innings", "balls_faced", "avg_runs", "strike_rate"],
                    title="High Volume Run Scorers"
                )
                st.plotly_chart(fig_runs, use_container_width=True)

        with col2:
            consistent_top = top_n(consistent_filtered, "consistency_score", top_n_players)
            if not consistent_top.empty:
                fig_consistent = px.bar(
                    consistent_top.sort_values("consistency_score"),
                    x="consistency_score",
                    y="player_name",
                    color="competition",
                    orientation="h",
                    hover_data=["innings", "total_runs", "avg_runs", "strike_rate", "batting_index"],
                    title="Most Consistent Batters"
                )
                st.plotly_chart(fig_consistent, use_container_width=True)

        st.markdown("### Top Performers Table")
        st.dataframe(format_numeric_table(top_overall), use_container_width=True)

with tab2:
    st.subheader("Player Archetypes")

    if roles_filtered.empty:
        st.warning("No archetype data available for the selected filters.")
    else:
        fig_roles = px.scatter(
            roles_filtered,
            x="strike_rate",
            y="avg_runs",
            color="role",
            hover_name="player_name",
            hover_data=["competition", "innings"],
            title="Batting Roles by Average and Strike Rate"
        )
        st.plotly_chart(fig_roles, use_container_width=True)

        role_counts = roles_filtered["role"].value_counts().reset_index()
        role_counts.columns = ["role", "players"]

        fig_role_counts = px.bar(
            role_counts,
            x="role",
            y="players",
            color="role",
            title="Number of Players by Role"
        )
        st.plotly_chart(fig_role_counts, use_container_width=True)

        st.markdown("### Role Breakdown Table")
        st.dataframe(format_numeric_table(roles_filtered), use_container_width=True)

with tab3:
    st.subheader("Hidden Gems")

    if underrated_filtered.empty:
        st.warning("No hidden gem data available for the selected filters.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            fig_underrated = px.bar(
                underrated_filtered.sort_values("batting_index"),
                x="batting_index",
                y="player_name",
                color="competition",
                orientation="h",
                hover_data=["innings", "total_runs", "avg_runs", "strike_rate", "consistency_score"],
                title="Underrated Players by Batting Index"
            )
            st.plotly_chart(fig_underrated, use_container_width=True)

        with col2:
            fig_underrated_scatter = px.scatter(
                underrated_filtered,
                x="strike_rate",
                y="avg_runs",
                size="batting_index",
                color="competition",
                hover_name="player_name",
                hover_data=["innings", "total_runs", "consistency_score"],
                title="Underrated Players Profile"
            )
            st.plotly_chart(fig_underrated_scatter, use_container_width=True)

        st.markdown("### Hidden Gems Table")
        st.dataframe(format_numeric_table(underrated_filtered), use_container_width=True)

        st.markdown("### Multi-League Performers")
        st.dataframe(
            format_numeric_table(
                top_n(multi_league_df, "batting_index", top_n_players)
            ),
            use_container_width=True
        )

with tab4:
    st.subheader("League Comparison")

    if league_env_filtered.empty:
        st.warning("No league-level data available for the selected filters.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            fig_league_sr = px.bar(
                league_env_filtered.sort_values("avg_strike_rate"),
                x="competition",
                y="avg_strike_rate",
                color="competition",
                title="Average Strike Rate by League"
            )
            st.plotly_chart(fig_league_sr, use_container_width=True)

        with col2:
            fig_league_avg = px.bar(
                league_env_filtered.sort_values("avg_runs"),
                x="competition",
                y="avg_runs",
                color="competition",
                title="Average Runs by League"
            )
            st.plotly_chart(fig_league_avg, use_container_width=True)

        col3, col4 = st.columns(2)

        with col3:
            fig_league_boundary = px.bar(
                league_env_filtered.sort_values("avg_boundary_pct"),
                x="competition",
                y="avg_boundary_pct",
                color="competition",
                title="Average Boundary Percentage by League"
            )
            st.plotly_chart(fig_league_boundary, use_container_width=True)

        with col4:
            fig_league_index = px.bar(
                league_env_filtered.sort_values("avg_batting_index"),
                x="competition",
                y="avg_batting_index",
                color="competition",
                title="Average Batting Index by League"
            )
            st.plotly_chart(fig_league_index, use_container_width=True)

        st.markdown("### Best Players by League")
        st.dataframe(
            format_numeric_table(by_league_filtered.head(100)),
            use_container_width=True
        )

        st.markdown("### League Environment Table")
        st.dataframe(format_numeric_table(league_env_filtered), use_container_width=True)

with tab5:
    st.subheader("Specialists")

    specialist_view = st.radio(
        "Choose specialist category",
        ["Explosive Hitters", "Reliable Anchors", "Boundary Hitters", "Efficiency + Aggression"],
        horizontal=True
    )

    if specialist_view == "Explosive Hitters":
        if explosive_filtered.empty:
            st.warning("No explosive hitters found for the selected filters.")
        else:
            col1, col2 = st.columns(2)

            with col1:
                fig_explosive = px.bar(
                    explosive_filtered.sort_values("strike_rate"),
                    x="strike_rate",
                    y="player_name",
                    color="competition",
                    orientation="h",
                    hover_data=["innings", "total_runs", "sixes_per_innings", "boundary_pct"],
                    title="Explosive Hitters by Strike Rate"
                )
                st.plotly_chart(fig_explosive, use_container_width=True)

            with col2:
                fig_explosive_scatter = px.scatter(
                    explosive_filtered,
                    x="sixes_per_innings",
                    y="strike_rate",
                    size="boundary_pct",
                    color="competition",
                    hover_name="player_name",
                    hover_data=["innings", "total_runs"],
                    title="Power Profile of Explosive Hitters"
                )
                st.plotly_chart(fig_explosive_scatter, use_container_width=True)

            st.dataframe(format_numeric_table(explosive_filtered), use_container_width=True)

    elif specialist_view == "Reliable Anchors":
        if anchors_filtered.empty:
            st.warning("No anchors found for the selected filters.")
        else:
            fig_anchors = px.bar(
                anchors_filtered.sort_values("consistency_score"),
                x="consistency_score",
                y="player_name",
                color="competition",
                orientation="h",
                hover_data=["innings", "total_runs", "avg_runs", "strike_rate"],
                title="Reliable Anchors by Consistency Score"
            )
            st.plotly_chart(fig_anchors, use_container_width=True)
            st.dataframe(format_numeric_table(anchors_filtered), use_container_width=True)

    elif specialist_view == "Boundary Hitters":
        if boundary_filtered.empty:
            st.warning("No boundary hitters found for the selected filters.")
        else:
            col1, col2 = st.columns(2)

            with col1:
                fig_boundary = px.bar(
                    boundary_filtered.sort_values("boundary_pct"),
                    x="boundary_pct",
                    y="player_name",
                    color="competition",
                    orientation="h",
                    hover_data=["innings", "total_runs", "total_fours", "total_sixes", "sixes_per_innings"],
                    title="Boundary Hitters by Boundary Percentage"
                )
                st.plotly_chart(fig_boundary, use_container_width=True)

            with col2:
                fig_sixes = px.scatter(
                    boundary_filtered,
                    x="total_sixes",
                    y="boundary_pct",
                    size="total_runs",
                    color="competition",
                    hover_name="player_name",
                    hover_data=["innings", "total_fours"],
                    title="Boundary Threat Profile"
                )
                st.plotly_chart(fig_sixes, use_container_width=True)

            st.dataframe(format_numeric_table(boundary_filtered), use_container_width=True)

    elif specialist_view == "Efficiency + Aggression":
        if efficiency_filtered.empty:
            st.warning("No efficiency/aggression data found for the selected filters.")
        else:
            col1, col2 = st.columns(2)

            with col1:
                fig_efficiency = px.bar(
                    efficiency_filtered.sort_values("efficiency_score"),
                    x="efficiency_score",
                    y="player_name",
                    color="competition",
                    orientation="h",
                    hover_data=["innings", "total_runs", "avg_runs", "strike_rate", "batting_index"],
                    title="Efficiency + Aggression Leaders"
                )
                st.plotly_chart(fig_efficiency, use_container_width=True)

            with col2:
                fig_efficiency_scatter = px.scatter(
                    efficiency_filtered,
                    x="strike_rate",
                    y="avg_runs",
                    size="efficiency_score",
                    color="competition",
                    hover_name="player_name",
                    hover_data=["innings", "total_runs", "batting_index"],
                    title="Efficiency vs Aggression Profile"
                )
                st.plotly_chart(fig_efficiency_scatter, use_container_width=True)

            st.dataframe(format_numeric_table(efficiency_filtered), use_container_width=True)

with tab6:
    st.subheader("Player Search")

    if player_base.empty:
        st.warning("No player data available for the selected filters.")
    else:
        selected_player = st.selectbox("Select a player", options=player_names)

        player_df = player_base[player_base["player_name"] == selected_player].copy()

        if player_df.empty:
            st.warning("No records found for that player.")
        else:
            overview = (
                player_df.groupby("player_name", as_index=False)
                .agg(
                    leagues_played=("competition", "nunique"),
                    total_innings=("innings", "sum"),
                    total_runs=("total_runs", "sum"),
                    avg_runs=("avg_runs", "mean"),
                    strike_rate=("strike_rate", "mean"),
                    batting_index=("batting_index", "mean"),
                    consistency_score=("consistency_score", "mean"),
                    efficiency_score=("efficiency_score", "mean"),
                )
            )

            row = overview.iloc[0]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Leagues", int(row["leagues_played"]) if pd.notna(row["leagues_played"]) else 0)
            c2.metric("Innings", int(row["total_innings"]) if pd.notna(row["total_innings"]) else 0)
            c3.metric("Runs", int(row["total_runs"]) if pd.notna(row["total_runs"]) else 0)
            c4.metric("Batting Index", round(row["batting_index"], 2) if pd.notna(row["batting_index"]) else 0)

            c5, c6, c7, c8 = st.columns(4)
            c5.metric("Average", round(row["avg_runs"], 2) if pd.notna(row["avg_runs"]) else 0)
            c6.metric("Strike Rate", round(row["strike_rate"], 2) if pd.notna(row["strike_rate"]) else 0)
            c7.metric("Consistency", round(row["consistency_score"], 2) if pd.notna(row["consistency_score"]) else 0)
            c8.metric("Efficiency", round(row["efficiency_score"], 2) if pd.notna(row["efficiency_score"]) else 0)

            st.markdown("### League-by-League Profile")
            st.dataframe(
                format_numeric_table(player_df.sort_values("batting_index", ascending=False)),
                use_container_width=True
            )

            fig_player_leagues = px.bar(
                player_df.sort_values("batting_index"),
                x="batting_index",
                y="competition",
                color="role",
                orientation="h",
                hover_data=["innings", "avg_runs", "strike_rate", "total_runs"],
                title=f"{selected_player}: Batting Index by League"
            )
            st.plotly_chart(fig_player_leagues, use_container_width=True)

            fig_player_scatter = px.scatter(
                player_df,
                x="strike_rate",
                y="avg_runs",
                color="competition",
                size="innings",
                hover_name="competition",
                title=f"{selected_player}: Average vs Strike Rate by League"
            )
            st.plotly_chart(fig_player_scatter, use_container_width=True)

with tab7:
    st.subheader("Compare Players")

    if player_base.empty or len(player_names) < 2:
        st.warning("Not enough player data available to compare.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            player_a = st.selectbox("Player A", options=player_names, index=0)

        with col2:
            default_b_index = 1 if len(player_names) > 1 else 0
            player_b = st.selectbox("Player B", options=player_names, index=default_b_index)

        compare_df = player_base[player_base["player_name"].isin([player_a, player_b])].copy()

        summary = (
            compare_df.groupby("player_name", as_index=False)
            .agg(
                leagues_played=("competition", "nunique"),
                innings=("innings", "sum"),
                total_runs=("total_runs", "sum"),
                avg_runs=("avg_runs", "mean"),
                strike_rate=("strike_rate", "mean"),
                batting_index=("batting_index", "mean"),
                consistency_score=("consistency_score", "mean"),
                efficiency_score=("efficiency_score", "mean"),
                boundary_pct=("boundary_pct", "mean"),
                sixes_per_innings=("sixes_per_innings", "mean"),
            )
        )

        st.markdown("### Comparison Table")
        st.dataframe(format_numeric_table(summary), use_container_width=True)

        metrics_long = summary.melt(
            id_vars="player_name",
            value_vars=[
                "avg_runs",
                "strike_rate",
                "batting_index",
                "consistency_score",
                "efficiency_score",
                "boundary_pct",
                "sixes_per_innings",
            ],
            var_name="metric",
            value_name="value"
        )

        fig_compare = px.bar(
            metrics_long,
            x="metric",
            y="value",
            color="player_name",
            barmode="group",
            title="Player Comparison Across Key Metrics"
        )
        st.plotly_chart(fig_compare, use_container_width=True)

        league_compare = compare_df[
            ["player_name", "competition", "avg_runs", "strike_rate", "batting_index", "role"]
        ].copy()

        st.markdown("### League-by-League Comparison")
        st.dataframe(
            format_numeric_table(league_compare.sort_values(["competition", "player_name"])),
            use_container_width=True
        )

        fig_compare_scatter = px.scatter(
            compare_df,
            x="strike_rate",
            y="avg_runs",
            color="player_name",
            symbol="competition",
            size="innings",
            hover_name="player_name",
            title="Average vs Strike Rate Comparison"
        )
        st.plotly_chart(fig_compare_scatter, use_container_width=True)

with tab8:
    st.subheader("Key Insights")

    if overall_filtered.empty:
        st.warning("No insight data available for the selected filters.")
    else:
        c1, c2, c3, c4 = st.columns(4)

        best_player = overall_filtered.sort_values("batting_index", ascending=False).iloc[0]
        c1.metric("Top Batter", best_player["player_name"])
        c2.metric("Top Batter Index", round(best_player["batting_index"], 2))

        if not league_env_filtered.empty:
            fastest_league = league_env_filtered.sort_values("avg_strike_rate", ascending=False).iloc[0]
            c3.metric("Fastest League", fastest_league["competition"])
            c4.metric("League Avg SR", round(fastest_league["avg_strike_rate"], 2))
        else:
            c3.metric("Fastest League", "N/A")
            c4.metric("League Avg SR", 0)

        st.markdown("### Main Conclusions")

        add_insight_card(
            "Best overall performer",
            f"{best_player['player_name']} has the highest batting index in the current filtered view "
            f"at {round(best_player['batting_index'], 2)}, combining run output and scoring speed at an elite level."
        )

        if not high_volume_filtered.empty:
            top_run_scorer = high_volume_filtered.sort_values("total_runs", ascending=False).iloc[0]
            add_insight_card(
                "Highest volume scorer",
                f"{top_run_scorer['player_name']} leads the selected group for total runs "
                f"with {int(top_run_scorer['total_runs'])} runs, showing sustained output rather than just short bursts."
            )

        if not consistent_filtered.empty:
            most_consistent = consistent_filtered.sort_values("consistency_score", ascending=False).iloc[0]
            add_insight_card(
                "Most reliable batter",
                f"{most_consistent['player_name']} posts the best consistency score at "
                f"{round(most_consistent['consistency_score'], 2)}, making them one of the safest repeat performers."
            )

        if not explosive_filtered.empty:
            most_explosive = explosive_filtered.sort_values("strike_rate", ascending=False).iloc[0]
            add_insight_card(
                "Most explosive profile",
                f"{most_explosive['player_name']} stands out as the most aggressive scorer with a strike rate of "
                f"{round(most_explosive['strike_rate'], 2)}."
            )

        if not anchors_filtered.empty:
            best_anchor = anchors_filtered.sort_values("consistency_score", ascending=False).iloc[0]
            add_insight_card(
                "Best anchor option",
                f"{best_anchor['player_name']} is the strongest anchor candidate in this view, "
                f"balancing stability with run production."
            )

        if not underrated_filtered.empty:
            top_hidden_gem = underrated_filtered.sort_values("batting_index", ascending=False).iloc[0]
            add_insight_card(
                "Best hidden gem",
                f"{top_hidden_gem['player_name']} looks undervalued, with a batting index of "
                f"{round(top_hidden_gem['batting_index'], 2)} and a profile that compares well with more obvious top names."
            )

        if not league_env_filtered.empty:
            fastest_league = league_env_filtered.sort_values("avg_strike_rate", ascending=False).iloc[0]
            toughest_league = league_env_filtered.sort_values("avg_strike_rate", ascending=True).iloc[0]

            add_insight_card(
                "League scoring environment",
                f"{fastest_league['competition']} appears to be the fastest-scoring environment "
                f"(average strike rate {round(fastest_league['avg_strike_rate'], 2)}), while "
                f"{toughest_league['competition']} looks slower paced on this metric "
                f"({round(toughest_league['avg_strike_rate'], 2)})."
            )

        if not roles_filtered.empty:
            role_counts = roles_filtered["role"].value_counts()
            top_role = role_counts.index[0]
            top_role_count = int(role_counts.iloc[0])

            add_insight_card(
                "Most common batting role",
                f"The most common player type in the filtered sample is '{top_role}', with "
                f"{top_role_count} players in that archetype."
            )

        if (
            not multi_league_df.empty
            and "leagues_played" in multi_league_df.columns
            and "player_name" in multi_league_df.columns
        ):
            multi_filtered = multi_league_df.copy()
            if "batting_index" in multi_filtered.columns:
                multi_filtered = multi_filtered.sort_values(
                    ["leagues_played", "batting_index"],
                    ascending=[False, False]
                )
            else:
                multi_filtered = multi_filtered.sort_values("leagues_played", ascending=False)

            multi_star = multi_filtered.iloc[0]
            add_insight_card(
                "Best cross-league performer",
                f"{multi_star['player_name']} stands out for cross-league performance, appearing across "
                f"{int(multi_star['leagues_played'])} leagues in the dataset."
            )

        st.markdown("### Analyst Notes")
        st.info(
            "This tab turns the dashboard from descriptive analytics into decision-support. "
            "Instead of only showing charts, it surfaces the main takeaways from the current filters."
        )

st.markdown("---")
st.markdown(
    """
    **How to use this dashboard**
    - Use the sidebar to filter by competition and minimum innings.
    - Start in **Overview** for the top performers.
    - Use **Player Archetypes** to understand playing style.
    - Use **Hidden Gems** to surface players who may be undervalued.
    - Use **League Comparison** to compare environments.
    - Use **Specialists** for role-specific analysis.
    - Use **Player Search** to inspect one batter across leagues.
    - Use **Compare Players** to compare two batters side by side.
    - Use **Key Insights** for automatically generated conclusions from the filtered data.
    """
)
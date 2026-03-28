from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
import streamlit as st
from psycopg.rows import dict_row

from database.db import get_db_pool

ARTIFACT_DIR = Path("../hackaton-frontend/artifacts/match_result_model_rf_v2")

PRIMARY = "#22c55e"
DARK_BG = "#0b1220"
CARD_BG = "#121a2b"
SOFT_BG = "#172036"
TEXT = "#e5eefc"
MUTED = "#8ea0bf"
BORDER = "#24314d"


@st.cache_resource
def load_model_artifacts():
    pipeline = joblib.load(ARTIFACT_DIR / "pipeline.joblib")
    label_encoder = joblib.load(ARTIFACT_DIR / "label_encoder.joblib")

    with open(ARTIFACT_DIR / "all_feature_columns.json", "r", encoding="utf-8") as f:
        feature_columns = json.load(f)

    return pipeline, label_encoder, feature_columns


@st.cache_data(ttl=60)
def run_query(query: str, params: tuple | None = None) -> pd.DataFrame:
    db_pool = get_db_pool()

    with db_pool.get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params or ())
            rows = cur.fetchall()

    return pd.DataFrame(rows)


@st.cache_data(ttl=60)
def load_matches() -> pd.DataFrame:
    query = """
        SELECT
            match_id,
            match_date,
            competition_name,
            season_name,
            home_team_name,
            away_team_name,
            home_score,
            away_score
        FROM silver.matches
        ORDER BY match_date DESC, match_id DESC
    """
    df = run_query(query)
    if not df.empty:
        df["match_date"] = pd.to_datetime(df["match_date"])
    return df


@st.cache_data(ttl=60)
def load_match_features(match_id: int) -> pd.DataFrame:
    query = """
        SELECT *
        FROM feature_store.training_match_pre_game_features_ml
        WHERE match_id = %s
        LIMIT 1
    """
    return run_query(query, (match_id,))


@st.cache_data(ttl=60)
def load_match_summary(match_id: int) -> pd.DataFrame:
    query = """
        SELECT
            match_id,
            competition_name,
            season_name,
            match_date,
            home_team_name,
            away_team_name,
            home_score,
            away_score,
            match_result
        FROM silver.matches
        WHERE match_id = %s
        LIMIT 1
    """
    df = run_query(query, (match_id,))
    if not df.empty:
        df["match_date"] = pd.to_datetime(df["match_date"])
    return df


@st.cache_data(ttl=60)
def load_recent_team_matches(team_name: str, limit: int = 5) -> pd.DataFrame:
    query = """
        SELECT
            match_date,
            home_team_name,
            away_team_name,
            home_score,
            away_score,
            match_result
        FROM silver.matches
        WHERE home_team_name = %s OR away_team_name = %s
        ORDER BY match_date DESC, match_id DESC
        LIMIT %s
    """
    df = run_query(query, (team_name, team_name, limit))
    if not df.empty:
        df["match_date"] = pd.to_datetime(df["match_date"])
    return df


@st.cache_data(ttl=60)
def load_team_comparison(match_id: int) -> pd.DataFrame:
    query = """
        SELECT
            match_id,
            home_team_name,
            away_team_name,
            home_last5_points_avg,
            away_last5_points_avg,
            home_last5_goals_for_avg,
            away_last5_goals_for_avg,
            home_last5_goals_against_avg,
            away_last5_goals_against_avg,
            home_last5_goal_diff_avg,
            away_last5_goal_diff_avg,
            home_home_last5_points_avg,
            away_away_last5_points_avg,
            home_last5_shots_avg,
            away_last5_shots_avg,
            home_last5_shots_on_target_avg,
            away_last5_shots_on_target_avg,
            diff_points_avg,
            diff_goal_diff_avg,
            diff_shots_avg,
            diff_shots_on_target_avg,
            home_last5_win_rate,
            away_last5_win_rate,
            home_distinct_players_last5,
            away_distinct_players_last5
        FROM feature_store.training_match_pre_game_features_ml
        WHERE match_id = %s
        LIMIT 1
    """
    return run_query(query, (match_id,))


@st.cache_data(ttl=60)
def load_today_like_matches(limit: int = 20) -> pd.DataFrame:
    query = """
        SELECT
            match_id,
            match_date,
            competition_name,
            home_team_name,
            away_team_name
        FROM silver.matches
        ORDER BY match_date DESC, match_id DESC
        LIMIT %s
    """
    df = run_query(query, (limit,))
    if not df.empty:
        df["match_date"] = pd.to_datetime(df["match_date"])
    return df


def make_prediction(match_features_df: pd.DataFrame) -> dict:
    pipeline, label_encoder, feature_columns = load_model_artifacts()

    df = match_features_df.copy()

    if "diff_days_rest" not in df.columns:
        df["diff_days_rest"] = (
            pd.to_numeric(df.get("home_days_since_last_match"), errors="coerce").fillna(0)
            - pd.to_numeric(df.get("away_days_since_last_match"), errors="coerce").fillna(0)
        )

    if "diff_home_strength" not in df.columns:
        df["diff_home_strength"] = (
            pd.to_numeric(df.get("home_home_last5_points_avg"), errors="coerce").fillna(0)
            - pd.to_numeric(df.get("away_away_last5_points_avg"), errors="coerce").fillna(0)
        )

    for col in feature_columns:
        if col in df.columns and col != "competition_name":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    missing_features = [col for col in feature_columns if col not in df.columns]
    if missing_features:
        raise KeyError(f"Features ausentes para previsão: {missing_features}")

    X = df[feature_columns].copy()
    pred_encoded = pipeline.predict(X)[0]
    pred_label = label_encoder.inverse_transform([pred_encoded])[0]

    probs = pipeline.predict_proba(X)[0]
    class_labels = label_encoder.inverse_transform(range(len(probs)))

    probability_map = {str(label): float(prob) for label, prob in zip(class_labels, probs)}

    return {
        "prediction": pred_label,
        "probabilities": probability_map,
    }


def prob_to_odds(prob: float) -> str:
    if prob is None or prob <= 0:
        return "-"
    return f"{(1 / prob):.2f}"


def result_label(value: str) -> str:
    mapping = {
        "H": "Vitória mandante",
        "D": "Empate",
        "A": "Vitória visitante",
    }
    return mapping.get(value, value)


def get_form_letter(row: pd.Series, team_name: str) -> str:
    is_home = row["home_team_name"] == team_name
    gf = row["home_score"] if is_home else row["away_score"]
    ga = row["away_score"] if is_home else row["home_score"]
    if gf > ga:
        return "W"
    if gf == ga:
        return "D"
    return "L"


def style_form_html(df: pd.DataFrame, team_name: str) -> str:
    letters = []
    for _, row in df.iterrows():
        outcome = get_form_letter(row, team_name)
        color = {"W": "#16a34a", "D": "#f59e0b", "L": "#ef4444"}[outcome]
        letters.append(
            f"<span style='display:inline-flex;align-items:center;justify-content:center;width:30px;height:30px;"
            f"border-radius:999px;background:{color};color:white;font-weight:700;margin-right:8px;'>{outcome}</span>"
        )
    return "".join(letters)


def metric_card(title: str, value: str, subtitle: str = "") -> str:
    return f"""
    <div style='background:{CARD_BG};border:1px solid {BORDER};border-radius:18px;padding:18px;'>
        <div style='color:{MUTED};font-size:13px;margin-bottom:6px;'>{title}</div>
        <div style='color:{TEXT};font-size:28px;font-weight:700;line-height:1.1;'>{value}</div>
        <div style='color:{MUTED};font-size:12px;margin-top:6px;'>{subtitle}</div>
    </div>
    """


def market_card(title: str, prob: float, selected: bool = False) -> str:
    border_color = PRIMARY if selected else BORDER
    glow = "box-shadow:0 0 0 1px rgba(34,197,94,.25) inset;" if selected else ""
    return f"""
    <div style='background:{CARD_BG};border:1px solid {border_color};{glow}border-radius:18px;padding:18px;text-align:center;'>
        <div style='color:{MUTED};font-size:13px;margin-bottom:10px;'>{title}</div>
        <div style='color:{TEXT};font-size:30px;font-weight:800;'>{prob * 100:.1f}%</div>
        <div style='color:{PRIMARY};font-size:14px;margin-top:8px;'>Odd justa {prob_to_odds(prob)}</div>
    </div>
    """


def comparison_bar(
    label: str,
    home_value: float | None,
    away_value: float | None,
    home_name: str,
    away_name: str,
) -> str:
    home = float(home_value or 0)
    away = float(away_value or 0)
    total = abs(home) + abs(away)
    if total == 0:
        home_pct = away_pct = 50
    else:
        home_pct = max(8, int((abs(home) / total) * 100))
        away_pct = max(8, 100 - home_pct)

    return f"""
    <div style='background:{CARD_BG};border:1px solid {BORDER};border-radius:18px;padding:14px 16px;margin-bottom:12px;'>
        <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;'>
            <div style='color:{TEXT};font-weight:600;'>{label}</div>
            <div style='color:{MUTED};font-size:13px;'>{home_name}: {home:.2f} • {away_name}: {away:.2f}</div>
        </div>
        <div style='display:flex;height:12px;border-radius:999px;overflow:hidden;background:{SOFT_BG};'>
            <div style='width:{home_pct}%;background:{PRIMARY};'></div>
            <div style='width:{away_pct}%;background:#3b82f6;'></div>
        </div>
    </div>
    """


def render_css() -> None:
    st.markdown(
        f"""
        <style>
            .stApp {{
                background: linear-gradient(180deg, {DARK_BG} 0%, #0f172a 100%);
                color: {TEXT};
            }}
            .block-container {{
                padding-top: 1.2rem;
                padding-bottom: 2rem;
                max-width: 1300px;
            }}
            h1, h2, h3, h4 {{ color: {TEXT}; }}
            [data-testid="stSidebar"] {{
                background: #0e1627;
                border-right: 1px solid {BORDER};
            }}
            div[data-testid="stMetric"] {{
                background:{CARD_BG};
                border:1px solid {BORDER};
                padding:14px;
                border-radius:16px;
            }}
            .small-muted {{ color:{MUTED}; font-size:13px; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(page_title="ML Futebol Dashboard", page_icon="⚽", layout="wide")
    render_css()

    st.markdown(
        f"""
        <div style='background:linear-gradient(135deg, #13203a 0%, #0f172a 100%);border:1px solid {BORDER};
                    border-radius:22px;padding:22px 24px;margin-bottom:20px;'>
            <div style='color:{PRIMARY};font-weight:700;letter-spacing:.08em;font-size:12px;margin-bottom:8px;'>ML FUTEBOL</div>
            <div style='color:{TEXT};font-size:34px;font-weight:800;line-height:1.1;'>Dashboard de Probabilidades e Insights</div>
            <div style='color:{MUTED};font-size:15px;margin-top:8px;'>Modelo de previsão + comparação estatística em estilo sportsbook.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    matches_df = load_matches()
    if matches_df.empty:
        st.warning("Nenhuma partida encontrada em silver.matches.")
        return

    with st.sidebar:
        st.subheader("Seleção de jogo")
        matches_df["label"] = (
            matches_df["match_date"].dt.strftime("%Y-%m-%d")
            + " | "
            + matches_df["competition_name"]
            + " | "
            + matches_df["home_team_name"]
            + " vs "
            + matches_df["away_team_name"]
        )

        selected_label = st.selectbox("Partida", matches_df["label"].tolist())
        st.markdown("---")
        st.caption("Últimos jogos carregados")

        quick_df = load_today_like_matches(12)
        for _, row in quick_df.iterrows():
            st.markdown(
                f"<div class='small-muted'>{row['match_date'].strftime('%Y-%m-%d')} • {row['competition_name']}</div>"
                f"<div style='margin-bottom:10px;color:{TEXT};font-size:14px;'>{row['home_team_name']} vs {row['away_team_name']}</div>",
                unsafe_allow_html=True,
            )

    selected_match = matches_df.loc[matches_df["label"] == selected_label].iloc[0]
    match_id = int(selected_match["match_id"])

    summary_df = load_match_summary(match_id)
    features_df = load_match_features(match_id)
    comparison_df = load_team_comparison(match_id)

    if summary_df.empty or features_df.empty:
        st.error("Não foi possível carregar resumo ou features da partida.")
        return

    summary = summary_df.iloc[0]
    prediction = make_prediction(features_df)
    probs = prediction["probabilities"]
    predicted = prediction["prediction"]

    home_name = summary["home_team_name"]
    away_name = summary["away_team_name"]

    st.markdown(
        f"""
        <div style='display:flex;justify-content:space-between;gap:18px;align-items:center;background:{CARD_BG};border:1px solid {BORDER};border-radius:24px;padding:24px;margin-bottom:20px;'>
            <div style='flex:1;text-align:center;'>
                <div style='color:{TEXT};font-size:28px;font-weight:800;'>{home_name}</div>
                <div style='color:{MUTED};font-size:14px;margin-top:6px;'>Mandante</div>
            </div>
            <div style='text-align:center;min-width:220px;'>
                <div style='color:{PRIMARY};font-size:13px;font-weight:700;margin-bottom:8px;'>{summary['competition_name']} • {summary['season_name']}</div>
                <div style='color:{TEXT};font-size:18px;font-weight:700;'>vs</div>
                <div style='color:{MUTED};font-size:13px;margin-top:8px;'>{summary['match_date'].strftime('%Y-%m-%d')}</div>
            </div>
            <div style='flex:1;text-align:center;'>
                <div style='color:{TEXT};font-size:28px;font-weight:800;'>{away_name}</div>
                <div style='color:{MUTED};font-size:14px;margin-top:6px;'>Visitante</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.markdown(metric_card("Previsão principal", result_label(predicted)), unsafe_allow_html=True)
    with m2:
        st.markdown(metric_card("Odd mandante", prob_to_odds(probs.get("H", 0))), unsafe_allow_html=True)
    with m3:
        st.markdown(metric_card("Odd empate", prob_to_odds(probs.get("D", 0))), unsafe_allow_html=True)
    with m4:
        st.markdown(metric_card("Odd visitante", prob_to_odds(probs.get("A", 0))), unsafe_allow_html=True)

    st.markdown("### Mercado 1X2")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(market_card(home_name, probs.get("H", 0), predicted == "H"), unsafe_allow_html=True)
    with c2:
        st.markdown(market_card("Empate", probs.get("D", 0), predicted == "D"), unsafe_allow_html=True)
    with c3:
        st.markdown(market_card(away_name, probs.get("A", 0), predicted == "A"), unsafe_allow_html=True)

    left, right = st.columns([1.15, 0.85])

    with left:
        st.markdown("### Comparação estatística")
        if not comparison_df.empty:
            comp = comparison_df.iloc[0]
            st.markdown(
                comparison_bar(
                    "Pontos médios nos últimos 5",
                    comp["home_last5_points_avg"],
                    comp["away_last5_points_avg"],
                    home_name,
                    away_name,
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                comparison_bar(
                    "Gols marcados médios",
                    comp["home_last5_goals_for_avg"],
                    comp["away_last5_goals_for_avg"],
                    home_name,
                    away_name,
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                comparison_bar(
                    "Gols sofridos médios",
                    comp["home_last5_goals_against_avg"],
                    comp["away_last5_goals_against_avg"],
                    home_name,
                    away_name,
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                comparison_bar(
                    "Saldo médio",
                    comp["home_last5_goal_diff_avg"],
                    comp["away_last5_goal_diff_avg"],
                    home_name,
                    away_name,
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                comparison_bar(
                    "Força mandante/visitante",
                    comp["home_home_last5_points_avg"],
                    comp["away_away_last5_points_avg"],
                    home_name,
                    away_name,
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                comparison_bar(
                    "Finalizações médias",
                    comp.get("home_last5_shots_avg"),
                    comp.get("away_last5_shots_avg"),
                    home_name,
                    away_name,
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                comparison_bar(
                    "Finalizações no alvo",
                    comp.get("home_last5_shots_on_target_avg"),
                    comp.get("away_last5_shots_on_target_avg"),
                    home_name,
                    away_name,
                ),
                unsafe_allow_html=True,
            )

    with right:
        st.markdown("### Forma recente")
        home_recent = load_recent_team_matches(home_name, 5)
        away_recent = load_recent_team_matches(away_name, 5)

        if not home_recent.empty:
            st.markdown(
                f"<div style='color:{TEXT};font-weight:700;margin-bottom:8px;'>{home_name}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(style_form_html(home_recent, home_name), unsafe_allow_html=True)

        if not away_recent.empty:
            st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
            st.markdown(
                f"<div style='color:{TEXT};font-weight:700;margin-bottom:8px;'>{away_name}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(style_form_html(away_recent, away_name), unsafe_allow_html=True)

        st.markdown("### Insights automáticos")
        insights = []
        if not comparison_df.empty:
            comp = comparison_df.iloc[0]

            if comp["diff_points_avg"] is not None:
                if comp["diff_points_avg"] > 0.5:
                    insights.append(f"{home_name} chega com melhor forma recente em pontos.")
                elif comp["diff_points_avg"] < -0.5:
                    insights.append(f"{away_name} chega com melhor forma recente em pontos.")
                else:
                    insights.append("A forma recente em pontos indica equilíbrio.")

            if comp["diff_goal_diff_avg"] is not None:
                if comp["diff_goal_diff_avg"] > 0.3:
                    insights.append(f"{home_name} apresenta melhor saldo recente de gols.")
                elif comp["diff_goal_diff_avg"] < -0.3:
                    insights.append(f"{away_name} apresenta melhor saldo recente de gols.")
                else:
                    insights.append("O saldo recente de gols está equilibrado.")

            if comp.get("diff_shots_on_target_avg") is not None:
                if comp["diff_shots_on_target_avg"] > 0.4:
                    insights.append(f"{home_name} cria mais volume de chances no alvo.")
                elif comp["diff_shots_on_target_avg"] < -0.4:
                    insights.append(f"{away_name} cria mais volume de chances no alvo.")

            if comp.get("home_last5_win_rate") is not None and comp.get("away_last5_win_rate") is not None:
                if comp["home_last5_win_rate"] - comp["away_last5_win_rate"] > 0.15:
                    insights.append(f"{home_name} tem taxa de vitória recente superior.")
                elif comp["away_last5_win_rate"] - comp["home_last5_win_rate"] > 0.15:
                    insights.append(f"{away_name} tem taxa de vitória recente superior.")

        for insight in insights[:4]:
            st.markdown(
                f"<div style='background:{CARD_BG};border:1px solid {BORDER};border-radius:16px;padding:12px 14px;margin-bottom:10px;color:{TEXT};'>{insight}</div>",
                unsafe_allow_html=True,
            )

    st.markdown("### Tabela comparativa")
    if not comparison_df.empty:
        comp = comparison_df.iloc[0]
        table_df = pd.DataFrame(
            {
                "Métrica": [
                    "Pontos médios últimos 5",
                    "Win rate últimos 5",
                    "Gols marcados médios",
                    "Gols sofridos médios",
                    "Saldo médio",
                    "Pontos em casa/fora",
                    "Finalizações médias",
                    "Finalizações no alvo",
                    "Jogadores distintos últimos 5",
                ],
                home_name: [
                    comp["home_last5_points_avg"],
                    comp.get("home_last5_win_rate"),
                    comp["home_last5_goals_for_avg"],
                    comp["home_last5_goals_against_avg"],
                    comp["home_last5_goal_diff_avg"],
                    comp["home_home_last5_points_avg"],
                    comp.get("home_last5_shots_avg"),
                    comp.get("home_last5_shots_on_target_avg"),
                    comp.get("home_distinct_players_last5"),
                ],
                away_name: [
                    comp["away_last5_points_avg"],
                    comp.get("away_last5_win_rate"),
                    comp["away_last5_goals_for_avg"],
                    comp["away_last5_goals_against_avg"],
                    comp["away_last5_goal_diff_avg"],
                    comp["away_away_last5_points_avg"],
                    comp.get("away_last5_shots_avg"),
                    comp.get("away_last5_shots_on_target_avg"),
                    comp.get("away_distinct_players_last5"),
                ],
            }
        )
        st.dataframe(table_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from psycopg.rows import dict_row
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    classification_report,
    confusion_matrix,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

from database.db import get_db_pool

TARGET_COLUMN = "target_result"
DATE_COLUMN = "match_date"
ARTIFACT_DIR = Path("artifacts/match_result_model_rf_v3_fbref")

BASE_NUMERIC_FEATURE_COLUMNS = [
    "home_matches_played_before",
    "away_matches_played_before",
    "home_days_since_last_match",
    "away_days_since_last_match",

    "home_last5_points_avg",
    "away_last5_points_avg",
    "home_last5_win_rate",
    "away_last5_win_rate",
    "home_last5_draw_rate",
    "away_last5_draw_rate",
    "home_last5_loss_rate",
    "away_last5_loss_rate",

    "home_last5_goals_for_avg",
    "away_last5_goals_for_avg",
    "home_last5_goals_against_avg",
    "away_last5_goals_against_avg",
    "home_last5_goal_diff_avg",
    "away_last5_goal_diff_avg",

    "home_home_last5_points_avg",
    "away_away_last5_points_avg",
    "home_home_last5_goals_for_avg",
    "away_away_last5_goals_for_avg",
    "home_home_last5_goals_against_avg",
    "away_away_last5_goals_against_avg",

    "home_distinct_players_last5",
    "away_distinct_players_last5",

    "home_last5_shots_avg",
    "away_last5_shots_avg",
    "home_last5_shots_on_target_avg",
    "away_last5_shots_on_target_avg",

    "diff_points_avg",
    "diff_win_rate",
    "diff_goals_for_avg",
    "diff_goals_against_avg",
    "diff_goal_diff_avg",
    "diff_shots_avg",
    "diff_shots_on_target_avg",

    "diff_days_rest",
    "diff_home_strength",
]

FBREF_NUMERIC_FEATURE_COLUMNS = [
    "home_lineup_xg_sum",
    "away_lineup_xg_sum",
    "home_lineup_xag_sum",
    "away_lineup_xag_sum",

    "home_lineup_shots_sum",
    "away_lineup_shots_sum",
    "home_lineup_shots_on_target_sum",
    "away_lineup_shots_on_target_sum",

    "home_lineup_tackles_sum",
    "away_lineup_tackles_sum",
    "home_lineup_interceptions_sum",
    "away_lineup_interceptions_sum",
    "home_lineup_blocks_sum",
    "away_lineup_blocks_sum",

    "home_goalkeeper_save_pct",
    "away_goalkeeper_save_pct",

    "home_fbref_matched_players",
    "away_fbref_matched_players",

    "diff_lineup_xg",
    "diff_lineup_xag",
    "diff_lineup_shots",
    "diff_lineup_shots_on_target",
    "diff_lineup_defense",
    "diff_goalkeeper_save_pct",
]

NUMERIC_FEATURE_COLUMNS = BASE_NUMERIC_FEATURE_COLUMNS + FBREF_NUMERIC_FEATURE_COLUMNS

CATEGORICAL_FEATURE_COLUMNS = [
    "competition_name",
]

ALL_FEATURE_COLUMNS = NUMERIC_FEATURE_COLUMNS + CATEGORICAL_FEATURE_COLUMNS

PYTHON_DERIVED_FEATURES = {
    "diff_days_rest",
    "diff_home_strength",
    "diff_lineup_xg",
    "diff_lineup_xag",
    "diff_lineup_shots",
    "diff_lineup_shots_on_target",
    "diff_lineup_defense",
    "diff_goalkeeper_save_pct",
}


def load_training_dataframe() -> pd.DataFrame:
    db_pool = get_db_pool()

    query = """
        SELECT *
        FROM feature_store.training_match_pre_game_features_v2_ml
        ORDER BY match_date, match_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            rows = cur.fetchall()

    if not rows:
        raise ValueError(
            "Nenhum dado encontrado em feature_store.training_match_pre_game_features_v2_ml."
        )

    df = pd.DataFrame(rows)
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])
    return df


def add_incremental_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    numeric_to_force = [
        "home_days_since_last_match",
        "away_days_since_last_match",
        "home_home_last5_points_avg",
        "away_away_last5_points_avg",
        "home_lineup_xg_sum",
        "away_lineup_xg_sum",
        "home_lineup_xag_sum",
        "away_lineup_xag_sum",
        "home_lineup_shots_sum",
        "away_lineup_shots_sum",
        "home_lineup_shots_on_target_sum",
        "away_lineup_shots_on_target_sum",
        "home_lineup_tackles_sum",
        "away_lineup_tackles_sum",
        "home_lineup_interceptions_sum",
        "away_lineup_interceptions_sum",
        "home_lineup_blocks_sum",
        "away_lineup_blocks_sum",
        "home_goalkeeper_save_pct",
        "away_goalkeeper_save_pct",
        "home_fbref_matched_players",
        "away_fbref_matched_players",
    ]

    for col in numeric_to_force:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["diff_days_rest"] = (
        df["home_days_since_last_match"] - df["away_days_since_last_match"]
    )

    df["diff_home_strength"] = (
        df["home_home_last5_points_avg"] - df["away_away_last5_points_avg"]
    )

    df["diff_lineup_xg"] = (
        df["home_lineup_xg_sum"] - df["away_lineup_xg_sum"]
    )

    df["diff_lineup_xag"] = (
        df["home_lineup_xag_sum"] - df["away_lineup_xag_sum"]
    )

    df["diff_lineup_shots"] = (
        df["home_lineup_shots_sum"] - df["away_lineup_shots_sum"]
    )

    df["diff_lineup_shots_on_target"] = (
        df["home_lineup_shots_on_target_sum"] - df["away_lineup_shots_on_target_sum"]
    )

    df["diff_lineup_defense"] = (
        df["home_lineup_tackles_sum"].fillna(0)
        + df["home_lineup_interceptions_sum"].fillna(0)
        + df["home_lineup_blocks_sum"].fillna(0)
        - df["away_lineup_tackles_sum"].fillna(0)
        - df["away_lineup_interceptions_sum"].fillna(0)
        - df["away_lineup_blocks_sum"].fillna(0)
    )

    df["diff_goalkeeper_save_pct"] = (
        df["home_goalkeeper_save_pct"] - df["away_goalkeeper_save_pct"]
    )

    return df


def temporal_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = df.sort_values([DATE_COLUMN, "match_id"]).reset_index(drop=True)

    n = len(df)
    train_end = int(n * 0.70)
    valid_end = int(n * 0.85)

    train_df = df.iloc[:train_end].copy()
    valid_df = df.iloc[train_end:valid_end].copy()
    test_df = df.iloc[valid_end:].copy()

    return train_df, valid_df, test_df


def build_xy(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    X = df[ALL_FEATURE_COLUMNS].copy()
    y = df[TARGET_COLUMN].copy()
    return X, y


def print_dataset_summary(name: str, df: pd.DataFrame) -> None:
    print(f"\n=== {name} ===")
    print(f"Linhas: {len(df)}")
    print(f"Período: {df[DATE_COLUMN].min().date()} até {df[DATE_COLUMN].max().date()}")
    print("Distribuição do target:")
    print(df[TARGET_COLUMN].value_counts(normalize=True).sort_index())
    print("\nDistribuição por competição:")
    print(df["competition_name"].value_counts().sort_index())


def print_null_summary(df: pd.DataFrame) -> None:
    null_summary = df[ALL_FEATURE_COLUMNS].isnull().sum().sort_values(ascending=False)
    null_summary = null_summary[null_summary > 0]

    print("\n=== Nulos por feature ===")
    if null_summary.empty:
        print("Nenhum nulo encontrado nas features.")
    else:
        print(null_summary.to_string())


def evaluate_split(
    name: str,
    pipeline: Pipeline,
    X: pd.DataFrame,
    y_true_encoded,
    label_encoder: LabelEncoder,
) -> dict:
    y_pred_encoded = pipeline.predict(X)

    acc = accuracy_score(y_true_encoded, y_pred_encoded)
    bal_acc = balanced_accuracy_score(y_true_encoded, y_pred_encoded)

    y_true = label_encoder.inverse_transform(y_true_encoded)
    y_pred = label_encoder.inverse_transform(y_pred_encoded)

    print(f"\n=== Avaliação: {name} ===")
    print(f"Accuracy: {acc:.4f}")
    print(f"Balanced accuracy: {bal_acc:.4f}")
    print("\nClassification report:")
    print(classification_report(y_true, y_pred, digits=4))
    print("Confusion matrix:")
    print(confusion_matrix(y_true, y_pred, labels=["A", "D", "H"]))

    return {
        "accuracy": float(acc),
        "balanced_accuracy": float(bal_acc),
    }


def get_feature_importances(pipeline: Pipeline) -> pd.DataFrame:
    preprocessor: ColumnTransformer = pipeline.named_steps["preprocessor"]
    model: RandomForestClassifier = pipeline.named_steps["model"]

    encoded_feature_names = preprocessor.get_feature_names_out()
    importances = model.feature_importances_

    return pd.DataFrame(
        {
            "feature": encoded_feature_names,
            "importance": importances,
        }
    ).sort_values("importance", ascending=False)


def save_artifacts(
    pipeline: Pipeline,
    label_encoder: LabelEncoder,
    feature_importances: pd.DataFrame,
    metrics: dict,
) -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    joblib.dump(pipeline, ARTIFACT_DIR / "pipeline.joblib")
    joblib.dump(label_encoder, ARTIFACT_DIR / "label_encoder.joblib")

    with open(ARTIFACT_DIR / "numeric_feature_columns.json", "w", encoding="utf-8") as f:
        json.dump(NUMERIC_FEATURE_COLUMNS, f, ensure_ascii=False, indent=2)

    with open(ARTIFACT_DIR / "categorical_feature_columns.json", "w", encoding="utf-8") as f:
        json.dump(CATEGORICAL_FEATURE_COLUMNS, f, ensure_ascii=False, indent=2)

    with open(ARTIFACT_DIR / "all_feature_columns.json", "w", encoding="utf-8") as f:
        json.dump(ALL_FEATURE_COLUMNS, f, ensure_ascii=False, indent=2)

    with open(ARTIFACT_DIR / "used_feature_columns.json", "w", encoding="utf-8") as f:
        json.dump(ALL_FEATURE_COLUMNS, f, ensure_ascii=False, indent=2)

    with open(ARTIFACT_DIR / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    feature_importances.to_csv(ARTIFACT_DIR / "feature_importances.csv", index=False)

    print(f"\nArtefatos salvos em: {ARTIFACT_DIR.resolve()}")


def validate_raw_dataframe(df: pd.DataFrame) -> None:
    required_base_columns = {
        TARGET_COLUMN,
        DATE_COLUMN,
        "match_id",
        "competition_name",
    }

    required_db_features = [
        col for col in ALL_FEATURE_COLUMNS
        if col not in PYTHON_DERIVED_FEATURES
    ]

    missing_base = [col for col in required_base_columns if col not in df.columns]
    if missing_base:
        raise ValueError(
            "Colunas-base ausentes no dataframe:\n- " + "\n- ".join(sorted(missing_base))
        )

    missing_features = [col for col in required_db_features if col not in df.columns]
    if missing_features:
        raise ValueError(
            "Features ausentes no dataframe:\n- " + "\n- ".join(missing_features)
        )


def validate_final_dataframe(df: pd.DataFrame) -> None:
    missing_features = [col for col in ALL_FEATURE_COLUMNS if col not in df.columns]
    if missing_features:
        raise ValueError(
            "Features ausentes após engenharia incremental:\n- " + "\n- ".join(missing_features)
        )


def main() -> None:
    df = load_training_dataframe()
    print(f"Total de linhas carregadas: {len(df)}")

    validate_raw_dataframe(df)

    print("\n=== Distribuição inicial por competição ===")
    print(df["competition_name"].value_counts().sort_index().to_string())

    # filtro de histórico mínimo + cobertura FBref mínima
    df = df[
        (df["home_matches_played_before"] >= 5)
        & (df["away_matches_played_before"] >= 5)
        & (df["home_fbref_matched_players"] >= 6)
        & (df["away_fbref_matched_players"] >= 6)
    ].copy()

    if df.empty:
        raise ValueError(
            "Após os filtros de histórico e cobertura FBref mínima, não restaram linhas para treino."
        )

    df = add_incremental_features(df)
    validate_final_dataframe(df)

    print(f"\nTotal de linhas após filtros: {len(df)}")
    print_null_summary(df)

    train_df, valid_df, test_df = temporal_split(df)

    print_dataset_summary("TRAIN", train_df)
    print_dataset_summary("VALID", valid_df)
    print_dataset_summary("TEST", test_df)

    X_train, y_train = build_xy(train_df)
    X_valid, y_valid = build_xy(valid_df)
    X_test, y_test = build_xy(test_df)

    label_encoder = LabelEncoder()
    y_train_enc = label_encoder.fit_transform(y_train)
    y_valid_enc = label_encoder.transform(y_valid)
    y_test_enc = label_encoder.transform(y_test)

    print("\nClasses do target:", list(label_encoder.classes_))

    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, NUMERIC_FEATURE_COLUMNS),
            ("cat", categorical_transformer, CATEGORICAL_FEATURE_COLUMNS),
        ]
    )

    model = RandomForestClassifier(
        n_estimators=500,
        max_depth=10,
        min_samples_leaf=4,
        random_state=42,
        n_jobs=-1,
        class_weight="balanced_subsample",
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    print("\nTreinando modelo V3 com FBref...")
    pipeline.fit(X_train, y_train_enc)

    valid_metrics = evaluate_split(
        "VALID",
        pipeline,
        X_valid,
        y_valid_enc,
        label_encoder,
    )

    test_metrics = evaluate_split(
        "TEST",
        pipeline,
        X_test,
        y_test_enc,
        label_encoder,
    )

    feature_importances = get_feature_importances(pipeline)

    print("\n=== Top 30 importâncias ===")
    print(feature_importances.head(30).to_string(index=False))

    metrics = {
        "valid": valid_metrics,
        "test": test_metrics,
        "n_rows_total": int(len(df)),
        "n_rows_train": int(len(train_df)),
        "n_rows_valid": int(len(valid_df)),
        "n_rows_test": int(len(test_df)),
        "classes": list(label_encoder.classes_),
        "feature_count_numeric": len(NUMERIC_FEATURE_COLUMNS),
        "feature_count_categorical": len(CATEGORICAL_FEATURE_COLUMNS),
        "feature_count_total": len(ALL_FEATURE_COLUMNS),
    }

    save_artifacts(
        pipeline=pipeline,
        label_encoder=label_encoder,
        feature_importances=feature_importances,
        metrics=metrics,
    )


if __name__ == "__main__":
    main()
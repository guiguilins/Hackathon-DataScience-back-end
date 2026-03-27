from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
from psycopg.rows import dict_row
from sklearn.metrics import accuracy_score, classification_report, log_loss
from xgboost import XGBClassifier

from database.db import get_db_pool


@dataclass
class InPlayMatchResultTrainer:
    train_ratio: float = 0.70
    valid_ratio: float = 0.15
    random_state: int = 42

    def run(self) -> dict[str, Any]:
        df = self._load_features()

        if df.empty:
            raise ValueError("A tabela feature_store.match_in_game_features está vazia.")

        self._validate_input(df)

        print("\n=== DTYPES DAS FEATURES ===")
        print(df[self._feature_columns()].dtypes)

        print("\n=== SAMPLE DAS FEATURES ===")
        print(df[self._feature_columns()].head())

        split_data = self._temporal_split_by_match(df)

        X_train, y_train = self._prepare_xy(split_data["train"])
        X_valid, y_valid = self._prepare_xy(split_data["valid"])
        X_test, y_test = self._prepare_xy(split_data["test"])

        model = self._train_model(
            X_train=X_train,
            y_train=y_train,
            X_valid=X_valid,
            y_valid=y_valid,
        )

        results = {
            "train": self._evaluate(model, X_train, y_train, "TRAIN"),
            "valid": self._evaluate(model, X_valid, y_valid, "VALID"),
            "test": self._evaluate(model, X_test, y_test, "TEST"),
            "model": model,
            "feature_columns": list(X_train.columns),
            "split_summary": self._build_split_summary(split_data),
        }

        self._print_summary(results)

        return results

    def _load_features(self) -> pd.DataFrame:
        db_pool = get_db_pool()

        query = """
        SELECT
            match_id,
            minute,
            match_date,
            competition_name,
            season_name,
            home_team_id,
            home_team_name,
            away_team_id,
            away_team_name,
            home_score_now,
            away_score_now,
            goal_diff_now,
            home_shots_cum,
            away_shots_cum,
            home_shots_on_target_cum,
            away_shots_on_target_cum,
            home_xg_cum,
            away_xg_cum,
            home_shots_last_10,
            away_shots_last_10,
            home_shots_on_target_last_10,
            away_shots_on_target_last_10,
            home_xg_last_10,
            away_xg_last_10,
            home_red_cards,
            away_red_cards,
            home_fouls_cum,
            away_fouls_cum,
            home_passes_cum,
            away_passes_cum,
            diff_shots_cum,
            diff_shots_on_target_cum,
            diff_xg_cum,
            diff_shots_last_10,
            diff_shots_on_target_last_10,
            diff_xg_last_10,
            diff_red_cards,
            diff_fouls_cum,
            diff_passes_cum,
            remaining_minutes,
            target_result_final
        FROM feature_store.match_in_game_features
        ORDER BY match_date, match_id, minute;
        """

        with db_pool.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query)
                rows = cur.fetchall()

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        df["match_date"] = pd.to_datetime(df["match_date"])

        numeric_cols = [
            "match_id",
            "minute",
            "home_team_id",
            "away_team_id",
            "home_score_now",
            "away_score_now",
            "goal_diff_now",
            "home_shots_cum",
            "away_shots_cum",
            "home_shots_on_target_cum",
            "away_shots_on_target_cum",
            "home_xg_cum",
            "away_xg_cum",
            "home_shots_last_10",
            "away_shots_last_10",
            "home_shots_on_target_last_10",
            "away_shots_on_target_last_10",
            "home_xg_last_10",
            "away_xg_last_10",
            "home_red_cards",
            "away_red_cards",
            "home_fouls_cum",
            "away_fouls_cum",
            "home_passes_cum",
            "away_passes_cum",
            "diff_shots_cum",
            "diff_shots_on_target_cum",
            "diff_xg_cum",
            "diff_shots_last_10",
            "diff_shots_on_target_last_10",
            "diff_xg_last_10",
            "diff_red_cards",
            "diff_fouls_cum",
            "diff_passes_cum",
            "remaining_minutes",
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _validate_input(self, df: pd.DataFrame) -> None:
        required_cols = {
            "match_id",
            "minute",
            "match_date",
            "target_result_final",
        }
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Colunas obrigatórias ausentes: {sorted(missing)}")

        invalid_targets = set(df["target_result_final"].dropna().unique()) - {"H", "D", "A"}
        if invalid_targets:
            raise ValueError(f"Targets inválidos encontrados: {sorted(invalid_targets)}")

        null_counts = df[self._feature_columns() + ["target_result_final"]].isnull().sum()
        null_counts = null_counts[null_counts > 0]
        if not null_counts.empty:
            raise ValueError(f"Existem nulos nas colunas: {null_counts.to_dict()}")

    def _temporal_split_by_match(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        match_df = (
            df[["match_id", "match_date"]]
            .drop_duplicates()
            .sort_values(["match_date", "match_id"])
            .reset_index(drop=True)
        )

        total_matches = len(match_df)
        if total_matches < 10:
            raise ValueError(
                f"Poucas partidas para split temporal robusto. Total encontrado: {total_matches}"
            )

        train_end = int(total_matches * self.train_ratio)
        valid_end = int(total_matches * (self.train_ratio + self.valid_ratio))

        train_matches = set(match_df.iloc[:train_end]["match_id"].tolist())
        valid_matches = set(match_df.iloc[train_end:valid_end]["match_id"].tolist())
        test_matches = set(match_df.iloc[valid_end:]["match_id"].tolist())

        train_df = df[df["match_id"].isin(train_matches)].copy()
        valid_df = df[df["match_id"].isin(valid_matches)].copy()
        test_df = df[df["match_id"].isin(test_matches)].copy()

        return {
            "train": train_df.sort_values(["match_date", "match_id", "minute"]).reset_index(drop=True),
            "valid": valid_df.sort_values(["match_date", "match_id", "minute"]).reset_index(drop=True),
            "test": test_df.sort_values(["match_date", "match_id", "minute"]).reset_index(drop=True),
        }

    def _prepare_xy(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
        X = df[self._feature_columns()].copy()

        for col in X.columns:
            X[col] = pd.to_numeric(X[col], errors="coerce")

        null_counts = X.isnull().sum()
        null_counts = null_counts[null_counts > 0]
        if not null_counts.empty:
            raise ValueError(
                f"Nulos encontrados nas features após cast numérico: {null_counts.to_dict()}"
            )

        y = df["target_result_final"].map({"H": 0, "D": 1, "A": 2})

        if y.isnull().any():
            invalid_rows = df.loc[y.isnull(), ["match_id", "minute", "target_result_final"]]
            raise ValueError(
                f"Foram encontrados targets inválidos ou nulos:\n{invalid_rows.head(20).to_string(index=False)}"
            )

        y = y.astype(int)

        return X.astype("float32"), y

    def _train_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_valid: pd.DataFrame,
        y_valid: pd.Series,
    ) -> XGBClassifier:
        model = XGBClassifier(
            objective="multi:softprob",
            num_class=3,
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_lambda=1.0,
            random_state=self.random_state,
            eval_metric="mlogloss",
            tree_method="hist",
        )

        model.fit(
            X_train,
            y_train,
            eval_set=[(X_train, y_train), (X_valid, y_valid)],
            verbose=False,
        )
        return model

    def _evaluate(
        self,
        model: XGBClassifier,
        X: pd.DataFrame,
        y: pd.Series,
        split_name: str,
    ) -> dict[str, Any]:
        pred = model.predict(X)
        proba = model.predict_proba(X)

        metrics = {
            "split": split_name,
            "rows": len(X),
            "accuracy": float(accuracy_score(y, pred)),
            "log_loss": float(log_loss(y, proba, labels=[0, 1, 2])),
            "classification_report": classification_report(
                y,
                pred,
                labels=[0, 1, 2],
                target_names=["H", "D", "A"],
                digits=4,
                zero_division=0,
            ),
        }
        return metrics

    def _build_split_summary(self, split_data: dict[str, pd.DataFrame]) -> dict[str, Any]:
        summary = {}
        for name, df in split_data.items():
            summary[name] = {
                "rows": len(df),
                "matches": df["match_id"].nunique(),
                "date_min": str(df["match_date"].min().date()) if not df.empty else None,
                "date_max": str(df["match_date"].max().date()) if not df.empty else None,
                "target_distribution": (
                    df["target_result_final"].value_counts(normalize=True).sort_index().to_dict()
                    if not df.empty else {}
                ),
            }
        return summary

    def _print_summary(self, results: dict[str, Any]) -> None:
        print("\n=== SPLIT SUMMARY ===")
        for split_name, info in results["split_summary"].items():
            print(f"\n=== {split_name.upper()} ===")
            print(f"Rows: {info['rows']}")
            print(f"Matches: {info['matches']}")
            print(f"Period: {info['date_min']} até {info['date_max']}")
            print("Target distribution:")
            print(info["target_distribution"])

        for split_name in ["train", "valid", "test"]:
            metrics = results[split_name]
            print(f"\n=== METRICS {split_name.upper()} ===")
            print(f"Rows: {metrics['rows']}")
            print(f"Accuracy: {metrics['accuracy']:.4f}")
            print(f"Log loss: {metrics['log_loss']:.4f}")
            print(metrics["classification_report"])

        model = results["model"]
        feature_names = results["feature_columns"]
        importances = pd.DataFrame(
            {
                "feature": feature_names,
                "importance": model.feature_importances_,
            }
        ).sort_values("importance", ascending=False)

        print("\n=== TOP 20 FEATURE IMPORTANCES ===")
        print(importances.head(20).to_string(index=False))

    @staticmethod
    def _feature_columns() -> list[str]:
        return [
            "minute",
            "home_score_now",
            "away_score_now",
            "goal_diff_now",
            "home_shots_cum",
            "away_shots_cum",
            "home_shots_on_target_cum",
            "away_shots_on_target_cum",
            "home_xg_cum",
            "away_xg_cum",
            "home_shots_last_10",
            "away_shots_last_10",
            "home_shots_on_target_last_10",
            "away_shots_on_target_last_10",
            "home_xg_last_10",
            "away_xg_last_10",
            "home_red_cards",
            "away_red_cards",
            "home_fouls_cum",
            "away_fouls_cum",
            "home_passes_cum",
            "away_passes_cum",
            "diff_shots_cum",
            "diff_shots_on_target_cum",
            "diff_xg_cum",
            "diff_shots_last_10",
            "diff_shots_on_target_last_10",
            "diff_xg_last_10",
            "diff_red_cards",
            "diff_fouls_cum",
            "diff_passes_cum",
            "remaining_minutes",
        ]


if __name__ == "__main__":
    trainer = InPlayMatchResultTrainer(
        train_ratio=0.70,
        valid_ratio=0.15,
        random_state=42,
    )
    results = trainer.run()
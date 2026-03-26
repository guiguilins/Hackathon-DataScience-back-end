from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd

ARTIFACT_DIR = Path("artifacts/match_result_model")


def load_artifacts():
    model = joblib.load(ARTIFACT_DIR / "model.joblib")
    imputer = joblib.load(ARTIFACT_DIR / "imputer.joblib")
    label_encoder = joblib.load(ARTIFACT_DIR / "label_encoder.joblib")

    with open(ARTIFACT_DIR / "feature_columns.json", "r", encoding="utf-8") as f:
        feature_columns = json.load(f)

    return model, imputer, label_encoder, feature_columns


def predict_from_dict(feature_dict: dict) -> dict:
    model, imputer, label_encoder, feature_columns = load_artifacts()

    row = {col: feature_dict.get(col) for col in feature_columns}
    X = pd.DataFrame([row])

    X_imp = imputer.transform(X)

    pred_encoded = model.predict(X_imp)[0]
    pred_label = label_encoder.inverse_transform([pred_encoded])[0]

    probs = model.predict_proba(X_imp)[0]
    class_labels = label_encoder.inverse_transform(range(len(probs)))

    probability_map = {
        str(label): float(prob)
        for label, prob in zip(class_labels, probs)
    }

    return {
        "prediction": pred_label,
        "probabilities": probability_map,
    }
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def load_table_file(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path)

    if suffix == ".json":
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return pd.DataFrame(data)

        if isinstance(data, dict):
            return pd.json_normalize(data)

        raise ValueError(f"Formato JSON não suportado em {file_path}")

    raise ValueError(f"Extensão não suportada: {file_path.suffix}")


def sanitize_value(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()

    if isinstance(value, (np.integer,)):
        return int(value)

    if isinstance(value, (np.floating,)):
        if pd.isna(value):
            return None
        return float(value)

    if isinstance(value, (np.bool_,)):
        return bool(value)

    if isinstance(value, dict):
        return {str(k): sanitize_value(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [sanitize_value(v) for v in value]

    if isinstance(value, np.ndarray):
        return [sanitize_value(v) for v in value.tolist()]

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    return value


def dataframe_to_json_records(df: pd.DataFrame) -> list[dict]:
    records = df.to_dict(orient="records")
    sanitized_records = []

    for record in records:
        sanitized = {str(key): sanitize_value(value) for key, value in record.items()}
        sanitized_records.append(sanitized)

    return sanitized_records
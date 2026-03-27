from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import kagglehub
import pandas as pd

from database.db import get_db_pool

DATASET_REF = "hubertsidorowicz/football-players-stats-2025-2026"
TARGET_TABLE = "silver.fbref_player_stats"


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None

    value = str(value).strip().lower()
    if not value:
        return None

    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()

    return value or None


def log(message: str) -> None:
    print(f"[INFO] {message}")


def to_int(value):
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def to_numeric(value):
    if pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized_map = {str(col).strip().lower(): col for col in df.columns}

    for candidate in candidates:
        key = candidate.strip().lower()
        if key in normalized_map:
            return normalized_map[key]

    return None


def resolve_columns(df: pd.DataFrame) -> dict[str, str | None]:
    return {
        "season_name": find_col(df, ["season", "Season"]),
        "competition_name": find_col(df, ["competition", "Competition", "comp", "Comp", "league", "League"]),
        "squad_name": find_col(df, ["squad", "Squad", "team", "Team"]),
        "player_name": find_col(df, ["player", "Player"]),
        "pos": find_col(df, ["pos", "Pos"]),
        "age": find_col(df, ["age", "Age"]),
        "birth_year": find_col(df, ["born", "Born", "birth_year"]),
        "minutes_played": find_col(df, ["min", "Min", "minutes", "Minutes"]),
        "starts": find_col(df, ["starts", "Starts"]),
        "nineties": find_col(df, ["90s", "nineties", "Nineties"]),
        "goals": find_col(df, ["gls", "Gls", "goals", "Goals"]),
        "assists": find_col(df, ["ast", "Ast", "assists", "Assists"]),
        "xg": find_col(df, ["xg", "xG"]),
        "xag": find_col(df, ["xag", "xAG"]),
        "shots_total": find_col(df, ["sh", "Sh", "shots", "Shots"]),
        "shots_on_target": find_col(df, ["sot", "SoT", "shots_on_target"]),
        "tackles": find_col(df, ["tkl", "Tkl", "tackles", "Tackles"]),
        "interceptions": find_col(df, ["int", "Int", "interceptions", "Interceptions"]),
        "blocks": find_col(df, ["blocks", "Blocks", "blk", "Blk"]),
        "clearances": find_col(df, ["clr", "Clr", "clearances", "Clearances"]),
        "aerials_won": find_col(df, ["won", "Won", "aerials_won"]),
        "clean_sheets": find_col(df, ["cs", "CS", "clean_sheets"]),
        "save_pct": find_col(df, ["save%", "Save%", "save_pct"]),
    }


def build_records(df: pd.DataFrame, source_file: str) -> list[dict]:
    colmap = resolve_columns(df)
    records: list[dict] = []

    if colmap["player_name"] is None:
        log(f"Arquivo {source_file} ignorado: coluna de jogador não encontrada.")
        return records

    for _, row in df.iterrows():
        player_name = row[colmap["player_name"]] if colmap["player_name"] else None
        squad_name = row[colmap["squad_name"]] if colmap["squad_name"] else None

        if pd.isna(player_name) or str(player_name).strip() == "":
            continue

        raw_payload = {
            col: (None if pd.isna(val) else val)
            for col, val in row.to_dict().items()
        }

        record = {
            "source_file": source_file,
            "season_name": (
                row[colmap["season_name"]]
                if colmap["season_name"] and not pd.isna(row[colmap["season_name"]])
                else "2025/2026"
            ),
            "competition_name": (
                row[colmap["competition_name"]]
                if colmap["competition_name"] and not pd.isna(row[colmap["competition_name"]])
                else None
            ),
            "squad_name": None if pd.isna(squad_name) else str(squad_name),
            "normalized_squad_name": normalize_text(squad_name),
            "player_name": str(player_name),
            "normalized_player_name": normalize_text(player_name),
            "pos": (
                None
                if colmap["pos"] is None or pd.isna(row[colmap["pos"]])
                else str(row[colmap["pos"]])
            ),
            "age": to_int(row[colmap["age"]]) if colmap["age"] else None,
            "birth_year": to_int(row[colmap["birth_year"]]) if colmap["birth_year"] else None,
            "minutes_played": to_numeric(row[colmap["minutes_played"]]) if colmap["minutes_played"] else None,
            "starts": to_int(row[colmap["starts"]]) if colmap["starts"] else None,
            "nineties": to_numeric(row[colmap["nineties"]]) if colmap["nineties"] else None,
            "goals": to_numeric(row[colmap["goals"]]) if colmap["goals"] else None,
            "assists": to_numeric(row[colmap["assists"]]) if colmap["assists"] else None,
            "xg": to_numeric(row[colmap["xg"]]) if colmap["xg"] else None,
            "xag": to_numeric(row[colmap["xag"]]) if colmap["xag"] else None,
            "shots_total": to_numeric(row[colmap["shots_total"]]) if colmap["shots_total"] else None,
            "shots_on_target": to_numeric(row[colmap["shots_on_target"]]) if colmap["shots_on_target"] else None,
            "tackles": to_numeric(row[colmap["tackles"]]) if colmap["tackles"] else None,
            "interceptions": to_numeric(row[colmap["interceptions"]]) if colmap["interceptions"] else None,
            "blocks": to_numeric(row[colmap["blocks"]]) if colmap["blocks"] else None,
            "clearances": to_numeric(row[colmap["clearances"]]) if colmap["clearances"] else None,
            "aerials_won": to_numeric(row[colmap["aerials_won"]]) if colmap["aerials_won"] else None,
            "clean_sheets": to_numeric(row[colmap["clean_sheets"]]) if colmap["clean_sheets"] else None,
            "save_pct": to_numeric(row[colmap["save_pct"]]) if colmap["save_pct"] else None,
            "raw_payload": json.dumps(raw_payload, ensure_ascii=False),
        }
        records.append(record)

    return records


def truncate_table(cur) -> None:
    cur.execute(f"TRUNCATE TABLE {TARGET_TABLE} RESTART IDENTITY;")


def insert_records(cur, records: list[dict]) -> None:
    if not records:
        return

    columns = [
        "source_file",
        "season_name",
        "competition_name",
        "squad_name",
        "normalized_squad_name",
        "player_name",
        "normalized_player_name",
        "pos",
        "age",
        "birth_year",
        "minutes_played",
        "starts",
        "nineties",
        "goals",
        "assists",
        "xg",
        "xag",
        "shots_total",
        "shots_on_target",
        "tackles",
        "interceptions",
        "blocks",
        "clearances",
        "aerials_won",
        "clean_sheets",
        "save_pct",
        "raw_payload",
    ]

    sql = f"""
        INSERT INTO {TARGET_TABLE} ({", ".join(columns)})
        VALUES ({", ".join(["%s"] * len(columns))})
    """

    params = [tuple(record[col] for col in columns) for record in records]
    cur.executemany(sql, params)


def list_csv_files(dataset_path: Path) -> list[Path]:
    csv_files = sorted(dataset_path.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Nenhum CSV encontrado em {dataset_path}")
    return csv_files


def main() -> None:
    log(f"Baixando dataset Kaggle: {DATASET_REF}")
    dataset_path = Path(kagglehub.dataset_download(DATASET_REF))
    log(f"Dataset disponível em: {dataset_path}")

    csv_files = list_csv_files(dataset_path)
    log(f"CSV(s) encontrado(s): {len(csv_files)}")

    db_pool = get_db_pool()
    total_inserted = 0

    with db_pool.get_cursor() as (conn, cur):
        truncate_table(cur)
        log(f"Tabela truncada: {TARGET_TABLE}")

        for csv_file in csv_files:
            log(f"Lendo arquivo: {csv_file.name}")
            df = pd.read_csv(csv_file)
            log(f"Shape: {df.shape}")

            records = build_records(df, source_file=csv_file.name)
            insert_records(cur, records)
            total_inserted += len(records)

            log(f"Registros inseridos de {csv_file.name}: {len(records)}")

        conn.commit()

    log(f"Ingestão concluída. Total inserido em {TARGET_TABLE}: {total_inserted}")


if __name__ == "__main__":
    main()
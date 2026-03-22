from __future__ import annotations

import json

from database.db import get_db_pool
from ingestion.kaggle_client import download_dataset, list_dataset_files, filter_files_by_extension
from ingestion.utils import load_table_file, dataframe_to_json_records

DATASET_REF = "hubertsidorowicz/football-players-stats-2025-2026"


def truncate_target_table() -> None:
    db_pool = get_db_pool()
    with db_pool.get_cursor() as (_, cur):
        cur.execute("TRUNCATE TABLE raw.fbref_players RESTART IDENTITY;")


def insert_records(source_dataset: str, source_file: str, records: list[dict]) -> None:
    if not records:
        print(f"Nenhum registro para inserir em raw.fbref_players a partir de {source_file}")
        return

    db_pool = get_db_pool()

    sql = """
        INSERT INTO raw.fbref_players (source_dataset, source_file, payload)
        VALUES (%s, %s, %s::jsonb)
    """

    with db_pool.get_cursor() as (_, cur):
        for record in records:
            payload = json.dumps(record, ensure_ascii=False, allow_nan=False)
            cur.execute(sql, (source_dataset, source_file, payload))

    print(f"Inseridos {len(records)} registros em raw.fbref_players ({source_file})")


def main() -> None:
    print("Baixando dataset FBref...")
    dataset_path = download_dataset(DATASET_REF)
    print(f"Dataset salvo em: {dataset_path}")

    all_files = list_dataset_files(dataset_path)
    data_files = filter_files_by_extension(all_files, (".csv", ".json"))

    if not data_files:
        raise FileNotFoundError("Nenhum arquivo CSV/JSON encontrado no dataset FBref.")

    print("Arquivos encontrados:")
    for file_path in data_files:
        print(f" - {file_path.name}")

    truncate_target_table()

    for file_path in data_files:
        print(f"Processando {file_path.name} -> raw.fbref_players")
        df = load_table_file(file_path)
        records = dataframe_to_json_records(df)
        insert_records(
            source_dataset=DATASET_REF,
            source_file=file_path.name,
            records=records,
        )

    print("\nIngestão FBref finalizada com sucesso.")


if __name__ == "__main__":
    main()
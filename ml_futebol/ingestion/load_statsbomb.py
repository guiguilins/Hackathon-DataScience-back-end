from __future__ import annotations

import json
from pathlib import Path

from database.db import get_db_pool
from ingestion.kaggle_client import download_dataset, list_dataset_files, filter_files_by_extension
from ingestion.utils import load_table_file, dataframe_to_json_records

DATASET_REF = "saurabhshahane/statsbomb-football-data"


def resolve_target_table(file_path: Path) -> str | None:
    parts = [part.lower() for part in file_path.parts]

   # if "events" in parts:
   #     return "raw.statsbomb_events"

    if "lineups" in parts:
        return "raw.statsbomb_lineups"

    if "matches" in parts:
        return "raw.statsbomb_matches"

    return None


def truncate_target_table(table_name: str) -> None:
    db_pool = get_db_pool()
    with db_pool.get_cursor() as (_, cur):
        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")


def insert_records(table_name: str, source_dataset: str, source_file: str, records: list[dict]) -> None:
    if not records:
        print(f"Nenhum registro para inserir em {table_name} a partir de {source_file}")
        return

    db_pool = get_db_pool()

    sql = f"""
        INSERT INTO {table_name} (source_dataset, source_file, payload)
        VALUES (%s, %s, %s::jsonb)
    """

    inserted = 0

    with db_pool.get_cursor() as (_, cur):
        for record in records:
            payload = json.dumps(record, ensure_ascii=False, allow_nan=False)
            cur.execute(sql, (source_dataset, source_file, payload))
            inserted += 1

    print(f"Inseridos {inserted} registros em {table_name} ({source_file})")


def main() -> None:
    print("Baixando dataset StatsBomb...")
    dataset_path = download_dataset(DATASET_REF)
    print(f"Dataset salvo em: {dataset_path}")

    all_files = list_dataset_files(dataset_path)
    data_files = filter_files_by_extension(all_files, (".csv", ".json"))

    if not data_files:
        raise FileNotFoundError("Nenhum arquivo CSV/JSON encontrado no dataset StatsBomb.")

    print("Arquivos encontrados:")
    for file_path in data_files:
        print(f" - {file_path}")

    grouped_files: dict[str, list[Path]] = {
        "raw.statsbomb_matches": [],
        "raw.statsbomb_events": [],
        "raw.statsbomb_lineups": [],
    }

    ignored_files: list[Path] = []

    for file_path in data_files:
        table_name = resolve_target_table(file_path)
        if table_name:
            grouped_files[table_name].append(file_path)
        else:
            ignored_files.append(file_path)

    print("\nResumo de arquivos mapeados:")
    total_mapped = 0
    for table_name, files in grouped_files.items():
        print(f"{table_name}: {len(files)} arquivo(s)")
        total_mapped += len(files)

    if ignored_files:
        print("\nArquivos ignorados:")
        for f in ignored_files[:20]:
            print(f" - {f}")
        if len(ignored_files) > 20:
            print(f"... e mais {len(ignored_files) - 20} arquivo(s)")

    if total_mapped == 0:
        raise ValueError("Nenhum arquivo do dataset foi mapeado para as tabelas raw.")

    for table_name, files in grouped_files.items():
        if files:
            print(f"\nLimpando tabela {table_name}...")
            truncate_target_table(table_name)

            for file_path in files:
                print(f"Processando {file_path} -> {table_name}")
                df = load_table_file(file_path)
                records = dataframe_to_json_records(df)

                print(f"{file_path.name}: {len(df)} linhas, {len(records)} registros")

                insert_records(
                    table_name=table_name,
                    source_dataset=DATASET_REF,
                    source_file=str(file_path.relative_to(dataset_path)),
                    records=records,
                )

    print("\nIngestão StatsBomb finalizada com sucesso.")


if __name__ == "__main__":
    main()
from __future__ import annotations

import csv
from pathlib import Path

from database.db import get_db_pool


MATCH_IDS = [
    3754319, 266670, 3754058, 3754346, 3754251,
    3825714, 3754069, 3754036, 3754101, 3825715,
    3754122, 3754140, 3825717, 3825721, 3825718,
    3825719, 3825716, 3825720, 3754114, 3825722,
]

OUTPUT_DIR = Path(r"C:\temp")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def export_query_to_csv(query: str, params: tuple, output_file: Path) -> None:
    db_pool = get_db_pool()

    with db_pool.get_cursor(dict_cursor=True) as (_, cur):
        cur.execute(query, params)
        rows = cur.fetchall()

        if not rows:
            print(f"[WARN] Nenhum dado encontrado para: {output_file.name}")
            return

        columns = rows[0].keys()

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] Arquivo gerado: {output_file}")
    print(f"[INFO] Total de linhas: {len(rows)}\n")


def main() -> None:
    # ⚠️ psycopg exige lista para ANY
    params = (MATCH_IDS,)

    query_matches = """
        SELECT *
        FROM silver.matches
        WHERE match_id = ANY(%s)
        ORDER BY match_id;
    """

    query_in_game_features = """
        SELECT *
        FROM feature_store.match_in_game_features
        WHERE match_id = ANY(%s)
        ORDER BY match_id;
    """

    print("🚀 Exportando silver.matches...")
    export_query_to_csv(
        query=query_matches,
        params=params,
        output_file=OUTPUT_DIR / "matches_demo.csv",
    )

    print("🚀 Exportando feature_store.match_in_game_features...")
    export_query_to_csv(
        query=query_in_game_features,
        params=params,
        output_file=OUTPUT_DIR / "match_in_game_features_demo.csv",
    )

    print("✅ Export finalizado!")


if __name__ == "__main__":
    main()
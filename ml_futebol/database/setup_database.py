from pathlib import Path

from database.db import get_db_pool

SQL_DIR = Path("sql")


def read_sql_files() -> list[Path]:
    if not SQL_DIR.exists():
        raise FileNotFoundError(f"Pasta SQL não encontrada: {SQL_DIR.resolve()}")

    sql_files = sorted(SQL_DIR.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError("Nenhum arquivo .sql encontrado na pasta sql/")

    return sql_files


def execute_sql_file(file_path: Path) -> None:
    sql_content = file_path.read_text(encoding="utf-8")

    print(f"Executando: {file_path.name}")

    db_pool = get_db_pool()
    with db_pool.get_cursor() as (_, cur):
        cur.execute(sql_content)


def main():
    sql_files = read_sql_files()

    print("Iniciando setup do banco...")
    for file_path in sql_files:
        execute_sql_file(file_path)

    print("Setup finalizado com sucesso.")


if __name__ == "__main__":
    db_pool = get_db_pool()
    try:
        main()
    finally:
        db_pool.close_all()
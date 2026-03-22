import psycopg

conn = psycopg.connect(
    dbname="ml_futebol",
    user="postgres",
    password="postgres",
    host="127.0.0.1",
    port=55432,
)

with conn.cursor() as cur:
    cur.execute("SELECT current_database(), current_user;")
    print(cur.fetchone())

conn.close()
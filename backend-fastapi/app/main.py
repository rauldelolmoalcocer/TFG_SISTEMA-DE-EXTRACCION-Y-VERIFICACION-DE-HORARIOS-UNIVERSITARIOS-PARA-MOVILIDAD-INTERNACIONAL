from fastapi import FastAPI
import psycopg2

app = FastAPI()

@app.get("/demo")
def demo():
    conn = psycopg2.connect(
        host="db",
        dbname="tfg",
        user="postgres",
        password="postgres",
        port=5432,
    )
    cur = conn.cursor()
    cur.execute("SELECT mensaje FROM demo LIMIT 1;")
    row = cur.fetchone()
    cur.close()
    conn.close()

    return {"mensaje": row[0] if row else "Sin datos"}
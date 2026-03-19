from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2

app = FastAPI()

class LoginRequest(BaseModel):
    username: str
    password: str

def get_connection():
    return psycopg2.connect(
        host="db",
        dbname="tfg",
        user="postgres",
        password="postgres",
        port=5432,
    )

@app.get("/demo")
def demo():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT mensaje FROM demo LIMIT 1;")
    row = cur.fetchone()
    cur.close()
    conn.close()

    return {"mensaje": row[0] if row else "Sin datos"}

@app.post("/login")
def login(data: LoginRequest):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT id, username FROM usuarios WHERE username = %s AND password = %s",
        (data.username, data.password)
    )
    user = cur.fetchone()

    cur.close()
    conn.close()

    if user:
        return {
            "success": True,
            "user": {
                "id": user[0],
                "username": user[1]
            }
        }
    else:
        return {
            "success": False,
            "message": "Usuario o contraseña incorrectos"
        }
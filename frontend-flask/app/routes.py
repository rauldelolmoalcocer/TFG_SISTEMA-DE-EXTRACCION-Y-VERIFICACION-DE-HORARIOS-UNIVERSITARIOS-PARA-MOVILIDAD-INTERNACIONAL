from flask import Blueprint, render_template, request, redirect, url_for, session
import requests

bp = Blueprint("main", __name__)

BACKEND_URL = "http://backend:8000"

@bp.route("/")
def index():
    if "user" not in session:
        return redirect(url_for("main.login"))

    return render_template("index.html", user=session["user"])

@bp.route("/login", methods=["GET", "POST"])
def login():
    error = None

    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        try:
            res = requests.post(
                f"{BACKEND_URL}/login",
                json={
                    "username": username,
                    "password": password
                },
                timeout=5
            )

            data = res.json()

            if data.get("success"):
                session["user"] = data["user"]["username"]
                return redirect(url_for("main.index"))
            else:
                error = data.get("message", "Credenciales incorrectas")

        except Exception:
            error = "No se pudo conectar con el backend"

    return render_template("login.html", error=error)

@bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("main.login"))
from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
import requests

bp = Blueprint("main", __name__)

BACKEND_URL = "http://backend:8000"


# =========================================================
# HOME
# =========================================================

@bp.route("/")
def index():
    if "user" not in session:
        return redirect(url_for("main.login"))

    return render_template("index.html", user=session["user"])


# =========================================================
# LOGIN
# =========================================================

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


# =========================================================
# LOGOUT
# =========================================================

@bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("main.login"))


# =========================================================
# PANEL DE DESCARGAS
# =========================================================

@bp.route("/descargas")
def descargas():
    if "user" not in session:
        return redirect(url_for("main.login"))

    return render_template("download_panel.html", user=session["user"])


# =========================================================
# PROXY HACIA FASTAPI
# =========================================================

@bp.route("/start-download", methods=["POST"])
def start_download():
    if "user" not in session:
        return jsonify({
            "success": False,
            "message": "Sesión no válida"
        }), 401

    data = request.get_json(silent=True) or {}

    try:
        response = requests.post(
            f"{BACKEND_URL}/download/start",
            json=data,
            timeout=15
        )
        return jsonify(response.json()), response.status_code

    except Exception:
        return jsonify({
            "success": False,
            "message": "No se pudo conectar con el backend"
        }), 500


@bp.route("/status", methods=["GET"])
def status():
    if "user" not in session:
        return jsonify({
            "running": False,
            "logs": ["Sesión no válida"],
            "files": []
        }), 401

    try:
        response = requests.get(
            f"{BACKEND_URL}/download/status",
            timeout=15
        )
        return jsonify(response.json()), response.status_code

    except Exception:
        return jsonify({
            "running": False,
            "logs": ["No se pudo conectar con el backend"],
            "files": []
        }), 500
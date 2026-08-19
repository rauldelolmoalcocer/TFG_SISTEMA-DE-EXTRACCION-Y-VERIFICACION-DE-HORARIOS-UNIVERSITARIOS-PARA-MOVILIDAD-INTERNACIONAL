from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify, Response
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

@bp.route("/horarios")
def horarios():
    if "user" not in session:
        return redirect(url_for("main.login"))
    return render_template("horarios.html", user=session["user"])


@bp.route("/descargas")
def descargas():
    if "user" not in session:
        return redirect(url_for("main.login"))

    return render_template("download_panel.html", user=session["user"])


@bp.route("/revision")
def revision():
    if "user" not in session:
        return redirect(url_for("main.login"))

    return render_template("review.html", user=session["user"])


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


@bp.route("/pause-download", methods=["POST"])
def pause_download():
    if "user" not in session:
        return jsonify({"success": False, "message": "Sesión no válida"}), 401

    try:
        response = requests.post(f"{BACKEND_URL}/download/pause", timeout=10)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/resume-download", methods=["POST"])
def resume_download():
    if "user" not in session:
        return jsonify({"success": False, "message": "Sesión no válida"}), 401

    try:
        response = requests.post(f"{BACKEND_URL}/download/resume", timeout=10)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


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


# =========================================================
# EXTRACCIÓN PDF → JSON
# =========================================================

@bp.route("/dump-db", methods=["POST"])
def dump_db():
    if "user" not in session:
        return jsonify({"success": False, "message": "Sesión no válida"}), 401

    try:
        response = requests.post(
            f"{BACKEND_URL}/extract/start",
            timeout=15,
        )
        return jsonify(response.json()), response.status_code

    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/extract-status", methods=["GET"])
def extract_status():
    if "user" not in session:
        return jsonify({"success": False, "message": "Sesión no válida"}), 401

    try:
        response = requests.get(
            f"{BACKEND_URL}/extract/status",
            timeout=15,
        )
        return jsonify(response.json()), response.status_code

    except Exception:
        return jsonify({"running": False, "logs": ["No se pudo conectar con el backend"]}), 500


# =========================================================
# ABRIR PDF DESDE EL FRONTEND (PROXY AL BACKEND)
# =========================================================

@bp.route("/pdf/<path:filename>", methods=["GET"])
def open_pdf(filename):
    if "user" not in session:
        return jsonify({
            "success": False,
            "message": "Sesión no válida"
        }), 401

    try:
        response = requests.get(
            f"{BACKEND_URL}/download/file/{filename}",
            timeout=30,
            stream=True
        )

        if response.status_code != 200:
            return jsonify({
                "success": False,
                "message": "No se pudo abrir el PDF"
            }), response.status_code

        return Response(
            response.iter_content(chunk_size=8192),
            content_type=response.headers.get("Content-Type", "application/pdf"),
            headers={
                "Content-Disposition": response.headers.get(
                    "Content-Disposition",
                    f'inline; filename="{filename}"'
                )
            }
        )

    except Exception:
        return jsonify({
            "success": False,
            "message": "No se pudo abrir el PDF"
        }), 500


# =========================================================
# REVISIÓN MANUAL (PROXY HACIA FASTAPI)
# =========================================================

def _require_session():
    if "user" not in session:
        return jsonify({"success": False, "message": "Sesión no válida"}), 401
    return None


@bp.route("/review-summary", methods=["GET"])
def review_summary():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.get(f"{BACKEND_URL}/review/summary", timeout=15)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-filters", methods=["GET"])
def review_filters():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.get(f"{BACKEND_URL}/review/filters", timeout=15)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-tree", methods=["GET"])
def review_tree():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.get(
            f"{BACKEND_URL}/review/tree",
            params=request.args.to_dict(),
            timeout=15,
        )
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-records", methods=["GET"])
def review_records():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.get(
            f"{BACKEND_URL}/review/records",
            params=request.args.to_dict(),
            timeout=15,
        )
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-records/<item_id>", methods=["GET", "PUT"])
def review_record_detail(item_id):
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        if request.method == "GET":
            response = requests.get(
                f"{BACKEND_URL}/review/records/{item_id}", timeout=15
            )
        else:
            response = requests.put(
                f"{BACKEND_URL}/review/records/{item_id}",
                json=request.get_json(silent=True) or {},
                timeout=15,
            )
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-records/<item_id>/status", methods=["POST"])
def review_record_status(item_id):
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.post(
            f"{BACKEND_URL}/review/records/{item_id}/status",
            json=request.get_json(silent=True) or {},
            timeout=15,
        )
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-records/<item_id>/restore", methods=["POST"])
def review_record_restore(item_id):
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.post(
            f"{BACKEND_URL}/review/records/{item_id}/restore", timeout=15
        )
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500
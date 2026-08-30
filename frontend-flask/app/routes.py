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


@bp.route("/ia")
def ai_panel():
    if "user" not in session:
        return redirect(url_for("main.login"))

    return render_template("ai_panel.html", user=session["user"])


@bp.route("/volcado")
def volcado():
    if "user" not in session:
        return redirect(url_for("main.login"))

    return render_template("db_dump.html", user=session["user"])


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
            json=request.get_json(silent=True) or {},
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


@bp.route("/review-records/<item_id>/duplicate", methods=["POST"])
def review_record_duplicate(item_id):
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.post(
            f"{BACKEND_URL}/review/records/{item_id}/duplicate", timeout=15
        )
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-llm-review", methods=["POST"])
def review_llm_review_start():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.post(
            f"{BACKEND_URL}/review/llm-review",
            json=request.get_json(silent=True) or {},
            timeout=15,
        )
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-llm-review-status", methods=["GET"])
def review_llm_review_status():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.get(f"{BACKEND_URL}/review/llm-review/status", timeout=15)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-llm-review-pause", methods=["POST"])
def review_llm_review_pause():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.post(f"{BACKEND_URL}/review/llm-review/pause", timeout=15)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-llm-review-resume", methods=["POST"])
def review_llm_review_resume():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.post(f"{BACKEND_URL}/review/llm-review/resume", timeout=15)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/review-llm-review-cancel", methods=["POST"])
def review_llm_review_cancel():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.post(f"{BACKEND_URL}/review/llm-review/cancel", timeout=15)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


# =========================================================
# ADMINISTRACIÓN DE IA (Ollama)
# =========================================================

@bp.route("/ai-settings", methods=["GET"])
def ai_settings_get():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.get(f"{BACKEND_URL}/ai/settings", timeout=10)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/ai-settings", methods=["PUT"])
def ai_settings_put():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.put(
            f"{BACKEND_URL}/ai/settings",
            json=request.get_json(silent=True) or {},
            timeout=10,
        )
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/ai-status", methods=["GET"])
def ai_status():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.get(f"{BACKEND_URL}/ai/status", timeout=10)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


# =========================================================
# VOLCADO A BASE DE DATOS (PROXY HACIA FASTAPI)
# =========================================================

@bp.route("/db-dump-start", methods=["POST"])
def db_dump_start():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.post(f"{BACKEND_URL}/db-dump/start", timeout=15)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


@bp.route("/db-dump-status", methods=["GET"])
def db_dump_status():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.get(f"{BACKEND_URL}/db-dump/status", timeout=15)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({
            "running": False,
            "logs": ["No se pudo conectar con el backend"],
            "errors": [],
        }), 500


@bp.route("/db-dump-cancel", methods=["POST"])
def db_dump_cancel():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.post(f"{BACKEND_URL}/db-dump/cancel", timeout=15)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"success": False, "message": "No se pudo conectar con el backend"}), 500


# =========================================================
# GESTOR DE HORARIOS: DATOS REALES (PROXY HACIA FASTAPI)
# =========================================================

@bp.route("/schedule-degrees", methods=["GET"])
def schedule_degrees():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.get(f"{BACKEND_URL}/schedule/degrees", timeout=15)
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"detail": "No se pudo conectar con el backend"}), 502


@bp.route("/schedule-subjects", methods=["GET"])
def schedule_subjects():
    unauthorized = _require_session()
    if unauthorized:
        return unauthorized

    try:
        response = requests.get(
            f"{BACKEND_URL}/schedule/subjects",
            params=request.args.to_dict(),
            timeout=15,
        )
        return jsonify(response.json()), response.status_code
    except Exception:
        return jsonify({"detail": "No se pudo conectar con el backend"}), 502
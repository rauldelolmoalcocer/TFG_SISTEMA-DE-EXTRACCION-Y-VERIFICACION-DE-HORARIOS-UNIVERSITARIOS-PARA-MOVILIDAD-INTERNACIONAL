from flask import Blueprint, render_template
import requests

bp = Blueprint("main", __name__)

@bp.get("/")
def home():
    try:
        r = requests.get("http://backend:8000/demo", timeout=2)
        data = r.json()
    except Exception as e:
        data = {"error": str(e)}

    return render_template("index.html", data=data)

# app/ai_settings.py
#
# Configuración de la conexión con la IA local (Ollama), editable desde
# el panel de administración de IA sin tener que tocar docker-compose.yml
# ni reiniciar el contenedor. Se persiste en un JSON dentro de la carpeta
# de extracción (ya montada como volumen), con la variable de entorno
# OLLAMA_HOST y el modelo por defecto del paquete como valores iniciales.

import json
import os

_SETTINGS_FILENAME = "ai_settings.json"

DEFAULT_HOST = "http://localhost:11434"
DEFAULT_MODEL = "qwen2.5:3b"  # mismo valor por defecto que pdf_table_extractor.llm_review.DEFAULT_MODEL


def _settings_path(output_dir: str) -> str:
    return os.path.join(output_dir, _SETTINGS_FILENAME)


def load_ai_settings(output_dir: str) -> dict:
    path = _settings_path(output_dir)

    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {}

    host = data.get("ollama_host") or os.environ.get("OLLAMA_HOST") or DEFAULT_HOST
    model = data.get("model") or DEFAULT_MODEL
    return {"ollama_host": host, "model": model}


def save_ai_settings(output_dir: str, ollama_host: str, model: str = None) -> dict:
    ollama_host = (ollama_host or "").strip()
    if not ollama_host:
        raise ValueError("La dirección de Ollama no puede estar vacía.")
    if not (ollama_host.startswith("http://") or ollama_host.startswith("https://")):
        raise ValueError("La dirección debe empezar por http:// o https://")

    model = (model or "").strip() or DEFAULT_MODEL

    settings = {"ollama_host": ollama_host, "model": model}

    os.makedirs(output_dir, exist_ok=True)
    path = _settings_path(output_dir)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)

    return settings

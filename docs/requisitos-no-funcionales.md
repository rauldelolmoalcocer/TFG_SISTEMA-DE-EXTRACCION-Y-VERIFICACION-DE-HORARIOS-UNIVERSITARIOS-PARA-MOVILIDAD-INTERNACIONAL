# Requisitos no funcionales — Sistema de Extracción y Verificación de Horarios Universitarios

Catálogo derivado por **ingeniería inversa del código** del repositorio
(`frontend-flask/`, `backend-fastapi/`, `db/`, `docker-compose.yml`). Cada
requisito indica su **evidencia** en el código y su **naturaleza**:

- **Cumplido**: propiedad observable y respaldada por el código.
- **Parcial**: implementado a medias o con una carencia relevante conocida.
- **Restricción**: decisión tecnológica impuesta por la implementación (no es
  un objetivo de calidad, pero condiciona el sistema).

Categorías según ISO/IEC 25010 (adaptadas).

---

## RNF-USA · Usabilidad e interfaz

| Código | Requisito | Evidencia | Naturaleza |
|---|---|---|---|
| RNF-USA-01 | Toda la interfaz y los mensajes están en español. | `lang="es"` en todas las plantillas; textos y errores en español; `toLocaleTimeString("es-ES")` en JS | Cumplido |
| RNF-USA-02 | La interfaz es responsive (móvil / escritorio). | `@media` en `horarios.css`, `download_panel.css`, `users_admin.css`, `review.css`; `@media (prefers-reduced-motion)` | Cumplido |
| RNF-USA-03 | Los procesos largos muestran progreso y registro en vivo. | Barra de progreso + `logBox` con sondeo cada 1–2 s en descarga, extracción, volcado y revisión IA | Cumplido |
| RNF-USA-04 | Navegación uniforme entre paneles y con identidad institucional (UAH). | Topbar común con enlaces cruzados y logo en todas las páginas | Cumplido |

## RNF-REN · Rendimiento y eficiencia

| Código | Requisito | Evidencia | Naturaleza |
|---|---|---|---|
| RNF-REN-01 | La descarga de PDF es concurrente y cortés con el servidor origen. | `ThreadPoolExecutor(max_workers=4)`, `delay_between_requests=0.8` en `crawler.py` | Cumplido |
| RNF-REN-02 | No se descargan ni reprocesan documentos duplicados. | Dedupe por URL y por hash SHA-256 (incl. ficheros ya presentes) en `crawler.py` | Cumplido |
| RNF-REN-03 | El volcado a BD evita consultas por fila y usa una sola transacción. | Cachés en memoria en `dbdump/loader.py`; `autocommit=False` + `commit()` final | Cumplido |
| RNF-REN-04 | La revisión con IA procesa en lotes pequeños con guardado incremental. | `DEFAULT_BATCH_SIZE=6` en `llm_review.py`; `save_reviewed` tras cada registro | Cumplido |
| RNF-REN-05 | La consulta de horarios se apoya en índices y condensa los datos antes de enviarlos. | Índices en `db/createdatabase.sql` (`idx_sessions_*`, `idx_subjects_*`); condensación por (asignatura+grupo) en `schedule/db.py` | Cumplido |
| RNF-REN-06 | El listado de revisión está paginado. | `store.paginate` (`page_size` ≤ 200) | Cumplido |
| RNF-REN-07 | La detección de solapes se ejecuta en el cliente, sin carga al servidor. | `horarios.js::getConflictPairs` | Cumplido |

## RNF-FIA · Fiabilidad y tolerancia a fallos

| Código | Requisito | Evidencia | Naturaleza |
|---|---|---|---|
| RNF-FIA-01 | Las escrituras de ficheros de estado son atómicas. | `_atomic_write_json` (`tmp` + `os.replace`) en `review/store.py`, `ai_settings.py` | Cumplido |
| RNF-FIA-02 | El volcado a BD es transaccional: un fallo deja la BD intacta. | `run_dump` → `rollback()` en excepción / cancelación | Cumplido |
| RNF-FIA-03 | Un fallo aislado (un PDF, un registro, un lote de IA) no aborta el proceso completo. | try/except por elemento en `pdf_extractor.py`, `store.llm_review_items`, `llm_review.review_entries` | Cumplido |
| RNF-FIA-04 | Las llamadas a servicios externos tienen timeout. | Ollama: 180 s por registro / 5 s el *check*; proxy Flask: 5–30 s; crawler: 15 s | Cumplido |
| RNF-FIA-05 | La copia revisable nunca se pierde: se respalda antes de reconstruir y se regenera bajo demanda tras un reinicio. | `refresh_reviewed_from_extraction` (renombra a `.bak.json`), `ensure_reviewed` | Cumplido |
| RNF-FIA-06 | Solo se ejecuta un proceso largo de cada tipo a la vez. | Guardas `running` en descarga, extracción, revisión IA y volcado | Cumplido |
| RNF-FIA-07 | Los procesos largos se pueden pausar / reanudar / cancelar sin corromper el estado. | `pause_event`, `cancel_event`; cancelación entre elementos, nunca a mitad de una llamada | Cumplido |

## RNF-SEG · Seguridad

| Código | Requisito | Evidencia | Naturaleza |
|---|---|---|---|
| RNF-SEG-01 | Las contraseñas se guardan con hash (pbkdf2), nunca en claro. | `auth/users.py::hash_password` (werkzeug) | Cumplido |
| RNF-SEG-02 | Los endpoints internos (login, registro, usuarios, cuenta) exigen clave compartida frontend↔backend. | `main.py::require_internal_key` (`X-Internal-Key`) | Cumplido |
| RNF-SEG-03 | La entrada del usuario se valida (URL, nombre de usuario, longitud de contraseña, host de IA). | `start_download`, `auth/users.py`, `ai_settings.save_ai_settings` | Cumplido |
| RNF-SEG-04 | El crawler se limita a un dominio, verifica TLS y se identifica. | `same_domain_only`, `verify_ssl=True`, `User-Agent` propio en `crawler.py` | Cumplido |
| RNF-SEG-05 | La gestión de usuarios está restringida a administradores y garantiza que siempre haya un administrador activo. | `_require_admin`, guardas "último administrador activo" | Cumplido |
| RNF-SEG-06 | El resto de la API del backend debería exigir autenticación. | Solo `/login`, `/register`, `/users*`, `/account/*` están protegidos; el resto de endpoints están abiertos en `:8000` | Parcial |
| RNF-SEG-07 | Los secretos no deberían estar en el repositorio ni tener valores por defecto. | `SECRET_KEY = "clave_secreta"` fija en `app/__init__.py`; credenciales de PostgreSQL e `INTERNAL_API_KEY` con valor por defecto en `docker-compose.yml`; sin HTTPS ni bloqueo por intentos fallidos | Parcial |

## RNF-MAN · Mantenibilidad y arquitectura

| Código | Requisito | Evidencia | Naturaleza |
|---|---|---|---|
| RNF-MAN-01 | Arquitectura en dos servicios (frontend-proxy + API) y módulos por responsabilidad. | `frontend-flask` / `backend-fastapi`; módulos `crawler`, `extractor`, `review`, `dbdump`, `schedule`, `auth`, `ai_settings` | Cumplido |
| RNF-MAN-02 | El extractor de PDF es un paquete instalable e independiente, con sus propias pruebas. | `pdf_table_extractor/pyproject.toml`, `.../tests` | Cumplido |
| RNF-MAN-03 | El pipeline está desacoplado en etapas que comparten disco y pueden ejecutarse por separado. | `pipeline.py` (`run_diagnostics` / `run_normalization`), etapa LLM opcional | Cumplido |
| RNF-MAN-04 | Los cambios de esquema se versionan en migraciones SQL idempotentes. | `db/003_usuarios_auth.sql`, `db/004_usuarios_is_active.sql` | Cumplido |
| RNF-MAN-05 | La configuración de IA es editable en caliente sin reiniciar el contenedor. | `ai_settings.json` + panel `/ia` | Cumplido |
| RNF-MAN-06 | El sistema debería tener pruebas automatizadas del backend y del frontend. | Solo el paquete extractor tiene tests; `frontend-flask/tests/test_routes.py` está obsoleto; `pythonPruebas/` es código heredado | Parcial |

## RNF-POR · Portabilidad y despliegue

| Código | Requisito | Evidencia | Naturaleza |
|---|---|---|---|
| RNF-POR-01 | Todo el sistema se despliega con Docker Compose (frontend, backend, PostgreSQL, pgAdmin). | `docker-compose.yml`, `Dockerfile` × 2 (`python:3.11-slim`) | Restricción |
| RNF-POR-02 | Los servicios escuchan en `0.0.0.0` (accesibles desde la LAN). | `run.py` (`host="0.0.0.0"`), `uvicorn --host 0.0.0.0`, `ports:` en compose | Cumplido |
| RNF-POR-03 | Los datos persisten a reinicios de contenedor mediante volúmenes / bind-mounts. | `pgdata`, `pgadmin_data`, `./downloads`, `./extracted` | Cumplido |
| RNF-POR-04 | Ollama está desacoplado y es sustituible por dirección. | `OLLAMA_HOST` / `host.docker.internal` / `extra_hosts`; panel de IA | Cumplido |
| RNF-POR-05 | El entorno funciona sobre Windows con Docker en WSL. | Verificado en esta implantación (`wsl -e docker`) | Cumplido |
| RNF-POR-06 | La configuración sensible se pasa por variables de entorno. | `ENABLE_LLM_REVIEW`, `OLLAMA_HOST`, `INTERNAL_API_KEY` | Cumplido |

## RNF-INT · Interoperabilidad

| Código | Requisito | Evidencia | Naturaleza |
|---|---|---|---|
| RNF-INT-01 | El backend expone una API HTTP/JSON estilo REST con especificación OpenAPI automática. | FastAPI (`/docs`, `/openapi.json` activos) | Cumplido |
| RNF-INT-02 | La integración con la IA se hace por la API HTTP de Ollama. | `/api/chat` (razonamiento), `/api/tags` (disponibilidad / modelos) | Cumplido |
| RNF-INT-03 | Los ficheros de intercambio en disco llevan versión de esquema. | `schema_version` en `reviewed_schedules.json`, `extraction_run.json` | Cumplido |
| RNF-INT-04 | La API admite consumo desde otros orígenes. | `CORSMiddleware(allow_origins=["*"])` | Cumplido (con la salvedad de RNF-SEG-07) |

## RNF-TEC · Restricciones tecnológicas (impuestas por el código)

- **Backend:** Python 3.11, FastAPI + Uvicorn, `psycopg2` (sin ORM).
- **Frontend:** Python + Flask (plantillas Jinja + JS/CSS propio; Bootstrap solo en login y registro).
- **Base de datos:** PostgreSQL 15.
- **Procesamiento de PDF:** pdfplumber / PyMuPDF. **Crawling:** BeautifulSoup + requests.
- **Seguridad:** werkzeug (hashing de contraseñas).
- **IA:** local y **opcional**, modelo open-weight vía `ollama-python`; modelo por
  defecto `qwen2.5:3b` (decisión de privacidad / coste: sin servicios en la nube).
- **Capacidad configurada:** crawler `max_pages=50`, `max_depth=2`, 4 descargas
  simultáneas (en la práctica solo rastrea la página indicada); revisión
  `page_size ≤ 200`; lotes de IA de 6; timeout de IA de 180 s por registro.

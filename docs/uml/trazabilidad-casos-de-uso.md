# Trazabilidad: casos de uso ↔ requisitos funcionales

Correspondencia entre los casos de uso del diagrama
(`casos-de-uso-general.puml`, v2 — con las extensiones `<<extend>>` de
revisión, horarios y usuarios) y las familias de requisitos funcionales de
`docs/requisitos-funcionales.md`. Un requisito puede estar cubierto por
varios casos de uso y un caso de uso puede cubrir varios requisitos; no se
exige una relación 1:1.

| Caso de uso | Requisitos funcionales cubiertos | Evidencia principal en código |
|---|---|---|
| **CU-01 Registrarse** | RF-AUT-17, RF-AUT-18 | `routes.py::register_page`/`register_submit` → `main.py::register` → `auth/users.py::register_user` |
| **CU-02 Iniciar sesión** | RF-AUT-01, RF-AUT-02, RF-AUT-03, RF-AUT-04, RF-AUT-07, RF-AUT-08, RF-AUT-16, RF-AUT-18 (bloqueo si inactiva) | `routes.py::login` → `main.py::login` → `auth/users.py::authenticate` |
| **CU-03 Cerrar sesión** | RF-AUT-05 | `routes.py::logout` (`session.clear()`) |
| **CU-04 Cambiar mi contraseña** | RF-AUT-14 | `routes.py::account_password` → `main.py::account_change_password` → `auth/users.py::change_own_password` |
| **CU-05 Descargar horarios desde una fuente web** | RF-DES-01 … RF-DES-10 | `main.py::start_download`/`pause_download`/`resume_download`/`download_status` → `crawler/crawler.py::PdfCrawler` |
| **CU-06 Consultar documentos descargados** | RF-DES-11, RF-DES-12 | `main.py::download_status` (`files[]`), `main.py::download_file` / `routes.py::open_pdf` |
| **CU-07 Extraer y normalizar los horarios de los PDF** | RF-EXT-01 … RF-EXT-12 | `main.py::start_extraction`/`extraction_status` → `extractor/pdf_extractor.py::run_extraction` → `pdf_table_extractor/pipeline.py` |
| **CU-08 Consultar y revisar los horarios extraídos** *(base)* | RF-REV-01, RF-REV-02, RF-REV-03, RF-REV-04, RF-REV-05, RF-REV-06, RF-REV-14 | `templates/review.html` + `static/review.js` (resumen, árbol, filtros, tabla, detalle); `main.py` `GET /review/summary,/filters,/tree,/records,/records/{id}` |
| **CU-08X Corregir un registro extraído** *(`<<extend>>` de CU-08)* | RF-REV-07, RF-REV-08, RF-REV-09, RF-REV-10, RF-REV-11, RF-REV-12, RF-REV-13 | `main.py` `PUT /review/records/{id}`, `POST .../status,/restore,/duplicate` → `review/store.py` (`apply_edit`, `set_status`, `restore_item`, `duplicate_item`) |
| **CU-09 Resolver con IA las asignaturas mezcladas** *(`<<extend>>` de CU-07 y de CU-08X)* | RF-IA-01 … RF-IA-12; RF-EXT-08 | `main.py::start_llm_review` (+ status/pause/resume/cancel) → `review/store.py::llm_review_items` → `pdf_table_extractor/llm_review.py::review_entries` → Ollama |
| **CU-10 Configurar y verificar el servicio de IA** | RF-CFG-01 … RF-CFG-06 | `main.py::get_ai_settings`/`update_ai_settings`/`ai_status`; `app/ai_settings.py` |
| **CU-11 Volcar los horarios revisados a la base de datos** | RF-BD-01 … RF-BD-12 | `main.py::db_dump_start`/`db_dump_status`/`db_dump_cancel` → `dbdump/loader.py::run_dump` |
| **CU-12 Consultar horarios** *(base)* | RF-CON-01, RF-CON-02, RF-CON-03, RF-CON-04 | `main.py::schedule_degrees`/`schedule_subjects` → `schedule/db.py`; `static/horarios.js` (carga de titulaciones/asignaturas, búsqueda) |
| **CU-12X Construir el horario semanal** *(`<<extend>>` de CU-12)* | RF-CON-05, RF-CON-06, RF-CON-09 | `static/horarios.js` (`addSelectedSubject`, `removeSubject`, `clearSchedule`, `renderSchedule`) |
| **CU-13 Detectar solapamientos** *(`<<extend>>` de CU-12X)* | RF-CON-07, RF-CON-08 | `static/horarios.js::getConflictPairs` / `renderConflicts` (cliente) |
| **CU-14 Consultar usuarios** *(base)* | RF-AUT-09, RF-AUT-15 | `templates/users_admin.html`, `routes.py::_require_admin`; `main.py::users_list` → `auth/users.py::list_users` |
| **CU-15 Dar de alta un usuario** *(`<<extend>>` de CU-14)* | RF-AUT-10 | `main.py::users_create` → `auth/users.py::create_user` |
| **CU-16 Modificar un usuario** *(`<<extend>>` de CU-14: rol, contraseña, aprobación/veto)* | RF-AUT-11, RF-AUT-12, RF-AUT-19, RF-AUT-08 (garantía de administrador) | `main.py::users_set_role`/`users_set_password`/`users_set_active` → `auth/users.py::set_role`/`set_password`/`set_active` |
| **CU-17 Dar de baja un usuario** *(`<<extend>>` de CU-14)* | RF-AUT-13 | `main.py::users_delete` → `auth/users.py::delete_user` |

## Requisitos transversales (RF-SOP)

No se elevan a caso de uso; se cumplen en toda la aplicación:

| Requisito | Cómo se cubre | Se ve en |
|---|---|---|
| RF-SOP-01 (frontend como pasarela con error controlado) | `try/except` en cada ruta proxy de `routes.py` | Todas las secuencias |
| RF-SOP-02 (registro/log en memoria de los procesos largos) | `add_log`/`add_error` en `CrawlerState`/`ExtractorState`/`DumpState`/`llm_review_state`, servidos en `*/status` | Secuencias de descarga, extracción, revisión IA, volcado |
| RF-SOP-03 (CORS abierto) | `CORSMiddleware(allow_origins=["*"])` en `main.py` | — |

## Requisitos sin caso de uso propio (justificación)

- **Pausar / reanudar / cancelar / consultar estado** (RF-DES-09/10, RF-IA-10/11, RF-EXT-11, RF-BD-10/11): control del proceso largo; se describen en la secuencia del CU, no como CU ni `<<extend>>` (no son condicionales sobre una elección del usuario, son control de un proceso ya en marcha).
- **Filtros, árbol y buscadores** de CU-08 y CU-12: en la primera vuelta se plantearon como candidatas a `<<extend>>`, pero al no cambiar el resultado observable del caso base (solo acotan qué se ve) se mantienen como parte del flujo interno de la consulta, no como extensión.
- **Clave interna frontend↔backend** (RF-AUT-16): mecanismo transversal de seguridad, en las secuencias de CU-01, CU-02, CU-04, CU-14/15/16/17 como cabecera `X-Internal-Key`.

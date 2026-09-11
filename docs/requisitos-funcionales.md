# Requisitos funcionales — Sistema de Extracción y Verificación de Horarios Universitarios

Catálogo obtenido por **ingeniería inversa del código** del repositorio
(`frontend-flask/`, `backend-fastapi/`, `db/`, `docker-compose.yml`). Refleja
el comportamiento **actual**, no el README ni funcionalidad planificada.

- **Estado** `Implementado`: existe flujo completo respaldado por código.
- **Estado** `Parcial`: existe código, con una salvedad relevante indicada.
- Columna **Evidencia**: fichero y, cuando es posible, función/endpoint.

---

## RF-AUT · Autenticación, usuarios y sesión

| Código | Requisito | Evidencia | Estado |
|---|---|---|---|
| RF-AUT-01 | El sistema debe ofrecer un formulario de inicio de sesión con usuario y contraseña. | `templates/login.html`, `routes.py::login` (GET) | Implementado |
| RF-AUT-02 | El sistema debe validar las credenciales contra la tabla `usuarios` comparando la contraseña con su *hash* y, si son correctas, abrir sesión con el nombre, el id y el rol del usuario. | `routes.py::login` (POST) → `main.py::login` → `auth/users.py::authenticate` (`check_password_hash`); `session["user"|"user_id"|"is_admin"]` | Implementado |
| RF-AUT-03 | El sistema debe mostrar un mensaje de error cuando las credenciales no son válidas. | `main.py::login`, `login.html` bloque `error` | Implementado |
| RF-AUT-04 | El sistema debe impedir el acceso a cualquier pantalla sin sesión iniciada, redirigiendo al login. | `routes.py` (todas las vistas: `if "user" not in session: redirect(login)`) | Implementado |
| RF-AUT-05 | El sistema debe permitir cerrar la sesión. | `routes.py::logout` → `session.clear()` | Implementado |
| RF-AUT-06 | Las llamadas de datos del frontend deben rechazarse con 401 si no hay sesión. | `routes.py::_require_session` en los *proxy* `/review-*`, `/db-dump-*`, `/schedule-*`, etc. | Parcial (la mayoría de endpoints de FastAPI en `:8000` no comprueban sesión; la protección está en el proxy Flask) |
| RF-AUT-07 | El sistema debe almacenar las contraseñas cifradas (*hash* pbkdf2), nunca en claro; una contraseña antigua en claro se reescribe como *hash* en el primer inicio de sesión. | `auth/users.py::hash_password`, `_verify`, `authenticate` (`_looks_hashed`) | Implementado |
| RF-AUT-08 | El sistema debe distinguir dos roles (administrador / usuario) y garantizar que siempre exista al menos un administrador: si la tabla está vacía se crea `admin/admin`, y el primer usuario que inicia sesión sin haber administradores pasa a serlo. | `auth/users.py::ensure_admin_exists`, `authenticate` (auto-promoción); `db/003_usuarios_auth.sql` (`is_admin`) | Implementado |
| RF-AUT-09 | El sistema debe permitir a un administrador consultar la lista de usuarios (nombre, rol, fecha de alta). | `templates/users_admin.html`; `routes.py::users_list` → `main.py::users_list` → `auth/users.py::list_users` (`GET /users`) | Implementado |
| RF-AUT-10 | El sistema debe permitir a un administrador dar de alta un usuario nuevo (nombre, contraseña, rol), validando el formato del nombre, la longitud mínima de contraseña y que el nombre no esté repetido. | `routes.py::users_create` → `main.py::users_create` → `auth/users.py::create_user` (`POST /users`) | Implementado |
| RF-AUT-11 | El sistema debe permitir a un administrador cambiar el rol de un usuario, impidiendo dejar el sistema sin ningún administrador. | `auth/users.py::set_role` (`PUT /users/{id}/role`) | Implementado |
| RF-AUT-12 | El sistema debe permitir a un administrador restablecer la contraseña de cualquier usuario. | `auth/users.py::set_password` (`PUT /users/{id}/password`) | Implementado |
| RF-AUT-13 | El sistema debe permitir a un administrador eliminar un usuario, impidiendo eliminar la propia cuenta y el último administrador. | `auth/users.py::delete_user` (`DELETE /users/{id}`) | Implementado |
| RF-AUT-14 | El sistema debe permitir a cualquier usuario con sesión cambiar su propia contraseña, comprobando primero la actual. | `templates/account.html`; `routes.py::account_password` → `auth/users.py::change_own_password` (`POST /account/password`) | Implementado |
| RF-AUT-15 | El sistema debe restringir las páginas y endpoints de gestión de usuarios al rol de administrador. | `routes.py::_require_admin`, `routes.py::users_admin` (redirige si no admin) | Implementado |
| RF-AUT-16 | El sistema debe proteger los endpoints de autenticación y gestión de usuarios del backend con una clave interna compartida con el frontend, rechazando (401) las llamadas directas a `:8000` sin esa clave. | `main.py::require_internal_key` (cabecera `X-Internal-Key` = `INTERNAL_API_KEY`), `routes.py::_internal_headers`; `docker-compose.yml` | Implementado |
| RF-AUT-17 | El sistema debe ofrecer una página pública de registro (sin sesión) donde un visitante crea una cuenta con usuario y contraseña. | `templates/register.html`, `routes.py::register_page` (`/registro`) + `routes.py::register_submit` → `main.py::register` → `auth/users.py::register_user` (`POST /register`) | Implementado |
| RF-AUT-18 | Una cuenta creada por registro público debe quedar como usuario normal e **inactiva**: no puede iniciar sesión (mensaje "tu cuenta todavía no está activa") hasta que un administrador la apruebe. | `auth/users.py::register_user` (`is_admin=False, is_active=False`), `authenticate` (lanza `UserError` si `not is_active`) | Implementado |
| RF-AUT-19 | El sistema debe permitir a un administrador aprobar una cuenta pendiente y vetar (desactivar sin borrar) una cuenta activa, impidiendo vetar la propia cuenta y el último administrador activo. | `auth/users.py::set_active`; `templates/users_admin.html` (botones "Aprobar"/"Vetar", estado Activo/Pendiente); `routes.py::users_set_active` → `PUT /users/{id}/active` | Implementado |

---

## RF-DES · Descarga de horarios desde la web (crawler)

| Código | Requisito | Evidencia | Estado |
|---|---|---|---|
| RF-DES-01 | El sistema debe permitir iniciar una descarga indicando una URL de origen. | `download_panel.html`, `routes.py::start_download` → `main.py::start_download` (`POST /download/start`) | Implementado |
| RF-DES-02 | El sistema debe validar que la URL no esté vacía y empiece por `http://` o `https://`. | `main.py::start_download` | Implementado |
| RF-DES-03 | El sistema debe impedir lanzar una descarga si ya hay otra en curso. | `main.py::start_download` (`download_state["running"]`) | Implementado |
| RF-DES-04 | El sistema debe localizar en la página indicada los enlaces a PDF y descargarlos. | `crawler.py::PdfCrawler.run` (extrae `<a href>`, `is_pdf_url`, `download_pdf`) | Implementado (solo la página indicada; el código no encola páginas HTML hijas pese a `max_depth`/`max_pages`) |
| RF-DES-05 | El sistema debe descargar los PDF en paralelo. | `ThreadPoolExecutor(max_workers=4)` en `crawler.run` | Implementado |
| RF-DES-06 | El sistema debe descartar recursos que no sean PDF real (por `Content-Type` o extensión). | `crawler.download_pdf` (`"application/pdf" not in content_type and not is_pdf_url`) | Implementado |
| RF-DES-07 | El sistema debe evitar descargas duplicadas por URL y por contenido idéntico (hash SHA-256), también frente a PDF ya presentes en la carpeta. | `crawler`: `downloaded_urls`, `content_hashes`, `_index_existing_files` | Implementado |
| RF-DES-08 | El sistema debe guardar cada PDF con un nombre de fichero seguro y único (sin sobrescribir salvo configuración). | `build_safe_filename`, `ensure_unique_filepath` | Implementado |
| RF-DES-09 | El sistema debe permitir pausar y reanudar la descarga en curso. | `main.py::pause_download`/`resume_download`, `crawler` `pause_event.wait()` | Implementado |
| RF-DES-10 | El sistema debe informar del estado de la descarga: en curso/pausada, páginas rastreadas, PDF encontrados, PDF descargados, actividad, registro y errores. | `main.py::download_status` (`GET /download/status`) | Implementado |
| RF-DES-11 | El sistema debe listar los PDF descargados disponibles. | `main.py::refresh_files`, `download_state["files"]`, `download_panel.html` | Implementado |
| RF-DES-12 | El sistema debe permitir abrir/descargar un PDF concreto ya descargado. | `main.py::download_file` (`GET /download/file/{filename}`), `routes.py::open_pdf` (`/pdf/<f>`) | Implementado |

---

## RF-EXT · Extracción y normalización de horarios

| Código | Requisito | Evidencia | Estado |
|---|---|---|---|
| RF-EXT-01 | El sistema debe permitir lanzar la extracción sobre todos los PDF de la carpeta de descargas. | `routes.py::dump_db` → `main.py::start_extraction` (`POST /extract/start`) | Implementado |
| RF-EXT-02 | El sistema debe impedir lanzar una extracción si ya hay otra en curso, y avisar si no hay PDF. | `main.py::start_extraction` | Implementado |
| RF-EXT-03 | El sistema debe realizar un diagnóstico estructural de cada PDF (texto, palabras, tablas) y escribir sus resultados intermedios. | `pdf_extractor.py::_run_diagnostics_with_progress`, `pdf_table_extractor` `analyze_pdf`/`write_pdf_outputs` | Implementado |
| RF-EXT-04 | El sistema debe normalizar los resultados a registros de horario (grado, año, semestre, grupo, día, horas, asignatura, aulas, anotaciones). | `run_normalization` → `all_schedules.json`; `schedule_models.ScheduleEntry` | Implementado |
| RF-EXT-05 | El sistema debe registrar aparte las tablas que no ha podido interpretar. | `skipped_tables.json`, items origin `"skipped"` | Implementado |
| RF-EXT-06 | El sistema debe clasificar cada registro como VALID, WARNING o INVALID según sus avisos y la sospecha de asignaturas mezcladas. | `store.py::_compute_entry_status` | Implementado |
| RF-EXT-07 | El sistema debe asignar a cada registro un identificador estable derivado de su origen. | `store.py::_make_id` (SHA-1 de `origin|file|page|seq`) | Implementado |
| RF-EXT-08 | El sistema debe permitir activar opcionalmente una revisión con IA durante la extracción (por petición o por variable de entorno). | `start_extraction` (`enable_llm_review`), `run_extraction` → `_run_llm_review_stage` → `review_entries` | Implementado |
| RF-EXT-09 | El sistema debe generar un manifiesto de la ejecución (PDF encontrados/procesados/fallidos, ficheros fallidos, resultado de la etapa IA). | `extraction_run.json` en `run_extraction` | Implementado |
| RF-EXT-10 | Al terminar la extracción, el sistema debe (re)generar la copia revisable de los horarios sin destruir la anterior. | `refresh_reviewed_from_extraction` → `reviewed_schedules.json`; la previa se renombra a `reviewed_schedules.<timestamp>.bak.json` | Implementado |
| RF-EXT-11 | El sistema debe informar del progreso de la extracción: en curso, ficheros totales/procesados, porcentaje, fichero actual, registro y errores. | `main.py::extraction_status` (`GET /extract/status`) | Implementado |
| RF-EXT-12 | Un fallo al procesar un PDF concreto no debe abortar la extracción del resto. | `_run_diagnostics_with_progress` (try/except por fichero, `failed_files`) | Implementado |

---

## RF-REV · Revisión y corrección manual

Todas operan sobre `reviewed_schedules.json`; nunca modifican `all_schedules.json`.

| Código | Requisito | Evidencia | Estado |
|---|---|---|---|
| RF-REV-01 | El sistema debe mostrar un resumen de la extracción: PDF procesados/fallidos, nº de registros válidos, con avisos, incorrectos, tablas omitidas, revisados y revisados por IA. | `main.py::review_summary` → `store.compute_summary` (`GET /review/summary`) | Implementado |
| RF-REV-02 | El sistema debe ofrecer los valores disponibles para filtrar (grados, años, semestres, grupos, días, PDF, problemas, estados). | `store.compute_filter_options` (`GET /review/filters`) | Implementado |
| RF-REV-03 | El sistema debe mostrar los registros agrupados jerárquicamente por Grado → Año académico → Semestre → Grupo con sus recuentos. | `store.compute_tree` (`GET /review/tree`) | Implementado |
| RF-REV-04 | El sistema debe listar los registros de forma paginada y ordenada (grado, año, semestre, grupo, día, hora, asignatura). | `store.paginate`, `store.sort_items` (`GET /review/records`) | Implementado |
| RF-REV-05 | El sistema debe permitir filtrar por estado, grado, año, semestre, grupo, día, PDF, tipo de problema y revisado-por-IA, y buscar por texto libre (asignatura, aula, grupo, texto original, fichero, grado). | `store.apply_filters`, `store._matches_search` | Implementado |
| RF-REV-06 | El sistema debe permitir consultar el detalle completo de un registro (valores actuales, originales, origen y metadatos de extracción). | `main.py::review_record_detail` (`GET /review/records/{id}`) | Implementado |
| RF-REV-07 | El sistema debe permitir editar los campos de un registro (grado, año, semestre, grupo, día, horas, asignatura, aulas, anotaciones, sospecha de mezcla). | `main.py::review_record_edit` → `store.apply_edit` (`PUT /review/records/{id}`) | Implementado |
| RF-REV-08 | El sistema debe validar el contenido de la edición (todos los campos presentes, `rooms`/`notes` listas, `multiple_entries_suspected` booleano) y rechazar con error los datos inválidos. | `store._validate_current_payload` → HTTP 400 | Implementado |
| RF-REV-09 | Al editar, el sistema debe marcar el registro como revisado, indicar si difiere del original y recalcular su estado. | `store.apply_edit` (`reviewed`, `manually_modified`, `_compute_entry_status`) | Implementado |
| RF-REV-10 | El sistema debe permitir cambiar manualmente el estado de un registro a VALID, WARNING o INVALID. | `main.py::review_record_status` → `store.set_status` (`POST /review/records/{id}/status`) | Implementado |
| RF-REV-11 | El sistema debe permitir restaurar un registro a su valor original. | `store.restore_item` (`POST /review/records/{id}/restore`) | Implementado |
| RF-REV-12 | El sistema debe permitir duplicar un registro en otro independiente (para separar a mano una celda con varias asignaturas), conservando su trazabilidad al origen. | `store.duplicate_item` (`POST /review/records/{id}/duplicate`) | Implementado |
| RF-REV-13 | El sistema debe guardar los cambios de forma atómica y serializar las escrituras concurrentes. | `store._atomic_write_json`, `review_lock` | Implementado |
| RF-REV-14 | El sistema debe reconstruir la copia revisable si no existe (p. ej. tras reiniciar), y devolver 404 si aún no hay ninguna extracción. | `_require_reviewed`, `ensure_reviewed` | Implementado |

---

## RF-IA · Revisión asistida por IA (bajo demanda)

| Código | Requisito | Evidencia | Estado |
|---|---|---|---|
| RF-IA-01 | El sistema debe permitir enviar a la IA local (Ollama) los registros con asignaturas mezcladas sin resolver, todos o una selección concreta. | `main.py::start_llm_review` (`POST /review/llm-review`, `item_ids` opcional) | Implementado |
| RF-IA-02 | El sistema debe impedir lanzar una revisión con IA si ya hay otra en curso, y devolver 404 si no hay datos que revisar. | `start_llm_review` | Implementado |
| RF-IA-03 | Por cada registro, la IA debe confirmarlo como una sola asignatura o separarlo en las asignaturas que contenga, indicando su confianza (alta/media/baja). | `llm_review.py::_SYSTEM_PROMPT`, `_apply_review` | Implementado |
| RF-IA-04 | Si la IA resuelve el registro como varias asignaturas, el sistema debe crear registros nuevos independientes para las adicionales, trazados al origen. | `store._new_item_from_llm_entry` (`llm_split_from`) | Implementado |
| RF-IA-05 | Una respuesta de baja confianza debe conservar el aviso original en vez de eliminarlo. | `llm_review._apply_review` (`if review.confidence != "low"`) | Implementado |
| RF-IA-06 | El sistema debe reintentar una vez una respuesta del modelo mal formada, realimentando el error, antes de dar el lote por fallido. | `llm_review._review_batch` | Implementado |
| RF-IA-07 | Un fallo por registro (Ollama caído, *timeout*, salida inválida) debe registrarse y no detener el resto del lote. | `store.llm_review_items` (try/except por item, `errors`) | Implementado |
| RF-IA-08 | El sistema debe aplicar un *timeout* máximo por llamada a Ollama. | `store.OLLAMA_CALL_TIMEOUT_SECONDS = 180` | Implementado |
| RF-IA-09 | El sistema debe guardar el resultado tras cada registro (recuperación ante caída). | `_run_llm_review_background::on_item_done` → `save_reviewed` | Implementado |
| RF-IA-10 | El sistema debe permitir pausar, reanudar y cancelar la revisión con IA; la pausa/cancelación surte efecto entre registros, nunca a mitad de una llamada. | `main.py::pause/resume/cancel_llm_review`, `_llm_review_should_continue` | Implementado |
| RF-IA-11 | El sistema debe informar del progreso: en curso, pausada, intentados/total, resueltos, separados, errores y registro. | `main.py::llm_review_status` (`GET /review/llm-review/status`) | Implementado |
| RF-IA-12 | El sistema debe usar el host y el modelo de Ollama configurados en cada momento. | `_run_llm_review_background` → `ai_settings.load_ai_settings` | Implementado |

---

## RF-CFG · Administración del servicio de IA (Ollama)

| Código | Requisito | Evidencia | Estado |
|---|---|---|---|
| RF-CFG-01 | El sistema debe permitir consultar la configuración actual de IA (host y modelo). | `main.py::get_ai_settings` (`GET /ai/settings`); por defecto `OLLAMA_HOST`/`http://localhost:11434` y `qwen2.5:3b` | Implementado |
| RF-CFG-02 | El sistema debe permitir cambiar el host y el modelo de Ollama sin reiniciar, persistiéndolos. | `main.py::update_ai_settings` → `ai_settings.save_ai_settings` (`ai_settings.json`) (`PUT /ai/settings`) | Implementado |
| RF-CFG-03 | El sistema debe validar que el host no esté vacío y empiece por `http://` o `https://`. | `ai_settings.save_ai_settings` → HTTP 400 | Implementado |
| RF-CFG-04 | El sistema debe comprobar la disponibilidad de Ollama y medir la latencia. | `main.py::ai_status` → `GET {host}/api/tags` (timeout 5 s) (`GET /ai/status`) | Implementado |
| RF-CFG-05 | El sistema debe listar los modelos disponibles en el servidor Ollama consultado. | `ai_status` (`models` de `/api/tags`), `ai_panel.html` (recuento + lista + *datalist*) | Implementado |
| RF-CFG-06 | Si Ollama no responde o devuelve error, el sistema debe indicarlo como "no conectado" con el detalle del error. | `ai_status` (except / `status_code != 200`) | Implementado |

---

## RF-BD · Volcado a la base de datos

| Código | Requisito | Evidencia | Estado |
|---|---|---|---|
| RF-BD-01 | El sistema debe permitir volcar a PostgreSQL los horarios revisados. | `routes.py::db_dump_start` → `main.py::db_dump_start` → `dbdump/loader.py::run_dump` (`POST /db-dump/start`) | Implementado |
| RF-BD-02 | El sistema debe volcar solo los registros en estado VALID o WARNING y descartar los INVALID y las tablas omitidas, informando de cuántos se descartan. | `loader.DUMPABLE_STATUSES`, `run_dump` (`discarded`) | Implementado |
| RF-BD-03 | El sistema debe reconstruir las tablas desde cero en cada volcado (vaciar y recargar). | `run_dump` → `TRUNCATE ... CASCADE` de las 11 tablas de horario/catálogos | Implementado |
| RF-BD-04 | Todo el volcado debe realizarse en una única transacción: si algo falla, se revierte y la base de datos queda intacta. | `conn.autocommit=False`, `commit()` final, `rollback()` en excepción/cancelación | Implementado |
| RF-BD-05 | El sistema debe crear/normalizar los catálogos a partir de los datos: universidad, titulaciones, años académicos, grupos, asignaturas, aulas y fuentes. | `run_dump` (`get_degree`, `get_year`, `get_group`, `get_subject`, `get_room`, `get_source`) | Implementado |
| RF-BD-06 | El sistema debe normalizar días de la semana, horas (`HH:MM`), rango de años (`AAAA/AAAA`) y nombre normalizado de asignatura al volcar. | `_canon_day`, `_clean_time`, `_parse_year_range`, `_normalize_subject` | Implementado |
| RF-BD-07 | El sistema debe guardar por cada sesión sus metadatos de extracción (página, texto original, estrategia, índice de tabla, filas, horas en bruto, datos de la revisión IA). | `run_dump` → `INSERT INTO extraction_metadata` | Implementado |
| RF-BD-08 | El sistema debe registrar como incidencias los avisos (WARNING) y las notas (INFO) de cada sesión. | `run_dump` → `INSERT INTO extraction_issues` | Implementado |
| RF-BD-09 | El sistema debe conservar el texto original y el nombre "limpio" por separado (raw vs normalizado). | `subject_name_raw`/`room_raw`/`group_name_raw` vs `subjects.name`/`rooms.code` | Implementado |
| RF-BD-10 | El sistema debe impedir lanzar un volcado si ya hay otro en curso y permitir cancelarlo (con reversión de lo hecho). | `db_dump_start` (`running`), `db_dump_cancel` (`cancel_requested`) | Implementado |
| RF-BD-11 | El sistema debe informar del progreso del volcado: en curso, procesados/total, porcentaje, registro, errores, fin y recuento de filas insertadas por tabla. | `DumpState.snapshot` (`GET /db-dump/status`) | Implementado |
| RF-BD-12 | Las tablas ajenas al horario (`usuarios`, `demo`) no deben verse afectadas por el volcado. | `loader.TRUNCATE_TABLES` (no las incluye) | Implementado |

---

## RF-CON · Consulta de horarios y detección de solapes

| Código | Requisito | Evidencia | Estado |
|---|---|---|---|
| RF-CON-01 | El sistema debe ofrecer la lista de titulaciones que tienen horarios en la base de datos, con su nº de sesiones. | `schedule/db.py::list_degrees` (`GET /schedule/degrees`) | Implementado |
| RF-CON-02 | El sistema debe ofrecer, opcionalmente filtradas por titulación, las asignaturas con su grupo y sus sesiones (día, hora inicio/fin, aula, estado), condensando en una entrada las sesiones de la misma asignatura y grupo. | `schedule/db.py::list_subjects` (`GET /schedule/subjects`) | Implementado |
| RF-CON-03 | El sistema debe excluir de la rejilla las sesiones sin día u hora completa y contabilizarlas aparte. | `list_subjects` (`unscheduled_count`) | Implementado |
| RF-CON-04 | Ante un fallo de base de datos el sistema debe responder 503; si no hay datos volcados, 404. | `main.py::schedule_degrees`/`schedule_subjects` (`ScheduleError` → 503; vacío → 404) | Implementado |
| RF-CON-05 | El sistema debe permitir seleccionar una titulación y buscar/añadir asignaturas a un horario semanal. | `horarios.js` (`loadDegrees`, `loadSubjects`, `filterSubjects`, `addSelectedSubject`) | Implementado |
| RF-CON-06 | El sistema debe representar las asignaturas seleccionadas en una rejilla semanal (Lunes–Viernes, franjas horarias). | `horarios.js::renderSchedule` | Implementado |
| RF-CON-07 | El sistema debe detectar y mostrar los solapes entre las asignaturas seleccionadas (mismo día y franja), con una leyenda y el listado de choques (día, hora, asignaturas implicadas). | `horarios.js::getConflictPairs`, `renderConflicts`; `horarios.html` leyenda | Implementado (cálculo en el navegador) |
| RF-CON-08 | El sistema debe señalar visualmente las sesiones que arrastran un aviso (WARNING) de la extracción. | `horarios.js` (`block.classList.add("warning-src")`), `horarios.css` | Implementado |
| RF-CON-09 | El sistema debe permitir quitar asignaturas y limpiar el horario. | `horarios.js::removeSubject`, `clearSchedule` | Implementado |

---

## RF-SOP · Transversales

| Código | Requisito | Evidencia | Estado |
|---|---|---|---|
| RF-SOP-01 | El frontend debe actuar como pasarela hacia el backend, devolviendo un error controlado si el backend no responde. | `routes.py` (todas las rutas *proxy*, `try/except` → 500/502) | Implementado |
| RF-SOP-02 | El sistema debe mantener y exponer un registro (log) en memoria de cada proceso largo (descarga, extracción, revisión IA, volcado). | `CrawlerState`/`ExtractorState`/`DumpState`/`llm_review_state` `add_log`/`add_error`, servido en los `*/status` | Implementado (es *logging*, no un panel administrativo consultable) |
| RF-SOP-03 | El sistema debe permitir el consumo de la API desde otro origen (CORS abierto). | `main.py` `CORSMiddleware(allow_origins=["*"])` | Implementado |

---

## Observaciones / requisitos NO cubiertos por el código

- **Autorización de grano grueso.** Hay dos roles (administrador / usuario), pero solo la gestión de usuarios se restringe por rol; el resto de funciones (crawler, extracción, revisión, volcado, configuración de IA) las puede usar cualquier usuario con sesión. No hay un perfil "estudiante" separado.
- **La mayoría de la API FastAPI sigue sin autenticación** en `:8000`: solo `/login` y `/users*`/`/account/*` están protegidos (clave interna). El resto de endpoints se pueden llamar directamente.
- **`login.html`** mantiene un enlace "¿Ha olvidado su contraseña?" desactivado (no hay recuperación de contraseña por correo).
- **No hay gestión (CRUD) de universidades, titulaciones, planes ni asignaturas**: los catálogos solo los crea el volcado. La tabla `study_plans` del esquema no se rellena nunca.
- **`GET /demo`** y el árbol `pythonPruebas/` son código heredado/experimental, fuera del flujo actual.
- El botón "Volcar a base de datos" del panel de descargas **lanza la extracción**, no el volcado (el volcado real está en la página `/volcado`).

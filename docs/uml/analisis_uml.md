# Análisis UML — reconstrucción desde el código

> Documento previo a los diagramas. Resume qué se ha encontrado en el
> repositorio y qué decisiones UML se han tomado y por qué. Todo lo que
> aparece aquí y en los `.puml` está justificado en la implementación
> actual; cuando no hay evidencia suficiente, se dice explícitamente.

## 1. Estructura real del repositorio

| Zona | Ruta | Contenido |
|---|---|---|
| Frontend | `frontend-flask/app/` | `routes.py` (un único Blueprint: sirve plantillas + **hace de proxy HTTP** al backend con `requests`), `templates/`, `static/*.js` y `*.css`. `run.py` arranca `app.run(host="0.0.0.0", port=5000)`. |
| Backend | `backend-fastapi/app/` | `main.py` (FastAPI, ~40 endpoints), y módulos: `crawler/`, `extractor/` (incl. el paquete instalable `pdf_table_extractor`), `review/store.py`, `dbdump/loader.py`, `schedule/db.py`, `auth/users.py`, `ai_settings.py`. Uvicorn en `0.0.0.0:8000`. |
| Base de datos | `db/` | `createdatabase.sql` (12 tablas de horario), `init.sql` (solo `demo`, montado en el arranque del contenedor), `003_usuarios_auth.sql` + `004_usuarios_is_active.sql` (migraciones de `usuarios`, aplicadas a mano), `queries.sql` (consultas de depuración manuales). |
| Despliegue | `docker-compose.yml`, `*/Dockerfile` | 4 servicios: `frontend` (Flask, :5000), `backend` (FastAPI, :8000), `db` (postgres:15, :5432), `pgadmin` (:5050). Volúmenes `pgdata`, `pgadmin_data`; bind-mounts `./downloads`, `./extracted`. Var. de entorno: `ENABLE_LLM_REVIEW`, `OLLAMA_HOST`, `INTERNAL_API_KEY`. |
| Prototipos | `pythonPruebas/` | Código anterior (`pdf_extractorV4/V5`, paquete `schedule_parser`). **No lo importa nada del sistema en ejecución.** Obsoleto. |

## 2. Flujo real (extremo a extremo)

```
Fuente web  --(requests+BeautifulSoup, PdfCrawler)-->  ./downloads/*.pdf
   -> /extract/start (hilo)  ->  pdfplumber (diagnóstico)  ->  normalización
        ->  all_schedules.json  ->  [etapa IA opcional -> Ollama]
        ->  refresh_reviewed_from_extraction  ->  reviewed_schedules.json
             (cada item clasificado VALID / WARNING / INVALID)
   -> revisión manual  (edita reviewed_schedules.json, nunca el original)
   -> [revisión con IA bajo demanda -> Ollama]  (sobre reviewed_schedules.json)
   -> /db-dump/start (hilo)  ->  TRUNCATE + recarga en PostgreSQL (1 transacción)
   -> /schedule/degrees + /schedule/subjects  ->  rejilla semanal (JS)
        ->  detección de solapes  **en el navegador**  (horarios.js)
```

Tres procesos largos corren en **hilos** dentro del backend, con estado en
memoria y endpoints de sondeo: descarga (`real_download_process`),
extracción (`run_extraction`), revisión con IA
(`_run_llm_review_background`) y volcado (`run_dump`).

## 3. Actores identificados (solo desde el código)

| Actor | Evidencia | ¿Actor UML? |
|---|---|---|
| **Visitante** (sin sesión) | `routes.py`: `/login` y `/registro` no comprueban sesión; el resto redirige a `/login`. | Sí (humano) |
| **Usuario** (autenticado) | `session["user"|"user_id"|"is_admin"]`; todas las vistas operativas exigen sesión. | Sí (humano) |
| **Administrador** | `usuarios.is_admin`; `routes.py::_require_admin`; único recurso restringido: `/usuarios` y `/users*`. | Sí (humano, `Administrador --|> Usuario`) |
| **Servicio de IA local (Ollama)** | `llm_review.py` (`ollama.Client(...).chat`), `main.py::ai_status` (`GET {host}/api/tags`). Fuera del `docker-compose`. | Sí (sistema externo, secundario) |
| **Fuente web de horarios** | `crawler/crawler.py::PdfCrawler` hace `requests.get` sobre la URL que teclea el usuario y descarga sus PDF. | Sí (sistema externo, secundario) |
| PostgreSQL | Servicio del propio `docker-compose`, acceso con `psycopg2`. | **No** — infraestructura interna → diagrama de despliegue |
| pgAdmin | Herramienta de operación, no la usa el sistema. | No |

No se ha encontrado evidencia de ningún otro actor externo (no hay
integración con LDAP/SSO, colas, webhooks, ni clientes automatizados
distintos del propio frontend).

## 4. Casos de uso confirmados en el código

| CU | Confirmado por |
|---|---|
| CU-01 Registrarse | `templates/register.html`, `routes.py::register_page`/`register_submit` → `main.py::register` → `auth/users.py::register_user` (cuenta `is_active=false`). |
| CU-02 Iniciar sesión | `routes.py::login` → `main.py::login` → `auth/users.py::authenticate` (`check_password_hash`, comprueba `is_active`, re-hash de contraseña antigua, auto-promoción a admin si no hay ninguno). |
| CU-03 Cerrar sesión | `routes.py::logout` → `session.clear()`. |
| CU-04 Cambiar mi contraseña | `templates/account.html` → `routes.py::account_password` → `main.py::account_change_password` → `auth/users.py::change_own_password` (verifica la actual). |
| CU-05 Descargar horarios desde una fuente web | `templates/download_panel.html` → `routes.py::start_download`/`pause_download`/`resume_download`/`status` → `main.py::start_download` → hilo `real_download_process` → `crawler/crawler.py::PdfCrawler.run`. |
| CU-06 Consultar documentos descargados | `GET /download/status` devuelve `files[]` (`refresh_files`); `routes.py::open_pdf` → `GET /download/file/{filename}` (`FileResponse`); lista clicable en `download_panel.html`. |
| CU-07 Extraer y normalizar los horarios de los PDF | `routes.py::dump_db` → `main.py::start_extraction` → hilo `run_extraction` → `_run_diagnostics_with_progress` (pdfplumber) + `run_normalization` + `_run_llm_review_stage` (opcional) + `refresh_reviewed_from_extraction`. |
| CU-08 Revisar y corregir los horarios extraídos | `templates/review.html` + `static/review.js`; `main.py` `/review/summary,/filters,/tree,/records`, `GET/PUT /review/records/{id}`, `POST .../status,/restore,/duplicate`; `review/store.py` (`apply_edit`, `set_status`, `restore_item`, `duplicate_item`, escritura atómica, `review_lock`). |
| CU-09 Resolver con IA las asignaturas mezcladas | `review.js` (botón "Revisar todo con IA"); `main.py::start_llm_review` (+ `/status,/pause,/resume,/cancel`) → hilo `_run_llm_review_background` → `review/store.py::llm_review_items` → `pdf_table_extractor/llm_review.py::review_entries` → `ollama.Client.chat`. También como etapa 3 opcional dentro de `run_extraction`. |
| CU-10 Configurar y verificar el servicio de IA | `templates/ai_panel.html`; `main.py::get_ai_settings`/`update_ai_settings` (persiste `ai_settings.json`) / `ai_status` (`GET {host}/api/tags`, timeout 5 s). |
| CU-11 Volcar los horarios revisados a la base de datos | `templates/db_dump.html` + enlace en `review.html`; `main.py::db_dump_start` (+ `/status,/cancel`) → hilo `dbdump/loader.py::run_dump` (filtra VALID+WARNING, `TRUNCATE ... CASCADE`, recarga de catálogos + sesiones, `commit`/`rollback`). |
| CU-12 Consultar horarios | `templates/horarios.html` + `static/horarios.js`; `main.py::schedule_degrees`/`schedule_subjects` → `schedule/db.py::list_degrees`/`list_subjects` (consulta a PostgreSQL, condensación por asignatura+grupo). |
| CU-13 Detectar solapamientos | `static/horarios.js::getConflictPairs` / `renderConflicts`. **100 % en el cliente**, sin llamada al backend. Se dispara al seleccionar asignaturas cuyo día/hora coinciden. |
| CU-14 Gestionar usuarios *(ver corrección en §4bis: se desdobla en CU-14 Consultar + CU-15/16/17 `<<extend>>`)* | `templates/users_admin.html` + `routes.py::_require_admin`; `main.py` `GET /users`, `POST /users`, `PUT /users/{id}/password`, `PUT /users/{id}/role`, `PUT /users/{id}/active`, `DELETE /users/{id}`; `auth/users.py` (`list_users`, `create_user`, `set_password`, `set_role`, `set_active`, `delete_user`, guardas de "último administrador activo" y "no borrarte a ti mismo"). |

### Acciones que NO se elevan a caso de uso (son operaciones internas del CU)

Pausar / reanudar / cancelar / consultar estado de los procesos largos;
filtros, árbol de agrupación, paginación y resumen en la revisión; abrir
el modal de resumen tras la extracción; validación de URL; *timeout* de
Ollama; escritura atómica de JSON; copia de seguridad de
`reviewed_schedules.json`; sondeo periódico del frontend; `GET /demo`
(andamiaje). Se describen, cuando procede, en la secuencia del CU
correspondiente.

## 5. Decisiones sobre `<<include>>`, `<<extend>>` y generalización

### `<<include>>`: ninguno

No se ha encontrado ningún comportamiento que **siempre** se ejecute
dentro de varios casos de uso y que tenga sentido como caso de uso
reutilizable. En particular:

- "Iniciar sesión" es una **precondición** (middleware de sesión de
  Flask), no un subflujo que los demás CU invoquen: se documenta como
  nota, no como `<<include>>` desde cada CU.
- El encadenamiento extracción → revisión → volcado es **secuencia
  temporal** (cada etapa lee el fichero que dejó la anterior), no
  inclusión.

### `<<extend>>`: dos, ambos justificados por el código

1. **"Resolver con IA las asignaturas mezcladas" `<<extend>>` "Extraer y
   normalizar los horarios de los PDF"**
   Condición: *revisión con IA activada en la extracción*.
   Evidencia: `main.py::start_extraction` recibe `enable_llm_review`
   (casilla del panel); `run_extraction` llama a `_run_llm_review_stage`,
   que solo se ejecuta `if enabled`. La extracción se completa con
   normalidad sin esa etapa (es el valor por defecto). Es un
   comportamiento **opcional y condicional** añadido al CU base → `<<extend>>`.
   "Resolver con IA…" se mantiene además como **caso de uso independiente**
   (endpoint propio `/review/llm-review`, actor propio Ollama, se lanza a
   demanda desde la pantalla de revisión), por lo que aparece asociado a
   Usuario y a Ollama y, adicionalmente, como extensión de la extracción.

2. **"Detectar solapamientos" `<<extend>>` "Consultar horarios"**
   Condición: *hay asignaturas seleccionadas cuyo día y franja horaria
   coinciden*.
   Evidencia: `horarios.js` pinta la rejilla semanal (CU base completo) y,
   **solo si** el usuario añade asignaturas que chocan, `getConflictPairs`
   calcula los solapes y `renderConflicts` los muestra. Es cálculo
   **en el navegador**, opcional y condicional → `<<extend>>`.

### Generalización

- **`Administrador --|> Usuario`** (generalización de actor). El código lo
  respalda: un administrador es una fila de `usuarios` con
  `is_admin = true`; puede ejecutar todos los CU del usuario y, además,
  los de gestión de usuarios (`routes.py::_require_admin` es lo único que
  distingue). Evita duplicar asociaciones.
- **Visitante** y **Usuario** son actores distintos por estado de sesión,
  **no** una generalización (uno no es especialización del otro).
- No se ha encontrado ninguna generalización entre casos de uso con
  evidencia suficiente.

## 4bis. Revisión (2ª vuelta): tres CU eran en realidad base + extensiones

Una segunda pasada, aplicando el criterio de `<<extend>>` de forma
sistemática a **toda** la interfaz (no solo a los dos casos ya
identificados), encontró el mismo patrón en otros tres sitios: una acción
de *consulta* que es un objetivo completo por sí sola, y una o varias
acciones de *escritura* que solo ocurren si el actor decide actuar sobre
algo concreto. Detalle exhaustivo, acción por acción, en
[`auditoria-acciones-ui.md`](auditoria-acciones-ui.md). Cambios:

| Antes (1ª vuelta) | Ahora (2ª vuelta) |
|---|---|
| CU-08 "Revisar y corregir los horarios extraídos" (un óvalo) | **CU-08** "Consultar y revisar…" (base: resumen/árbol/filtros/tabla/detalle) + **CU-08X** "Corregir un registro extraído" (`<<extend>>`: editar/marcar estado/restaurar/duplicar, condición: el usuario elige actuar sobre un registro) |
| CU-12 "Consultar horarios" (un óvalo, incluía añadir/quitar asignaturas) | **CU-12** "Consultar horarios" (base: cargar titulaciones/asignaturas) + **CU-12X** "Construir el horario semanal" (`<<extend>>`, condición: el usuario añade una asignatura). CU-13 "Detectar solapamientos" pasa a extender CU-12X, no CU-12 directamente (los solapes solo tienen sentido una vez hay algo construido) |
| CU-14 "Gestionar usuarios" (un óvalo con alta/rol/contraseña/aprobación/baja) | **CU-14** "Consultar usuarios" (base: `GET /users`) + **CU-15** "Dar de alta un usuario", **CU-16** "Modificar un usuario" (rol/contraseña/aprobación-veto), **CU-17** "Dar de baja un usuario" (los tres `<<extend>>` de CU-14, condición: qué botón pulsa el administrador sobre la lista) |

**Por qué la 1ª versión se quedaba corta:** fundir consulta + acciones de
escritura en un único óvalo ocultaba que la consulta **funciona sola**
(se puede abrir la pantalla de usuarios solo para mirar, o la de horarios
solo para ver qué asignaturas hay, sin cambiar nada) y que las acciones de
escritura son **condicionales** (dependen de qué botón se pulse). Eso es
exactamente la definición de `<<extend>>`, y no dibujarlo escondía una
relación real del sistema.

**Por qué NO se ha aplicado el mismo criterio a CU-05 (descarga) ni a
CU-07 (extracción) más allá de lo ya modelado:** en esos dos casos no hay
una "consulta" separada de la "acción": iniciar la descarga o la
extracción **es** la acción; pausar/reanudar/cancelar no son alternativas
condicionales sobre un resultado observable nuevo, son control del mismo
proceso ya en marcha (no cumplen el criterio "aporta un resultado
observable adicional"). Se mantienen como operación interna.

## 6. Dónde ocurre realmente cada cosa (para las secuencias)

- **Frontend = proxy.** Cada ruta de `routes.py` reenvía con `requests` a
  `http://backend:8000/...` y devuelve el JSON. La sesión vive en Flask;
  el backend no comprueba sesión (salvo la clave interna en
  autenticación y usuarios).
- **Clave interna.** `POST /login`, `POST /register`, `/users*` y
  `/account/password` exigen la cabecera `X-Internal-Key` =
  `INTERNAL_API_KEY` (`main.py::require_internal_key`); el frontend la
  añade con `_internal_headers()`.
- **Procesos largos.** Se lanzan con `threading.Thread(daemon=True)`; el
  frontend hace *polling* a `GET /*/status` cada 1–2 s.
- **Persistencia intermedia en ficheros.** La extracción y la revisión
  trabajan sobre JSON en `./extracted` (`all_schedules.json`,
  `reviewed_schedules.json`, `ai_settings.json`); PostgreSQL solo entra en
  juego en el **volcado** y en la **consulta**.
- **Detección de solapes: cliente.** No hay endpoint de solapes; es
  `horarios.js`.
- **Transaccionalidad del volcado.** `run_dump` abre `autocommit=False`,
  hace `TRUNCATE ... CASCADE` de 11 tablas, recarga y `commit()`; ante
  excepción o cancelación, `rollback()`.

## 7. Diagramas generados

| Fichero | Tipo | Alcance |
|---|---|---|
| `casos-de-uso-general.puml` | Casos de uso | 14 CU, 5 actores, 2 `<<extend>>`, 1 generalización de actor |
| `componentes.puml` | Componentes | Piezas lógicas reales y sus dependencias |
| `despliegue.puml` | Despliegue | 4 contenedores + Ollama externo + fuente web |
| `modelo-datos.puml` | Entidad-relación | Esquema PostgreSQL (12 + `usuarios` + `demo`) |
| `clases.puml` | Clases | Solo lo orientado a objetos (crawler, estado de procesos, modelos del extractor, DTOs Pydantic, excepciones) |
| `actividad-extraccion-verificacion.puml` | Actividad | Pipeline completo |
| `secuencia-*.puml` (×9) | Secuencia | Inicio de sesión, registro, descarga, extracción, revisión manual, revisión con IA, volcado a BD, consulta de horarios, gestión de usuarios |
| `trazabilidad-casos-de-uso.md` | Tabla | CU ↔ requisitos funcionales (RF-*) |
| `README.md` | Informe | Resumen, actores, CU, include/extend, generalizaciones, trazabilidad y discrepancias |

## 8. Discrepancias y observaciones (detalle en `README.md`)

- El botón **"Volcar a base de datos"** del panel de descargas en realidad
  lanza la **extracción** (`/dump-db` → `/extract/start`); el volcado real
  está en la página `/volcado`. Etiqueta engañosa, código en uso.
- La **mayor parte de la API FastAPI (`:8000`) no tiene autenticación**;
  solo autenticación y usuarios están protegidos por la clave interna.
- **`SECRET_KEY` de Flask fija** en `app/__init__.py`; credenciales de BD e
  `INTERNAL_API_KEY` con valor por defecto en `docker-compose.yml`.
- El esquema tiene `study_plans` y campos de catálogo (universidades,
  planes, ECTS…) que **el volcado no rellena**: solo se crean por el
  volcado `universities` (1 fila fija), `degrees`, `academic_years`,
  `groups`, `subjects`, `rooms`, `sources`.
- `GET /demo` + tabla `demo` y el árbol `pythonPruebas/` son **código
  heredado**, fuera del flujo actual.

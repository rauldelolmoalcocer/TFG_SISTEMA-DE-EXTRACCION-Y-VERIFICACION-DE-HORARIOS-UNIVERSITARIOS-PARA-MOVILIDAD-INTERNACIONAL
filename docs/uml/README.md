# Documentación UML — reconstrucción desde el código

Todos los diagramas de esta carpeta se han obtenido por **ingeniería
inversa del código real** del repositorio (`frontend-flask/`,
`backend-fastapi/`, `db/`, `docker-compose.yml`). No reflejan el README de
raíz ni funcionalidad planificada. La justificación detallada está en
[`analisis_uml.md`](analisis_uml.md).

> Regla aplicada: ante la duda, se indica **"no hay evidencia suficiente"**
> en lugar de inventar una relación o funcionalidad.

---

## 1. Diagramas generados

| Fichero | Tipo | Qué representa |
|---|---|---|
| [`analisis_uml.md`](analisis_uml.md) | Informe previo | Estructura del repo, flujo real, actores y decisiones UML |
| [`auditoria-acciones-ui.md`](auditoria-acciones-ui.md) | Auditoría | Cada botón/enlace/formulario de la interfaz, clasificado; confirma que no faltan casos de uso |
| [`casos-de-uso-general.puml`](casos-de-uso-general.puml) | Casos de uso | 14 CU, 5 actores, 2 `<<extend>>`, 1 generalización de actor |
| [`componentes.puml`](componentes.puml) | Componentes | Piezas lógicas (frontend, API, servicios de dominio, almacenamiento) y dependencias |
| [`despliegue.puml`](despliegue.puml) | Despliegue | 4 contenedores Docker + Ollama externo + fuente web + volúmenes |
| [`modelo-datos.puml`](modelo-datos.puml) | Entidad-relación | Esquema PostgreSQL: 12 tablas de horario + `usuarios` + `demo` |
| [`clases.puml`](clases.puml) | Clases | Solo lo orientado a objetos (crawler, estado de procesos, modelos del extractor, DTOs Pydantic, excepciones) |
| [`actividad-extraccion-verificacion.puml`](actividad-extraccion-verificacion.puml) | Actividad | Pipeline completo descarga → extracción → revisión → volcado → consulta |
| [`secuencia-inicio-sesion.puml`](secuencia-inicio-sesion.puml) | Secuencia | CU-02 |
| [`secuencia-registro.puml`](secuencia-registro.puml) | Secuencia | CU-01 + aprobación (CU-16, `<<extend>>` de CU-14) |
| [`secuencia-descarga.puml`](secuencia-descarga.puml) | Secuencia | CU-05 |
| [`secuencia-extraccion.puml`](secuencia-extraccion.puml) | Secuencia | CU-07 (con etapa de IA opcional) |
| [`secuencia-revision-manual.puml`](secuencia-revision-manual.puml) | Secuencia | CU-08 |
| [`secuencia-revision-ia.puml`](secuencia-revision-ia.puml) | Secuencia | CU-09 |
| [`secuencia-volcado-bd.puml`](secuencia-volcado-bd.puml) | Secuencia | CU-11 (transaccional) |
| [`secuencia-consulta-horarios.puml`](secuencia-consulta-horarios.puml) | Secuencia | CU-12 + CU-13 (solapes en cliente) |
| [`secuencia-gestion-usuarios.puml`](secuencia-gestion-usuarios.puml) | Secuencia | CU-14 (consultar) y sus extensiones CU-15/CU-16/CU-17 (alta/modificar/baja) |
| [`trazabilidad-casos-de-uso.md`](trazabilidad-casos-de-uso.md) | Tabla | CU ↔ requisitos funcionales (RF-*) |

---

## 2. Cómo compilar

En este entorno **no hay PlantUML ni Java instalados**, así que los `.puml`
no se han podido renderizar aquí. Opciones:

- **VS Code**: extensión *PlantUML* (jebbs) → abrir el `.puml` y `Alt+D`.
- **Servidor web**: pegar el contenido en <https://www.plantuml.com/plantuml>.
- **Local** (requiere Java + Graphviz para todo lo que no sea secuencia):
  ```
  java -jar plantuml.jar -tsvg docs/uml/*.puml
  ```

Los ficheros usan sólo sintaxis PlantUML estándar (`@startuml/@enduml`,
`rectangle`, `usecase`, `actor`, `participant`, `alt/opt/loop/par/break/group`,
`note`, `entity`, `component`, `package`). Se ha revisado el equilibrado de
todos los bloques.

---

## 3. Actores identificados

| Actor | Tipo | Evidencia en código |
|---|---|---|
| **Visitante** (sin sesión) | Humano | `/login` y `/registro` no comprueban sesión; el resto de vistas redirige a `/login` (`routes.py`) |
| **Usuario** (autenticado) | Humano | `session["user"|"user_id"|"is_admin"]`; toda vista operativa exige sesión |
| **Administrador** | Humano — `Administrador --\|> Usuario` | `usuarios.is_admin`; `routes.py::_require_admin`; único recurso restringido: `/usuarios` y `/users*` |
| **Fuente web de horarios** | Sistema externo (secundario) | `crawler/crawler.py::PdfCrawler` hace `requests.get` sobre la URL indicada y descarga PDF |
| **Servicio de IA local (Ollama)** | Sistema externo (secundario) | `llm_review.py` (`ollama.Client(...).chat`), `main.py::ai_status` (`GET {host}/api/tags`) |

**PostgreSQL no es actor**: es infraestructura interna del `docker-compose`
(aparece en el diagrama de despliegue). No se ha encontrado evidencia de
ningún otro actor externo (sin LDAP/SSO, colas, webhooks ni clientes
automatizados distintos del propio frontend).

---

## 4. Casos de uso identificados

Revisión (2ª vuelta): tres "casos de uso" que en la primera versión eran un
único óvalo genérico (revisión, horarios, usuarios) se han separado en una
**base** (funciona sola, es un objetivo completo) y una o varias
**extensiones `<<extend>>`** (condicionales: solo ocurren si el actor
decide actuar sobre algo concreto). Detalle del porqué en
[`auditoria-acciones-ui.md`](auditoria-acciones-ui.md).

| ID | Caso de uso | Actor principal | Relación | Endpoint(s) clave |
|---|---|---|---|---|
| CU-01 | Registrarse | Visitante | — | `POST /register` |
| CU-02 | Iniciar sesión | Visitante | — | `POST /login` |
| CU-03 | Cerrar sesión | Usuario | — | `GET /logout` (Flask) |
| CU-04 | Cambiar mi contraseña | Usuario | — | `POST /account/password` |
| CU-05 | Descargar horarios desde una fuente web | Usuario / Fuente web | — | `POST /download/start` (+ pause/resume/status) |
| CU-06 | Consultar documentos descargados | Usuario | — | `GET /download/status`, `GET /download/file/{filename}` |
| CU-07 | Extraer y normalizar los horarios de los PDF | Usuario | — | `POST /extract/start`, `GET /extract/status` |
| CU-08 | Consultar y revisar los horarios extraídos | Usuario | **base** | `GET /review/{summary,filters,tree,records,records/{id}}` |
| CU-08X | Corregir un registro extraído | Usuario | `<<extend>>` de CU-08 | `PUT /review/records/{id}`, `POST .../status,/restore,/duplicate` |
| CU-09 | Resolver con IA las asignaturas mezcladas | Usuario / Ollama | `<<extend>>` de CU-07 **y** de CU-08X | `POST /review/llm-review` (+ status/pause/resume/cancel) |
| CU-10 | Configurar y verificar el servicio de IA | Usuario / Ollama | — | `GET/PUT /ai/settings`, `GET /ai/status` |
| CU-11 | Volcar los horarios revisados a la base de datos | Usuario | — | `POST /db-dump/start` (+ status/cancel) |
| CU-12 | Consultar horarios | Usuario | **base** | `GET /schedule/degrees`, `GET /schedule/subjects` |
| CU-12X | Construir el horario semanal | Usuario | `<<extend>>` de CU-12 | *(cliente: `horarios.js`)* |
| CU-13 | Detectar solapamientos | Usuario | `<<extend>>` de CU-12X | *(sin endpoint — cliente)* |
| CU-14 | Consultar usuarios | Administrador | **base** | `GET /users` |
| CU-15 | Dar de alta un usuario | Administrador | `<<extend>>` de CU-14 | `POST /users` |
| CU-16 | Modificar un usuario (rol, contraseña, aprobación/veto) | Administrador | `<<extend>>` de CU-14 | `PUT /users/{id}/{role,password,active}` |
| CU-17 | Dar de baja un usuario | Administrador | `<<extend>>` de CU-14 | `DELETE /users/{id}` |

---

## 5. Relaciones `<<include>>` encontradas

**Ninguna.** No se ha encontrado ningún comportamiento que **siempre** se
ejecute como parte de varios casos de uso y que tenga sentido modelar como
caso de uso reutilizable.

---

## 6. Relaciones `<<extend>>` encontradas

Criterio aplicado de forma sistemática (y por igual en las tres zonas donde
aparece): una extensión se dibuja solo si (a) depende de una decisión
condicional del actor — no ocurre siempre — y (b) el caso base es un
objetivo completo sin ella.

| Extensión | Caso base | Condición | Evidencia |
|---|---|---|---|
| **CU-08X Corregir un registro extraído** | **CU-08 Consultar y revisar…** | el usuario elige actuar sobre un registro concreto (editar / marcar estado / restaurar / duplicar) | `main.py` `PUT/POST /review/records/{id}...`; la consulta (resumen, árbol, filtros, tabla, detalle) es plenamente funcional sin editar nada |
| **CU-09 Resolver con IA las asignaturas mezcladas** | **CU-07 Extraer y normalizar…** | "revisión con IA activada en la extracción" (casilla) | `main.py::start_extraction` (`enable_llm_review`); `run_extraction` solo ejecuta `_run_llm_review_stage` `if enabled` |
| **CU-09 Resolver con IA las asignaturas mezcladas** | **CU-08X Corregir un registro extraído** | el usuario pulsa "Resolver con IA" (uno o todos) sobre registros ya abiertos para corregir | `review.js` (`llmReviewOneBtn`/`llmReviewAllBtn`) → `POST /review/llm-review` |
| **CU-12X Construir el horario semanal** | **CU-12 Consultar horarios** | el usuario añade una asignatura a la selección | `horarios.js::addSelectedSubject`; la consulta de titulaciones/asignaturas es completa sin construir nada |
| **CU-13 Detectar solapamientos** | **CU-12X Construir el horario semanal** | las asignaturas añadidas coinciden en día y franja | `horarios.js::getConflictPairs`/`renderConflicts`, en el cliente |
| **CU-15 Dar de alta un usuario** | **CU-14 Consultar usuarios** | el administrador pulsa "Crear usuario" | `main.py::users_create` → `auth/users.py::create_user`; la lista de usuarios es consultable sin dar de alta a nadie |
| **CU-16 Modificar un usuario** | **CU-14 Consultar usuarios** | el administrador actúa sobre una fila existente (rol / contraseña / aprobar-vetar) | `main.py::users_set_role`/`users_set_password`/`users_set_active` |
| **CU-17 Dar de baja un usuario** | **CU-14 Consultar usuarios** | el administrador pulsa "Eliminar" | `main.py::users_delete` → `auth/users.py::delete_user` |

Nótese que **CU-09 extiende dos bases distintas** (CU-07 y CU-08X): el
código ofrece la misma capacidad de IA desde dos puntos de entrada
diferentes (la extracción automática y la revisión manual a demanda), y
ambos son igualmente reales en el código.

---

## 7. Generalizaciones encontradas

| Generalización | Justificación en código |
|---|---|
| **`Administrador --\|> Usuario`** (actor) | Un administrador es una fila de `usuarios` con `is_admin=true`; ejecuta todos los CU del usuario y, además, CU-14 (y sus extensiones). `routes.py::_require_admin` es lo único que los distingue. Se dibuja como generalización para no duplicar asociaciones. |

- **Visitante** y **Usuario** son actores distintos por *estado de sesión*,
  no una generalización.
- **No se ha encontrado ninguna generalización entre casos de uso** con
  evidencia suficiente.

---

## 8. Dónde deliberadamente NO se usa include/extend (y por qué)

| Situación | Por qué no es include/extend |
|---|---|
| "Iniciar sesión" respecto a los demás CU | Es una **precondición** (middleware de sesión de Flask), no un subflujo que los CU invoquen. Se documenta como nota. |
| Extracción → revisión → volcado | **Secuencia temporal** (cada etapa lee el fichero que dejó la anterior), no inclusión. |
| Pausar / reanudar / cancelar / estado de los procesos largos | No dependen de una elección del actor sobre **qué** hacer, sino de controlar un proceso ya en marcha; van en la **secuencia** del CU, no como CU ni como extend. |
| Filtros, árbol, buscador y paginación de CU-08 y CU-12 | Se valoraron como candidatas a `<<extend>>` (son opcionales), pero no **añaden un resultado observable nuevo** — solo acotan qué se ve del mismo resultado. Un `<<extend>>` debe aportar comportamiento adicional, no solo una vista parcial. Se mantienen como flujo interno de la consulta. |
| "Aprobar cuenta" respecto a "Registrarse" | Distinto actor (Administrador) y distinto momento; no es el mismo actor decidiendo una opción dentro de su propia interacción (que es lo que pide `<<extend>>`), sino un evento de negocio posterior. Se refleja con una nota entre CU-01 y CU-16, no con una relación formal. |
| Editar / marcar estado / restaurar / duplicar entre sí (dentro de CU-08X) | Son **flujos alternativos de una misma extensión** (formas distintas de "corregir"), no extensiones unas de otras. |
| Clave interna `X-Internal-Key` | Mecanismo transversal de seguridad; aparece en las **secuencias**, no como CU. |
| Validación de URL, *timeout* de Ollama, escritura atómica, hash SHA-256 | Detalles de implementación; se describen en la secuencia del CU. |

> **Corrección respecto a la 1ª versión:** en la primera pasada, "Revisar y
> corregir…", "Consultar horarios" y "Gestionar usuarios" eran tres óvalos
> únicos que mezclaban una consulta de solo lectura con acciones
> condicionales de escritura. Se han separado en base + `<<extend>>`
> (CU-08/CU-08X, CU-12/CU-12X, CU-14/CU-15/CU-16/CU-17) porque, revisando
> el código con más detalle, esas acciones de escritura sí cumplen el
> criterio de `<<extend>>`: son condicionales y el caso base funciona
> perfectamente sin ellas. Detalle en
> [`auditoria-acciones-ui.md`](auditoria-acciones-ui.md).

---

## 9. Correspondencia casos de uso ↔ requisitos funcionales

Tabla completa en [`trazabilidad-casos-de-uso.md`](trazabilidad-casos-de-uso.md).
Resumen:

| CU | Familias RF |
|---|---|
| CU-01, CU-02, CU-03, CU-04, CU-14, CU-15, CU-16, CU-17 | **RF-AUT** |
| CU-05, CU-06 | **RF-DES** |
| CU-07 | **RF-EXT** (+ RF-EXT-08 compartido con CU-09) |
| CU-08, CU-08X | **RF-REV** |
| CU-09 | **RF-IA** (+ RF-EXT-08) |
| CU-10 | **RF-CFG** |
| CU-11 | **RF-BD** |
| CU-12, CU-12X, CU-13 | **RF-CON** |
| (transversal, sin CU) | **RF-SOP** |

---

## 10. Discrepancias entre requisitos/documentación y código

| Discrepancia | Detalle |
|---|---|
| Botón mal etiquetado | En el panel de descargas, **"Volcar a base de datos"** llama a `/dump-db` → `/extract/start`: **lanza la extracción**, no el volcado. El volcado real está en la página `/volcado`. |
| README de raíz vs implementación | El README menciona ORM (SQLAlchemy) — no existe, se usa `psycopg2` directo — y "análisis de compatibilidad" en servidor — la detección de solapes es 100 % JavaScript en el cliente. |
| Perfil "estudiante" | El README lo cita; el código **no** distingue ese perfil: solo hay `is_admin`. |
| Migraciones fuera del arranque | `db/003_*` y `db/004_*` **no** están en `docker-entrypoint-initdb.d`; se aplican a mano. Sólo `db/init.sql` (tabla `demo`) se ejecuta al inicializar el contenedor. |

---

## 11. Funcionalidades presentes en el código pero no (o poco) documentadas

| Elemento | Estado |
|---|---|
| `GET /demo` + tabla `demo` | Andamiaje inicial; no accesible desde la interfaz. |
| Auto-creación de `admin/admin` si `usuarios` está vacía, y auto-promoción del primer usuario a administrador | En `auth/users.py::ensure_admin_exists` / `authenticate`. Es un mecanismo de arranque no descrito en los requisitos. |
| Re-hash transparente de contraseñas antiguas en claro al iniciar sesión | `auth/users.py::authenticate`. |
| Copia de seguridad automática de `reviewed_schedules.json` (`*.bak.json`) antes de reconstruirlo | `review/store.py::refresh_reviewed_from_extraction`. |
| OpenAPI automática del backend en `/docs`, `/redoc`, `/openapi.json` | FastAPI por defecto; no documentado como funcionalidad. |
| `CORSMiddleware(allow_origins=["*"])` | Permite el consumo de la API desde cualquier origen. |
| `pythonPruebas/` (prototipos `pdf_extractorV4/V5`, paquete `schedule_parser` con sus tests) | Código anterior, no importado por el sistema en ejecución. |

---

## 12. Requisitos documentados que no parecen completamente implementados

| Requisito / expectativa | Estado real |
|---|---|
| Autenticación de toda la API | **Parcial**: sólo `/login`, `/register`, `/account/*` y `/users*` exigen la clave interna; el resto de endpoints del backend (`:8000`) no comprueban nada. |
| Gestión de universidades / titulaciones / planes / asignaturas | **No implementada como CRUD**: los catálogos sólo los crea el volcado. `study_plans` y campos como `ects`, `subject_type`, `building`, `campus` **nunca se rellenan**. |
| Trazabilidad de la fuente (`sources.file_hash`, `source_url`, `generated_at`) | **Parcial**: el volcado crea filas en `sources` con `file_name` y `academic_year_id`, pero no rellena `file_hash` ni `source_url`. |
| Secretos fuera del repositorio | **No cumplido**: `SECRET_KEY` fija en `app/__init__.py`; credenciales de PostgreSQL e `INTERNAL_API_KEY` con valor por defecto en `docker-compose.yml`; sin HTTPS ni límite de intentos de acceso. |
| Pruebas automatizadas | **Parcial**: sólo el paquete `pdf_table_extractor` tiene tests; `frontend-flask/tests/test_routes.py` está obsoleto (espera una respuesta JSON en `/` que ya no existe). |

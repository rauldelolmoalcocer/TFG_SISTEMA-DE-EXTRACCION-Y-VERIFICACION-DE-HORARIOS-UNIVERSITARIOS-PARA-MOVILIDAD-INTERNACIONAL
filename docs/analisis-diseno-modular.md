# Análisis técnico para el apartado 11.2 (Diseño modular)

> Documento de trabajo, NO es texto de memoria. Sirve de base para que
> otra persona redacte el apartado 11.2. Cada afirmación está respaldada
> por ficheros concretos del repositorio, citados entre paréntesis.

---

## 1. Estructura general del proyecto

El repositorio separa físicamente tres grandes bloques:

- **`frontend-flask/`** — interfaz web (Flask) y proxy HTTP hacia el backend.
- **`backend-fastapi/`** — toda la lógica (FastAPI), organizada en
  subpaquetes por responsabilidad dentro de `app/`: `crawler/`,
  `extractor/` (que a su vez contiene el paquete instalable
  `pdf_table_extractor`), `review/`, `dbdump/`, `schedule/`, `auth/`, y el
  módulo suelto `ai_settings.py`.
- **`db/`** — esquema SQL y migraciones.

Comprobación de la lista de responsabilidades propuesta, una por una:

| Responsabilidad | ¿Existe como módulo propio? | Dónde |
|---|---|---|
| Frontend | Sí | `frontend-flask/app/` |
| Backend / API | Sí | `backend-fastapi/app/main.py` |
| Acceso a base de datos | **No como capa única** — cada módulo de negocio (`auth`, `schedule`, `dbdump`) hace sus propias consultas `psycopg2` directas. No hay repositorio ni ORM compartido. | `auth/users.py`, `schedule/db.py`, `dbdump/loader.py` |
| Procesamiento de PDF | Sí, y dividido en dos fases físicamente distintas (ver §6-7) | `pdf_table_extractor/extractor/`, `.../diagnostics/` |
| Descarga de documentos | Sí | `crawler/crawler.py` |
| Localización de documentos | **No es un módulo separado** — está fundida con la descarga en la misma clase (ver §4) | `crawler/crawler.py` |
| Extracción de información | Sí | `pdf_table_extractor/extractor/`, `diagnostics/analyzer.py` |
| Reconstrucción de horarios | Sí, es el módulo más elaborado del sistema | `pdf_table_extractor/normalizer/schedule_builder.py` |
| Normalización | **No es un módulo único** — ocurre en dos sitios distintos y con objetivos distintos (ver §8) | `normalizer/{cell_parser,time_parser,header_parser}.py` **y**, más tarde, `dbdump/loader.py` |
| Validación | **No es un módulo único** — hay validación estructural en la reconstrucción y validación semántica en la revisión (ver §9) | `normalizer/schedule_builder.py` + `review/store.py` |
| Revisión manual | Sí | `review/store.py` + `templates/review.html` + `static/review.js` |
| Persistencia | Sí (el volcado) | `dbdump/loader.py` |
| Consulta de información | Sí | `schedule/db.py` |
| Autenticación y usuarios | Sí | `auth/users.py` |
| Integración con IA | Sí, y es opcional/auxiliar en dos puntos distintos | `pdf_table_extractor/llm_review.py`, `review/store.py::llm_review_items`, `ai_settings.py` |
| Configuración | Sí, pero limitada a la IA — no hay un módulo de configuración general | `ai_settings.py` |

**Código antiguo detectado:** `pythonPruebas/` contiene una implementación anterior completa del mismo problema (`pdf_extractorV4.py`, `python_pdf_extractorV5_normalized.py`, y un paquete propio `schedule_parser/` con `pdf_processor.py`, `subject_parser.py`, `validator.py`, `normalizer.py`, etc., más sus tests). **No lo importa nada del sistema en ejecución** (comprobado: ningún fichero de `backend-fastapi/` ni `frontend-flask/` referencia `pythonPruebas`). Es la única implementación anterior de "validator" o "normalizer" independientes que existe en el repo; la actual (`pdf_table_extractor`) no tiene ningún fichero llamado así.

---

## 2. Módulos reales del sistema

### 2.1 Frontend (interfaz + pasarela HTTP)

- **Archivos**: `frontend-flask/app/routes.py` (único Blueprint), `app/templates/*.html`, `app/static/*.js`, `app/__init__.py`, `run.py`.
- **Responsabilidad**: renderizar las páginas (Jinja2) y actuar como intermediario HTTP hacia el backend. No contiene lógica de negocio: valida como mucho la sesión de Flask antes de reenviar.
- **Entrada**: peticiones HTTP del navegador (formularios, `fetch()` desde el JS de cada página).
- **Procesamiento**: comprobar sesión (`session["user"]`), añadir la cabecera interna `X-Internal-Key` en las llamadas de autenticación/usuarios, reenviar con la librería `requests` al backend, devolver la respuesta (JSON o HTML renderizado).
- **Salida**: HTML al navegador, o JSON reenviado tal cual desde el backend.
- **Comunicación**: con el **Backend** exclusivamente por HTTP (`BACKEND_URL = "http://backend:8000"`, `routes.py:7`). No accede nunca directamente a PostgreSQL ni a Ollama (comprobado: no hay `import psycopg2` ni `ollama` en `frontend-flask/`).

### 2.2 Backend — orquestación de la API

- **Archivos**: `backend-fastapi/app/main.py`.
- **Responsabilidad**: define todos los endpoints HTTP, mantiene el estado en memoria de los 4 procesos largos (descarga, extracción, revisión IA, volcado) y delega el trabajo real en los módulos de los apartados siguientes. Es el único punto que conoce a todos los demás módulos a la vez.
- **Entrada**: peticiones HTTP del frontend.
- **Procesamiento**: validar el cuerpo de la petición (modelos Pydantic), lanzar hilos (`threading.Thread`) para los procesos largos, invocar la función del módulo correspondiente.
- **Salida**: JSON de respuesta / disparo de un hilo en segundo plano.
- **Comunicación**: con `crawler`, `pdf_extractor` (que a su vez usa `pdf_table_extractor`), `review.store`, `dbdump.loader`, `schedule.db`, `auth.users`, `ai_settings` — todos importados directamente como funciones Python, no por red.

### 2.3 Localización + Descarga (crawler)

- **Archivos**: `backend-fastapi/app/crawler/crawler.py` (clases `PdfCrawler`, `CrawlerConfig`, `CrawlerState`); orquestado desde `main.py::real_download_process`.
- **Responsabilidad**: dado un punto de partida (una URL), encontrar enlaces a PDF en esa página y descargarlos. **Localización y descarga están implementadas en la misma clase y no se pueden separar**: `PdfCrawler.run()` hace ambas cosas en el mismo bucle (ver §4 y §5 para el detalle).
- **Entrada**: una URL (tecleada por el usuario) + configuración fija (`CrawlerConfig`: profundidad, nº máx. de páginas, workers, timeout...).
- **Procesamiento**: petición HTTP a la URL, parseo del HTML con BeautifulSoup, filtrado de enlaces `.pdf`, descarga en paralelo (`ThreadPoolExecutor`), deduplicación por URL y por hash SHA-256 del contenido.
- **Salida**: ficheros PDF en el sistema de ficheros (`./downloads`, montado como `/data/downloaded_pdfs`).
- **Comunicación**: con la **fuente web externa** (HTTP saliente); no se comunica con ningún otro módulo del sistema salvo para reportar su estado (`CrawlerState`) a `main.py`.

### 2.4 Extracción / diagnóstico estructural de PDF

- **Archivos**: `pdf_table_extractor/extractor/pdf_reader.py`, `word_extractor.py`, `table_detector.py`, `strategies.py`; `pdf_table_extractor/diagnostics/analyzer.py` (`analyze_pdf`), `report.py`.
- **Responsabilidad**: leer un PDF y producir una descripción **estructural y agnóstica** de su contenido — nunca interpreta qué significa el contenido, solo qué forma tiene (dónde hay palabras, líneas, rectángulos, tablas candidatas).
- **Entrada**: un fichero PDF.
- **Procesamiento**: por cada página, extrae palabras con coordenadas (`extract_words`), líneas y rectángulos (`extract_lines_and_rects`), y ejecuta **tres estrategias de detección de tablas en paralelo** sobre la misma página (ver §6), puntuando cuál es la mejor candidata de forma genérica (caracteres medios por celda, regularidad de filas).
- **Salida**: un `PdfAnalysis` con, por página, las tablas detectadas por cada estrategia y cuál se considera la mejor (`best_strategy`); se escribe a disco como `metadata.json`, `words.json`, `tables_<estrategia>.json`, `diagnostic.json`.
- **Comunicación**: no llama a ningún otro módulo; es una etapa puramente de lectura de fichero. Su salida en disco es la entrada de la reconstrucción (§2.5).

### 2.5 Reconstrucción de horarios

- **Archivos**: `pdf_table_extractor/normalizer/schedule_builder.py` (`build_entries_for_pdf`), apoyado en `cell_parser.py`, `time_parser.py`, `header_parser.py`.
- **Responsabilidad**: es el módulo que **de verdad entiende que está mirando un horario**. Coge la tabla "ganadora" de cada página (de la etapa anterior) y decide si de verdad es una rejilla de horario (fila 0 = días, columna 0 = horas), y si lo es, la recorre celda a celda para producir un registro por cada combinación (día, franja horaria, asignatura).
- **Entrada**: `diagnostic.json` + `tables_<estrategia>.json` + `metadata.json` de un PDF ya diagnosticado.
- **Procesamiento** (ver detalle en §7): comprobar que la fila 0 son nombres de día (`_looks_like_day_header`) y la columna 0 son horas (`_looks_like_schedule_grid`); agrupar filas físicas en "bloques de tiempo" (una franja puede ocupar varias filas de la rejilla real, `_time_blocks`); fusionar bloques consecutivos con el mismo texto (una clase que ocupa varias franjas seguidas); extraer del texto de cada página el grado/grupo/semestre/curso (`header_parser`); y, si una tabla no tiene su propia fila de días (paginación de una rejilla en dos páginas), heredar la de la página anterior (`_try_inherit_header`).
- **Salida**: una lista de `ScheduleEntry` (un dataclass con día, horas, asignatura, aulas, avisos...) y una lista de tablas descartadas (`skipped`), cada una con el motivo del descarte.
- **Comunicación**: usa internamente `cell_parser` (separar aula/asignatura del texto de la celda) y `time_parser` (convertir el texto de la hora a `HH:MM`); no se comunica con nada fuera del propio paquete.

### 2.6 Orquestación de la extracción (las 3 etapas)

- **Archivos**: `backend-fastapi/app/extractor/pdf_extractor.py` (`run_extraction`, `ExtractorState`).
- **Responsabilidad**: coordinar en orden las tres etapas — diagnóstico (§2.4), normalización/reconstrucción (§2.5) y, opcionalmente, revisión con IA (§2.8) — e informar del progreso.
- **Entrada**: carpeta con los PDF descargados + flag `enable_llm_review`.
- **Procesamiento**: llama a `analyze_pdf` por cada PDF, luego a `run_normalization` (que internamente llama a `build_entries_for_pdf` por cada PDF), y si procede a `review_entries` (IA); escribe `extraction_run.json` con el resumen; llama a `review.store.refresh_reviewed_from_extraction` para generar la copia revisable.
- **Salida**: `all_schedules.json`, `skipped_tables.json`, `extraction_run.json`, y (vía `review.store`) `reviewed_schedules.json`.
- **Comunicación**: con `pdf_table_extractor` (paquete completo) y con `review.store` (al final, para construir la copia revisable).

### 2.7 Revisión (estado + edición manual)

- **Archivos**: `backend-fastapi/app/review/store.py`.
- **Responsabilidad**: doble — (a) calcular y mantener el **estado de validación** de cada registro (VALID/WARNING/INVALID) y (b) ofrecer las operaciones de **revisión manual** (editar, cambiar estado, restaurar, duplicar) sobre una copia independiente del resultado de la extracción.
- **Entrada**: `all_schedules.json` + `skipped_tables.json` (para construir la copia inicial) o `reviewed_schedules.json` ya existente (para las operaciones siguientes) + las peticiones de edición del usuario.
- **Procesamiento**: `_compute_entry_status` (ver §9); `apply_edit`/`set_status`/`restore_item`/`duplicate_item`; filtrado, agrupación en árbol y paginación para la pantalla de revisión; escritura atómica del fichero.
- **Salida**: `reviewed_schedules.json` actualizado; esta es la única fuente que lee el volcado a BD.
- **Comunicación**: con el **Backend** (invocado desde `main.py`); internamente puede invocar la **revisión con IA** (§2.8) para uno o varios registros.

### 2.8 Revisión asistida por IA

- **Archivos**: `pdf_table_extractor/llm_review.py` (`review_entries`); `review/store.py::llm_review_items` (adaptador que lo reutiliza sobre `reviewed_schedules.json`); también invocado dentro de `pdf_extractor.py::_run_llm_review_stage` durante la extracción.
- **Responsabilidad**: pedir a un modelo de lenguaje local que decida si una celda marcada como "sospechosa de mezclar asignaturas" es en realidad una sola asignatura con una anotación rara, o dos asignaturas distintas que hay que separar.
- **Entrada**: el texto crudo de la celda + las aulas/anotaciones ya detectadas por `cell_parser`. **Nunca se le envía el PDF ni conocimiento externo sobre titulaciones.**
- **Procesamiento**: construye un lote pequeño (6 celdas), llama a Ollama (`ollama.Client.chat`, `format="json"`), valida la respuesta con Pydantic, reintenta una vez si el formato es inválido.
- **Salida**: para cada celda, o bien la misma entrada resuelta (nombre limpio + aula) o varias entradas nuevas (una por asignatura separada), cada una con su nivel de confianza.
- **Comunicación**: con **Ollama** por HTTP; con `review.store` (que aplica el resultado a `reviewed_schedules.json`) o con la orquestación de extracción (que lo aplica a las entradas antes de que existan como JSON revisable).
- **Es opcional en ambos puntos donde aparece**: en la extracción, solo se ejecuta si el usuario marca la casilla; en la revisión, solo si el usuario pulsa "Resolver con IA". El sistema completo funciona sin este módulo.

### 2.9 Configuración del servicio de IA

- **Archivos**: `backend-fastapi/app/ai_settings.py`.
- **Responsabilidad**: guardar y recuperar la dirección de Ollama y el modelo a usar, persistidos en un fichero (`ai_settings.json`) para poder cambiarlos sin reiniciar el contenedor.
- **Entrada**: nueva dirección/modelo (desde el panel de administración) o, por defecto, la variable de entorno `OLLAMA_HOST`.
- **Procesamiento**: validar formato (`http://` o `https://`), leer/escribir el JSON.
- **Salida**: `{ollama_host, model}`, consumido por §2.6 y §2.8 en cada ejecución.
- **Comunicación**: ninguna con Ollama directamente — solo guarda el dato; quien llama a Ollama son los módulos de IA.

### 2.10 Persistencia (volcado a PostgreSQL)

- **Archivos**: `backend-fastapi/app/dbdump/loader.py` (`run_dump`).
- **Responsabilidad**: convertir `reviewed_schedules.json` en filas de PostgreSQL. Es el único módulo que escribe en las tablas de horario.
- **Entrada**: `reviewed_schedules.json` (filtrando solo `VALID`/`WARNING`).
- **Procesamiento**: `TRUNCATE` de las tablas de horario, normalización de valores (ver §8), inserción de catálogos con caché en memoria y de las sesiones, todo en una única transacción.
- **Salida**: filas en `universities, degrees, academic_years, groups, subjects, rooms, sources, class_sessions, class_session_rooms, extraction_metadata, extraction_issues`.
- **Comunicación**: con **PostgreSQL** directamente (`psycopg2`, sin ORM); con el sistema de ficheros para leer el JSON de origen.

### 2.11 Consulta de horarios

- **Archivos**: `backend-fastapi/app/schedule/db.py` (`list_degrees`, `list_subjects`).
- **Responsabilidad**: leer de PostgreSQL lo que ya se ha volcado y devolverlo condensado (por asignatura + grupo) para pintar la rejilla semanal.
- **Entrada**: `degree_id` opcional.
- **Procesamiento**: `SELECT` con `JOIN` sobre `class_sessions`, agregación en Python para agrupar sesiones de la misma asignatura/grupo.
- **Salida**: lista de titulaciones o de (asignatura + grupo + sus sesiones).
- **Comunicación**: con **PostgreSQL** directamente; con el **Backend**, que expone esto como endpoint; **no se comunica con el frontend directamente** — el frontend solo consume el endpoint HTTP.

### 2.12 Autenticación y usuarios

- **Archivos**: `backend-fastapi/app/auth/users.py`.
- **Responsabilidad**: validar credenciales, gestionar altas/bajas/roles/aprobación de cuentas, hash de contraseñas.
- **Entrada**: credenciales o datos de usuario desde el backend.
- **Procesamiento**: `werkzeug.security` para el hash, `psycopg2` directo sobre la tabla `usuarios`, reglas de negocio (no quedarse sin administrador, no auto-eliminarse).
- **Salida**: usuario autenticado / usuario creado o modificado.
- **Comunicación**: con **PostgreSQL** directamente; con el **Backend**, que además añade la protección de la clave interna (`X-Internal-Key`) delante de estos endpoints.

### 2.13 Modelo de datos (no es código, pero es un "módulo" de diseño)

- **Archivos**: `db/createdatabase.sql`, `db/003_usuarios_auth.sql`, `db/004_usuarios_is_active.sql`.
- Define el esquema que consumen §2.10, §2.11 y §2.12. No contiene lógica, solo estructura y restricciones (claves foráneas, valores por defecto, índices).

---

## 3. Flujo completo de procesamiento

El flujo propuesto en el enunciado se confirma **casi exactamente**, con dos matices importantes: (a) "extracción" y "reconstrucción" son dos etapas bien diferenciadas en el código (diagnóstico agnóstico vs. interpretación como horario) pero ambas ocurren dentro de la misma llamada a "extracción" desde el punto de vista del usuario; y (b) la IA no es una etapa fija del flujo, es opcional y aparece en dos puntos distintos.

```
Localización + Descarga   (crawler.py, una sola clase)
        ↓  (ficheros PDF en ./downloads)
Diagnóstico / Extracción estructural   (pdf_table_extractor.diagnostics + .extractor)
        ↓  (diagnostic.json + tables_*.json + metadata.json, en ./extracted/<pdf>/)
Reconstrucción del horario   (pdf_table_extractor.normalizer.schedule_builder)
        ↓  (all_schedules.json + skipped_tables.json)
   [IA opcional, si la casilla estaba marcada]   (pdf_table_extractor.llm_review)
        ↓
Clasificación de estado (VALID/WARNING/INVALID)   (review.store, al construir reviewed_schedules.json)
        ↓  (reviewed_schedules.json)
Revisión manual   (review.store + pantalla de revisión)
        ↓  (reviewed_schedules.json actualizado)
   [IA bajo demanda, si el usuario lo pide]   (pdf_table_extractor.llm_review, vía review.store)
        ↓
Persistencia / volcado   (dbdump.loader)
        ↓  (filas en PostgreSQL)
Consulta   (schedule.db)
```

Cada flecha es una entrega de información **por fichero** (JSON en disco) hasta la persistencia, y **por base de datos** a partir de ahí — en ningún punto anterior al volcado se usa PostgreSQL.

---

## 4. Localización de documentos

**No existe un módulo de "localización" separado de la descarga.** Todo está en `PdfCrawler` (`crawler/crawler.py`):

- La "localización" consiste en: partir de la URL que da el usuario, descargar esa página, y con BeautifulSoup extraer todos los `<a href>`; de esos enlaces, quedarse con los que terminan en `.pdf` (`is_pdf_url`) o cuya respuesta HTTP declara `Content-Type: application/pdf`.
- Si la URL de partida **ya es** un PDF, se descubre igual (se comprueba antes de intentar parsear HTML).
- No hay ningún paso de "búsqueda" más allá de esa única página: el crawler **no sigue enlaces a otras páginas HTML** aunque la configuración (`max_depth`, `max_pages`) sugiera que sí — el bucle de `run()` solo encola páginas para el rastreo inicial y nunca vuelve a meter en la cola un enlace que no sea un PDF. En la práctica, "localizar" == "leer los enlaces de una única página".
- **Qué se usa para localizar**: la URL tecleada por el usuario, nada más (no hay ningún catálogo ni configuración de "universidades conocidas").
- **Resultado si no se encuentra nada**: no hay ningún error explícito — simplemente `total_pdfs_found` se queda en 0 y el proceso termina normalmente, informándolo en el log (`CrawlerState.logs`).
- **Conclusión**: localización y descarga son la misma responsabilidad, en el mismo módulo, sin separación de código entre ambas.

---

## 5. Descarga de documentos

- **Componente**: `PdfCrawler.download_pdf` (`crawler/crawler.py`), ejecutado en paralelo con un `ThreadPoolExecutor` de 4 workers.
- **Cómo recibe las URLs**: las que ha encontrado el propio paso de localización, dentro de la misma clase (no hay una interfaz entre "aquí están las URLs" y "descárgalas").
- **Dónde se almacenan**: en el sistema de ficheros, carpeta `DOWNLOAD_FOLDER = /data/downloaded_pdfs` (bind-mount a `./downloads`), con nombre de fichero saneado (`build_safe_filename`).
- **Comprobaciones de error**: sí — `response.raise_for_status()` ante fallos HTTP; se comprueba que el `Content-Type` sea realmente PDF antes de guardar; cada descarga individual está en un `try/except` que no aborta el resto del lote.
- **Control de documentos ya existentes**: sí, dos mecanismos combinados — deduplicación por URL ya vista en esta misma ejecución, y deduplicación por **hash SHA-256 del contenido**, comparado tanto contra lo descargado en esta ejecución como contra los PDF que ya hubiera en la carpeta antes de empezar (`_index_existing_files`).
- **Qué devuelve tras descargar**: no hay un valor de retorno consumido por otro módulo — el resultado se refleja en `CrawlerState` (contadores, lista de rutas guardadas, logs) que el backend expone vía `GET /download/status`. La siguiente etapa (extracción) no recibe una lista de ficheros directamente: **relee la carpeta de descargas por su cuenta** cuando se lanza.

---

## 6. Procesamiento y extracción de PDF

**Confirmado: se usa únicamente `pdfplumber`.** `PyMuPDF` aparece en `backend-fastapi/requirements.txt` pero **no se importa en ningún fichero activo** del proyecto (comprobado con búsqueda de `import fitz` / `pymupdf` en `backend-fastapi/` completo: cero resultados). Sí se usa PyMuPDF en los prototipos antiguos de `pythonPruebas/` (`pdf_extractorV4.py`, `python_pdf_extractorV5_normalized.py`, `schedule_parser/pdf_processor.py`), que no forman parte del sistema en ejecución. Es razonable interpretar la dependencia `pymupdf` en `requirements.txt` como un resto de una versión anterior del backend, ya no utilizada.

Lo que hace pdfplumber, módulo por módulo:

- **`extractor/pdf_reader.py`**: abre el PDF y extrae los metadatos básicos (nº de páginas, tamaño, texto plano de cada página) — es el único sitio donde se pide "todo el texto de la página" de una vez.
- **`extractor/word_extractor.py`**: extrae **palabras individuales con sus coordenadas** (`x0, x1, top, bottom`) vía `page.extract_words()`. Se guardan como dato diagnóstico, no se usan como regla de posición fija.
- **`extractor/table_detector.py`**: extrae **líneas y rectángulos** (`extract_lines_and_rects`, para detectar rejillas dibujadas) y ejecuta la detección de tablas propiamente dicha (`run_strategy`), con tres configuraciones distintas de pdfplumber (`extractor/strategies.py`):
  - **`lines`**: solo líneas de separación dibujadas.
  - **`text`**: solo alineación de texto (para PDF sin líneas visibles).
  - **`combined`**: líneas en un eje, texto en el otro (para PDF con líneas horizontales pero columnas solo por alineación).
- **`diagnostics/analyzer.py`**: ejecuta **las tres estrategias en cada página** y elige la mejor de forma genérica (caracteres medios por celda, regularidad de filas — nunca mirando el contenido).

**Diferencia entre extracción y procesamiento en este proyecto**: la "extracción" (`extractor/` + `diagnostics/`) es puramente estructural — identifica dónde hay una tabla y qué texto hay en cada celda, sin saber que se trata de un horario. El "procesamiento" que le da sentido a eso (saber que la fila 0 son días, que hay que separar el aula del nombre de la asignatura, etc.) es ya la **reconstrucción** (§7), un paquete distinto (`normalizer/`). La estructura intermedia que separa ambas fases son los ficheros `diagnostic.json`, `tables_<estrategia>.json` y `metadata.json`, escritos a disco entre una fase y otra.

---

## 7. Reconstrucción de horarios

Módulo: `pdf_table_extractor/normalizer/schedule_builder.py`. Es, con diferencia, la lógica más elaborada del proyecto.

**Problema que resuelve**: una tabla detectada por pdfplumber es solo una matriz de celdas de texto; no sabe que la fila 0 son días de la semana, que la columna 0 son horas, ni que una clase puede ocupar varias filas físicas (por una línea decorativa de más) o varias franjas seguidas (una clase de dos horas). `schedule_builder.py` reconstruye esa semántica:

1. Comprueba que la fila 0 contiene nombres de día reales (comparando contra una lista cerrada de nombres de día, sin acentos) — si no, la tabla se descarta como "no es un horario".
2. Comprueba que al menos un 60% de las etiquetas no vacías de la columna 0 se interpretan como una franja horaria — si no, se descarta igualmente.
3. Agrupa filas físicas consecutivas en "bloques de tiempo" (`_time_blocks`), porque una rejilla con líneas más finas que las franjas reales puede partir una clase en 2-3 filas.
4. Para cada columna (día) y cada bloque, junta el texto de las celdas que caen dentro; si varios bloques seguidos tienen el mismo texto, los fusiona en una sola entrada (una clase que dura varias franjas).
5. Extrae del texto de la página el grado, grupo, semestre y curso académico (`header_parser.py`).
6. Si una tabla no tiene fila de días propia (una rejilla que continúa en la página siguiente), **hereda** la fila de días y los datos de cabecera de la página anterior, marcando esas entradas con un aviso para que un humano lo confirme (`_try_inherit_header`).

**Entrada**: la salida en disco de la etapa de extracción (una tabla + el texto plano de la página).
**Salida**: una lista de `ScheduleEntry` (día, hora inicio/fin, asignatura, aulas, grado, grupo, semestre, curso, avisos) y una lista de tablas descartadas con el motivo.

**Estrategias según el tipo de PDF**: no hay una reconstrucción distinta por tipo de PDF — la reconstrucción es **una sola**, pero opera sobre el resultado de la estrategia de detección de tabla que haya ganado en cada página (líneas / texto / combinada, ver §6), que sí varía según cómo esté maquetado el PDF de origen.

---

## 8. Normalización

**No hay un único módulo de normalización — hay dos, con objetivos distintos:**

### 8.1 Normalización de campo individual, durante la reconstrucción

- **`time_parser.py`**: convierte el texto de una franja horaria (con separadores `:`, `.`, `,`, `_`, y rango con `/`, `-`, `–`, `—`, `:`) a un par `"HH:MM"`. Devuelve `None` si no reconoce el patrón — nunca inventa una hora.
- **`cell_parser.py`**: separa, del texto de una celda, el nombre de la asignatura de los códigos de aula entre paréntesis, usando una regla estructural (un paréntesis con un dígito dentro es aula; sin dígito, es una anotación como el nombre de un grupo).
- **`header_parser.py`**: extrae grado/grupo/semestre/curso del texto de la página con expresiones regulares en español ("GRADO", "MÁSTER", "CUATRIMESTRE", "GRUPO").

Esta normalización es **local a cada valor** y ocurre como parte del mismo paso que construye el `ScheduleEntry` — no hay una etapa posterior dedicada solo a "limpiar" lo ya reconstruido dentro de este paquete.

### 8.2 Normalización de catálogo, durante el volcado a base de datos

En `dbdump/loader.py`, justo antes de cada `INSERT`, se vuelve a normalizar — pero con un objetivo distinto: **no volver a parsear el dato**, sino ponerlo en una forma canónica para no duplicar catálogo:

- Día de la semana → capitalización y tildes correctas ("MIÉRCOLES"/"miercoles" → "Miércoles").
- Curso académico `"2026/2027"` → año de inicio y fin como enteros.
- Nombre de asignatura → versión en mayúsculas, sin acentos y con espacios colapsados (`normalized_name`), usada solo para decidir si dos filas son "la misma asignatura" al construir el catálogo — el nombre "bonito" (`name`) no se toca.
- Confianza de la IA (`"high"/"medium"/"low"`) → valor numérico, porque la columna de la base de datos es `numeric`, no texto.

**Diferencia entre reconstrucción y normalización en este proyecto**: la reconstrucción decide **qué estructura tiene** el dato (qué es un día, qué es una hora, dónde empieza la asignatura); la normalización decide **qué forma exacta** debe tener ese dato ya identificado, y ocurre dos veces con propósitos distintos: una vez para poder construir el `ScheduleEntry` (parseo), y otra para poder insertarlo sin duplicar catálogo (canonicalización).

---

## 9. Validación

Tampoco es un módulo único: hay **validación estructural** (durante la reconstrucción) y **validación semántica** (durante la revisión), y son conceptualmente distintas.

### 9.1 Validación estructural — `schedule_builder.py`

Decide si una tabla **es o no** un horario aprovechable. No produce "avisos", produce un descarte binario: la tabla entra en `skipped_tables.json` con un motivo (`too_few_rows`, `no_day_header`, `no_time_column`, `no_strategy_selected`, `table_missing`) o no entra en absoluto en el resultado.

### 9.2 Validación semántica — `review/store.py::_compute_entry_status`

Sobre los registros que sí se han podido construir, decide su estado:

- **`INVALID`**: si `multiple_entries_suspected` es verdadero (la celda parece mezclar dos asignaturas) — se considera que el dato no es fiable, no simplemente dudoso.
- **`WARNING`**: si tiene algún otro aviso (p. ej., no se pudo parsear una hora) pero no mezcla asignaturas.
- **`VALID`**: si no tiene ningún aviso.
- Las tablas descartadas en la etapa estructural (§9.1) se representan también como registros, con estado `INVALID` fijo (`origin: "skipped"`).

**Información incompleta**: no se rechaza — un registro sin asignatura identificada, sin grupo o sin aula simplemente tiene esos campos a `None`/vacío y sigue teniendo un estado (`VALID` o `WARNING` según si eso generó o no un aviso).

**Dónde se guarda el resultado**: en el propio JSON (`reviewed_schedules.json`, campo `status` de cada item) hasta el volcado; a partir de ahí, también en `class_sessions.status` de PostgreSQL (columna de texto, se copia el mismo valor).

**Diferencia entre validación automática y revisión manual**: la automática (§9.1 y §9.2) ocurre sola, sin intervención humana, en el momento de la extracción. La revisión manual (§10) es el proceso por el cual una persona confirma, corrige o descarta lo que la validación automática ha decidido, y puede cambiar el estado a mano en cualquier momento posterior.

---

## 10. Revisión manual

- **Cómo llegan los datos a revisión**: automáticamente, al terminar cada extracción (`refresh_reviewed_from_extraction`, invocado desde `pdf_extractor.py`), se construye o reconstruye `reviewed_schedules.json` a partir de `all_schedules.json` + `skipped_tables.json`. La versión anterior, si existía, se renombra a `.bak.json` en vez de borrarse.
- **Qué puede modificar el usuario**: todos los campos "current" de un registro (grado, curso, semestre, grupo, día, horas, asignatura, aulas, marca de "mezcla sospechosa", notas) mediante edición; también puede forzar directamente el estado (`VALID`/`WARNING`/`INVALID`), restaurar el registro a su valor original, o duplicarlo en dos registros independientes (para separar a mano una celda que mezclaba dos asignaturas).
- **Qué estados pueden cambiar**: cualquier transición entre `VALID`, `WARNING` e `INVALID`, manual o automáticamente recalculada tras una edición (`apply_edit` vuelve a llamar a la misma función de validación semántica del §9.2 tras cada cambio).
- **Qué ocurre después de aceptar o corregir un registro**: se marca `reviewed = true` y, si el contenido difiere del original, `manually_modified = true`; se guarda de forma atómica en el mismo fichero.
- **Cómo vuelve esa información al flujo principal**: no "vuelve" a la extracción — el flujo es de un solo sentido. El fichero `reviewed_schedules.json`, ya con las correcciones, es exactamente lo que lee el volcado (§2.10) cuando el usuario decide persistir.

---

## 11. Persistencia

- **Qué componentes se comunican con la base de datos**: tres, cada uno de forma independiente y sin capa compartida: `dbdump/loader.py` (escritura del horario), `schedule/db.py` (lectura del horario) y `auth/users.py` (usuarios). Los tres usan `psycopg2` directamente, con SQL escrito a mano.
- **¿Existe una capa específica de persistencia?** No, en el sentido de un DAO/repositorio reutilizable. Cada módulo abre su propia conexión (`get_connection()`, definida en `main.py`, se les pasa como parámetro) y escribe sus propias sentencias SQL. No hay ORM (ni SQLAlchemy pese a que lo menciona el README de raíz, ni ningún otro).
- **Qué se almacena**: el horario ya revisado (catálogos + sesiones + metadatos de extracción + incidencias) y, en una tabla completamente aparte y sin relación, los usuarios.
- **En qué momento del flujo se almacena**: solo cuando el usuario pulsa "Volcar a base de datos" — es una acción explícita, no automática, y ocurre después de toda la revisión.
- **Relación con la validación y la revisión**: el volcado **filtra** por el resultado de la validación/revisión (solo entran `VALID` y `WARNING`) pero no vuelve a validar nada por su cuenta; confía en el estado que ya trae cada registro en el JSON.

---

## 12. Consulta de horarios

- **Módulo**: `schedule/db.py`, expuesto por `main.py` en dos endpoints (titulaciones y asignaturas).
- **Filtros principales**: por titulación (`degree_id`); dentro de una titulación, no hay más filtro en el backend — el filtrado por texto (buscar una asignatura) se hace en el propio navegador, sobre la lista ya cargada.
- **Cómo se recuperan asignaturas/horarios/grupos**: una única consulta con varios `JOIN` (`class_sessions` con `subjects`, `degrees`, `groups`, `rooms`) agregada en SQL (`string_agg` para las aulas) y luego condensada en Python agrupando por `(asignatura, grupo)`, de modo que una asignatura con varias sesiones a la semana llega como un único objeto con una lista de sesiones dentro.
- **Qué componente consulta PostgreSQL**: solo `schedule/db.py`.
- **Cómo llega la información al frontend**: `schedule/db.py` → `main.py` (endpoint) → `routes.py` (proxy) → `static/horarios.js` (`fetch`), que además hace en el propio navegador la composición del horario semanal y la detección de solapes — el backend nunca calcula solapes.

---

## 13. Frontend e interfaz

Organización funcional real (sin entrar en estética), por página:

| Página | Qué hace el propio frontend | Qué delega en el backend |
|---|---|---|
| `login.html` / `register.html` | Mostrar el formulario, validar campos vacíos en el propio navegador antes de enviar | Comprobar credenciales, crear la cuenta |
| `index.html` | Mostrar las tarjetas de navegación (ocultando "Usuarios" si no es admin) | Nada — es estática |
| `download_panel.html` + `.js` | Sondear el estado cada 1-2 s y pintar barra de progreso/log/lista de PDF; mostrar el modal de resumen | Iniciar/pausar/reanudar la descarga; iniciar la extracción; servir cada PDF |
| `review.html` + `.js` | Filtrar/paginar/mostrar en cliente lo que ya ha traído del backend; construir el formulario de edición | Absolutamente toda la lógica de negocio: filtrar en servidor, editar, cambiar estado, restaurar, duplicar, lanzar la IA |
| `ai_panel.html` | Mostrar el estado de conexión y la lista de modelos ya consultados | Comprobar la conexión con Ollama, guardar la configuración |
| `db_dump.html` | Sondear el progreso del volcado | Ejecutar el volcado |
| `horarios.html` + `.js` | **Construir el horario semanal y detectar solapes — esto sí es lógica real en el propio frontend**, no una delegación | Traer la lista de titulaciones/asignaturas |
| `users_admin.html` + `.js` | Pintar la tabla, pedir confirmación antes de acciones destructivas | Listar, crear, aprobar/vetar, cambiar rol, resetear contraseña, eliminar usuarios |
| `account.html` | Formulario de cambio de contraseña | Verificar la contraseña actual y guardar la nueva |

En resumen: el frontend concentra presentación, sondeo de progreso y (solo en el gestor de horarios) el cálculo de solapes; toda decisión de negocio, todo acceso a datos y toda llamada a servicios externos vive en el backend.

---

## 14. Autenticación y gestión de usuarios

Sí es un módulo independiente y bien delimitado: `auth/users.py`, con su propia tabla (`usuarios`, sin relación con el resto del esquema).

- **Inicio de sesión**: `authenticate()` — compara contraseña con su hash; si la cuenta no está activa, corta el acceso con un mensaje específico.
- **Usuarios**: alta (`create_user`/`register_user`, la segunda fuerza rol normal y cuenta inactiva), baja (`delete_user`), consulta (`list_users`).
- **Roles**: un único booleano `is_admin`; no hay roles intermedios.
- **Permisos**: comprobados en el frontend (`routes.py::_require_admin`) antes de reenviar, y protegidos además por una clave compartida (`X-Internal-Key`) entre frontend y backend para que esos endpoints no puedan llamarse directamente saltándose el frontend.
- **Administración**: pantalla `users_admin.html`, solo accesible a administradores.
- **Almacenamiento de contraseñas**: hash `pbkdf2` vía `werkzeug`; una contraseña antigua en claro se re-hashea sola la primera vez que ese usuario entra.
- **Protección de operaciones**: reglas de negocio dentro del propio módulo (no se puede eliminar la propia cuenta, ni quitar el rol o vetar al último administrador activo), independientes de la comprobación de permisos del frontend.

---

## 15. Inteligencia artificial

- **Quién decide usarla**: el usuario, en dos momentos distintos y explícitos — una casilla al lanzar la extracción (`enable_llm_review`), o un botón en la pantalla de revisión ("Resolver con IA"/"Revisar todo con IA"). En ningún caso se activa sola.
- **Qué se le envía**: únicamente el texto crudo de la celda conflictiva y las aulas/anotaciones ya detectadas mecánicamente — nunca el PDF, nunca información de otras asignaturas o titulaciones.
- **Qué devuelve**: si la celda es una sola asignatura (con o sin anotación rara) o varias, el nombre y aula de cada una, y un nivel de confianza (alto/medio/bajo).
- **¿Obligatoria u opcional?** Completamente opcional en ambos puntos; el sistema entero funciona sin Ollama disponible (una llamada fallida solo deja ese registro con su aviso original, sin detener nada más).
- **Cómo encaja en reconstrucción/normalización/validación**: no participa en la reconstrucción (que ya ha terminado cuando la IA actúa) ni en la normalización de catálogo (§8.2); actúa **sobre el resultado de la validación semántica** (§9.2), específicamente sobre los registros que esa validación marcó como sospechosos de mezclar asignaturas, e indirectamente puede cambiar su estado (si resuelve la ambigüedad, dejan de estar `INVALID`).
- **Cómo se comunica el backend con Ollama**: no hay un "proxy" intermedio — `pdf_table_extractor/llm_review.py` llama directamente a la librería `ollama` (`ollama.Client(host=...).chat(...)`), con la dirección leída de `ai_settings.py`. No se ha encontrado ningún componente adicional entre el backend y Ollama.

La IA es, en todo el sistema, un **mecanismo auxiliar de apoyo a la validación**, nunca un componente central: si se elimina por completo, el resto del pipeline (descarga, extracción, reconstrucción, revisión manual, persistencia, consulta) sigue funcionando exactamente igual.

---

## 16. Dependencias entre módulos

Vista general del sistema:

```text
Navegador
   ↓  HTTP
Frontend (Flask) — presentación + proxy
   ↓  HTTP (red interna de Docker)
Backend (FastAPI) — orquestación
   ↓                    ↓                  ↓                 ↓
Crawler          pdf_table_extractor   review.store      auth.users
(descarga)       (extracción +          (estado +         dbdump.loader
                  reconstrucción)        revisión)         schedule.db
                       ↓                    ↓                  ↓
                  [Ollama, opcional]   [Ollama, opcional]  PostgreSQL
```

Procesamiento de horarios en concreto (sin las ramas de usuarios/config):

```text
Localización + Descarga           (crawler.py)
   ↓
Extracción estructural            (pdf_table_extractor.extractor/diagnostics)
   ↓
Reconstrucción del horario        (pdf_table_extractor.normalizer.schedule_builder)
   ↓
Normalización de campo (parseo)   (incluida en el paso anterior: cell_parser/time_parser)
   ↓
[IA opcional]                     (pdf_table_extractor.llm_review)
   ↓
Validación de estado              (review.store, al construir reviewed_schedules.json)
   ↓
Revisión manual                   (review.store)
   ↓
[IA opcional, bajo demanda]       (pdf_table_extractor.llm_review, vía review.store)
   ↓
Normalización de catálogo         (dbdump.loader, justo antes de insertar)
   ↓
Persistencia                      (dbdump.loader → PostgreSQL)
   ↓
Consulta                          (schedule.db ← PostgreSQL)
```

---

## 17. Acoplamiento y separación de responsabilidades

- **Módulos claramente independientes y sustituibles sin tocar el resto**: `crawler` (podría cambiarse por otro mecanismo de descarga sin afectar a la extracción, mientras deje los PDF en la misma carpeta); `ai_settings` + los puntos de uso de IA (todo el sistema funciona igual sin Ollama); `schedule/db.py` (consulta de solo lectura, no la usa ningún otro módulo de escritura).
- **Componentes que agrupan varias responsabilidades**: `main.py` concentra el estado en memoria de los 4 procesos largos, la validación de entrada y el enrutado — no está mal separado del resto, pero sí es un fichero grande porque todos los endpoints viven ahí. `review/store.py` agrupa a la vez la construcción del JSON revisable, la validación semántica, las operaciones de edición y la orquestación de la IA bajo demanda — son responsabilidades relacionadas pero distintas, dentro de un mismo fichero.
- **Dependencias importantes a tener en cuenta**: `schedule_builder.py` depende de que `diagnostics/analyzer.py` haya elegido ya una estrategia por página (no vuelve a decidir eso); `dbdump/loader.py` depende del formato exacto de `reviewed_schedules.json` que produce `review/store.py`; ambos ficheros JSON son, en la práctica, el "contrato" entre etapas, no una interfaz de código.
- **Elemento compartido más importante**: la conexión a PostgreSQL se crea con la misma función (`get_connection()` en `main.py`) pero cada módulo (`auth`, `schedule`, `dbdump`) abre y cierra la suya propia — no hay pool ni conexión compartida entre peticiones.
- **Lo que se podría cambiar sin alterar significativamente el resto**: la estrategia de detección de tablas (añadir una cuarta en `strategies.py`) no afecta a `schedule_builder.py`, que solo consume su resultado ya normalizado a filas de texto; el motor de IA (cambiar Ollama por otro servidor compatible) no afecta a nada fuera de `llm_review.py` y `ai_settings.py`, siempre que se mantenga el mismo formato de petición/respuesta.

---

## 18. Resultado final

### Tabla resumen

| Módulo | Responsabilidad | Archivos principales | Entrada | Salida | Se comunica con |
|---|---|---|---|---|---|
| Frontend | Interfaz + proxy HTTP | `frontend-flask/app/routes.py`, `templates/`, `static/*.js` | Peticiones del navegador | HTML / JSON reenviado | Backend (HTTP) |
| Backend (orquestación) | Endpoints, estado de procesos largos | `backend-fastapi/app/main.py` | Peticiones del frontend | JSON / hilos en marcha | Todos los módulos backend |
| Localización + Descarga | Encontrar y bajar PDF de una URL | `crawler/crawler.py` | URL del usuario | PDF en `./downloads` | Fuente web (HTTP) |
| Extracción estructural | Diagnóstico agnóstico de tablas en el PDF | `pdf_table_extractor/{extractor,diagnostics}/*.py` | Fichero PDF | `diagnostic.json`, `tables_*.json`, `metadata.json` | Ninguno (solo E/S de fichero) |
| Reconstrucción de horarios | Interpretar la tabla como rejilla día×hora | `pdf_table_extractor/normalizer/schedule_builder.py` (+ cell/time/header_parser) | Salida de la extracción | `ScheduleEntry[]`, tablas descartadas | Ninguno externo |
| Orquestación de extracción | Coordinar las 3 etapas y el progreso | `backend-fastapi/app/extractor/pdf_extractor.py` | Carpeta de PDF | `all_schedules.json`, `extraction_run.json` | `pdf_table_extractor`, `review.store` |
| Revisión (estado + edición) | Validar estado y permitir corrección manual | `backend-fastapi/app/review/store.py` | JSON de extracción / ediciones del usuario | `reviewed_schedules.json` | Backend; opcionalmente IA |
| Revisión con IA | Resolver celdas ambiguas | `pdf_table_extractor/llm_review.py` | Texto crudo de una celda | Asignatura(s) resuelta(s) + confianza | Ollama (HTTP) |
| Configuración de IA | Guardar host/modelo de Ollama | `backend-fastapi/app/ai_settings.py` | Datos del panel / env var | `ai_settings.json` | Ninguno (solo lo leen otros) |
| Persistencia (volcado) | Cargar el horario revisado en PostgreSQL | `backend-fastapi/app/dbdump/loader.py` | `reviewed_schedules.json` | Filas en PostgreSQL | PostgreSQL |
| Consulta de horarios | Leer horarios ya persistidos | `backend-fastapi/app/schedule/db.py` | `degree_id` opcional | Titulaciones / asignaturas+sesiones | PostgreSQL |
| Autenticación y usuarios | Login, roles, alta/baja | `backend-fastapi/app/auth/users.py` | Credenciales / datos de usuario | Usuario autenticado o modificado | PostgreSQL |

### A. Flujo principal del sistema

El usuario introduce una URL en el panel de descargas; el crawler localiza y descarga los PDF a la carpeta compartida. Al lanzar la extracción, cada PDF pasa primero por un diagnóstico estructural agnóstico (pdfplumber, tres estrategias) que produce una descripción genérica de sus tablas; sobre esa descripción, el módulo de reconstrucción decide qué tablas son realmente rejillas de horario y las convierte en registros día/hora/asignatura, normalizando sobre la marcha horas y separando aulas del nombre de la asignatura. Si estaba activada la casilla correspondiente, una IA local intenta resolver ahí mismo las celdas que mezclan más de una asignatura. Con ese resultado se construye una copia revisable en la que cada registro recibe un estado (válido, con aviso, o incorrecto) según reglas automáticas. El usuario revisa esa copia, corrige lo necesario y, opcionalmente, vuelve a apoyarse en la IA para los casos aún ambiguos. Cuando el conjunto se considera listo, el usuario dispara el volcado, que relee esa copia, normaliza de nuevo los valores a su forma canónica y reconstruye desde cero las tablas de PostgreSQL dentro de una única transacción. A partir de ahí, cualquier usuario puede consultar el horario ya persistido, y es en el propio navegador donde se compone la vista semanal y se detectan los solapes entre las asignaturas elegidas.

### B. Módulos que deberían aparecer en el apartado 11.2

Por orden de relevancia para explicar el diseño: (1) descarga/localización (crawler), (2) extracción estructural del PDF, (3) reconstrucción de horarios (el más importante conceptualmente), (4) revisión — estado automático + edición manual, (5) persistencia/volcado, (6) consulta de horarios, (7) autenticación y usuarios, (8) frontend como capa de presentación/proxy. Estos ocho cubren toda la cadena de valor del sistema y cada uno tiene una responsabilidad claramente distinguible de los demás.

### C. Módulos secundarios (mención breve)

- **Configuración de IA** (`ai_settings.py`): relevante solo como apoyo del punto siguiente.
- **Revisión con IA** (`llm_review.py`): merece una mención como mecanismo auxiliar y opcional, pero no como bloque de diseño al mismo nivel que la reconstrucción o la revisión manual, dado que el propio código lo trata como algo prescindible en dos puntos distintos.
- **Orquestación de la extracción** (`pdf_extractor.py`): puede explicarse como parte del módulo de extracción/reconstrucción, sin necesidad de tratarlo aparte.

### D. Aspectos dudosos (no se puede determinar con seguridad solo con el código)

- **Por qué `pymupdf` sigue en `requirements.txt`** si no se usa: no hay ningún comentario ni commit visible que lo explique; solo se puede constatar que no se importa en ningún fichero activo.
- **Si la máquina donde corre Ollama es física o lógicamente distinta de la del resto del sistema en el despliegue real**: el código solo demuestra que la arquitectura lo permite (dirección configurable), no dónde se ejecuta realmente en producción.
- **Si `study_plans` y otros campos del esquema (`ects`, `subject_type`, `building`, `campus`, `file_hash`, `source_url`) están pensados para un desarrollo futuro concreto o son simplemente diseño especulativo**: el código no los usa ni los menciona en ningún comentario que aclare la intención.

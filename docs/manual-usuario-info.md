# Información base para el Manual de Usuario (Anexo B del TFG)

> Documento de trabajo, **no es el manual final**. Reconstruye por ingeniería
> inversa del código (no del README) cómo utiliza la aplicación una persona
> real: pantallas, botones, campos, mensajes y flujos. Sirve de materia prima
> para redactar el Anexo B "Manual de usuario" de la memoria del TFG.
>
> Fuentes revisadas directamente para este documento: `frontend-flask/app/routes.py`,
> las 10 plantillas de `frontend-flask/app/templates/`, `review.js`, `horarios.js`
> y los `<script>` inline de cada plantilla, y — para mensajes y reglas exactas —
> `backend-fastapi/app/main.py` y `backend-fastapi/app/auth/users.py`. Contrastado
> con `docs/uml/auditoria-acciones-ui.md` y `docs/requisitos-funcionales.md`
> (documentos previos de ingeniería inversa de este mismo repositorio).
>
> Convención: cuando algo existe solo parcialmente o no está conectado, se
> marca explícitamente con **⚠ PARCIAL** o **⚠ NO FUNCIONA**. No se inventa
> ninguna funcionalidad no presente en el código.

---

## 1. VISIÓN GENERAL DE LA APLICACIÓN

La aplicación es un portal web (Universidad de Alcalá — Movilidad
Internacional) que permite:

1. Descargar automáticamente PDFs de horarios desde una URL de una fuente
   web (crawler).
2. Extraer y normalizar esos PDFs a registros de horario estructurados.
3. Revisar y corregir manualmente esos registros (validar, editar, marcar,
   duplicar), opcionalmente con ayuda de una IA local (Ollama) para separar
   asignaturas mezcladas en una misma celda.
4. Volcar los registros ya revisados a una base de datos PostgreSQL.
5. Consultar esos horarios ya volcados, construir un horario semanal
   seleccionando asignaturas y detectar solapes entre ellas.
6. Gestionar el acceso: registro público con aprobación, login, cambio de
   la propia contraseña y, para administradores, alta/baja/rol de usuarios.

**Tipos de usuario:** solo hay dos roles, **usuario** y **administrador**
(`is_admin` en la tabla `usuarios`). No existe un perfil "estudiante" o
"visitante" separado dentro de la aplicación ya autenticada.

**Diferencia usuario / administrador:** es mínima y se reduce a una sola
pantalla. Un administrador ve además la tarjeta y el enlace **"Gestión de
usuarios"** y puede entrar en `/usuarios` para dar de alta cuentas, aprobar
o vetar registros pendientes, cambiar el rol de otros usuarios, resetear
contraseñas y eliminar cuentas. **Todo lo demás de la aplicación —
descargar, extraer, revisar, usar la IA, volcar a base de datos y
consultar horarios — está disponible igual para cualquier usuario con
sesión iniciada, sin restricción de rol** (confirmado en
`routes.py`: ninguna de esas rutas comprueba `is_admin`, solo que exista
sesión).

### Flujo normal de uso

```
Inicio de sesión (/login)
  → (si es la primera vez y no tiene cuenta) Crear cuenta (/registro)
    → esperar aprobación de un administrador en /usuarios
  → Página de inicio (/) — tarjetas de acceso a cada módulo
  → Panel de descargas (/descargas): introducir URL → "Iniciar descarga"
  → (misma pantalla) "Volcar a base de datos" — en realidad lanza la
    EXTRACCIÓN de los PDF descargados (nombre engañoso del botón, ver §5/§6)
  → Modal "Extracción finalizada" → "Revisar resultados"
  → Revisión manual (/revision): filtrar, abrir registros, corregir,
    marcar como válido/incorrecto, opcionalmente "Resolver con IA"
  → Volcado a base de datos (/volcado): "Volcar a base de datos" (el
    volcado real a PostgreSQL)
  → Gestor de horarios (/horarios): elegir titulación, añadir asignaturas,
    ver el horario semanal y los solapes
  → Cierre de sesión (enlace "Cerrar sesión", solo visible desde
    Inicio / Mi cuenta / Gestión de usuarios — ver nota en §2)
```

La **Administración IA** (`/ia`) y **Mi cuenta** (`/cuenta`) son pantallas
de apoyo transversal, accesibles en cualquier momento del flujo anterior, no
pasos obligatorios de la secuencia.

---

## 2. INVENTARIO COMPLETO DE PANTALLAS

> Nota de navegación importante para el manual: **el enlace "Cerrar
> sesión" solo aparece en tres pantallas: Inicio (`/`), Mi cuenta
> (`/cuenta`) y Gestión de usuarios (`/usuarios`)**. Desde el Panel de
> descargas, Revisión manual, Administración IA, Volcado a BD y Gestor de
> horarios **no hay enlace directo para cerrar sesión** ni (salvo una
> excepción) para volver a Inicio con un solo clic: en esas cinco pantallas
> el logotipo de la cabecera es un enlace a `/` (vuelve a Inicio), **excepto
> en el Gestor de horarios**, donde el logotipo es una simple imagen sin
> enlace (`<div class="topbar-brand">` en `horarios.html`, frente a
> `<a class="topbar-brand" href="...">` en el resto de plantillas). Desde
> `/horarios` no hay ningún enlace de vuelta a Inicio ni de cierre de
> sesión; solo el resto de enlaces del menú superior (Panel de descargas,
> Revisión manual, Volcado a BD, Administración IA, Usuarios si es admin,
> Mi cuenta). Es una inconsistencia real de la interfaz, útil para
> mencionar en el manual ("para cerrar sesión, vaya primero a Inicio o a Mi
> cuenta").

### Login

- **Ruta:** `GET/POST /login`
- **Plantilla:** `templates/login.html`
- **Acceso:** público (sin sesión). Es la pantalla de entrada; con sesión
  activa, cualquier otra ruta protegida redirige aquí si no hay `session["user"]`.
- **Para qué sirve:** autenticar al usuario y abrir sesión.
- **Cómo se llega:** URL raíz del sitio sin sesión iniciada, o tras pulsar
  "Cerrar sesión" en cualquier pantalla que lo tenga.
- **Qué muestra:** cabecera institucional ("Acceso institucional"),
  formulario de acceso, enlace "¿Ha olvidado su contraseña?" y enlace
  "Crear una cuenta".
- **Campos:** Usuario o correo electrónico (texto), Contraseña (password).
  Ambos `required` en el HTML.
- **Botones:**
  - **"Iniciar sesión"** (submit del formulario, `POST /login` vía Flask,
    que reenvía a `POST http://backend:8000/login`). Comprueba usuario y
    contraseña contra la tabla `usuarios`; si son válidas y la cuenta está
    activa, abre sesión (`session["user"|"user_id"|"is_admin"]`) y
    redirige a `/`.
  - **"¿Ha olvidado su contraseña?"** — ⚠ **NO FUNCIONA**: el enlace tiene
    `onclick="return false;"`, no hace nada. No existe recuperación de
    contraseña por correo en todo el proyecto.
  - **"Crear una cuenta"** — navega a `/registro`.
- **Al terminar correctamente:** redirección a `/` (Inicio), con el nombre
  de usuario ya visible en la cabecera.
- **Errores/avisos posibles** (recarga de la misma página con un cuadro de
  alerta rojo por encima del formulario, `login.html` bloque `{% if error %}`):
  - "Usuario o contraseña incorrectos" (credenciales inválidas).
  - "Tu cuenta todavía no está activa. Un administrador debe aprobarla."
    (contraseña correcta pero cuenta pendiente de aprobación o vetada).
  - "No se pudo conectar con el backend" (si `backend:8000` no responde).
- **A dónde conduce:** página de Inicio (`/`).

### Registro (crear cuenta)

- **Ruta:** `GET /registro` (página), `POST /register` (envío del
  formulario, vía JavaScript/`fetch`, no recarga de página)
- **Plantilla:** `templates/register.html`
- **Acceso:** público, y solo si NO hay sesión iniciada (con sesión activa,
  `/registro` redirige a `/`).
- **Para qué sirve:** que un visitante solicite una cuenta nueva.
- **Cómo se llega:** enlace "Crear una cuenta" desde `/login`.
- **Qué muestra:** aviso de que la cuenta quedará **pendiente de
  aprobación** por un administrador; formulario de registro.
- **Campos:** Usuario (texto, placeholder "nombre.apellido"), Contraseña
  (mínimo 4 caracteres), Repetir contraseña.
- **Botón:** **"Crear cuenta"** — valida en el navegador que las dos
  contraseñas coincidan; si coinciden, envía `POST /register` (Flask) →
  `POST http://backend:8000/register`, que crea el usuario con
  `is_admin=False, is_active=False`.
- **Al terminar correctamente:** el formulario se oculta y aparece un
  cuadro verde: "Cuenta creada. Un administrador debe aprobarla antes de
  que puedas iniciar sesión." El usuario se queda en esta misma página (no
  hay redirección automática); debe volver a `/login` con el enlace "Ya
  tiene cuenta · Iniciar sesión", y no podrá entrar hasta que un
  administrador lo apruebe desde `/usuarios`.
- **Errores posibles** (cuadro rojo sobre el formulario):
  - "Las contraseñas no coinciden." (validación en el propio navegador,
    antes de llamar al servidor).
  - "El nombre de usuario debe tener entre 3 y 50 caracteres (letras,
    dígitos y . _ - @)." (formato de usuario inválido).
  - "Ya existe un usuario con el nombre «...»." (usuario duplicado).
  - "La contraseña debe tener al menos 4 caracteres."
  - "No se pudo conectar con el servidor." (fallo de red/backend).
- **A dónde conduce:** vuelta manual a `/login` para intentar entrar (una
  vez aprobado).

### Inicio (Home)

- **Ruta:** `GET /`
- **Plantilla:** `templates/index.html`
- **Acceso:** ambos (usuario y administrador), requiere sesión.
- **Para qué sirve:** punto de partida y menú principal; enlaza a todos los
  módulos.
- **Cómo se llega:** tras iniciar sesión; también pulsando el logotipo
  desde casi cualquier otra pantalla (salvo el Gestor de horarios, ver
  nota de navegación arriba).
- **Qué muestra:** saludo personalizado ("Bienvenido, «usuario»") y una
  rejilla de tarjetas, una por módulo:
  - 📅 **Gestor de horarios** → `/horarios`
  - 📥 **Panel de descargas** → `/descargas`
  - 🔍 **Revisión manual** → `/revision`
  - 🗄️ **Volcado a base de datos** → `/volcado`
  - 🤖 **Administración IA** → `/ia`
  - 👥 **Gestión de usuarios** → `/usuarios` — **solo visible si
    `is_admin` es verdadero** (`{% if is_admin %}` en `index.html`)
- **Campos:** ninguno (no hay formularios en esta pantalla).
- **Botones:** cada tarjeta completa es un enlace ("Abrir →"); en la
  cabecera, "Mi cuenta" y "Cerrar sesión".
- **A dónde conduce:** a cualquiera de los módulos anteriores.

### Panel de descargas

- **Ruta:** `GET /descargas`
- **Plantilla:** `templates/download_panel.html`
- **Acceso:** ambos, requiere sesión.
- **Para qué sirve:** iniciar la descarga de PDFs desde una URL, ver su
  progreso, y desde aquí mismo lanzar la extracción de esos PDFs.
- **Cómo se llega:** tarjeta "Panel de descargas" en Inicio, o el enlace
  "Panel de descargas" del menú superior en otras pantallas internas.
- **Qué muestra:** tres paneles — (1) formulario de descarga + botones +
  barra de progreso de la extracción + estado de BD; (2) registro (logs)
  del proceso en vivo; (3) lista de PDFs ya descargados.
- **Campos que puede introducir el usuario:**
  - Campo de texto **"URL de origen"** (`urlInput`), sin autocompletar
    nada; el usuario pega la URL de la página que lista los horarios.
  - Casilla **"Revisar con IA local (Ollama) los registros con warnings"**
    (`llmReviewToggle`) — opcional, condiciona la extracción posterior.
- **Botones:**
  - **"Iniciar descarga"** (`startButton`) → `POST /start-download` →
    `POST /download/start` del backend. Valida que la URL no esté vacía y
    empiece por `http://`/`https://`; rechaza si ya hay una descarga en
    curso.
  - **"Volcar a base de datos"** (`dumpDbButton`, clase `db-button`) — ⚠
    **el texto del botón es engañoso**: en realidad dispara
    `POST /dump-db` → `POST /extract/start`, es decir **lanza la
    EXTRACCIÓN** de los PDFs descargados (diagnóstico + normalización a
    registros de horario), no ningún volcado a PostgreSQL. El volcado real
    está en la pantalla `/volcado` (ver más abajo). Manda además
    `enable_llm_review` con el valor de la casilla de IA.
  - **"Pausar proceso"** (`pauseButton`) → `POST /pause-download` — pausa
    la descarga en curso (no afecta a la extracción).
  - **"Reanudar"** (`resumeButton`) → `POST /resume-download`.
  - Clic sobre cualquier nombre de PDF en la lista **"PDFs descargados"**
    → abre ese PDF en una pestaña nueva (`GET /pdf/<archivo>`, proxy a
    `GET /download/file/{filename}` del backend).
- **Qué información muestra mientras corre:**
  - `statusMessage`: "Iniciando proceso...", "Proceso en ejecución...",
    "Proceso pausado.", "Proceso finalizado.", o el error de conexión.
  - `logBox`: línea a línea, con hora, cada evento (se refresca solo cada
    2 segundos vía `GET /status`, y cada 1,5 s durante la extracción vía
    `GET /extract-status`).
  - `progressBar` / `progressText` ("Carga BD: N%") — en realidad muestra
    el progreso de la **extracción**, no de un volcado a BD (mismo error
    de nomenclatura que el botón).
  - `dbStatus` ("Inactivo" / "Iniciando..." / "Ejecutando" / "Finalizado"
    / "Finalizado con errores" / "Error") y `lastDumpDate` ("Nunca" o la
    fecha/hora local de cuando terminó la última extracción).
- **Al terminar correctamente la extracción:** aparece automáticamente el
  **modal "Extracción finalizada"** con un resumen (PDFs
  procesados/fallidos, registros válidos, con warnings, "asignaturas
  mezcladas" [= INVALID], tablas omitidas, y si la IA estaba activada,
  cuántos registros revisó ella o el motivo por el que se omitió esa
  etapa). Botones del modal: **"Cerrar"** (lo descarta) y **"Revisar
  resultados"** (navega a `/revision`).
- **Errores/avisos posibles:**
  - "Debes introducir una URL." (campo vacío, validación en el navegador).
  - "La URL está vacía" / "La URL debe empezar por http:// o https://"
    (validación del backend).
  - "Ya hay un proceso en ejecución" (segunda descarga mientras hay una
    activa).
  - "Ya hay una extracción en curso" (segunda extracción mientras hay una
    activa).
  - "No hay PDFs en la carpeta de descargas" (extracción sin nada que
    procesar).
  - "Error al conectar con backend." / "No se pudo conectar con el
    backend." (fallos de red).
- **A dónde conduce:** Revisión manual (`/revision`), normalmente tras el
  modal de resumen.

### Revisión manual

- **Ruta:** `GET /revision`
- **Plantilla:** `templates/review.html` + `static/review.js`
- **Acceso:** ambos, requiere sesión.
- **Para qué sirve:** consultar, filtrar, corregir y validar cada registro
  de horario extraído antes de volcarlo a la base de datos.
- **Cómo se llega:** tarjeta "Revisión manual" en Inicio, enlace del menú
  superior, o botón "Revisar resultados" del modal de extracción.
- **Si no hay ninguna extracción todavía:** en vez del contenido normal se
  muestra un aviso de página completa (`noDataBox`): *"Todavía no hay
  ninguna extracción con resultados que revisar. Ve al panel de descargas
  y ejecuta una extracción primero."*, con enlace a `/descargas`.
- **Qué muestra** (documentado en detalle en §7, aquí solo el inventario):
  tiras de resumen/pestañas por estado; árbol Grado → Año → Semestre →
  Grupo; filtros y buscador; tabla paginada de registros; y, al hacer clic
  en una fila, un modal de detalle/edición.
- **Campos que puede introducir el usuario:** texto de búsqueda libre; 8
  selects de filtro; y, dentro del modal de detalle, todos los campos
  editables de un registro (ver §7).
- **Botones principales de la pantalla** (fuera del modal): pestañas de
  estado (Todos/Válidos/Warnings/Incorrectos), hojas del árbol, "Limpiar
  filtros", paginación (‹ Anterior / Siguiente ›), enlace "🗄️ Volcar a base
  de datos" (va a `/volcado`), y **"🤖 Revisar todo con IA"**.
- **A dónde conduce:** `/volcado` (enlace directo en la cabecera de la
  tabla) una vez el usuario da por buena la revisión.

### Administración IA

- **Ruta:** `GET /ia`
- **Plantilla:** `templates/ai_panel.html`
- **Acceso:** ambos, requiere sesión (⚠ no está restringida a
  administradores, pese al nombre "Administración").
- **Para qué sirve:** configurar el servidor Ollama (host + modelo) que
  usan la extracción (si se activó la casilla de IA) y los botones
  "Resolver con IA" de la Revisión manual, y comprobar si ese servidor
  responde.
- **Cómo se llega:** tarjeta "Administración IA" en Inicio, o enlace
  "Administración IA" del menú superior.
- **Qué muestra:** estado de conexión (punto de color + etiqueta + botón
  "Comprobar conexión", que además se ejecuta solo al cargar la página),
  métricas (ping en ms, nº de modelos disponibles) y la lista de modelos
  ya descargados en ese servidor; debajo, el formulario de configuración.
- **Campos:**
  - **"Dirección (socket) del servidor"** (`hostInput`) — placeholder
    `http://host.docker.internal:11434`.
  - **"Modelo a usar"** (`modelInput`) — placeholder `qwen2.5:3b`, con
    autocompletado (`datalist`) de los modelos detectados al comprobar la
    conexión.
- **Botones:**
  - **"Comprobar conexión"** → `GET /ai-status` → `GET /ai/status` del
    backend, que hace `GET {host}/api/tags` con 5 s de timeout.
  - Clic en un **"chip"** de modelo de la lista → solo rellena el campo
    "Modelo" con ese nombre (atajo, no guarda nada por sí solo).
  - **"Guardar"** → `PUT /ai-settings` → `PUT /ai/settings`, que valida
    que el host no esté vacío y empiece por `http://`/`https://`, y
    persiste host+modelo en `ai_settings.json`. Tras guardar, comprueba la
    conexión otra vez automáticamente.
- **Al terminar correctamente:**
  - Comprobación con éxito: punto verde "Conectado", "Servidor: {host}",
    ping en ms, nº de modelos y la lista de modelos como chips.
  - Guardado con éxito: "Guardado." en verde debajo del formulario.
- **Errores/avisos posibles:**
  - "Sin comprobar" (estado inicial antes de la primera comprobación).
  - "Comprobando…" (mientras se resuelve la llamada).
  - "Sin conexión" + `{host} — {detalle del error}` (Ollama no responde o
    da error).
  - "No hay modelos descargados en el servidor." (Ollama responde pero sin
    modelos instalados).
  - "No se pudo guardar." (validación del host fallida) / "No se pudo
    conectar con el servidor." (fallo de red).
- **A dónde conduce:** no es un paso de un flujo, es una pantalla de
  configuración de apoyo; normalmente se visita antes de activar la
  casilla de IA en Descargas o de pulsar "Resolver con IA" en Revisión.

### Volcado a base de datos

- **Ruta:** `GET /volcado`
- **Plantilla:** `templates/db_dump.html`
- **Acceso:** ambos, requiere sesión.
- **Para qué sirve:** volcar (persistir) a PostgreSQL los registros ya
  revisados (`reviewed_schedules.json`), reconstruyendo las tablas de
  horario desde cero. **Este es el volcado real** (el botón del mismo
  nombre en Descargas no lo es, ver arriba).
- **Cómo se llega:** tarjeta "Volcado a base de datos" en Inicio, enlace
  del menú superior, o el enlace "🗄️ Volcar a base de datos" de la cabecera
  de la tabla en Revisión manual.
- **Qué muestra:** explicación de qué hace el volcado (vacía y recarga
  tablas de horario y catálogos; todo en una transacción; no toca
  `usuarios` ni `demo`); panel de acción con botones, barra de progreso y
  estado; panel de "Registro del proceso" (logs).
- **Campos que puede introducir el usuario:** ninguno — no hay opciones
  que elegir, es una acción de un solo botón.
- **Botones:**
  - **"Volcar a base de datos"** (`dumpButton`) → `POST /db-dump-start` →
    `POST /db-dump/start`. Arranca en segundo plano.
  - **"Cancelar volcado"** (`cancelButton`, deshabilitado hasta que hay un
    volcado en marcha) → `POST /db-dump-cancel` → `POST /db-dump/cancel`.
    La cancelación provoca un `ROLLBACK`: la base de datos queda como
    estaba antes de empezar.
- **Qué muestra mientras corre:** `statusMessage`, barra y texto de
  progreso (`N% · procesados/total registros`), estado (`Iniciando...` /
  `Ejecutando` / `Cancelando...` / `Finalizado` / `Finalizado con errores`
  / `Error`), fecha del último volcado, contador de registros, y — al
  terminar — una rejilla con el nº de filas insertadas por tabla
  (Sesiones, Asignaturas, Titulaciones, Grupos, Aulas, Años acad.,
  Fuentes, Sesión↔Aula, Incidencias, Descartados).
- **Al terminar correctamente:** `dbStatus` = "Finalizado", mensaje
  "Volcado completado.", rejilla de recuentos visible.
- **Errores/avisos posibles:**
  - "Ya hay un volcado en curso." (segundo intento mientras hay uno
    activo).
  - "No hay ningún volcado en curso." (cancelar sin nada en marcha).
  - "No hay datos revisados que volcar: …" (no existe
    `reviewed_schedules.json`, p. ej. si nunca se ha ejecutado una
    extracción) — el volcado arranca igualmente y termina con este error
    registrado en el log, `dbStatus` = "Finalizado con errores".
  - "El volcado terminó con errores. La base de datos no se ha
    modificado." (mensaje genérico cuando `errors` no está vacío;
    coherente con que todo el volcado es una única transacción con
    rollback).
  - "Error al conectar con el backend." (fallo de red).
- **Repetible:** sí, sin límite; cada ejecución vacía y recarga las
  tablas, así que **no hay riesgo de duplicados** por repetir el volcado
  varias veces (ver §9).
- **A dónde conduce:** Gestor de horarios (`/horarios`), para comprobar
  que los datos volcados ya aparecen ahí (no hay enlace directo en esta
  pantalla, es navegación manual por el menú).

### Gestor de horarios

- **Ruta:** `GET /horarios`
- **Plantilla:** `templates/horarios.html` + `static/horarios.js`
- **Acceso:** ambos, requiere sesión.
- **Para qué sirve:** consultar los horarios ya volcados en PostgreSQL,
  construir un horario semanal eligiendo asignaturas y detectar solapes
  entre ellas.
- **Cómo se llega:** tarjeta "Gestor de horarios" en Inicio, o enlace del
  menú superior en otras pantallas (⚠ desde esta misma pantalla no hay
  vuelta directa a Inicio, ver nota de navegación al inicio de §2).
- **Qué muestra** (detallado en §10): selector de titulación, buscador y
  lista de asignaturas encontradas, lista de asignaturas ya seleccionadas,
  leyenda de colores/avisos, lista de solapes, y la rejilla semanal
  (Lunes–Viernes, 08:00–21:00).
- **Campos que puede introducir el usuario:**
  - Select **"Carrera"** (`degreeSelect`) — incluye la opción "Todas las
    carreras".
  - Campo de texto **"Buscar"** (`subjectSearch`) — filtra en el propio
    navegador, sobre lo ya cargado, por nombre, grupo, titulación o
    semestre.
  - Lista **"Asignaturas encontradas"** (`subjectSelect`, selector
    múltiple de 5 filas visibles) para elegir una asignatura+grupo.
- **Botones:**
  - **"Añadir"** (o doble clic sobre una asignatura de la lista) — la
    incorpora al horario semanal.
  - **"Limpiar"** — vacía toda la selección.
  - **"Quitar"** (uno por cada asignatura ya añadida, en la lista
    "Asignaturas seleccionadas") — la retira.
- **Al terminar correctamente:** la asignatura aparece con su color en la
  lista "Asignaturas seleccionadas" y sus sesiones se pintan en la rejilla
  semanal; mensaje "Asignatura añadida correctamente. Sin solapes." o, si
  choca con otra ya puesta, "Hay solapes en el horario. Revisa las celdas
  marcadas." con la lista de solapes debajo de la leyenda.
- **Errores/avisos posibles** (`messageBox`, texto + color según tipo):
  - "No hay horarios en la base de datos. Ejecuta el volcado primero."
    (nunca se ha hecho un volcado con éxito).
  - "No hay asignaturas para esa titulación en la base de datos." (la
    titulación elegida no tiene datos).
  - "No se pudieron cargar las titulaciones/asignaturas: …" (error
    503, base de datos caída).
  - "Selecciona una asignatura válida." / "No se ha encontrado la
    asignatura." / "Esta asignatura ya está seleccionada." (validaciones
    del propio botón "Añadir").
  - "Asignatura añadida, pero sus sesiones no tienen día u hora en la BD:
    no se pueden pintar." / "...pero no tiene ninguna sesión registrada."
    (asignatura sin horario completo).
- **A dónde conduce:** no es un paso intermedio, es la pantalla final de
  consulta del flujo.

### Gestión de usuarios

- **Ruta:** `GET /usuarios`
- **Plantilla:** `templates/users_admin.html`
- **Acceso:** **solo administrador**. `routes.py::users_admin` redirige a
  `/` si `session.get("is_admin")` es falso; además la tarjeta/enlace ni
  siquiera se muestran a un usuario normal en el resto de plantillas.
- **Para qué sirve:** dar de alta cuentas, aprobar/vetar registros
  pendientes, cambiar el rol, resetear contraseñas y eliminar usuarios.
- **Cómo se llega:** tarjeta "Gestión de usuarios" en Inicio (solo
  visible si es admin), o enlace "Usuarios" del menú superior en el resto
  de pantallas (también solo si es admin).
- **Qué muestra:** formulario de alta; tabla de usuarios con columnas
  Usuario, Rol, Estado (Activo/Pendiente), Alta (fecha) y Acciones.
- **Campos:**
  - **"Usuario"** (`newUsername`) y **"Contraseña"** (`newPassword`,
    mínimo 4 caracteres) del formulario de alta.
  - Casilla **"Administrador"** (`newIsAdmin`) — si se marca, la cuenta
    nueva se crea ya como administradora.
- **Botones:**
  - **"Crear usuario"** → `POST /users-create` → `POST /users` (backend).
    A diferencia del registro público, esta alta queda **activa** de
    inmediato (no pendiente de aprobación).
  - Por cada fila de la tabla:
    - **"Aprobar"** (si está Pendiente) o **"Vetar"** (si está Activo) —
      mismo botón, mismo endpoint (`PUT /users/{id}/active`), solo cambia
      el valor booleano; pide confirmación (`confirm()` del navegador).
      Deshabilitado sobre la propia cuenta si ya está activa (no se puede
      vetar uno mismo).
    - **"Hacer admin"** / **"Quitar admin"** — mismo botón según el
      estado actual (`PUT /users/{id}/role`), con confirmación.
    - **"Resetear contraseña"** — pide la nueva contraseña con un
      `prompt()` del navegador, luego `PUT /users/{id}/password`.
    - **"Eliminar"** — con confirmación, `DELETE /users/{id}`.
      Deshabilitado sobre la propia cuenta.
- **Al terminar correctamente:** mensaje verde bajo la tabla o el
  formulario ("Usuario «...» creado.", "Cuenta aprobada.", "Cuenta
  vetada.", "Rol actualizado.", "Contraseña cambiada.", "Usuario
  eliminado."), y la tabla se recarga sola.
- **Errores/avisos posibles:**
  - "El nombre de usuario debe tener entre 3 y 50 caracteres (letras,
    dígitos y . _ - @)."
  - "Ya existe un usuario con el nombre «...»."
  - "La contraseña debe tener al menos 4 caracteres."
  - "No se puede quitar el rol al último administrador activo."
  - "No puedes vetar tu propia cuenta." / "No se puede vetar al último
    administrador activo."
  - "No puedes eliminar tu propia cuenta." / "No se puede eliminar al
    último administrador activo."
  - "El usuario no existe."
  - "No se pudo conectar con el servidor." / "No se pudo aplicar."
- **A dónde conduce:** no es parte de un flujo secuencial; se usa cuando
  hace falta gestionar el acceso.

### Mi cuenta

- **Ruta:** `GET /cuenta`
- **Plantilla:** `templates/account.html`
- **Acceso:** ambos, requiere sesión.
- **Para qué sirve:** que cualquier usuario cambie su propia contraseña.
- **Cómo se llega:** enlace "Mi cuenta" (presente en el menú de todas las
  pantallas internas) o desde Inicio.
- **Qué muestra:** "Sesión iniciada como «usuario»" (+ "administrador" si
  aplica) y un formulario de cambio de contraseña.
- **Campos:** **Contraseña actual**, **Nueva contraseña** (mínimo 4
  caracteres), **Repetir nueva contraseña**.
- **Botón:** **"Cambiar contraseña"** → valida en el navegador que la
  nueva y su repetición coincidan, luego `POST /account-password` →
  `POST /account/password` (comprueba primero la contraseña actual).
- **Al terminar correctamente:** "Contraseña cambiada correctamente." en
  verde, formulario vaciado.
- **Errores posibles:**
  - "La nueva contraseña y su repetición no coinciden." (validación en el
    navegador).
  - "La contraseña actual no es correcta."
  - "La contraseña debe tener al menos 4 caracteres."
  - "El usuario no existe." (caso límite, cuenta borrada mientras la
    sesión seguía activa).
  - "Error de conexión."
- **A dónde conduce:** no es un paso de flujo; desde aquí también hay
  enlaces "Inicio", "Gestión de usuarios" (si admin) y "Cerrar sesión".

---

## 3. FUNCIONALIDADES DESDE EL PUNTO DE VISTA DEL USUARIO

### Funcionalidad: Iniciar sesión

- **Objetivo para el usuario:** entrar al portal con su cuenta.
- **Desde qué pantalla:** `/login`.
- **Pasos exactos:**
  1. Escribir el usuario en el campo "Usuario o correo electrónico".
  2. Escribir la contraseña.
  3. Pulsar "Iniciar sesión".
- **Datos que debe introducir:** usuario, contraseña.
- **Botón que inicia el proceso:** "Iniciar sesión".
- **Qué hace la aplicación de forma visible:** recarga la página; si es
  correcto, aparece la pantalla de Inicio con su nombre.
- **Resultado mostrado:** pantalla de Inicio, con su usuario en la
  cabecera y (si es admin) la tarjeta de Gestión de usuarios visible.
- **Posibles mensajes de error:** "Usuario o contraseña incorrectos"; "Tu
  cuenta todavía no está activa. Un administrador debe aprobarla."; "No se
  pudo conectar con el backend".
- **Qué puede hacer después:** navegar a cualquier módulo desde las
  tarjetas de Inicio.

### Funcionalidad: Crear una cuenta

- **Objetivo:** solicitar acceso al portal si todavía no tiene cuenta.
- **Desde qué pantalla:** `/registro` (enlazada desde `/login`).
- **Pasos:**
  1. Escribir un nombre de usuario.
  2. Escribir una contraseña (mínimo 4 caracteres) y repetirla.
  3. Pulsar "Crear cuenta".
- **Datos:** usuario, contraseña, repetición de contraseña.
- **Botón:** "Crear cuenta".
- **Qué hace la aplicación:** valida en el navegador que las contraseñas
  coincidan, envía la solicitud y muestra el resultado sin recargar la
  página.
- **Resultado mostrado:** aviso verde de que la cuenta se ha creado y está
  pendiente de aprobación.
- **Mensajes de error:** contraseñas no coinciden; usuario con formato
  inválido; usuario ya existe; contraseña demasiado corta.
- **Qué puede hacer después:** esperar a que un administrador la apruebe
  desde `/usuarios`, y mientras tanto volver a `/login` (no podrá entrar
  hasta la aprobación).

### Funcionalidad: Descargar horarios desde una URL

- **Objetivo:** obtener automáticamente los PDF de horarios de una web.
- **Desde qué pantalla:** `/descargas`.
- **Pasos:**
  1. Pegar la URL de la página que lista los horarios en "URL de origen".
  2. (Opcional) marcar "Revisar con IA local" si se quiere que la
     extracción posterior use IA — esta casilla en realidad **no afecta a
     la descarga**, solo se usa más tarde al lanzar la extracción.
  3. Pulsar "Iniciar descarga".
- **Datos:** URL de origen.
- **Botón:** "Iniciar descarga".
- **Qué hace la aplicación de forma visible:** el botón se deshabilita un
  instante, aparece "Iniciando proceso...", y a partir de ahí el panel de
  logs y la lista de PDFs se van rellenando solos cada 2 segundos según
  avanza el rastreo/descarga.
- **Resultado mostrado:** lista de nombres de PDF descargados (clicables
  para abrirlos), mensaje "Proceso finalizado." al terminar.
- **Mensajes de error:** "Debes introducir una URL."; "La URL está
  vacía"/"La URL debe empezar por http:// o https://"; "Ya hay un proceso
  en ejecución"; "Error al conectar con backend."
- **Qué puede hacer después:** pausar/reanudar mientras corre; una vez
  terminada, lanzar la extracción con "Volcar a base de datos" (ver
  siguiente funcionalidad).

### Funcionalidad: Extraer y normalizar los PDF descargados

- **Objetivo:** convertir los PDF descargados en registros de horario
  estructurados y clasificados.
- **Desde qué pantalla:** `/descargas` (⚠ el botón se llama "Volcar a base
  de datos" pero no vuelca nada a PostgreSQL, ver §2/§5/§6).
- **Pasos:**
  1. Tener al menos un PDF ya descargado.
  2. (Opcional) marcar "Revisar con IA local".
  3. Pulsar "Volcar a base de datos".
- **Datos:** ninguno adicional, solo la casilla de IA opcional.
- **Botón que inicia el proceso:** "Volcar a base de datos" (`dumpDbButton`).
- **Qué hace la aplicación de forma visible:** el botón se deshabilita;
  "Estado BD" pasa a "Iniciando..." y luego "Ejecutando"; la barra de
  progreso ("Carga BD: N%") y el log avanzan solos cada 1,5 s.
- **Resultado mostrado:** al terminar, se abre automáticamente el modal
  "Extracción finalizada" con el resumen de PDFs procesados/fallidos y
  registros por estado (válidos, con warnings, "asignaturas mezcladas" =
  INVALID, tablas omitidas), y — si se activó la IA — cuántos registros
  resolvió o el motivo de que se omitiera.
- **Mensajes de error:** "Ya hay una extracción en curso"; "No hay PDFs en
  la carpeta de descargas"; "Error: ..." genérico si falla el arranque.
- **Qué puede hacer después:** pulsar "Revisar resultados" en el modal
  (va a `/revision`) o "Cerrar" para quedarse en la misma pantalla.

### Funcionalidad: Revisar y corregir un registro extraído

*(desarrollada en detalle en §7; aquí el resumen operativo)*

- **Objetivo:** comprobar que un registro extraído es correcto y
  corregirlo si no lo es.
- **Desde qué pantalla:** `/revision`.
- **Pasos:**
  1. Localizar el registro (filtros, árbol o búsqueda).
  2. Hacer clic en su fila para abrir el modal de detalle.
  3. Editar los campos que haga falta.
  4. Pulsar "Guardar cambios" (o "Marcar como válido/incorrecto",
     "Restaurar original", "Duplicar como registro nuevo", según el caso).
- **Datos que puede introducir:** titulación, año académico, semestre,
  grupo, día, hora inicio/fin, nombre de asignatura, aulas (separadas por
  coma), notas (una por línea), y la casilla "Sospecha de que esta celda
  mezcla más de una asignatura".
- **Botón que inicia el proceso:** "Guardar cambios".
- **Qué hace la aplicación de forma visible:** valida los campos, guarda,
  recalcula el estado (VALID/WARNING/INVALID) y marca el registro como
  revisado; refresca la tabla, el árbol y los contadores de resumen sin
  cerrar el modal.
- **Resultado mostrado:** "Cambios guardados." en verde dentro del modal;
  las insignias de estado se actualizan.
- **Mensajes de error:** "No se pudieron guardar los cambios." (con el
  detalle de la validación fallida) o "No se pudo conectar con el
  servidor."
- **Qué puede hacer después:** cerrar el modal (✕) y seguir con el
  siguiente registro, o ir al volcado.

### Funcionalidad: Resolver con IA (separar asignaturas mezcladas)

- **Objetivo:** que un modelo de IA local separe automáticamente una celda
  que mezcla varias asignaturas, o confirme que es una sola.
- **Desde qué pantalla:** `/revision` — bien desde el modal de detalle de
  un registro concreto ("🤖 Resolver con IA"), bien desde la cabecera de
  la tabla para todos los pendientes a la vez ("🤖 Revisar todo con IA").
- **Pasos (caso "todos"):**
  1. Pulsar "🤖 Revisar todo con IA".
  2. Esperar mientras el texto de estado indica el progreso ("Revisando
     con IA… n/total"); opcionalmente pulsar "⏸ Pausar" o "✕ Cancelar".
  3. Al terminar, revisar los registros que la IA haya separado o
     confirmado (marcados con la etiqueta "🤖 IA").
- **Datos que debe introducir:** ninguno (usa la configuración guardada en
  `/ia`).
- **Botón que inicia el proceso:** "🤖 Revisar todo con IA" o "🤖 Resolver
  con IA" (un solo registro).
- **Qué hace la aplicación de forma visible:** deshabilita los botones de
  IA mientras corre, muestra el contador de progreso, y al acabar
  refresca tabla/árbol/resumen y — si el modal de detalle sigue abierto —
  su contenido.
- **Resultado mostrado:** "IA: N resueltos, M separados[, E con error]" o
  "No había ningún registro pendiente de revisar con IA."
- **Mensajes de error:** "No se pudo iniciar la revisión con IA."; "IA: no
  se pudo resolver ninguno (N con error — comprueba la conexión en
  Administración IA)."; "No se pudo consultar el estado de la revisión con
  IA."
- **Qué puede hacer después:** seguir revisando manualmente los registros
  que la IA no haya podido resolver, o continuar hacia el volcado.

### Funcionalidad: Volcar los horarios revisados a PostgreSQL

- **Objetivo:** persistir de forma definitiva los registros ya revisados.
- **Desde qué pantalla:** `/volcado`.
- **Pasos:**
  1. Pulsar "Volcar a base de datos".
  2. Esperar (barra de progreso, registro/log) o, si hace falta,
     "Cancelar volcado".
- **Datos que debe introducir:** ninguno.
- **Botón que inicia el proceso:** "Volcar a base de datos".
- **Qué hace la aplicación de forma visible:** vacía y recarga las tablas
  de horario en una sola transacción; progreso en tiempo real (registros
  procesados/total).
- **Resultado mostrado:** "Volcado completado." + rejilla con el nº de
  filas insertadas por tabla.
- **Mensajes de error:** "Ya hay un volcado en curso."; "No hay datos
  revisados que volcar: …"; "El volcado terminó con errores. La base de
  datos no se ha modificado."
- **Qué puede hacer después:** ir al Gestor de horarios a comprobar los
  datos, o repetir el volcado más adelante tras revisar más registros
  (sin riesgo de duplicados, ver §9).

### Funcionalidad: Consultar y construir un horario semanal

*(desarrollada en detalle en §10)*

- **Objetivo:** ver qué asignaturas hay disponibles y montar un horario
  semanal comprobando que no se solapen.
- **Desde qué pantalla:** `/horarios`.
- **Pasos:**
  1. (Opcional) elegir una carrera en el desplegable.
  2. (Opcional) escribir texto en "Buscar" para acotar la lista.
  3. Seleccionar una asignatura en la lista y pulsar "Añadir" (o doble
     clic).
  4. Repetir para cada asignatura que se quiera incluir.
  5. Revisar la rejilla semanal y, si aparecen, los solapes listados
     debajo de la leyenda.
- **Datos que puede introducir:** carrera, texto de búsqueda.
- **Botón que inicia el proceso:** "Añadir".
- **Qué hace la aplicación de forma visible:** pinta un bloque de color en
  la rejilla por cada sesión de la asignatura añadida; si coincide en
  día/hora con otra ya puesta, ambos bloques se marcan en rojo y aparece
  el detalle del choque en la lista de solapes.
- **Resultado mostrado:** horario semanal con bloques de color, lista de
  asignaturas seleccionadas con su color, leyenda, y lista de solapes (o
  "✓ Sin solapes entre las asignaturas seleccionadas.").
- **Mensajes de error:** "No hay horarios en la base de datos. Ejecuta el
  volcado primero."; "Esta asignatura ya está seleccionada."; "Asignatura
  añadida, pero sus sesiones no tienen día u hora en la BD: no se pueden
  pintar."
- **Qué puede hacer después:** quitar asignaturas sueltas ("Quitar") o
  vaciar todo ("Limpiar") y volver a construir el horario.

### Funcionalidad: Gestionar usuarios (solo administrador)

*(detallada en §4)*

- **Objetivo:** dar de alta, aprobar, cambiar el rol, resetear la
  contraseña o eliminar cuentas de acceso al portal.
- **Desde qué pantalla:** `/usuarios`.
- **Pasos (alta):** escribir usuario y contraseña, marcar "Administrador"
  si procede, pulsar "Crear usuario".
- **Pasos (aprobar una cuenta pendiente):** localizar la fila con estado
  "Pendiente", pulsar "Aprobar", confirmar.
- **Botón que inicia cada proceso:** el botón correspondiente de la fila,
  o "Crear usuario" para el alta.
- **Qué hace la aplicación de forma visible:** pide confirmación con un
  cuadro del navegador (o un `prompt()` para la nueva contraseña), aplica
  el cambio y recarga la tabla.
- **Resultado mostrado:** mensaje de confirmación en verde bajo la tabla.
- **Mensajes de error:** ver la lista completa en la ficha de la pantalla
  "Gestión de usuarios" (§2).
- **Qué puede hacer después:** seguir gestionando otras cuentas o volver a
  Inicio.

### Funcionalidad: Cambiar la propia contraseña

- **Objetivo:** cualquier usuario cambia su contraseña sin ayuda de un
  administrador.
- **Desde qué pantalla:** `/cuenta`.
- **Pasos:** escribir la contraseña actual, la nueva (mínimo 4
  caracteres) y repetirla; pulsar "Cambiar contraseña".
- **Botón:** "Cambiar contraseña".
- **Resultado mostrado:** "Contraseña cambiada correctamente."
- **Mensajes de error:** contraseñas no coinciden; "La contraseña actual
  no es correcta."; contraseña demasiado corta.
- **Qué puede hacer después:** seguir navegando con la sesión actual (no
  hace falta volver a iniciar sesión).

### Funcionalidad: Cerrar sesión

- **Objetivo:** terminar la sesión.
- **Desde qué pantalla:** solo disponible desde Inicio (`/`), Mi cuenta
  (`/cuenta`) o Gestión de usuarios (`/usuarios`) — ver nota de
  navegación en §2.
- **Botón:** "Cerrar sesión".
- **Qué hace la aplicación:** `session.clear()` en el servidor Flask.
- **Resultado mostrado:** redirección a `/login`.

---

## 4. AUTENTICACIÓN Y GESTIÓN DE USUARIOS

- **Login:** formulario clásico servidor-a-servidor (recarga de página,
  no AJAX). Compara la contraseña con su hash (`pbkdf2:sha256` vía
  Werkzeug); admite además, de forma transparente, que la contraseña
  siguiera guardada en claro de una migración anterior — en ese caso, al
  entrar correctamente, la reescribe como hash sin que el usuario note
  nada (`auth/users.py::_verify`/`authenticate`).
- **Logout:** `session.clear()`, sin llamada al backend; visible solo
  desde 3 pantallas (ver §2/§3).
- **Creación de usuarios:** dos caminos distintos con distinto resultado:
  - **Registro público** (`/registro`, sin sesión): la cuenta queda
    **inactiva** (`is_active=False`) hasta que un administrador la
    aprueba.
  - **Alta por un administrador** (`/usuarios`, formulario "Nuevo
    usuario"): la cuenta queda **activa** de inmediato, y puede marcarse
    como administradora desde el propio formulario.
- **Cambio de rol:** solo desde `/usuarios`, botón "Hacer admin"/"Quitar
  admin" sobre una fila ya existente. No se puede dejar el sistema sin
  ningún administrador activo (bloqueado con mensaje explícito).
- **Activación/desactivación:** sí existe, con la etiqueta "Aprobar" /
  "Vetar" (mismo botón, mismo endpoint, booleano invertido). Vetar no
  borra la cuenta, solo le impide iniciar sesión. No se puede vetar la
  propia cuenta ni al último administrador activo.
- **Permisos:** exactamente dos: `is_admin` (acceso a `/usuarios`) e
  implícitamente "tiene sesión" (acceso a todo lo demás). No hay más
  niveles ni permisos por módulo.
- **Restricciones de administrador:** solo aplican a las acciones dentro
  de `/usuarios` (`routes.py::_require_admin` en el frontend, y la
  cabecera interna `X-Internal-Key` que exige el backend FastAPI para
  todo lo de autenticación/usuarios). El resto de la API FastAPI (por
  ejemplo, descarga, extracción, revisión, volcado, IA) **no exige rol ni
  esa cabecera** — solo el paso intermedio por Flask exige sesión
  iniciada, sin mirar el rol.
- **Credenciales incorrectas:** mensaje "Usuario o contraseña
  incorrectos", sin distinguir si el usuario no existe o la contraseña es
  errónea (por seguridad, no revela cuál de las dos falló).
- **Usuario administrador inicial:** si la tabla `usuarios` está
  totalmente vacía, se crea automáticamente `admin` / `admin` como
  administrador (`ensure_admin_exists`, se ejecuta en cada intento de
  login). Además, si en algún momento no queda ningún administrador
  activo, **el primer usuario que consiga iniciar sesión se convierte
  automáticamente en administrador**, sin que nadie tenga que hacer nada
  desde la interfaz. (En este despliegue concreto, la cuenta operativa
  documentada en la memoria del proyecto es `raul`/`1234`, ya
  administradora.)

### Qué existe solo en el backend (no hay botón/pantalla para ello)

- La clave interna `X-Internal-Key` que protege `/login`, `/register`,
  `/users*` y `/account/*` en el backend FastAPI: el usuario nunca la ve
  ni la introduce, la añade Flask automáticamente en cada llamada.
- La auto-promoción a administrador del primer usuario sin admins
  activos: ocurre silenciosamente al iniciar sesión, sin ningún aviso en
  pantalla de "ahora eres administrador".
- La reescritura de una contraseña antigua en claro a hash: transparente,
  no hay ningún mensaje ni pantalla al respecto.
- El endpoint `GET /demo` y la tabla `demo`: código heredado, sin ninguna
  pantalla ni botón que lo llame desde el frontend actual.

---

## 5. FLUJO DE LOCALIZACIÓN Y DESCARGA DE DOCUMENTOS

Desde el punto de vista de quien usa la aplicación (sin entrar en cómo
funciona el rastreador por dentro):

1. El usuario introduce **una única URL** en el Panel de descargas — no
   hay más parámetros de búsqueda, ni selección de universidad/fuente
   predefinida, ni un buscador interno de páginas.
2. Pulsa "Iniciar descarga". La aplicación valida que la URL no esté
   vacía y tenga el formato correcto antes de arrancar.
3. **Progreso visible:** un panel de "Logs del proceso" que se rellena
   solo (consultando el estado cada 2 segundos) con líneas de texto con
   marca de hora; no hay barra de progreso durante la descarga en sí
   (la barra de progreso de esa misma pantalla es, de hecho, la de la
   extracción posterior).
4. **Resultados:** conforme se van descargando PDFs, aparecen como
   elementos clicables en la lista "PDFs descargados" del panel derecho.
5. **Selección/descarga de documentos:** no hay un paso de "seleccionar
   cuáles descargar" — el rastreador descarga automáticamente todos los
   PDF que encuentra en esa página que cumplen los filtros internos
   (duplicados por URL/contenido descartados). El usuario solo puede, ya
   descargados, hacer clic en uno para **abrirlo** (se abre en una pestaña
   nueva del navegador, sirviéndose desde el propio backend).
6. **Si no se encuentra nada:** la lista se queda con el texto "Aún no hay
   PDFs descargados." No hay un mensaje explícito de "0 resultados", el
   log sí mostrará la actividad del rastreo.
7. **Si falla la web de origen** (no responde, URL incorrecta, etc.): el
   error se refleja como líneas en el log del proceso; no hay una pantalla
   de error dedicada. `statusMessage` puede mostrar "Error al conectar con
   backend." si el propio backend no responde.
8. **Dónde se ven los PDF descargados dentro de la aplicación:** únicamente
   en esa misma lista del Panel de descargas — no existe ningún otro
   visor, biblioteca o histórico de documentos en el resto de la
   aplicación. Al hacer clic se abren con el visor de PDF nativo del
   navegador (se sirven con `Content-Type: application/pdf`).
9. **Pausar / reanudar:** botones "Pausar proceso" y "Reanudar", operan
   sobre la descarga en curso (no sobre la extracción).

---

## 6. PROCESAMIENTO DE PDF

1. El usuario inicia el procesamiento pulsando **"Volcar a base de
   datos"** en el Panel de descargas (⚠ nombre engañoso, ver §2) —
   siempre procesa **todos** los PDF que haya en la carpeta de descargas
   en ese momento, no hay selección individual de qué PDF procesar.
2. Antes de arrancar puede marcar la casilla "Revisar con IA local" para
   activar, además, una etapa opcional de revisión con IA al final de la
   extracción.
3. **Mientras se procesa:**
   - Una barra de progreso (`progressBar`/`progressText`, texto "Carga
     BD: N%") que sube según el porcentaje de ficheros procesados.
   - Un panel de logs con líneas nuevas cada 1,5 segundos (sondeo a
     `/extract-status`), con el fichero actual y los eventos relevantes.
   - El estado (`dbStatus`) pasa de "Iniciando..." a "Ejecutando" y
     finalmente a "Finalizado" o "Finalizado con errores".
4. **Resultado que obtiene el usuario:** el modal "Extracción finalizada"
   con el resumen (ver §2), y a partir de ahí puede ir a Revisión manual.
5. **Dónde se muestran las tablas/registros obtenidos:** no en esta
   pantalla — únicamente en `/revision`, donde cada fila de la tabla de
   resultados es un registro de horario extraído de una tabla de un PDF.
6. **Errores visibles que puede recibir el usuario en esta etapa:**
   - "Ya hay una extracción en curso" (si pulsa el botón dos veces).
   - "No hay PDFs en la carpeta de descargas" (si no ha descargado nada
     antes).
   - Errores por fichero individual aparecen como líneas en el log, pero
     **no detienen el resto** — un PDF que falla no impide procesar los
     demás (`pdfs_failed` se contabiliza aparte en el resumen).
7. Aparte, si la casilla de IA estaba marcada y Ollama no responde, la
   extracción **termina igual con éxito**; el modal muestra en su lugar
   "Revisión por IA omitida: {motivo}" en vez del contador de registros
   revisados por IA.

---

## 7. REVISIÓN Y CORRECCIÓN DE REGISTROS

- **Cómo se accede:** `/revision` (tarjeta en Inicio o menú). Si nunca se
  ha ejecutado una extracción, se muestra un aviso de página completa en
  vez del contenido (ver §2).

- **Qué registros aparecen:** todos los de la última extracción
  (`reviewed_schedules.json` — la copia editable, nunca el
  `all_schedules.json` original), incluidas las "tablas omitidas" (un PDF
  del que el extractor no consiguió sacar ninguna tabla interpretable:
  aparecen con `origin = "skipped"`, sin datos de horario, solo para que
  quede constancia).

- **Estados existentes y qué significan para el usuario:**
  | Estado | Icono | Significado |
  |---|---|---|
  | **VALID** | ✓ verde | El registro no tiene avisos: se dará por bueno al volcar. |
  | **WARNING** | ⚠ ámbar | Tiene algún aviso (p. ej. un dato dudoso o incompleto), pero se sigue volcando a la base de datos. |
  | **INVALID** | ✕ rojo | Se considera incorrecto (p. ej. sospecha fundada de asignaturas mezcladas sin resolver); **se descarta del volcado**. |

  Además, badges informativas independientes del estado: "Modificado
  manualmente" (✎, si el usuario ya lo ha editado), "Revisado" (si ya se
  ha marcado como tal), "🤖 IA" (si pasó por la revisión con IA), "Tabla
  omitida" y "Asignaturas mezcladas" (origen/sospecha).

- **Campos editables** (formulario del modal de detalle): Titulación, Año
  académico, Semestre, Grupo, Día, Hora inicio, Hora fin, Nombre de
  asignatura, Aulas (lista separada por comas), casilla "Sospecha de
  mezcla" y Notas (una por línea). **No editable** en ese mismo modal (solo
  lectura): archivo de origen, página, texto original tal cual salió del
  PDF, estrategia de extracción, warnings/motivo, anotaciones no
  reconocidas como aula, resultado de la IA si la hubo, y la tabla en
  bruto tal como la leyó el extractor.

- **Cómo guarda cambios:** botón "Guardar cambios" dentro del modal; valida
  en el servidor que todos los campos estén presentes con el tipo
  correcto, guarda de forma atómica, recalcula el estado automáticamente
  y marca el registro como revisado y, si difiere del original, como
  "modificado manualmente". No hace falta cerrar el modal para ver el
  resultado: se repuebla con los datos ya guardados.

- **Cómo navega entre registros:** no hay botones "Siguiente
  registro"/"Anterior registro" dentro del modal — hay que cerrarlo y
  hacer clic en otra fila de la tabla. La navegación entre páginas de la
  tabla es aparte, con "‹ Anterior" / "Siguiente ›".

- **Filtros disponibles:** búsqueda de texto libre (con 350 ms de
  espera tras dejar de teclear, busca en asignatura, aula, grupo, texto
  original, fichero y grado); estado (pestañas); grado; año académico;
  semestre; grupo; día; PDF de origen; tipo de problema; y "revisado por
  IA" (todos / sí / no). Todos se combinan entre sí. El árbol lateral
  (Grado → Año → Semestre → Grupo) es un atajo visual para rellenar
  grado+año+semestre+grupo de golpe haciendo clic en una hoja.

- **Paginación:** sí, 50 registros por página fija (`pageSize` no es
  configurable desde la interfaz), con indicador "Página X de Y".

- **Cómo se muestran errores y advertencias:** el motivo de un WARNING o
  la razón original se lista en el modal de detalle, sección "Diagnóstico
  de la extracción" → "Warnings / motivo". Los errores de guardado
  aparecen como texto rojo dentro del propio modal ("No se pudieron
  guardar los cambios."), sin recargar la página.

- **¿Puede marcar registros como revisados?** No hay un botón explícito
  "Marcar como revisado" independiente: guardar una edición, o pulsar
  "Marcar como válido"/"Marcar como incorrecto", marca automáticamente el
  registro como revisado. Sí existen los dos botones de veredicto
  explícito:
  - **"Marcar como válido"** → estado VALID.
  - **"Marcar como incorrecto"** → estado INVALID.

- **Qué ocurre con los valores originales:** nunca se pierden. Cada
  registro guarda un valor "actual" (editable) y uno "original" (el que
  salió de la extracción); el botón **"Restaurar original"** descarta
  todos los cambios manuales y vuelve al valor original, recalculando el
  estado.

- **Acciones especiales disponibles:**
  - **"Duplicar como registro nuevo"** — crea una copia editable
    independiente del mismo registro (mismo día/hora/grado/grupo), pensada
    para separar a mano una celda que en realidad mezcla dos asignaturas:
    tras duplicar, el modal se reabre directamente sobre la copia nueva
    para terminar de rellenarla. La copia queda trazada a su origen.
  - **"🤖 Resolver con IA"** — manda ese único registro a la revisión por
    IA (ver §8).
  - **"🤖 Revisar todo con IA"** (fuera del modal, en la cabecera de la
    tabla) — manda todos los pendientes de golpe, con controles de
    pausa/cancelación mientras corre.

---

## 8. INTELIGENCIA ARTIFICIAL / OLLAMA

- **¿Puede el usuario activarla o configurarla?** Sí, desde la interfaz,
  de dos formas independientes:
  1. **Configuración del servidor** — pantalla `/ia` ("Administración
     IA"): host y modelo de Ollama, con comprobación de conexión y lista
     de modelos disponibles. Esta pantalla **no está restringida a
     administradores** pese a su nombre.
  2. **Activación puntual** — casilla "Revisar con IA local" en el Panel
     de descargas (solo afecta a esa extracción concreta) y los botones
     "🤖 Resolver con IA" / "🤖 Revisar todo con IA" en Revisión manual
     (bajo demanda, en cualquier momento posterior).
- **Parámetros que puede modificar el usuario:** únicamente **host** (URL
  del servidor Ollama) y **modelo** (nombre del modelo ya descargado en
  ese servidor, p. ej. `qwen2.5:3b`). No hay ningún otro parámetro
  ajustable desde la interfaz (ni temperatura, ni número de reintentos, ni
  timeout): esos valores están fijos en el backend.
- **Qué registros pueden enviarse a la IA:** en la práctica, los que
  tienen la sospecha de "asignaturas mezcladas" sin resolver (uno
  concreto, desde su modal de detalle, o todos los pendientes de una vez
  desde el botón de la cabecera de la tabla). No hay una selección
  intermedia por casillas de varias filas sueltas en la tabla.
- **Botón que inicia la revisión:** "🤖 Revisar todo con IA" (lote) o "🤖
  Resolver con IA" (uno solo), ambos en `/revision`; también puede
  dispararse automáticamente al final de una extracción si se marcó la
  casilla correspondiente en `/descargas`.
- **Qué resultado ve el usuario:** por cada registro, la IA lo confirma
  como una sola asignatura o lo separa en varias (creando registros nuevos
  trazados al original), indicando su confianza (alta/media/baja); un
  resultado de baja confianza conserva el aviso original en vez de darlo
  por resuelto. En pantalla: contador de progreso mientras corre
  ("Revisando con IA… n/total"), y al terminar un resumen ("IA: N
  resueltos, M separados[, E con error]"); dentro del modal de detalle de
  un registro ya revisado por IA, un bloque "Revisión por IA (Ollama)" con
  la confianza y la nota.
- **Qué ocurre si Ollama no está disponible:**
  - Al comprobar la conexión en `/ia`: "Sin conexión" + el motivo del
    error, sin bloquear el resto de la aplicación.
  - Durante una extracción con la casilla de IA marcada: la extracción
    **termina igual con éxito**, y el resumen indica "Revisión por IA
    omitida: {motivo}" en vez de un contador de resueltos.
  - Durante una revisión con IA bajo demanda: cada registro que falle
    queda registrado como error (sin detener el resto del lote); si
    fallan todos, el mensaje final es "IA: no se pudo resolver ninguno (N
    con error — comprueba la conexión en Administración IA)."
- **¿Es completamente opcional?** Sí. Toda la aplicación — descarga,
  extracción, revisión manual, volcado y consulta de horarios — funciona
  sin que Ollama esté nunca disponible; la IA solo ayuda a resolver más
  rápido un tipo concreto de aviso (celdas con asignaturas mezcladas), que
  también se puede resolver a mano con "Duplicar como registro nuevo".

### Solo interno (no hay pantalla/botón para ello)

- El *timeout* máximo por llamada a Ollama y el número de reintentos ante
  una respuesta mal formada están fijos en el backend, sin control desde
  la interfaz.
- El guardado incremental tras cada registro procesado (para poder
  recuperar el progreso si el proceso se cae a mitad) es transparente,
  sin ningún indicador visible aparte del contador de progreso.

---

## 9. VOLCADO / PERSISTENCIA EN POSTGRESQL

- **Cómo lo inicia el usuario:** un único botón, "Volcar a base de datos",
  en `/volcado`. No hay parámetros que rellenar antes.
- **Qué datos se vuelcan:** los registros del estado **VALID** y
  **WARNING** de `reviewed_schedules.json` (la copia ya revisada). Los
  **INVALID** y las tablas omitidas **se descartan** automáticamente (se
  cuentan como "Descartados" en el resumen final, pero no se insertan).
- **¿Puede elegir qué registros volcar?** No. Es todo o nada según su
  estado — no hay una pantalla de selección de registros concretos antes
  del volcado. La única forma de "excluir" un registro del volcado es
  dejarlo o marcarlo como INVALID en la Revisión manual antes de volcar.
- **Validaciones previas:** ninguna explícita en la pantalla (no hay una
  comprobación de "¿seguro?" ni una vista previa); la validación real ya
  se hizo registro a registro durante la Revisión manual.
- **Mensaje si funciona:** "Volcado completado." + rejilla de recuentos
  por tabla (sesiones, asignaturas, titulaciones, grupos, aulas, años
  académicos, fuentes, relación sesión↔aula, incidencias, descartados).
- **Mensaje si falla:** "El volcado terminó con errores. La base de datos
  no se ha modificado." (todo el proceso corre en una única transacción:
  si algo va mal, se hace `ROLLBACK` y la base de datos queda exactamente
  como estaba antes).
- **¿Puede repetirse?** Sí, sin ninguna limitación. Cada ejecución empieza
  vaciando (`TRUNCATE ... CASCADE`) las tablas de horario y sus catálogos
  antes de recargarlas.
- **¿Riesgo de duplicados según el comportamiento visible?** No: como cada
  volcado vacía y recarga las tablas afectadas desde cero, repetirlo varias
  veces con los mismos datos revisados produce siempre el mismo resultado,
  sin filas duplicadas acumulándose. Las tablas `usuarios` y `demo` quedan
  fuera de este vaciado, así que las cuentas de acceso nunca se pierden al
  volcar.

---

## 10. CONSULTA DE DATOS Y HORARIOS

Recorrido exacto que seguiría una persona que quiere montar y consultar un
horario, en `/horarios`:

1. **Selección de universidad:** no existe — la aplicación gestiona una
   sola universidad implícita (Universidad de Alcalá), no hay selector.
2. **Titulación:** desplegable "Carrera" (`degreeSelect`), con la opción
   por defecto "Todas las carreras". Al cambiarlo, se recargan las
   asignaturas disponibles para esa titulación.
3. **Curso académico:** no hay un filtro dedicado en esta pantalla (el año
   académico sí existe como dato en la base, pero no es un criterio de
   filtrado aquí; solo aparece en la Revisión manual).
4. **Asignatura:** campo de texto libre "Buscar" que filtra, **en el
   propio navegador** y sobre lo ya cargado, por nombre de asignatura,
   grupo, titulación o semestre; el resultado aparece en la lista
   "Asignaturas encontradas".
5. **Grupo:** no es un filtro aparte — cada entrada de la lista de
   asignaturas ya es una combinación (asignatura + grupo) condensada, y
   el grupo se muestra como parte del texto de cada opción (p. ej.
   "Programación · G1 · sem. 1").
6. **Semestre:** se muestra como parte del texto de cada asignatura (" ·
   sem. N"), pero tampoco hay un desplegable para filtrar por semestre.
7. **Cualquier otro filtro:** ninguno más existe en esta pantalla.
8. **Construcción del horario semanal:** seleccionar una entrada de la
   lista y pulsar "Añadir" (o doble clic) — se incorpora a la lista
   "Asignaturas seleccionadas" y sus sesiones se pintan en la rejilla.
   Se puede repetir con cuantas asignaturas se quiera.
9. **Cómo se representan las sesiones:** un bloque de color (uno de 8
   colores distintos, asignado por orden de aparición) dentro de la celda
   día×hora de la rejilla Lunes–Viernes, 08:00–21:00. Cada bloque muestra
   el nombre de la asignatura, el grupo, la hora de inicio-fin y, si la
   hay, el aula.
10. **Información de cada sesión:** nombre de asignatura, grupo, hora de
    inicio y fin, aula (si consta) — todo lo que trae `sessions[]` de
    `/schedule/subjects`. Las sesiones sin día o sin hora completa
    **no llegan a pintarse**: se cuentan aparte como "sin horario" (con un
    aviso "⚠ N sin horario" junto a la asignatura, tanto en la lista de
    resultados como en la de seleccionadas).
11. **Detección/representación de solapamientos:** automática, en el
    navegador, comparando cada par de asignaturas seleccionadas por
    día+franja horaria. Los bloques implicados se pintan en rojo
    (`conflict`/`conflict-cell`) y, debajo de la leyenda, aparece una
    lista con el detalle de cada choque ("Día hora–hora · Asignatura A
    (grupo) choca con Asignatura B (grupo, hora–hora)").
12. **Sesiones con aviso (WARNING) de la extracción:** se distinguen con
    un borde ámbar punteado en el bloque (no afecta a si se pintan o no,
    solo es un aviso visual de que ese dato venía marcado como dudoso).
13. **Acciones del usuario sobre la selección:** "Quitar" (una asignatura
    suelta) y "Limpiar" (vacía todo).

No existe ninguna pantalla ni endpoint para consultar "grupos" o
"sesiones" de forma independiente: solo existen como campos dentro de la
respuesta de asignaturas de esta misma pantalla.

---

## 11. MENSAJES, ERRORES Y ESTADOS

| Situación | Mensaje mostrado al usuario | Pantalla | Qué debe hacer el usuario |
|---|---|---|---|
| Credenciales incorrectas | "Usuario o contraseña incorrectos" | Login | Revisar usuario/contraseña e intentarlo de nuevo |
| Cuenta pendiente o vetada | "Tu cuenta todavía no está activa. Un administrador debe aprobarla." | Login | Esperar a que un administrador la apruebe en `/usuarios` |
| Backend caído en el login | "No se pudo conectar con el backend" | Login | Reintentar más tarde / avisar a soporte |
| Enlace de recuperación de contraseña | (no hace nada, `onclick="return false;"`) | Login | Usar "Crear una cuenta" o pedir a un admin que resetee su contraseña |
| Registro con contraseñas distintas | "Las contraseñas no coinciden." | Registro | Corregir el campo de repetición |
| Usuario duplicado | "Ya existe un usuario con el nombre «...»." | Registro / Gestión de usuarios | Elegir otro nombre de usuario |
| Contraseña demasiado corta | "La contraseña debe tener al menos 4 caracteres." | Registro / Mi cuenta / Gestión de usuarios | Elegir una contraseña más larga |
| Registro correcto | "Cuenta creada. Un administrador debe aprobarla antes de que puedas iniciar sesión." | Registro | Esperar aprobación y volver a `/login` |
| Sin sesión al llamar a datos | Redirección a `/login` (páginas) o `{"success": false, "message": "Sesión no válida"}` (llamadas internas) | Cualquiera | Iniciar sesión de nuevo |
| URL de descarga vacía o mal formada | "Debes introducir una URL." / "La URL está vacía" / "La URL debe empezar por http:// o https://" | Panel de descargas | Corregir la URL |
| Descarga ya en marcha | "Ya hay un proceso en ejecución" | Panel de descargas | Esperar a que termine o pausarla |
| Extracción ya en marcha | "Ya hay una extracción en curso" | Panel de descargas | Esperar a que termine |
| Sin PDFs para extraer | "No hay PDFs en la carpeta de descargas" | Panel de descargas | Descargar PDFs primero |
| Extracción finalizada | Modal "Extracción finalizada" con resumen | Panel de descargas | Pulsar "Revisar resultados" o "Cerrar" |
| Revisión por IA omitida durante la extracción | "Revisión por IA omitida: {motivo}" | Panel de descargas (modal) | Revisar la conexión en Administración IA si se quería usar |
| Sin extracción todavía | "Todavía no hay ninguna extracción con resultados que revisar." | Revisión manual | Ir al Panel de descargas y ejecutar una extracción |
| Edición de registro inválida | "No se pudieron guardar los cambios." (+ detalle) | Revisión manual (modal) | Corregir el campo señalado y reintentar |
| Edición guardada | "Cambios guardados." | Revisión manual (modal) | Continuar revisando otros registros |
| Registro restaurado | "Registro restaurado al original." | Revisión manual (modal) | — |
| Registro duplicado | "Registro duplicado. Edita esta copia con los datos que faltaban y guarda." | Revisión manual (modal) | Rellenar la copia nueva y guardarla |
| Revisión con IA ya en marcha | "Ya hay una revisión con IA en curso." | Revisión manual | Esperar o cancelar la actual |
| Revisión con IA sin nada pendiente | "No había ningún registro pendiente de revisar con IA." | Revisión manual | — |
| Revisión con IA completada | "IA: N resueltos, M separados[, E con error]" | Revisión manual | Revisar los registros marcados 🤖 IA |
| Revisión con IA: todos fallaron | "IA: no se pudo resolver ninguno (N con error — comprueba la conexión en Administración IA)." | Revisión manual | Ir a Administración IA y comprobar la conexión |
| Ollama sin conexión | "Sin conexión" + host + motivo del error | Administración IA | Revisar host/modelo o que Ollama esté encendido |
| Host de IA inválido al guardar | "No se pudo guardar." (+ detalle de validación) | Administración IA | Corregir el host (debe empezar por http(s)://) |
| Configuración de IA guardada | "Guardado." | Administración IA | Pulsar "Comprobar conexión" si hace falta verificar |
| Volcado ya en marcha | "Ya hay un volcado en curso." | Volcado a BD | Esperar a que termine o cancelarlo |
| Cancelar sin volcado activo | "No hay ningún volcado en curso." | Volcado a BD | — |
| Sin datos revisados que volcar | "No hay datos revisados que volcar: …" | Volcado a BD | Ejecutar antes una extracción y revisión |
| Volcado correcto | "Volcado completado." + rejilla de recuentos | Volcado a BD | Ir al Gestor de horarios a comprobar los datos |
| Volcado con errores | "El volcado terminó con errores. La base de datos no se ha modificado." | Volcado a BD | Revisar el registro/log y reintentar |
| Sin horarios volcados | "No hay horarios en la base de datos. Ejecuta el volcado primero." | Gestor de horarios | Ir a `/volcado` y ejecutar el volcado |
| Titulación sin asignaturas | "No hay asignaturas para esa titulación en la base de datos." | Gestor de horarios | Elegir otra titulación o "Todas las carreras" |
| Asignatura ya añadida | "Esta asignatura ya está seleccionada." | Gestor de horarios | — |
| Asignatura sin horario | "Asignatura añadida, pero sus sesiones no tienen día u hora en la BD: no se pueden pintar." | Gestor de horarios | Revisar/corregir ese registro en Revisión manual |
| Solapes detectados | "Hay solapes en el horario. Revisa las celdas marcadas." + lista de choques | Gestor de horarios | Quitar una de las asignaturas en conflicto si no es compatible |
| Sin solapes | "✓ Sin solapes entre las asignaturas seleccionadas." | Gestor de horarios | — |
| Intento de vetar/eliminar la última cuenta admin activa | "No se puede vetar al último administrador activo." / "No se puede eliminar al último administrador activo." / "No se puede quitar el rol al último administrador activo." | Gestión de usuarios | Nombrar antes a otro administrador |
| Intento de vetarse/eliminarse a uno mismo | "No puedes vetar tu propia cuenta." / "No puedes eliminar tu propia cuenta." | Gestión de usuarios | Pedir a otro administrador que lo haga |
| Contraseña actual incorrecta al cambiarla | "La contraseña actual no es correcta." | Mi cuenta | Introducir la contraseña actual correcta |
| Cambio de contraseña correcto | "Contraseña cambiada correctamente." | Mi cuenta | — |
| Fallo de red genérico | "No se pudo conectar con el backend." / "No se pudo conectar con el servidor." / "Error de conexión." | Cualquiera | Reintentar; si persiste, avisar a soporte técnico |

---

## 12. ELEMENTOS QUE NECESITAN CAPTURA PARA EL MANUAL

Lista mínima pero suficiente (12 capturas) para ilustrar el manual:

1. **Pantalla de login**
   - Pantalla: `/login`.
   - Qué debe aparecer: formulario vacío, con el enlace "Crear una
     cuenta" visible.
   - Acción previa: ninguna (cerrar sesión si ya hay una activa).
   - Sirve para: §"Acceso a la aplicación".

2. **Login con error de credenciales**
   - Pantalla: `/login`.
   - Qué debe aparecer: el cuadro de alerta roja "Usuario o contraseña
     incorrectos" sobre el formulario.
   - Acción previa: enviar el formulario con una contraseña incorrecta.
   - Sirve para: §"Acceso a la aplicación" / mensajes de error.

3. **Página de Inicio con las tarjetas**
   - Pantalla: `/`.
   - Qué debe aparecer: las 6 tarjetas (incluida "Gestión de usuarios",
     entrando como administrador para que salgan todas).
   - Acción previa: iniciar sesión como un usuario administrador.
   - Sirve para: §"Visión general" / mapa de módulos.

4. **Panel de descargas en marcha**
   - Pantalla: `/descargas`.
   - Qué debe aparecer: URL introducida, log con varias líneas, y al
     menos un PDF ya en la lista de la derecha.
   - Acción previa: lanzar una descarga real y esperar a que aparezca
     algún resultado.
   - Sirve para: §"Localización y descarga de documentos".

5. **Modal "Extracción finalizada"**
   - Pantalla: `/descargas` (modal superpuesto).
   - Qué debe aparecer: el resumen completo (PDFs procesados/fallidos,
     válidos, warnings, "asignaturas mezcladas", tablas omitidas).
   - Acción previa: completar una extracción entera.
   - Sirve para: §"Procesamiento de PDF".

6. **Revisión manual — vista general con filtros y árbol**
   - Pantalla: `/revision`.
   - Qué debe aparecer: las pestañas de estado con contadores, el árbol
     lateral abierto por una rama, la tabla con registros de distintos
     estados (colores VALID/WARNING/INVALID visibles).
   - Acción previa: tener al menos una extracción con resultados mixtos.
   - Sirve para: §"Revisión y corrección de registros".

7. **Modal de detalle/edición de un registro**
   - Pantalla: `/revision` (modal).
   - Qué debe aparecer: las tres secciones (información original,
     diagnóstico, datos editables) y los botones de acción de la parte
     inferior (Resolver con IA, Duplicar, Restaurar, Marcar
     incorrecto/válido, Guardar).
   - Acción previa: hacer clic sobre una fila con algún aviso.
   - Sirve para: §"Revisión y corrección de registros".

8. **Revisión con IA en curso**
   - Pantalla: `/revision`.
   - Qué debe aparecer: el texto "Revisando con IA… n/total" y los
     botones "⏸ Pausar"/"✕ Cancelar" visibles en la cabecera de la tabla.
   - Acción previa: pulsar "🤖 Revisar todo con IA" con Ollama accesible
     y varios registros mezclados pendientes.
   - Sirve para: §"Inteligencia artificial / Ollama".

9. **Administración IA — estado conectado**
   - Pantalla: `/ia`.
   - Qué debe aparecer: punto verde "Conectado", ping y nº de modelos, y
     la lista de chips de modelos.
   - Acción previa: tener Ollama accesible y pulsar "Comprobar conexión"
     (o simplemente cargar la página).
   - Sirve para: §"Inteligencia artificial / Ollama".

10. **Volcado a base de datos completado**
    - Pantalla: `/volcado`.
    - Qué debe aparecer: "Volcado completado." y la rejilla de recuentos
      por tabla ya rellena.
    - Acción previa: ejecutar un volcado completo con datos revisados
      disponibles.
    - Sirve para: §"Volcado / persistencia en PostgreSQL".

11. **Gestor de horarios con asignaturas y un solape**
    - Pantalla: `/horarios`.
    - Qué debe aparecer: al menos dos asignaturas seleccionadas con un
      solape visible (bloques en rojo) y la lista de solapes debajo de la
      leyenda.
    - Acción previa: volcar datos y añadir dos asignaturas que choquen en
      horario.
    - Sirve para: §"Consulta de datos y horarios".

12. **Gestión de usuarios (vista de administrador)**
    - Pantalla: `/usuarios`.
    - Qué debe aparecer: el formulario de alta y la tabla con al menos
      una cuenta "Pendiente" y otra "Activa", mostrando los botones
      Aprobar/Vetar, Hacer admin/Quitar admin, Resetear contraseña y
      Eliminar.
    - Acción previa: tener una cuenta registrada pendiente de aprobación
      (crear una desde `/registro` con otro usuario) y entrar como
      administrador.
    - Sirve para: §"Autenticación y gestión de usuarios".

---

## 13. PROPUESTA DE ÍNDICE DEL MANUAL DE USUARIO

```
ANEXO B. MANUAL DE USUARIO

B.1.  Introducción y visión general
B.2.  Acceso a la aplicación
      B.2.1. Iniciar sesión
      B.2.2. Crear una cuenta y aprobación por un administrador
      B.2.3. Cerrar sesión
      B.2.4. Cambiar mi contraseña
B.3.  Página de inicio y navegación entre módulos
B.4.  Panel de descargas
      B.4.1. Iniciar una descarga desde una URL
      B.4.2. Pausar y reanudar una descarga
      B.4.3. Consultar los PDF descargados
      B.4.4. Lanzar la extracción de los PDF
B.5.  Revisión manual de los registros extraídos
      B.5.1. Resumen, filtros, árbol y búsqueda
      B.5.2. Consultar el detalle de un registro
      B.5.3. Editar y guardar un registro
      B.5.4. Marcar como válido / incorrecto y restaurar el original
      B.5.5. Duplicar un registro (separar asignaturas mezcladas a mano)
B.6.  Revisión asistida por inteligencia artificial (Ollama)
      B.6.1. Configurar y comprobar la conexión con Ollama
      B.6.2. Resolver un registro o un lote completo con IA
B.7.  Volcado de los horarios revisados a la base de datos
B.8.  Consulta de horarios y detección de solapes
      B.8.1. Buscar y añadir asignaturas
      B.8.2. Leer el horario semanal y la leyenda
      B.8.3. Solapes entre asignaturas
B.9.  Gestión de usuarios (solo administradores)
      B.9.1. Aprobar o vetar una cuenta
      B.9.2. Dar de alta un usuario
      B.9.3. Cambiar el rol, resetear la contraseña, eliminar una cuenta
B.10. Mensajes, avisos y solución de problemas frecuentes
```

No se incluyen apartados de "gestión de titulaciones/asignaturas" ni
"recuperación de contraseña por correo": ninguna de las dos existe en el
código (ver §15).

---

## 14. BORRADOR DE INSTRUCCIONES PASO A PASO

### Iniciar sesión

1. Acceda a la dirección del portal; si no tiene una sesión abierta, verá
   la pantalla de acceso.
2. Introduzca su usuario y su contraseña.
3. Pulse "Iniciar sesión".
4. Si los datos son correctos y su cuenta está activa, el sistema le
   llevará a la página de Inicio con su nombre en la cabecera.
5. Si la operación no se realiza correctamente, aparecerá un mensaje en
   rojo indicando el motivo (contraseña incorrecta o cuenta pendiente de
   aprobación).

### Solicitar una cuenta nueva

1. En la pantalla de acceso, pulse "Crear una cuenta".
2. Introduzca el usuario que desea utilizar y una contraseña de al menos
   4 caracteres, repitiéndola en el segundo campo.
3. Pulse "Crear cuenta".
4. El sistema mostrará un aviso confirmando que la cuenta se ha creado y
   que queda pendiente de aprobación.
5. No podrá iniciar sesión hasta que un administrador apruebe su cuenta
   desde la pantalla de Gestión de usuarios.

### Descargar horarios desde una web

1. Acceda al Panel de descargas desde la tarjeta correspondiente en
   Inicio.
2. Introduzca en el campo "URL de origen" la dirección de la página que
   contiene los horarios.
3. Pulse "Iniciar descarga".
4. El sistema mostrará el progreso en el panel de "Logs del proceso" y
   los PDF encontrados irán apareciendo en la lista "PDFs descargados".
5. Si necesita detener el proceso temporalmente, pulse "Pausar proceso";
   para continuar, pulse "Reanudar".
6. Si la operación se realiza correctamente, el mensaje pasará a "Proceso
   finalizado." y los PDF quedarán listados y disponibles para abrir con
   un clic.

### Extraer los horarios de los PDF descargados

1. Con al menos un PDF ya descargado, en el Panel de descargas pulse el
   botón "Volcar a base de datos" (este botón procesa los PDF, no vuelca
   nada a la base de datos todavía).
2. Si desea que una inteligencia artificial ayude a resolver celdas con
   varias asignaturas mezcladas, marque antes la casilla "Revisar con IA
   local".
3. El sistema mostrará una barra de progreso y el registro de actividad
   mientras procesa cada PDF.
4. Al finalizar, aparecerá una ventana emergente con el resumen de
   resultados.
5. Pulse "Revisar resultados" para pasar a la Revisión manual, o "Cerrar"
   para quedarse en la misma pantalla.

### Revisar y corregir un registro

1. Acceda a Revisión manual.
2. Localice el registro que quiere comprobar usando la búsqueda, los
   filtros o el árbol de la izquierda.
3. Haga clic sobre la fila del registro para abrir su detalle.
4. Revise la información original (no editable) y corrija, si hace
   falta, los campos de la sección "Datos editables".
5. Pulse "Guardar cambios".
6. Si la operación se realiza correctamente, verá el mensaje "Cambios
   guardados." dentro de la propia ventana.
7. Si el registro es correcto, puede además pulsar "Marcar como válido";
   si es incorrecto y no se puede corregir, "Marcar como incorrecto".
8. Si se ha equivocado, pulse "Restaurar original" para deshacer todos
   los cambios manuales.

### Separar una celda con varias asignaturas mezcladas

**A mano:**
1. Abra el registro con la marca "Asignaturas mezcladas".
2. Pulse "Duplicar como registro nuevo".
3. El sistema abrirá automáticamente la copia recién creada.
4. Edite en la copia los datos de la segunda asignatura que faltaba y
   pulse "Guardar cambios".

**Con IA:**
1. Abra el registro con la marca "Asignaturas mezcladas".
2. Pulse "🤖 Resolver con IA" (o, para todos los pendientes a la vez,
   "🤖 Revisar todo con IA" desde la cabecera de la tabla).
3. Espere a que el sistema termine (se puede pausar o cancelar mientras
   tanto).
4. Revise el resultado: la IA puede confirmar el registro como una sola
   asignatura o separarlo en varios registros nuevos.

### Volcar los horarios revisados a la base de datos

1. Acceda a la pantalla "Volcado a base de datos".
2. Pulse "Volcar a base de datos".
3. El sistema mostrará el progreso y el registro de la operación.
4. Si la operación se realiza correctamente, verá "Volcado completado."
   y un resumen con el número de filas insertadas por tabla.
5. Si necesita detenerlo, pulse "Cancelar volcado": la base de datos
   quedará exactamente como estaba antes de empezar.

### Consultar y montar un horario semanal

1. Acceda al Gestor de horarios.
2. (Opcional) elija una carrera en el desplegable "Carrera".
3. Escriba en "Buscar" para localizar una asignatura.
4. Selecciónela en la lista y pulse "Añadir" (o haga doble clic sobre
   ella).
5. Repita el paso anterior con cada asignatura que quiera incluir.
6. El sistema pintará las sesiones en la rejilla semanal; si dos
   asignaturas chocan en el mismo día y hora, ambas aparecerán marcadas en
   rojo y el choque se detallará debajo de la leyenda.
7. Para quitar una asignatura, pulse "Quitar" junto a su nombre; para
   empezar de nuevo, pulse "Limpiar".

### Gestionar usuarios (solo administradores)

1. Acceda a "Gestión de usuarios".
2. Para dar de alta una cuenta: rellene "Usuario" y "Contraseña", marque
   "Administrador" si procede, y pulse "Crear usuario".
3. Para aprobar una cuenta pendiente de un registro público: localice su
   fila (estado "Pendiente") y pulse "Aprobar".
4. Para retirar el acceso a una cuenta activa: pulse "Vetar" en su fila.
5. Para cambiar el rol: pulse "Hacer admin" o "Quitar admin".
6. Para forzar una nueva contraseña: pulse "Resetear contraseña" e
   introduzca la nueva en el cuadro que aparece.
7. Para eliminar una cuenta definitivamente: pulse "Eliminar" y confirme.
8. El sistema no le dejará vetar, quitar el rol o eliminar a la última
   cuenta de administrador activa, ni actuar sobre su propia cuenta en
   esos tres casos.

---

## 15. DUDAS O INFORMACIÓN QUE NO PUEDES DEDUCIR DEL CÓDIGO

- **Apariencia visual real (colores, tipografías, responsive):** el CSS
  define el diseño, pero cómo se ve realmente en un navegador y en
  distintos tamaños de pantalla solo puede comprobarse ejecutando la
  aplicación y haciendo las capturas de §12.
- **Textos generados dinámicamente que dependen de datos reales:** nombres
  concretos de titulaciones, asignaturas, grupos, aulas y PDF que
  aparecerán en pantalla dependen por completo de la fuente web usada en
  cada descarga y no pueden anticiparse desde el código.
- **Tiempos reales de cada proceso:** cuánto tarda una descarga, una
  extracción, una revisión con IA o un volcado depende del tamaño de los
  PDF, del número de registros y del rendimiento del servidor Ollama; no
  es deducible del código, solo observable en ejecución.
- **Calidad real de la extracción y de la IA:** qué proporción de
  registros saldrá VALID/WARNING/INVALID, o qué tan bien separa Ollama las
  asignaturas mezcladas, depende de los PDF concretos usados como fuente y
  del modelo de IA elegido; no es un comportamiento fijo del código.
- **Credenciales y URL reales del despliegue en producción:** el manual no
  debe incluir ninguna URL, usuario o contraseña real; solo lo que ve
  cualquier usuario final en el formulario.
- **Compatibilidad de navegador:** el código usa JavaScript moderno
  (`fetch`, `async/await`, `<details>`) sin comprobar la versión del
  navegador; qué navegadores concretos son compatibles no está
  documentado ni forzado por el código.
- **Política real de aprobación de cuentas:** el código permite a
  cualquier administrador aprobar cualquier cuenta pendiente, pero no hay
  ninguna regla de negocio (p. ej. "solo se aprueban universitarios con
  correo institucional") — si existe tal política, es organizativa, no
  está en el código.
- **Qué fuentes web están soportadas en la práctica:** el rastreador
  acepta cualquier URL `http(s)://`, pero solo puede confirmarse contra
  qué páginas concretas de horarios funciona bien probándolo realmente;
  el código no mantiene una lista cerrada de fuentes compatibles.

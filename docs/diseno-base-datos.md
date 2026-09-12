# Diseño de la base de datos

Explicación extendida de cómo está construida la base de datos de PostgreSQL
del proyecto: por qué tiene esta forma, qué problema resuelve cada tabla, y
cómo se llena realmente a partir del PDF. Reconstruido a partir de
`db/createdatabase.sql`, `db/003_usuarios_auth.sql`,
`db/004_usuarios_is_active.sql` y `backend-fastapi/app/dbdump/loader.py`
(el código que carga los datos).

Ver también [`docs/uml/modelo-datos.puml`](uml/modelo-datos.puml) para el
diagrama entidad-relación completo.

---

## 1. El problema que tenía que resolver

El sistema extrae horarios de PDF universitarios con un pipeline de reglas
(y, opcionalmente, con ayuda de una IA local) que produce resultados **con
distinta calidad**: registros perfectamente identificados, registros con
avisos (una celda que quizá mezcla dos asignaturas), y tablas que no se han
podido leer en absoluto. Antes de guardar nada de forma definitiva, un
humano revisa y corrige ese resultado a mano.

Eso condiciona el diseño en tres decisiones de fondo:

1. **La base de datos no es el primer sitio donde vive el dato.** Mientras
   se extrae y se revisa, todo vive en ficheros JSON
   (`all_schedules.json`, `reviewed_schedules.json`) en el sistema de
   ficheros. PostgreSQL solo entra en juego **al final**, cuando el
   revisor decide "esto ya está listo" y pulsa "Volcar a base de datos".
2. **Nunca se puede perder el dato original.** Aunque se normalice un
   nombre de asignatura o se identifique un aula, el texto tal cual venía
   del PDF se guarda siempre en paralelo, por si la normalización se
   equivocó.
3. **Casi nada es obligatorio.** Un PDF real trae asignaturas sin aula
   asignada todavía, horarios sin grupo, sesiones cuya asignatura no se ha
   podido casar con el catálogo. La base de datos tiene que **admitir
   huecos** en vez de rechazar la fila.

Esas tres ideas explican por qué casi todas las claves foráneas del
esquema son `NULL`-ables con `ON DELETE SET NULL`, y por qué cada tabla
"resuelta" tiene su columna `_raw` hermana.

---

## 2. Por qué PostgreSQL y por qué UUID

- **PostgreSQL** porque es gratuito, corre perfectamente en un contenedor
  Docker junto al resto del sistema, y tiene tipos y funciones (`UUID`,
  `pgcrypto`, `TIME`, índices compuestos) que encajan bien con este
  problema sin necesitar nada exótico.
- **`UUID` como clave primaria** (con `gen_random_uuid()`, de la extensión
  `pgcrypto`) en vez de un `SERIAL` autoincremental, en casi todas las
  tablas del horario. Motivo: los identificadores se generan en el
  momento de insertar durante el volcado, sin depender de una secuencia
  compartida, y no colisionan aunque en el futuro se junten datos de
  varias fuentes o se recargue la base varias veces (que es exactamente lo
  que hace el volcado: vaciar y recargar). La excepción es `usuarios`, que
  sigue usando `SERIAL` porque es una tabla mucho más simple, ajena al
  pipeline de horarios y con muy pocas filas.

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

Esta línea es la que habilita `gen_random_uuid()`; sin ella, ningún
`DEFAULT gen_random_uuid()` del resto del fichero funcionaría.

---

## 3. Las tablas de catálogo (el "diccionario" del sistema)

Son las tablas que describen **qué existe**, independientemente de que
haya o no una clase concreta programada. Se crean (o se reutilizan si ya
existen) automáticamente durante el volcado, a partir de lo que aparece en
los PDF — no hay ninguna pantalla para darlas de alta a mano.

| Tabla | Qué representa | Quién la rellena y cómo |
|---|---|---|
| `universities` | La universidad (en la práctica, una única fila: "Universidad de Alcalá"). | El volcado inserta esa fila fija al principio de cada carga. |
| `degrees` | Una titulación ("GRADO EN INGENIERÍA INFORMÁTICA…"). | Se crea la primera vez que aparece ese nombre de titulación en un registro; las siguientes veces se reutiliza (caché en memoria durante el volcado). |
| `study_plans` | El plan de estudios de una titulación (con año de inicio/fin). | **Existe en el esquema pero el volcado nunca la rellena** — no hay ese dato en el PDF. Es la tabla más "de futuro" de las doce. |
| `subjects` | Una asignatura, con su nombre "limpio" (`name`) y uno normalizado para buscar (`normalized_name`: mayúsculas, sin acentos, espacios colapsados). | Se crea agrupando por `(nombre normalizado, titulación)`, así "Redes de Comunicaciones" y "REDES DE COMUNICACIONES" no generan dos filas distintas. |
| `academic_years` | Un curso académico ("2026/2027"), con año de inicio y fin ya separados. | Se parsea el texto `"AAAA/AAAA"` del PDF con una expresión regular. |
| `groups` | Un grupo de un curso ("3ºB"), con el curso (el número inicial del nombre) y el semestre. | Igual que `subjects`: caché por `(nombre, titulación, año académico)`. |
| `rooms` | Un aula (`código`, p.ej. "NA7"). | Se crea la primera vez que aparece ese código de aula en cualquier sesión. |
| `sources` | Un fichero PDF de origen, con su nombre y (si se llegara a rellenar) su hash y URL. | Una fila por cada nombre de PDF distinto que aparece en los registros volcados. |

Todas estas tablas cuelgan, directa o indirectamente, de `universities` y
de `degrees` con `ON DELETE SET NULL`: si algún día se borrara una
universidad o una titulación, las asignaturas, grupos, etc. **no
desaparecen**, simplemente se quedan sin esa referencia. Es una decisión
deliberada para no perder datos de horario por una limpieza de catálogo.

---

## 4. El núcleo: `class_sessions`

Esta es la tabla más importante del esquema: **una fila por cada sesión de
clase** (una asignatura, un día, una franja horaria). Todo lo demás gira
alrededor de ella.

```sql
CREATE TABLE class_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_id UUID NULL,
    degree_id UUID NULL,
    group_id UUID NULL,
    academic_year_id UUID NULL,
    subject_name_raw TEXT NULL,
    group_name_raw TEXT NULL,
    room_raw TEXT NULL,
    day_of_week VARCHAR(20) NULL,
    time_start TIME NULL,
    time_end TIME NULL,
    semester INTEGER NULL,
    class_type VARCHAR(50) NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'UNRESOLVED',
    confidence NUMERIC(5,4) NULL,
    multiple_entries_suspected BOOLEAN NOT NULL DEFAULT FALSE,
    ...
);
```

Puntos de diseño que merece la pena explicar:

- **Doble representación de cada dato: resuelto + crudo.** `subject_id`
  apunta al catálogo (`subjects`), pero `subject_name_raw` guarda el texto
  tal cual salió del PDF. Lo mismo con `group_name_raw` y `room_raw`. Si
  la resolución automática se equivoca, el dato original nunca se pierde:
  se puede volver a intentar sin re-extraer el PDF.
- **`subject_id`, `degree_id`, `group_id`, `academic_year_id` son todos
  `NULL`-ables.** Un registro se guarda **aunque no se haya podido
  identificar** su asignatura o su grupo — precisamente eso es lo que
  ocurre en las extracciones con avisos.
- **`status`** replica en la base de datos el mismo semáforo que usa la
  pantalla de revisión: `VALID`, `WARNING`, `INVALID`. El volcado solo
  carga los registros `VALID` y `WARNING`; los `INVALID` y las tablas que
  no se pudieron leer se descartan ahí, no en la base de datos.
- **`confidence`** guarda la confianza de la IA cuando ha intervenido
  (`high`/`medium`/`low` traducido a `0.9`/`0.6`/`0.3` porque la columna es
  numérica, no texto).
- **`multiple_entries_suspected`** es la marca de "esta celda podría tener
  dos asignaturas mezcladas" heredada tal cual del extractor.
- **`day_of_week` es texto libre, no un `ENUM`.** Se decidió así para no
  tener que migrar el esquema si algún PDF trae un valor inesperado; la
  normalización ("MIÉRCOLES" → "Miércoles") se hace en el código del
  volcado, no en la base de datos.

---

## 5. La relación aula↔sesión: por qué hace falta una tabla intermedia

Una clase puede impartirse en **0, 1 o varias aulas a la vez** (por
ejemplo, una clase con teoría y prácticas separadas físicamente pero en el
mismo horario). Eso es una relación **muchos-a-muchos**, que en un modelo
relacional no se puede resolver con una simple columna `room_id` en
`class_sessions`: hace falta una tabla puente.

```sql
CREATE TABLE class_session_rooms (
    class_session_id UUID NOT NULL,
    room_id UUID NOT NULL,
    PRIMARY KEY (class_session_id, room_id),
    ...
    ON DELETE CASCADE  -- en ambos sentidos
);
```

Aquí, a diferencia de las tablas de catálogo, las FK son `ON DELETE
CASCADE`: si se borra la sesión (porque se vuelve a volcar todo desde
cero), las filas de esta tabla puente no tienen ningún sentido por sí
solas y se van con ella. Lo mismo pasa si se borra un aula.

---

## 6. Trazabilidad: `extraction_metadata` y `extraction_issues`

Estas dos tablas no aportan horario, aportan **auditoría**: de dónde salió
cada sesión y qué se detectó al extraerla.

- **`extraction_metadata`** (1 fila por sesión, `ON DELETE CASCADE` desde
  `class_sessions`): página del PDF, texto crudo de la celda, estrategia
  de extracción usada, índice de la tabla, filas ocupadas, horas "en
  bruto" antes de parsear, y el resultado de la revisión con IA
  (`llm_reviewed`, `llm_confidence`, `llm_note`) si la hubo. Es lo que
  permite, dado un registro raro en la base de datos, volver exactamente
  al PDF y a la celda que lo originó.
- **`extraction_issues`** (0..N filas por sesión, también `ON DELETE
  CASCADE`): un aviso por línea (`WARNING` si venía del extractor,
  `INFO` si es una nota añadida a mano en la revisión). Es el equivalente
  en base de datos de la columna "Problemas" de la pantalla de revisión.

Separar esto de `class_sessions` en dos tablas en vez de meter columnas
sueltas responde a que **cada sesión puede tener varias incidencias** (otra
relación 1:N) y a que los metadatos de extracción son bastante voluminosos
(texto completo de la celda) y solo interesan cuando alguien investiga un
dato dudoso, no en el uso normal de "consultar mi horario".

---

## 7. Los índices: para qué sirve cada uno

```sql
CREATE INDEX idx_subjects_name        ON subjects(normalized_name);
CREATE INDEX idx_subjects_degree      ON subjects(degree_id);
CREATE INDEX idx_subjects_official_code ON subjects(official_code);
CREATE INDEX idx_sessions_subject     ON class_sessions(subject_id);
CREATE INDEX idx_sessions_degree      ON class_sessions(degree_id);
CREATE INDEX idx_sessions_group       ON class_sessions(group_id);
CREATE INDEX idx_sessions_academic_year ON class_sessions(academic_year_id);
CREATE INDEX idx_sessions_day_time    ON class_sessions(day_of_week, time_start);
CREATE INDEX idx_extraction_source    ON extraction_metadata(source_id);
```

Todos apuntan a las columnas por las que realmente se filtra en la
aplicación:

- Los cuatro `idx_sessions_*` sobre `class_sessions` existen porque **el
  gestor de horarios filtra exactamente por eso**: `schedule/db.py` hace
  `WHERE cs.degree_id = ...` para listar asignaturas de una titulación, y
  luego agrupa por asignatura/grupo/año.
- `idx_sessions_day_time` es un índice **compuesto** (día + hora de
  inicio) porque la consulta que más se repite en el uso normal es
  "sesiones de tal día a partir de tal hora" — para eso un índice
  compuesto es mucho más eficiente que dos índices separados.
- `idx_subjects_name` sobre `normalized_name` acelera precisamente la
  búsqueda que hace el volcado para no duplicar asignaturas.
- `idx_extraction_source` ayuda a las consultas de auditoría ("todos los
  registros que vinieron de este PDF").

No hay índice sobre `usuarios.username` porque ya lo trae gratis el
`UNIQUE` de esa columna (en PostgreSQL, un `UNIQUE CONSTRAINT` crea su
propio índice automáticamente).

---

## 8. La tabla de usuarios: por qué está fuera de `createdatabase.sql`

`usuarios` no nació con el resto del esquema — se creó a mano, mucho más
simple, solo para tener un login. Con el tiempo hubo que ampliarla en dos
pasos, cada uno con su propio fichero de migración (para no reescribir la
tabla original):

1. **`db/003_usuarios_auth.sql`**: pasa de guardar la contraseña **en
   claro** a guardar un **hash** (`password_hash`, con pbkdf2 vía
   `werkzeug`), y añade `is_admin` (rol) y `created_at`.
2. **`db/004_usuarios_is_active.sql`**: añade `is_active`, para poder
   registrar cuentas que quedan pendientes de aprobación por un
   administrador antes de poder iniciar sesión.

```sql
-- estado final de usuarios, tras ambas migraciones
id             serial PRIMARY KEY
username       varchar(50) UNIQUE NOT NULL
password_hash  text NOT NULL
is_admin       boolean NOT NULL DEFAULT false
is_active      boolean NOT NULL DEFAULT true
created_at     timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
```

Estas migraciones **no se ejecutan solas** al arrancar el contenedor (a
diferencia de `db/init.sql`, que sí se monta en
`docker-entrypoint-initdb.d` y solo crea la tabla `demo` de prueba): hay
que aplicarlas a mano una vez, con `psql -f`. El código de
`auth/users.py` está escrito para no depender de que esa migración de
datos se haga a mano: si encuentra una contraseña antigua en texto plano,
la reescribe como hash **la primera vez que ese usuario inicia sesión**, y
si la tabla está completamente vacía, se crea sola un usuario
`admin`/`admin`.

`usuarios` y `demo` **no tienen ninguna relación** (ninguna FK) con las
doce tablas de horario, y el volcado nunca las toca.

---

## 9. Cómo se llena de verdad: el volcado (`dbdump/loader.py`)

Tener el esquema no basta; lo interesante es **cómo pasa el JSON revisado a
convertirse en estas filas**. El proceso, disparado desde el botón "Volcar
a base de datos":

1. **Filtra** los registros de `reviewed_schedules.json`: solo entran los
   `VALID` y `WARNING` (los `INVALID` y las tablas omitidas se cuentan
   pero se descartan).
2. **Vacía y recarga** (no hace *upsert* fila a fila): al principio de
   cada volcado hace
   `TRUNCATE class_session_rooms, extraction_issues, extraction_metadata,
   class_sessions, subjects, groups, sources, rooms, academic_years,
   degrees, universities CASCADE`. Esto es intencionado: como los
   catálogos (`degrees`, `subjects`, `rooms`…) se reconstruyen enteros a
   partir del JSON en cada volcado, intentar conservarlos entre volcados
   solo añadiría complejidad de sincronización sin necesidad real en este
   proyecto.
3. **Todo en una única transacción** (`autocommit = False`): si algo falla
   a mitad, se hace `ROLLBACK` y la base de datos queda exactamente como
   estaba antes de empezar. Solo al final, si todo ha ido bien, `COMMIT`.
4. **Normaliza al vuelo** mientras inserta: el día de la semana a
   mayúscula inicial con tilde correcta, la hora a `HH:MM`, el curso
   académico `"2026/2027"` a `(2026, 2027)`, y el nombre de asignatura a
   su versión "normalizada" para el catálogo — todo en Python, antes del
   `INSERT`, nunca con lógica dentro de la base de datos.
5. Usa **cachés en memoria** (diccionarios Python) para no repetir un
   `SELECT` de "¿existe ya esta titulación?" en cada una de las ~2500
   filas: la primera vez que aparece una titulación se inserta y se
   recuerda su `id`; las siguientes veces se reutiliza directamente.

En resumen: **la base de datos es un espejo fiel y desechable del último
JSON revisado**, no una fuente de verdad que se vaya editando poco a poco
con el tiempo — la fuente de verdad, mientras se está corrigiendo, es
siempre el fichero `reviewed_schedules.json`.

---

## 10. Lo que el diseño deja abierto (limitaciones conocidas)

Por transparencia, esto es lo que el esquema contempla pero el código
**no** llega a usar hoy:

- `study_plans` existe pero nunca se rellena — no hay ese dato en los PDF.
- `subjects.ects`, `subjects.subject_type`, `subjects.language`,
  `rooms.building`, `rooms.campus` están en el esquema pero el volcado no
  los calcula ni los pide al PDF.
- `sources.file_hash` y `sources.source_url` se quedan vacíos: el volcado
  solo rellena `file_name`.
- No hay ninguna pantalla de administración para editar el catálogo
  (titulaciones, aulas…) a mano; todo se genera desde el volcado.

Ninguno de estos puntos rompe nada — son columnas `NULL`-ables pensadas
para un futuro en el que se disponga de esos datos (por ejemplo, si se
llegara a cruzar con el catálogo oficial de la universidad), pero hoy se
quedan vacías.

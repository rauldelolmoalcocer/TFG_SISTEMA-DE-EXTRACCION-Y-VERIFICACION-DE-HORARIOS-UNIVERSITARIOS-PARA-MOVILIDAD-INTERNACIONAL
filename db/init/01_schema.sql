-- =========================================================
-- 01_schema.sql
--
-- Esquema completo de la base de datos "tfg", generado automáticamente
-- por PostgreSQL la primera vez que arranca sobre un volumen vacío
-- (mecanismo estándar /docker-entrypoint-initdb.d de la imagen oficial
-- postgres:15 -- ver docker-compose.yml, servicio "db").
--
-- Contenido: extensiones + tabla heredada "demo" (usada solo por el
-- endpoint GET /demo, backend-fastapi/app/main.py) + el esquema
-- académico/horarios completo (universidades, titulaciones, planes de
-- estudio, asignaturas, años académicos, grupos, aulas, fuentes,
-- sesiones de clase y sus metadatos/incidencias de extracción), que
-- rellena backend-fastapi/app/dbdump/loader.py y consulta
-- backend-fastapi/app/schedule/db.py. Es el mismo contenido que ya
-- existía en db/createdatabase.sql (aplicado hasta ahora a mano), sin
-- ningún cambio de nombres de tabla/columna: se ha verificado contra
-- cada INSERT/SELECT del backend antes de copiarlo aquí.
--
-- La tabla de usuarios/autenticación se crea aparte, en 02_usuarios.sql.
--
-- IMPORTANTE: este script solo se ejecuta en la inicialización de un
-- volumen "pgdata" vacío. No se vuelve a ejecutar en arranques
-- posteriores del contenedor, y no debe contener ninguna sentencia
-- destructiva (no hay DROP/TRUNCATE aquí).
-- =========================================================


-- =========================================================
-- EXTENSIONES
-- =========================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- =========================================================
-- TABLA HEREDADA "demo"
-- Usada únicamente por GET /demo (backend-fastapi/app/main.py:313-324),
-- endpoint de prueba de conexión backend -> PostgreSQL. Se mantiene tal
-- cual estaba en db/init.sql para no romper ese endpoint.
-- =========================================================

CREATE TABLE demo (
    id SERIAL PRIMARY KEY,
    mensaje TEXT NOT NULL
);

INSERT INTO demo (mensaje) VALUES ('Hola desde PostgreSQL → FastAPI → Flask');


-- =========================================================
-- UNIVERSIDADES
-- =========================================================

CREATE TABLE universities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    name TEXT NOT NULL,
    acronym VARCHAR(50) NULL,
    country VARCHAR(100) NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- =========================================================
-- TITULACIONES
-- =========================================================

CREATE TABLE degrees (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    university_id UUID NULL,

    official_code VARCHAR(100) NULL,
    name TEXT NOT NULL,
    degree_type VARCHAR(50) NULL,   -- GRADO, MASTER, DOBLE GRADO, etc.

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_degree_university
        FOREIGN KEY (university_id)
        REFERENCES universities(id)
        ON DELETE SET NULL
);


-- =========================================================
-- PLANES DE ESTUDIO
-- =========================================================

CREATE TABLE study_plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    degree_id UUID NOT NULL,

    official_code VARCHAR(100) NULL,
    name TEXT NULL,

    start_year INTEGER NULL,
    end_year INTEGER NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_study_plan_degree
        FOREIGN KEY (degree_id)
        REFERENCES degrees(id)
        ON DELETE CASCADE
);


-- =========================================================
-- ASIGNATURAS
-- =========================================================

CREATE TABLE subjects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    degree_id UUID NULL,
    study_plan_id UUID NULL,

    -- Código externo/oficial.
    -- NUNCA se utiliza como PK.
    official_code VARCHAR(100) NULL,

    name TEXT NULL,
    normalized_name TEXT NULL,

    ects NUMERIC(4,1) NULL,

    course INTEGER NULL,
    semester INTEGER NULL,

    subject_type VARCHAR(50) NULL,
    -- BASICA / OBLIGATORIA / OPTATIVA / etc.

    language VARCHAR(100) NULL,

    status VARCHAR(30) NOT NULL DEFAULT 'UNRESOLVED',

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_subject_degree
        FOREIGN KEY (degree_id)
        REFERENCES degrees(id)
        ON DELETE SET NULL,

    CONSTRAINT fk_subject_study_plan
        FOREIGN KEY (study_plan_id)
        REFERENCES study_plans(id)
        ON DELETE SET NULL
);


-- =========================================================
-- AÑOS ACADÉMICOS
-- =========================================================

CREATE TABLE academic_years (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    name VARCHAR(20) NOT NULL,
    -- Ejemplo: 2026/2027

    start_year INTEGER NULL,
    end_year INTEGER NULL,

    CONSTRAINT uq_academic_year_name
        UNIQUE (name)
);


-- =========================================================
-- GRUPOS
-- =========================================================

CREATE TABLE groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    degree_id UUID NULL,
    academic_year_id UUID NULL,

    name VARCHAR(100) NULL,
    -- 1A, 2B, SA1, etc.

    course INTEGER NULL,
    semester INTEGER NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_group_degree
        FOREIGN KEY (degree_id)
        REFERENCES degrees(id)
        ON DELETE SET NULL,

    CONSTRAINT fk_group_academic_year
        FOREIGN KEY (academic_year_id)
        REFERENCES academic_years(id)
        ON DELETE SET NULL
);


-- =========================================================
-- AULAS
-- =========================================================

CREATE TABLE rooms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    university_id UUID NULL,

    code VARCHAR(100) NULL,
    -- NA7, NL8, SA6, OA3...

    name TEXT NULL,
    building TEXT NULL,
    campus TEXT NULL,

    CONSTRAINT fk_room_university
        FOREIGN KEY (university_id)
        REFERENCES universities(id)
        ON DELETE SET NULL
);


-- =========================================================
-- FUENTES
-- =========================================================

CREATE TABLE sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    university_id UUID NULL,

    file_name TEXT NOT NULL,
    file_hash TEXT NULL,

    source_url TEXT NULL,

    academic_year_id UUID NULL,

    generated_at TIMESTAMP NULL,
    imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_source_university
        FOREIGN KEY (university_id)
        REFERENCES universities(id)
        ON DELETE SET NULL,

    CONSTRAINT fk_source_academic_year
        FOREIGN KEY (academic_year_id)
        REFERENCES academic_years(id)
        ON DELETE SET NULL
);


-- =========================================================
-- SESIONES / CLASES EXTRAÍDAS DEL HORARIO
-- =========================================================

CREATE TABLE class_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Puede ser NULL si todavía no hemos podido
    -- identificar correctamente la asignatura.
    subject_id UUID NULL,

    degree_id UUID NULL,
    group_id UUID NULL,
    academic_year_id UUID NULL,

    -- Conservamos SIEMPRE lo extraído originalmente.
    subject_name_raw TEXT NULL,
    group_name_raw TEXT NULL,
    room_raw TEXT NULL,

    day_of_week VARCHAR(20) NULL,

    time_start TIME NULL,
    time_end TIME NULL,

    semester INTEGER NULL,

    class_type VARCHAR(50) NULL,
    -- GG, GP, laboratorio, teoría...

    status VARCHAR(30) NOT NULL DEFAULT 'UNRESOLVED',

    confidence NUMERIC(5,4) NULL,

    multiple_entries_suspected BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_session_subject
        FOREIGN KEY (subject_id)
        REFERENCES subjects(id)
        ON DELETE SET NULL,

    CONSTRAINT fk_session_degree
        FOREIGN KEY (degree_id)
        REFERENCES degrees(id)
        ON DELETE SET NULL,

    CONSTRAINT fk_session_group
        FOREIGN KEY (group_id)
        REFERENCES groups(id)
        ON DELETE SET NULL,

    CONSTRAINT fk_session_academic_year
        FOREIGN KEY (academic_year_id)
        REFERENCES academic_years(id)
        ON DELETE SET NULL
);


-- =========================================================
-- RELACIÓN SESIÓN <-> AULAS
-- Permite que una clase pueda tener 0, 1 o varias aulas.
-- =========================================================

CREATE TABLE class_session_rooms (
    class_session_id UUID NOT NULL,
    room_id UUID NOT NULL,

    PRIMARY KEY (class_session_id, room_id),

    CONSTRAINT fk_session_room_session
        FOREIGN KEY (class_session_id)
        REFERENCES class_sessions(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_session_room_room
        FOREIGN KEY (room_id)
        REFERENCES rooms(id)
        ON DELETE CASCADE
);


-- =========================================================
-- INFORMACIÓN DE EXTRACCIÓN
-- =========================================================

CREATE TABLE extraction_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    class_session_id UUID NOT NULL,
    source_id UUID NULL,

    page INTEGER NULL,

    raw_text TEXT NULL,

    extraction_strategy VARCHAR(100) NULL,

    table_index INTEGER NULL,

    row_start INTEGER NULL,
    row_end INTEGER NULL,

    time_start_raw TEXT NULL,
    time_end_raw TEXT NULL,

    llm_reviewed BOOLEAN NOT NULL DEFAULT FALSE,
    llm_confidence NUMERIC(5,4) NULL,
    llm_note TEXT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_extraction_session
        FOREIGN KEY (class_session_id)
        REFERENCES class_sessions(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_extraction_source
        FOREIGN KEY (source_id)
        REFERENCES sources(id)
        ON DELETE SET NULL
);


-- =========================================================
-- PROBLEMAS / INCIDENCIAS DETECTADAS
-- =========================================================

CREATE TABLE extraction_issues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    class_session_id UUID NOT NULL,

    severity VARCHAR(20) NULL,
    -- INFO / WARNING / ERROR

    issue_code VARCHAR(100) NULL,

    message TEXT NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_issue_session
        FOREIGN KEY (class_session_id)
        REFERENCES class_sessions(id)
        ON DELETE CASCADE
);


-- =========================================================
-- ÍNDICES
-- =========================================================

CREATE INDEX idx_subjects_name
    ON subjects(normalized_name);

CREATE INDEX idx_subjects_degree
    ON subjects(degree_id);

CREATE INDEX idx_subjects_official_code
    ON subjects(official_code);

CREATE INDEX idx_sessions_subject
    ON class_sessions(subject_id);

CREATE INDEX idx_sessions_degree
    ON class_sessions(degree_id);

CREATE INDEX idx_sessions_group
    ON class_sessions(group_id);

CREATE INDEX idx_sessions_academic_year
    ON class_sessions(academic_year_id);

CREATE INDEX idx_sessions_day_time
    ON class_sessions(day_of_week, time_start);

CREATE INDEX idx_extraction_source
    ON extraction_metadata(source_id);

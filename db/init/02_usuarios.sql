-- =========================================================
-- 02_usuarios.sql
--
-- Tabla de autenticación/gestión de usuarios. Hasta ahora esta tabla no
-- tenía ningún CREATE TABLE versionado en el repositorio: existía ya en
-- la base de datos de despliegue (creada a mano en algún momento) y solo
-- estaban versionadas dos migraciones incrementales sobre ella
-- (db/003_usuarios_auth.sql y db/004_usuarios_is_active.sql). Esta es la
-- definición completa y final -- el resultado de aplicar esas dos
-- migraciones sobre la tabla original -- para que una instalación desde
-- cero no dependa de ningún paso manual.
--
-- Columnas verificadas una a una contra cada consulta SQL real de
-- backend-fastapi/app/auth/users.py (todas las funciones: authenticate,
-- ensure_admin_exists, list_users, _insert_user, set_password, set_role,
-- set_active, delete_user, change_own_password):
--   id, username, password_hash, is_admin, is_active, created_at.
--
-- Deliberadamente NO se inserta aquí ningún usuario (ni "admin"/"admin"
-- ni ningún otro): la creación del administrador inicial ya la garantiza
-- el propio backend en tiempo de ejecución
-- (auth/users.py::ensure_admin_exists, invocada en cada POST /login) y no
-- debe duplicarse en SQL para no tener dos mecanismos de arranque
-- distintos que puedan desincronizarse. Este script solo crea la
-- estructura; la tabla queda vacía tras la inicialización.
--
-- IMPORTANTE: este script solo se ejecuta en la inicialización de un
-- volumen "pgdata" vacío. No se vuelve a ejecutar en arranques
-- posteriores del contenedor.
-- =========================================================

CREATE TABLE usuarios (
    id            serial PRIMARY KEY,
    username      varchar(50) NOT NULL UNIQUE,
    password_hash text NOT NULL,
    is_admin      boolean NOT NULL DEFAULT false,
    is_active     boolean NOT NULL DEFAULT true,
    created_at    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

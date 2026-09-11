-- =========================================================
-- Migración de la tabla `usuarios` para autenticación real
-- (hash de contraseña) y gestión de usuarios con rol.
--
-- La tabla `usuarios` original era:
--     id serial PK, username varchar(50) UNIQUE, password varchar(100)
--
-- Idempotente: se puede ejecutar varias veces sin error.
-- Aplicar con:
--   wsl -e docker exec -i tfg_db psql -U postgres -d tfg -f - < db/003_usuarios_auth.sql
-- =========================================================

-- 1) Renombrar `password` -> `password_hash` (solo si aún no se hizo)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'usuarios' AND column_name = 'password'
    ) THEN
        ALTER TABLE usuarios RENAME COLUMN password TO password_hash;
    END IF;
END $$;

-- 2) El hash de werkzeug no cabe en varchar(100): pasar a text
ALTER TABLE usuarios ALTER COLUMN password_hash TYPE text;

-- 3) Rol y fecha de alta
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS is_admin boolean NOT NULL DEFAULT false;
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- Nota: las contraseñas antiguas en claro se reescriben como hash
-- automáticamente la primera vez que ese usuario inicia sesión
-- (ver app/auth/users.py::authenticate). Y el primer usuario que inicia
-- sesión cuando no hay ningún administrador se convierte en administrador.

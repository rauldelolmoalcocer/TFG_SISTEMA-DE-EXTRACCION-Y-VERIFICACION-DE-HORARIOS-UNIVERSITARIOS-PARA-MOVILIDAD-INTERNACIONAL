-- =========================================================
-- Registro público con aprobación: estado activo/inactivo de la cuenta.
--
-- - Usuarios ya existentes: activos (DEFAULT true).
-- - Alta desde el panel de administración: activa.
-- - Alta por registro público (/register): inactiva -> el usuario no
--   puede iniciar sesión hasta que un administrador la aprueba.
--
-- Idempotente. Aplicar con:
--   wsl -e docker exec -i tfg_db psql -U postgres -d tfg -f - < db/004_usuarios_is_active.sql
-- =========================================================

ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true;

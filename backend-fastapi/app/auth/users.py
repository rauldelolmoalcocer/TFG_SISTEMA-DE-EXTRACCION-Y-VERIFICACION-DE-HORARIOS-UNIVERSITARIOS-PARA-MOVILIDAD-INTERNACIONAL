# app/auth/users.py
#
# Autenticación y gestión de usuarios. La tabla `usuarios` es:
#   id serial PK, username varchar(50) UNIQUE, password_hash text,
#   is_admin boolean, created_at timestamp
# (ver db/003_usuarios_auth.sql).
#
# Contraseñas: hash con werkzeug (pbkdf2). Se admite una contraseña
# antigua en claro una sola vez -- al iniciar sesión con ella se
# reescribe ya como hash ("upgrade on login"), así no hace falta migrar
# los datos existentes a mano.
#
# Arranque: si la tabla está vacía se crea admin/admin; y el primer
# usuario que inicia sesión cuando todavía no hay ningún administrador se
# convierte en administrador. Con eso el sistema nunca se queda sin acceso.

import re

from werkzeug.security import check_password_hash, generate_password_hash

MIN_PASSWORD_LEN = 4
USERNAME_RE = re.compile(r"^[A-Za-z0-9._@-]{3,50}$")

BOOTSTRAP_USERNAME = "admin"
BOOTSTRAP_PASSWORD = "admin"

_HASH_PREFIXES = ("pbkdf2:", "scrypt:", "argon2")


class UserError(Exception):
    """Error de dominio (usuario duplicado, contraseña corta, último
    administrador, etc.). main.py lo traduce a HTTP 400."""


# ==========================================
# HASHING
# ==========================================

def hash_password(password: str) -> str:
    return generate_password_hash(password, method="pbkdf2:sha256")


def _looks_hashed(value: str) -> bool:
    return isinstance(value, str) and value.startswith(_HASH_PREFIXES)


def _verify(stored: str, password: str) -> bool:
    if _looks_hashed(stored):
        return check_password_hash(stored, password)
    # Contraseña antigua en claro.
    return stored == password


# ==========================================
# VALIDACIÓN
# ==========================================

def _validate_username(username: str) -> str:
    username = (username or "").strip()
    if not USERNAME_RE.match(username):
        raise UserError(
            "El nombre de usuario debe tener entre 3 y 50 caracteres "
            "(letras, dígitos y . _ - @)."
        )
    return username


def _validate_password(password: str):
    if not password or len(password) < MIN_PASSWORD_LEN:
        raise UserError(
            f"La contraseña debe tener al menos {MIN_PASSWORD_LEN} caracteres."
        )


# ==========================================
# ARRANQUE / BOOTSTRAP
# ==========================================

def ensure_admin_exists(conn) -> None:
    """Si la tabla `usuarios` está totalmente vacía, crea admin/admin para
    que exista al menos un acceso. No hace nada si ya hay algún usuario."""
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM usuarios")
    if cur.fetchone()[0] == 0:
        cur.execute(
            "INSERT INTO usuarios (username, password_hash, is_admin) "
            "VALUES (%s, %s, TRUE)",
            (BOOTSTRAP_USERNAME, hash_password(BOOTSTRAP_PASSWORD)),
        )
        conn.commit()


# ==========================================
# AUTENTICACIÓN
# ==========================================

def authenticate(conn, username: str, password: str):
    """Devuelve {id, username, is_admin} si las credenciales son válidas,
    None si el usuario o la contraseña no son correctos, o lanza UserError
    si la contraseña es correcta pero la cuenta todavía no está activa
    (pendiente de aprobación o vetada por un administrador).

    Efectos secundarios seguros: reescribe una contraseña antigua en claro
    como hash, y promueve al primer usuario a administrador si todavía no
    hay ninguno."""
    username = (username or "").strip()
    if not username or not password:
        return None

    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, password_hash, is_admin, is_active "
        "FROM usuarios WHERE username = %s",
        (username,),
    )
    row = cur.fetchone()
    if row is None:
        return None

    user_id, uname, stored, is_admin, is_active = row

    if not _verify(stored, password):
        return None

    if not is_active:
        raise UserError(
            "Tu cuenta todavía no está activa. Un administrador debe aprobarla."
        )

    # Migración transparente: contraseña en claro -> hash.
    if not _looks_hashed(stored):
        cur.execute(
            "UPDATE usuarios SET password_hash = %s WHERE id = %s",
            (hash_password(password), user_id),
        )
        conn.commit()

    # Si no hay ningún administrador todavía, este usuario lo pasa a ser.
    if not is_admin:
        cur.execute("SELECT count(*) FROM usuarios WHERE is_admin AND is_active")
        if cur.fetchone()[0] == 0:
            cur.execute("UPDATE usuarios SET is_admin = TRUE WHERE id = %s", (user_id,))
            conn.commit()
            is_admin = True

    return {"id": user_id, "username": uname, "is_admin": bool(is_admin)}


# ==========================================
# GESTIÓN (solo administradores)
# ==========================================

def list_users(conn) -> list[dict]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, is_admin, is_active, created_at "
        "FROM usuarios ORDER BY is_active, is_admin DESC, username"
    )
    return [
        {
            "id": r[0],
            "username": r[1],
            "is_admin": bool(r[2]),
            "is_active": bool(r[3]),
            "created_at": r[4].strftime("%Y-%m-%d %H:%M") if r[4] else None,
        }
        for r in cur.fetchall()
    ]


def _insert_user(conn, username, password, is_admin, is_active) -> dict:
    username = _validate_username(username)

    cur = conn.cursor()
    cur.execute("SELECT 1 FROM usuarios WHERE username = %s", (username,))
    if cur.fetchone():
        raise UserError(f"Ya existe un usuario con el nombre «{username}».")

    _validate_password(password)

    cur.execute(
        "INSERT INTO usuarios (username, password_hash, is_admin, is_active) "
        "VALUES (%s, %s, %s, %s) RETURNING id, created_at",
        (username, hash_password(password), bool(is_admin), bool(is_active)),
    )
    new_id, created_at = cur.fetchone()
    conn.commit()
    return {
        "id": new_id,
        "username": username,
        "is_admin": bool(is_admin),
        "is_active": bool(is_active),
        "created_at": created_at.strftime("%Y-%m-%d %H:%M") if created_at else None,
    }


def create_user(conn, username: str, password: str, is_admin: bool) -> dict:
    """Alta desde el panel de administración: la cuenta queda activa."""
    return _insert_user(conn, username, password, is_admin, is_active=True)


def register_user(conn, username: str, password: str) -> dict:
    """Alta por registro público: usuario normal y cuenta INACTIVA hasta
    que un administrador la aprueba."""
    return _insert_user(conn, username, password, is_admin=False, is_active=False)


def _get_user(conn, user_id: int):
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, is_admin, is_active FROM usuarios WHERE id = %s",
        (user_id,),
    )
    return cur.fetchone()


def _active_admin_count(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM usuarios WHERE is_admin AND is_active")
    return cur.fetchone()[0]


def set_password(conn, user_id: int, new_password: str) -> None:
    _validate_password(new_password)
    if _get_user(conn, user_id) is None:
        raise UserError("El usuario no existe.")
    cur = conn.cursor()
    cur.execute(
        "UPDATE usuarios SET password_hash = %s WHERE id = %s",
        (hash_password(new_password), user_id),
    )
    conn.commit()


def set_role(conn, user_id: int, is_admin: bool) -> None:
    user = _get_user(conn, user_id)
    if user is None:
        raise UserError("El usuario no existe.")

    # user = (id, username, is_admin, is_active)
    if not is_admin and user[2] and user[3] and _active_admin_count(conn) <= 1:
        raise UserError("No se puede quitar el rol al último administrador activo.")

    cur = conn.cursor()
    cur.execute("UPDATE usuarios SET is_admin = %s WHERE id = %s", (bool(is_admin), user_id))
    conn.commit()


def set_active(conn, user_id: int, is_active: bool, acting_user_id=None) -> None:
    """Aprobar (is_active=True) o vetar (is_active=False) una cuenta."""
    user = _get_user(conn, user_id)
    if user is None:
        raise UserError("El usuario no existe.")

    if not is_active:
        if acting_user_id is not None and int(user_id) == int(acting_user_id):
            raise UserError("No puedes vetar tu propia cuenta.")
        if user[2] and user[3] and _active_admin_count(conn) <= 1:
            raise UserError("No se puede vetar al último administrador activo.")

    cur = conn.cursor()
    cur.execute("UPDATE usuarios SET is_active = %s WHERE id = %s", (bool(is_active), user_id))
    conn.commit()


def delete_user(conn, user_id: int, acting_user_id: int) -> None:
    user = _get_user(conn, user_id)
    if user is None:
        raise UserError("El usuario no existe.")

    if acting_user_id is not None and int(user_id) == int(acting_user_id):
        raise UserError("No puedes eliminar tu propia cuenta.")

    if user[2] and user[3] and _active_admin_count(conn) <= 1:
        raise UserError("No se puede eliminar al último administrador activo.")

    cur = conn.cursor()
    cur.execute("DELETE FROM usuarios WHERE id = %s", (user_id,))
    conn.commit()


def change_own_password(conn, user_id: int, current_password: str, new_password: str) -> None:
    cur = conn.cursor()
    cur.execute("SELECT password_hash FROM usuarios WHERE id = %s", (user_id,))
    row = cur.fetchone()
    if row is None:
        raise UserError("El usuario no existe.")

    if not _verify(row[0], current_password or ""):
        raise UserError("La contraseña actual no es correcta.")

    _validate_password(new_password)
    cur.execute(
        "UPDATE usuarios SET password_hash = %s WHERE id = %s",
        (hash_password(new_password), user_id),
    )
    conn.commit()

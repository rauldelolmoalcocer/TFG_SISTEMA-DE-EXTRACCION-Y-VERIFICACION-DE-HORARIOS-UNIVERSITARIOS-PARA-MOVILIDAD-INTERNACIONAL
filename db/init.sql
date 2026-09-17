-- ⚠ OBSOLETO / SIN USO: docker-compose.yml ya NO monta este fichero.
-- Sustituido por db/init/01_schema.sql (mismo contenido de "demo",
-- además de todo el esquema académico y de usuarios). Se conserva aquí
-- solo como referencia histórica; no se ejecuta en ninguna instalación
-- nueva. Ver el diagnóstico de inicialización de PostgreSQL en el
-- historial de conversación / memoria del proyecto para el detalle.

CREATE TABLE demo (
    id SERIAL PRIMARY KEY,
    mensaje TEXT NOT NULL
);

INSERT INTO demo (mensaje) VALUES ('Hola desde PostgreSQL → FastAPI → Flask');
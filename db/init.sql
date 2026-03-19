CREATE TABLE demo (
    id SERIAL PRIMARY KEY,
    mensaje TEXT NOT NULL
);

INSERT INTO demo (mensaje) VALUES ('Hola desde PostgreSQL → FastAPI → Flask');
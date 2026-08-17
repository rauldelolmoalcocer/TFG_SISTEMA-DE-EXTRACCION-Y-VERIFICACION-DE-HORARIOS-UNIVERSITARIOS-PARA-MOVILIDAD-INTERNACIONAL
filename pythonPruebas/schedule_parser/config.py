# config.py

INPUT_DIR = r"C:\Users\rauldelolmo123\Desktop\Pruebas\downloads_test"
OUTPUT_DIR = r"C:\Users\rauldelolmo123\Desktop\Pruebas\parsed_output"

GLOBAL_OUTPUT_FILE = "all_schedules.json"

DAYS = [
    "LUNES",
    "MARTES",
    "MIERCOLES",
    "JUEVES",
    "VIERNES"
]

# Distancia vertical máxima para considerar dos elementos
# pertenecientes a la misma línea.
LINE_Y_TOLERANCE = 4

# Confianza mínima para introducir un registro
# en el dataset principal.
MIN_ACCEPTED_CONFIDENCE = 0.55
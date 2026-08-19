# _pathfix.py
#
# schedule_parser usa imports planos (from validator import ...), no
# imports relativos de paquete. Este helper añade el directorio padre
# al sys.path para que los tests puedan importar esos módulos sin
# reestructurar el proyecto.

import os
import sys

PACKAGE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

if PACKAGE_DIR not in sys.path:
    sys.path.insert(0, PACKAGE_DIR)

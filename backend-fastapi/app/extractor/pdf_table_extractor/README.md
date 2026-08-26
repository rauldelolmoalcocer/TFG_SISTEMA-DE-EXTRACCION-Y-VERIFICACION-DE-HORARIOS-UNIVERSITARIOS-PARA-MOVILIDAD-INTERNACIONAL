# pdf_table_extractor

Paquete Python de **diagnóstico + normalización** para investigar qué
estructura es capaz de recuperar
[pdfplumber](https://github.com/jsvine/pdfplumber) de PDFs de horarios
universitarios, sin asumir un formato fijo, y convertirla en registros
planos listos para cargar en una base de datos.

Pensado para instalarse como dependencia de un proyecto más grande
(`pip install -e /ruta/a/pdf_table_extractor` o añadido a su
`pyproject.toml`/`requirements.txt`) y usarse como librería:

```python
from pathlib import Path
from pdf_table_extractor import run_diagnostics, run_normalization

summary = run_diagnostics(Path("input_pdfs"), Path("output"))   # etapa 1
entries, skipped = run_normalization(Path("output"))             # etapa 2
```

o desde la línea de comandos, una vez instalado:

```bash
pdf-extract-diagnose --input-dir input_pdfs --output-dir output
pdf-extract-normalize --output-dir output
```

Tres etapas, la tercera opcional:

1. **Diagnóstico** (`run_diagnostics` / `pdf-extract-diagnose`): para cada
   PDF de la carpeta de entrada, extrae y compara la información
   estructural que pdfplumber detecta bajo distintas estrategias. Escribe
   `output/<pdf>/{metadata,words,tables_*,diagnostic}.json`.
2. **Normalización** (`run_normalization` / `pdf-extract-normalize`): lee
   ese `output/` ya generado y construye, por cada "cuadrante"
   día+franja horaria+asignatura, un registro plano en
   `output/<pdf>/schedule.json` (y `output/all_schedules.json` con todos
   los PDFs juntos, para una carga masiva en BD).
3. **Revisión con LLM local, opcional** (`review_entries` / `pdf-extract-llm-review`):
   envía a un modelo abierto corriendo en local (Ollama) solo los cuadrantes
   que la etapa 2 marcó con algún warning (sobre todo
   `multiple_entries_suspected`) para que decida, a partir del
   texto crudo de la celda, si es una sola asignatura mal etiquetada o
   varias solapadas de verdad -- ver la sección dedicada más abajo.

## Por qué

Los PDFs de horarios proceden de distintas facultades/titulaciones y no
comparten formato: tamaño de página, presencia de líneas de tabla, celdas
combinadas, nombres de días, formatos de hora, etc. varían de un documento a
otro. Por eso este proyecto:

- **No** usa reglas de coordenadas absolutas (`x < 100 => lunes`).
- Prueba **varias estrategias** de detección de tablas de pdfplumber contra
  cada página, y dentro de cada estrategia contra cada PDF.
- Registra observaciones (celdas combinadas, celdas multilínea, filas
  irregulares) como **warnings**, sin intentar "arreglarlas" con
  heurísticas agresivas ni inventar contenido.

## Instalación

```bash
cd pdf_table_extractor
python -m venv .venv
.venv\Scripts\activate   # Windows
pip install -e .
```

Requiere Python 3.11+. `pip install -e .` deja el paquete `pdf_table_extractor`
importable y registra los dos comandos `pdf-extract-diagnose`/`pdf-extract-normalize`
en el entorno -- es la misma instalación que usaría un proyecto más grande
que dependa de este paquete.

## Uso

Como librería (ver ejemplo arriba), o por CLI:

1. Pon los PDFs a analizar en una carpeta, p. ej. `input_pdfs/`.
2. Ejecuta:

```bash
pdf-extract-diagnose --input-dir input_pdfs --output-dir output
pdf-extract-normalize --output-dir output
```

3. Revisa `output/<nombre_pdf>/` para cada PDF procesado, `output/summary.json`
   para las estadísticas agregadas del diagnóstico, y `output/all_schedules.json`
   para los cuadrantes normalizados de todos los PDFs juntos.

## Salida generada

Por cada PDF, en `output/<nombre_pdf>/`:

| Archivo                  | Contenido                                                             |
| ------------------------ | ---------------------------------------------------------------------- |
| `metadata.json`          | Nº de páginas, tamaño de cada página, texto extraído (`extract_text`). |
| `words.json`             | Palabras detectadas con su bounding box (`extract_words`).             |
| `tables_lines.json`      | Tablas detectadas con la estrategia `lines` (líneas visibles).         |
| `tables_text.json`       | Tablas detectadas con la estrategia `text` (alineación de texto).      |
| `tables_combined.json`   | Tablas detectadas con la estrategia combinada (líneas + texto).        |
| `diagnostic.json`        | Resumen estructurado por página/estrategia + warnings.                 |
| `diagnostic_report.txt`  | El mismo resumen en formato de informe legible.                        |
| `schedule.json`          | Cuadrantes normalizados (día/hora/asignatura/aula), uno por clase.     |
| `skipped_tables.json`    | Tablas/páginas que no produjeron ningún cuadrante, y por qué.          |

Y en la raíz de `output/`:

- `summary.json` -- estadísticas agregadas del diagnóstico (etapa 1).
- `all_schedules.json` -- los `schedule.json` de todos los PDFs
  concatenados en una sola lista, lista para una carga masiva en BD.
- `skipped_tables.json` -- los `skipped_tables.json` de todos los PDFs
  concatenados.

## Estrategias probadas

Definidas en `src/pdf_table_extractor/extractor/strategies.py`:

- **`lines`**: `vertical_strategy = horizontal_strategy = "lines"`.
- **`text`**: `vertical_strategy = horizontal_strategy = "text"`.
- **`combined`**: `vertical_strategy = "lines"`, `horizontal_strategy = "text"`
  (para PDFs que solo dibujan líneas en un eje).

Ninguna estrategia se asume "la buena": el "best candidate" por página se
elige con una puntuación genérica (`diagnostics.analyzer._strategy_quality`),
pero **todas** las estrategias se guardan siempre para poder compararlas,
gane o no la puntuación.

La puntuación usa la media de **caracteres por celda no vacía**, penalizada
solo por filas irregulares (`jagged_row`). Se probaron y descartaron dos
alternativas más obvias, validadas contra los 33 PDFs de `input_pdfs/` (no
solo el PDF de ejemplo):

- Contar celdas no vacías en bruto premia la sobre-segmentación: partir una
  celda real en diez fragmentos siempre "parece" más rico.
- Comparar el texto capturado contra **todo el texto de la página** castiga
  injustamente a una tabla bien delimitada que, con razón, no incluye texto
  que está fuera de ella (un título encima de la rejilla, una nota debajo).

Con la métrica final, la estrategia correcta gana en el 95% de las 205
páginas de prueba; el resto son casos legítimos donde la página no es
realmente una rejilla de horario (texto informativo).

## Celdas combinadas / multilínea

pdfplumber no expone directamente "rowspan"/"colspan". Esta primera versión:

- Detecta celdas con `"\n"` en el contenido → warning `multiline_cell`.
- Detecta filas cuyo nº de celdas no coincide con la cabecera → warning
  `jagged_row`.
- Detecta celdas cuyo bounding box coincide con el de la fila anterior en la
  misma columna (indicio de fusión vertical) → warning `possible_merged_cell`.
- Detecta columnas cuyo contenido se repite en la fila inmediatamente
  anterior → warning `repeated_cell_value`: cuando una rejilla con líneas
  corta una celda fusionada verticalmente en varias franjas, pdfplumber
  repite el texto en cada franja en lugar de dejarlo en blanco. Es la señal
  más fiable de una asignatura que ocupa varias horas.

Estos casos se **registran**, no se "arreglan": el objetivo de esta fase es
diagnosticar, no normalizar.

## Diagnóstico de cobertura de página

Además de comparar estrategias entre sí, cada página comprueba si la
estrategia ganadora capturó una fracción razonable del texto que
`extract_words()` detectó en toda la página (`LOW_COVERAGE_THRESHOLD` en
`src/pdf_table_extractor/diagnostics/analyzer.py`). Si una página con texto significativo cae por
debajo del umbral, se añade un warning `Low text coverage`: es la señal de
que la "tabla" detectada puede ser espuria (p. ej. un único fragmento de una
página que en realidad es texto corrido, no una rejilla de horario) y que
conviene revisar el texto plano en `metadata.json` en vez de fiarse de la
tabla. El umbral es deliberadamente conservador (pocos falsos positivos):
en el corpus de prueba, las páginas con cobertura baja pero legítima
(una tabla bien detectada con un título fuera de ella) no lo disparan.

## Normalización → `schedule.json` (`run_normalization`)

Convierte la tabla ganadora de cada página en una lista plana de
**cuadrantes** (día + franja horaria + asignatura), un JSON por elemento:

```json
{
  "source_file": "Semestre1_GII.pdf",
  "page": 1,
  "strategy": "lines",
  "table_index": 0,
  "row_span": [1, 4],
  "degree": "GRADO EN INGENIERÍA INFORMÁTICA (G781) (GII)",
  "group": "1ºA",
  "semester": 1,
  "course_year": "2026/2027",
  "day": "LUNES",
  "time_start": "08:00",
  "time_end": "11:55",
  "time_start_raw": "8:00/8:55",
  "time_end_raw": "11:00/11:55",
  "subject_raw": "FÍSICA - 1A\n(NA7)",
  "subject_name": "FÍSICA - 1A",
  "rooms": ["NA7"],
  "non_room_annotations": [],
  "multiple_entries_suspected": false,
  "warnings": []
}
```

Cómo se construye cada campo, y qué es genérico frente a heurístico:

- **`day`**: literalmente el texto de la cabecera de esa columna (fila 0 de
  la tabla). Nunca se compara contra una lista fija de nombres de días para
  decidir *qué* día es -- pero antes de confiar en que la fila 0 sea
  realmente una cabecera, sí se comprueba que contenga nombres de día
  reconocibles (`_looks_like_day_header`, vocabulario de calendario
  universal: lunes...domingo, en español e inglés, sin acentos). Sin este
  chequeo, una tabla cuya fila 0 en realidad es una línea de título o
  contenido de asignatura (visto en `Semestre1Transversales.pdf` y en
  varias páginas más, ~18 tablas del corpus) contaminaba silenciosamente el
  campo `day` de todos sus registros. Si la fila 0 no pasa el chequeo, la
  tabla entera se descarta.
- **`time_start`/`time_end`**: se colapsan las filas consecutivas que
  representan la misma franja horaria en bloques anclados a la primera
  columna con una etiqueta de hora no vacía (`_time_blocks` en
  `schedule_builder.py`) -- esto cubre tanto una asignatura de varias horas
  (texto repetido en varias filas) como una rejilla con líneas más finas que
  el contenido semántico (nombre de asignatura y aula cayendo en filas
  físicas distintas, visto en `Semestre2_INFOADE.pdf`). Una etiqueta que no
  parsea como hora (`src/pdf_table_extractor/normalizer/time_parser.py`) dispara un warning en vez
  de inventarse un valor. El propio corpus reveló dos erratas reales de
  formato (`18_00` por `18:00`, `19:00:20:00` por `19:00-20:00`) que
  ampliaron el conjunto de separadores aceptados -- no listas específicas
  de un PDF, sino tolerancia a variantes de puntuación en HH:MM.
- **`degree`/`group`/`semester`/`course_year`**: extraídos con expresiones
  regulares del texto plano de la página (`src/pdf_table_extractor/normalizer/header_parser.py`).
  Es el único punto de todo el pipeline que asume vocabulario académico en
  español ("GRADO", "MÁSTER", "CUATRIMESTRE", "GRUPO") -- inevitable sin un
  LLM. Se validó contra grados, dobles grados y másters (con y sin
  "GRUPO"); un campo que no aparece se deja en `null`, nunca se adivina.
- **`rooms`**: grupos entre paréntesis del texto de la celda que
  estructuralmente parecen un aula (`src/pdf_table_extractor/normalizer/cell_parser.py`). La regla
  es genérica, no una lista de institutos: en todo el corpus de prueba, un
  código de aula siempre lleva una cifra (`NA7`, `OL24`, `SA1/OL12`),
  mientras que un acrónimo o marcador de grupo nunca la lleva (`GII`,
  `GTIC`, `GG`). Se añade "lab"/"laboratorio" como única palabra de
  vocabulario genérico (no ligada a ninguna titulación).
- **`non_room_annotations`**: los paréntesis que no pasan ese filtro se
  conservan aquí en vez de descartarse -- nunca se pierde información,
  aunque no se sepa qué significan.
- **`multiple_entries_suspected`**: `true` cuando, tras el filtro anterior,
  quedan más de un grupo que sí parece un aula. Es una señal, no una
  decisión: valida bien los solapamientos reales (dos asignaturas en la
  misma celda, cada una con su propia aula con cifra), pero sigue sin ser
  perfecta -- una anotación desconocida que por casualidad contenga una
  cifra (p. ej. un código de curso como `"(2C)"`) puede seguir generando un
  falso positivo. Separar esto al 100% requeriría analizar las posiciones
  de palabras (`words.json`) o ir añadiendo cada anotación nueva a mano
  (dejaría de ser genérico) -- deliberadamente no implementado aquí.
  Antes de este filtro, el corpus de prueba marcaba un 23.9% de los
  cuadrantes como sospechosos; después, un 16.7% -- el resto son
  solapamientos reales o casos no resueltos.

Antes de generar entradas, cada tabla pasa dos chequeos genéricos de
plausibilidad, en este orden:

1. **`_looks_like_day_header`**: la fila 0 debe contener nombres de día
   reconocibles en al menos la mitad de sus celdas.
2. **`_looks_like_schedule_grid`**: la columna 0 debe parsear como hora en
   al menos el 60% de sus filas etiquetadas.

Si cualquiera de los dos falla, la tabla entera se descarta y se registra
por qué -- nunca se generan "asignaturas" a partir de fragmentos de frases,
ni se etiqueta un registro con un día que en realidad es una línea de
título.

Esos descartes **no desaparecen en el log de consola**: cada uno se
guarda en `output/<pdf>/skipped_tables.json` (y agregado en
`output/skipped_tables.json`), con la página, la estrategia, el motivo
(`no_day_header`, `no_time_column`, `no_strategy_selected`, `too_few_rows`,
`table_missing`) y el contenido crudo que provocó el descarte -- para que
"por qué no hay cuadrantes de esta página" sea siempre trazable en un
archivo, no solo algo que se veía al ejecutar. En el corpus de prueba: 21
tablas/páginas descartadas de 205, la mayoría (18) por cabecera sin
nombres de día.

**Resultado final tras los tres fixes de esta sección** (1955 cuadrantes,
de los 33 PDFs de prueba): 83.2% sin ningún warning, 16.7% marcados como
posible doble asignatura, 7.3% sin aula detectada, y solo 2 con hora sin
parsear.

## Revisión con LLM local (opcional) -- `pdf_table_extractor.llm_review`

Etapa 3, **completamente opcional**: nada del paquete base la importa, y
`run_diagnostics`/`run_normalization` funcionan igual sin ella. Usa un
modelo **abierto, en local, vía [Ollama](https://ollama.com)** -- sin API
de pago, sin nube, sin enviar datos fuera del ordenador. Requiere:

1. Instalar el extra `llm` (`pip install "pdf-table-extractor[llm]"`, que
   añade `ollama` + `pydantic`).
2. Tener [Ollama](https://ollama.com) instalado y corriendo
   (`http://localhost:11434` por defecto).
3. Haber descargado el modelo (`ollama pull qwen2.5:3b`, el modelo por
   defecto -- pequeño y rápido, adecuado para este volumen de llamadas
   cortas de clasificación).

**Qué hace:** de los cuadrantes que la etapa 2 ya generó, solo envía al
modelo local los que tienen **algún warning** (sobre todo
`multiple_entries_suspected`, ~16.7% del corpus de prueba) -- los
cuadrantes limpios ni se tocan ni pasan por el modelo. Para cada celda
marcada, el modelo recibe únicamente el texto crudo de esa celda
(`subject_raw`) más las aulas/anotaciones que el parser de reglas ya había
detectado, y decide:

- Si es **una sola asignatura** con una anotación que no era un aula de
  verdad (p. ej. un código de titulación compartida) -> se corrige
  `subject_name`/`rooms` y se limpia el warning.
- Si son **de verdad dos o más asignaturas solapadas** en la misma celda
  -> se genera un registro `ScheduleEntry` independiente por cada una.
- Si **no está seguro** -> lo dice (`confidence: "low"`) y mantiene el
  warning en vez de forzar una respuesta -- esta etapa reduce el residuo
  ambiguo, no promete eliminarlo.

El modelo nunca ve nombres de asignaturas de otras celdas ni conocimiento
externo sobre titulaciones concretas: solo el texto ya extraído de esa
celda. Cada registro tocado queda marcado con `llm_reviewed: true`,
`llm_confidence` y `llm_note` -- para poder auditar qué cambió el modelo y
por qué, nunca una sustitución silenciosa.

```python
from pdf_table_extractor.llm_review import review_entries

reviewed = review_entries(entries)  # usa qwen2.5:3b en localhost:11434 por defecto
```

```bash
pdf-extract-llm-review --output-dir output
# lee output/all_schedules.json, escribe output/all_schedules_reviewed.json
# (el original NUNCA se sobreescribe)
```

Configurable por código o CLI (`--model`, `--host`, `--batch-size`) --
p. ej. para usar otro modelo ya instalado (`ollama pull llama3.2` o un
`deepseek-r1` de razonamiento) o un servidor Ollama en otra máquina.

**Probado de verdad contra Ollama** (no solo con un cliente simulado), 17
cuadrantes marcados de una muestra de 196: separa correctamente los
solapamientos reales en todos los casos inspeccionados, en ~66 s por lote
de 6 celdas, sin ningún reintento. El fallo real que apareció: un modelo de
3B es inconsistente extrayendo el aula al campo correcto -- a menudo la
deja pegada dentro de `subject_name` (p. ej. `"...4A1 (lab)"` con
`room: null`) en vez de separarla. Por eso `_apply_review` **reaplica el
mismo parser de reglas de `cell_parser.py`** sobre el `subject_name` que
devuelve el modelo: baja los aulas sin extraer del ~40% al ~14% -- el 14%
restante son casos donde el modelo omitió el aula del todo (no un fallo de
separación recuperable por regex).

**Por qué lotes de solo 6 celdas por defecto, y no 25 como con un modelo
grande hospedado:** un modelo local de 3B parámetros sigue instrucciones de
formato con mucha menos fiabilidad que uno grande -- pedirle 25 objetos JSON
de una vez aumenta mucho el riesgo de una respuesta cortada o mal formada.
Por el mismo motivo, cada lote tiene **un reintento** con el error de
validación incluido en el mensaje antes de rendirse; si ambos intentos
fallan (modelo no disponible, salida sigue sin validar), ese lote se deja
sin tocar y se registra el fallo -- nunca se descartan entradas
silenciosamente. La respuesta del modelo también se limpia de bloques
`<think>...</think>` (modelos de razonamiento como `deepseek-r1`) y de
texto suelto alrededor del JSON antes de intentar parsearla.

## Estructura del proyecto

```
pdf_table_extractor/
├── pyproject.toml                 # Metadata del paquete, dependencias, entry points de CLI
├── README.md
└── src/pdf_table_extractor/
    ├── __init__.py                 # API pública: run_diagnostics, run_normalization, modelos...
    ├── pipeline.py                 # run_diagnostics() / run_normalization() -- lo que importa un proyecto externo
    ├── cli/
    │   ├── diagnose.py             # Entry point `pdf-extract-diagnose`
    │   ├── normalize.py            # Entry point `pdf-extract-normalize`
    │   └── llm_review.py           # Entry point `pdf-extract-llm-review` (extra `llm`)
    ├── llm_review.py               # Etapa 3 opcional: revisión con LLM local vía Ollama (extra `llm`)
    ├── extractor/
    │   ├── pdf_reader.py           # Apertura de PDF + metadata por página
    │   ├── word_extractor.py       # extract_words()
    │   ├── table_detector.py       # find_tables()/extract_tables() + warnings
    │   └── strategies.py           # Configuraciones de table_settings a probar
    ├── diagnostics/
    │   ├── analyzer.py             # Orquesta lectura + estrategias por PDF
    │   └── report.py               # Informe de texto + escritura de JSON
    ├── normalizer/
    │   ├── time_parser.py          # Rango horario -> ("HH:MM", "HH:MM")
    │   ├── header_parser.py        # Texto de página -> grado/grupo/semestre/curso
    │   ├── cell_parser.py          # Celda -> nombre de asignatura + aula(s)
    │   └── schedule_builder.py     # Agrupa filas/bloques -> ScheduleEntry
    ├── models/
    │   ├── extraction_models.py    # Dataclasses de la representación intermedia
    │   └── schedule_models.py      # Dataclass ScheduleEntry (registro final, incl. llm_reviewed/llm_confidence/llm_note)
    └── utils/json_utils.py         # Serialización UTF-8 de dataclasses
```

`input_pdfs/` y `output/` no se distribuyen con el paquete -- son carpetas
de trabajo que crea quien lo use (por defecto, relativas al directorio
desde el que se ejecute la CLI; ver `--input-dir`/`--output-dir`).

## Próximos pasos (no implementados aquí)

- La separación fiable de `multiple_entries_suspected` ahora tiene una vía
  **opcional** (`pdf_table_extractor.llm_review`, ver arriba). Sin esa
  etapa, sigue siendo un TODO basado en reglas: analizar las posiciones de
  palabras (`words.json`) dentro del bbox de la celda para separar
  sub-columnas sin depender de un LLM.
- Afinar la distinción aula/anotación para los casos raros donde una
  anotación no-aula sí contiene una cifra (p. ej. un código de curso como
  `"(2C)"`), que hoy sigue colándose como falso positivo -- tanto en el
  filtro de reglas como, potencialmente, en la revisión con LLM.
- Cargar `output/all_schedules.json` (o `all_schedules_reviewed.json` si se
  usó la etapa 3) en la base de datos destino y definir el esquema/índices
  (asignatura, día, hora, grado, grupo).

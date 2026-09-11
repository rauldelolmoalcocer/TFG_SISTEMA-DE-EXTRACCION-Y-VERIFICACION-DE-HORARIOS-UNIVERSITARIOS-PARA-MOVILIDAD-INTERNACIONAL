# Auditoría de acciones de interfaz — descubrimiento de casos de uso

Exploración exhaustiva de **todo lo que un actor puede iniciar desde la
interfaz** (botones, formularios, enlaces, JavaScript), sin usar el
catálogo de 14 casos de uso como lista cerrada. Fuentes: cada plantilla en
`frontend-flask/app/templates/` y cada script en `frontend-flask/app/static/*.js`,
contrastados con los endpoints de `backend-fastapi/app/main.py`.

**Conclusión: no aparecen actores ni objetivos de negocio nuevos.** Ningún
óvalo adicional asociado directamente a un actor. Lo que aporta esta
auditoría es la justificación explícita, acción por acción, de por qué
cada botón/menú/filtro concreto no se eleva a caso de uso independiente.

> **Corrección tras revisión (2ª vuelta):** al aplicar el criterio de
> `<<extend>>` con más rigor a todo lo de abajo, tres óvalos que aquí se
> habían fundido en uno solo (Revisión manual, Consulta de horarios,
> Gestión de usuarios) resultaron ser en realidad una **consulta base**
> (funciona sola) + una o varias **extensiones condicionales**. El
> catálogo pasó de 14 a **17 casos de uso** (CU-08X, CU-12X, y CU-14 se
> desdobló en CU-14/15/16/17). Las notas de diseño de más abajo que decían
> "se descarta partir en dos" están **corregidas** donde corresponde;
> se deja constancia de la primera decisión y de por qué se revirtió. Ver
> `casos-de-uso-general.puml` (v2) y la sección 4bis de `analisis_uml.md`.

Clasificación usada: **CASO DE USO PRINCIPAL** · **CASO DE USO SECUNDARIO**
· **PARTE DE OTRO CU** · **FLUJO ALTERNATIVO** · **OPERACIÓN INTERNA** ·
**NO ES CASO DE USO**.

---

## Autenticación y cuenta

| Acción (dónde) | Actor | Objetivo | ¿Independiente? | Clasificación |
|---|---|---|---|---|
| Enviar formulario de login (`login.html`) | Visitante | Entrar al portal | Sí | **CU PRINCIPAL** → CU-02 |
| Enlace "Crear una cuenta" (`login.html`) | Visitante | Navegar a `/registro` | No (navegación) | NO ES CASO DE USO |
| Enviar formulario de registro (`register.html`) | Visitante | Solicitar una cuenta | Sí | **CU SECUNDARIO** → CU-01 |
| "Cerrar sesión" (topbar, todas las páginas) | Usuario | Terminar la sesión | Sí | **CU SECUNDARIO** → CU-03 |
| Formulario "Cambiar mi contraseña" (`account.html`) | Usuario | Cambiar su propia contraseña | Sí | **CU SECUNDARIO** → CU-04 |
| Enlace "Mi cuenta" / "Usuarios" (todas las topbars) | Usuario / Admin | Navegar | No | NO ES CASO DE USO |

## Descargas

| Acción | Actor | Objetivo | ¿Independiente? | Clasificación |
|---|---|---|---|---|
| "Iniciar descarga" + URL (`download_panel.html`) | Usuario | Obtener PDF de una fuente | Sí | **CU PRINCIPAL** → CU-05 |
| "Pausar proceso" / "Reanudar" | Usuario | Controlar la descarga en curso | No (requiere CU-05 activo) | OPERACIÓN INTERNA de CU-05 |
| Clic en un PDF de la lista descargada (`fileList`) | Usuario | Ver un PDF ya descargado | **Sí** — funciona sin lanzar ninguna descarga nueva | **CU SECUNDARIO** → CU-06 |
| Refresco automático de la lista/log (`refreshStatus`, cada 2 s) | — | Mostrar progreso | No lo inicia el usuario | OPERACIÓN INTERNA / NO ES CASO DE USO |
| Casilla "Revisar con IA local" | Usuario | Parametrizar la extracción | No es una acción en sí, es una condición | CONDICIÓN de CU-07 (`<<extend>>` con CU-09) |
| Modal "Extracción finalizada" → botón "Revisar resultados" | Usuario | Navegar a `/revision` | No | NO ES CASO DE USO (navegación) |
| Modal → botón "Cerrar" | Usuario | Descartar el modal | No | OPERACIÓN INTERNA |

## Extracción

| Acción | Actor | Objetivo | ¿Independiente? | Clasificación |
|---|---|---|---|---|
| Botón "Volcar a base de datos" del panel de descargas (en realidad dispara `/extract/start`) | Usuario | Extraer y normalizar los PDF descargados | Sí | **CU PRINCIPAL** → CU-07 |
| Barra de progreso / log de extracción | — | Mostrar avance | No lo inicia el usuario | OPERACIÓN INTERNA |
| Resumen modal (VALID/WARNING/INVALID/omitidas) | Usuario | Ver el resultado | Consecuencia automática de CU-07 | PARTE DE CU-07 |

## Revisión manual

| Acción (`review.html` / `review.js`) | Actor | Objetivo | ¿Independiente? | Clasificación |
|---|---|---|---|---|
| Pestañas de estado (Todos/Válidos/Warnings/Incorrectos) | Usuario | Filtrar por estado | No es un fin en sí, apoya la consulta | PARTE DE CU-08 |
| Árbol Grado→Año→Semestre→Grupo (clic en una hoja) | Usuario | Filtrar por rama | Idem | PARTE DE CU-08 |
| Buscador de texto libre, selects de filtro, "Limpiar filtros" | Usuario | Acotar la tabla | Idem | PARTE DE CU-08 |
| Paginación (Anterior/Siguiente) | Usuario | Navegar por resultados | Idem | OPERACIÓN INTERNA |
| Clic en una fila → modal de detalle | Usuario | Ver un registro completo | **Sí** — se puede abrir y mirar sin guardar ningún cambio | PARTE DE CU-08 |
| Guardar edición (`saveEdit`) | Usuario | Corregir los datos de un registro | **Sí** — solo ocurre si el usuario decide editar ESE registro; CU-08 es completo sin ella | **`<<extend>>`** → CU-08X |
| "Marcar como válido" / "Marcar como incorrecto" (`markStatus`) | Usuario | Cambiar el veredicto de un registro | Variante de la misma extensión | FLUJO ALTERNATIVO dentro de CU-08X |
| "Restaurar" (`restoreCurrent`) | Usuario | Deshacer los cambios propios | Variante | FLUJO ALTERNATIVO dentro de CU-08X |
| "Duplicar" (`duplicateRecord`) — separar una celda con dos asignaturas | Usuario | Partir un registro mezclado en dos | Variante especial, mismo mecanismo genérico | FLUJO ALTERNATIVO dentro de CU-08X |

**Nota de diseño (corregida en la 2ª vuelta):** en la primera pasada se
descartó partir CU-08 en consulta + corrección, razonando que "revisar" ya
implica mirar antes de corregir. Al aplicar el criterio de `<<extend>>`
con más rigor (¿es condicional? ¿el caso base es completo sin ello?), la
respuesta a ambas es sí: se puede abrir `/revision` y navegar el resumen,
el árbol y la tabla entera **sin editar nada**, y editar/marcar/restaurar/
duplicar solo ocurre si el usuario elige un registro concreto para
actuar. Se corrige a **CU-08 (base) + CU-08X (`<<extend>>`)**.

## Revisión con IA

| Acción | Actor | Objetivo | ¿Independiente? | Clasificación |
|---|---|---|---|---|
| "Resolver con IA" (un registro, desde el modal de detalle) | Usuario | Pedir a la IA que resuelva ESE registro | Variante por alcance | **CU PRINCIPAL** → CU-09 (alcance = 1 registro) |
| "🤖 Revisar todo con IA" (cabecera de la tabla) | Usuario | Pedir a la IA que resuelva todos los pendientes | Misma acción, alcance = todos | **CU PRINCIPAL** → CU-09 (alcance = todos), mismo endpoint `POST /review/llm-review` |
| "⏸ Pausar" / "▶ Reanudar" | Usuario | Controlar el lote en curso | No (requiere CU-09 activo) | OPERACIÓN INTERNA de CU-09 |
| "✕ Cancelar" | Usuario | Detener el lote | Idem | OPERACIÓN INTERNA de CU-09 |
| Texto de progreso ("Revisando con IA… n/total") | — | Informar | Automático (`setInterval`, no hay botón "consultar progreso") | NO ES CASO DE USO |

## Configuración de IA

| Acción (`ai_panel.html`) | Actor | Objetivo | ¿Independiente? | Clasificación |
|---|---|---|---|---|
| "Comprobar conexión" (también se ejecuta sola al cargar la página) | Usuario | Saber si Ollama responde y qué modelos tiene | Sí, se puede hacer sin guardar nada | PARTE DE CU-10 |
| Clic en un modelo de la lista (`model-chip`) | Usuario | Rellenar el campo "Modelo" | Puro atajo de formulario | OPERACIÓN INTERNA / NO ES CASO DE USO |
| Formulario "Guardar" (host + modelo) | Usuario | Cambiar la configuración de IA | Sí | PARTE DE CU-10 (misma pantalla, mismo objetivo global "tener la IA lista") |

**Nota de diseño explícita:** "comprobar disponibilidad" y "consultar
modelos" (candidatos que proponía el enunciado) son, en el código, el
**mismo** resultado de una sola llamada (`GET /ai/status`); no hay forma de
pedir solo uno de los dos. Y "configurar" sin más no tiene sentido para el
usuario si no puede comprobar que funciona. Se mantienen fundidos en un
único CU-10.

## Volcado a base de datos

| Acción (`db_dump.html`) | Actor | Objetivo | ¿Independiente? | Clasificación |
|---|---|---|---|---|
| "Volcar a base de datos" | Usuario | Persistir los horarios revisados | Sí | **CU PRINCIPAL** → CU-11 |
| "Cancelar volcado" | Usuario | Abortar el volcado en curso | No (requiere CU-11 activo) | OPERACIÓN INTERNA de CU-11 |
| Resumen de filas insertadas por tabla | — | Informar | Automático | OPERACIÓN INTERNA |

## Consulta y composición del horario

| Acción (`horarios.html` / `horarios.js`) | Actor | Objetivo | ¿Independiente? | Clasificación |
|---|---|---|---|---|
| Selector de titulación (`degreeSelect`) | Usuario | Acotar asignaturas | Paso, no un fin en sí | PARTE DE CU-12 |
| Buscador de asignatura (`subjectSearch`) | Usuario | Encontrar una asignatura | Filtro sobre lo ya cargado, en cliente | PARTE DE CU-12 |
| "Añadir" / doble clic en una asignatura | Usuario | Incluirla en el horario semanal | **Sí** — condicional (solo si el usuario decide añadir algo) y CU-12 (ver titulaciones/asignaturas) es completo sin ello | **`<<extend>>`** → CU-12X |
| "Quitar" (por asignatura) | Usuario | Sacarla del horario | Variante de la misma extensión | FLUJO ALTERNATIVO dentro de CU-12X |
| "Limpiar" (vaciar toda la selección) | Usuario | Reiniciar el horario | Variante | FLUJO ALTERNATIVO dentro de CU-12X |
| Aparición automática de la leyenda y la lista de solapes al añadir asignaturas que chocan | — (consecuencia) | Avisar de conflictos | Condicional, en el cliente, solo tiene sentido si ya se ha construido algo | **`<<extend>>`** → CU-13 (extiende CU-12X, no CU-12 directamente) |
| "consultar titulaciones/asignaturas/grupos/sesiones" (candidatos del enunciado) | Usuario | — | "Grupo" y "sesión" no son consultables por separado: solo existen como campos dentro de la respuesta de `/schedule/subjects`, sin endpoint ni pantalla propia | PARTE DE CU-12 (titulaciones/asignaturas) · **NO ES CASO DE USO** independiente (grupos/sesiones: no hay tal consulta en el código) |

**Nota de diseño (corregida en la 2ª vuelta):** "añadir asignatura" se
había fundido en CU-12 como su "acción principal". Revisado: cargar
titulaciones/asignaturas (`GET /schedule/degrees,/subjects`) es un
objetivo completo por sí solo (se puede consultar el catálogo sin
construir nada), y añadir es condicional. Se corrige a **CU-12 (base) +
CU-12X "Construir el horario semanal" (`<<extend>>`)**, y CU-13 pasa a
extender CU-12X.

## Gestión de usuarios

| Acción (`users_admin.html`) | Actor | Objetivo | ¿Independiente? | Clasificación |
|---|---|---|---|---|
| Carga de la tabla al abrir `/usuarios` | Administrador | Ver la plantilla de usuarios | **Sí** — objetivo completo por sí solo (comprobar quién tiene acceso, sin tocar nada) | **CU base** → CU-14 |
| Formulario "Nuevo usuario" | Administrador | Dar de alta una cuenta | **Sí** — condicional (solo si pulsa "Crear usuario"); CU-14 es completo sin ello | **`<<extend>>`** → CU-15 |
| "Hacer admin" / "Quitar admin" (mismo botón, texto según estado) | Administrador | Cambiar el rol | Condicional, sobre una fila ya existente | **`<<extend>>`** → CU-16 |
| "Resetear contraseña" | Administrador | Forzar una contraseña nueva | Condicional | **`<<extend>>`** → CU-16 (misma extensión que rol/aprobación) |
| "Aprobar" / "Vetar" (mismo botón y mismo endpoint `PUT /users/{id}/active`, solo cambia el booleano) | Administrador | Activar/desactivar el acceso | Variante — **es literalmente la misma operación con el valor invertido** | FLUJO ALTERNATIVO dentro de CU-16 |
| "Eliminar" | Administrador | Borrar la cuenta | Condicional | **`<<extend>>`** → CU-17 |

**Nota de diseño (corregida en la 2ª vuelta):** en la primera pasada,
alta/rol/contraseña/aprobación/baja se fundieron en un único "Gestionar
usuarios" por evitar el antipatrón "un óvalo por botón". Revisado con el
criterio estricto de `<<extend>>`: **consultar la lista SÍ es un objetivo
completo por sí solo** (un administrador puede abrir la pantalla solo para
comprobar quién tiene acceso), y alta/modificación/baja son cada una
**condicionales** sobre esa base — encajan exactamente en `<<extend>>`, a
diferencia de "pausar una descarga", que no tiene ningún resultado
observable propio sin la descarga ya en marcha. Se corrige a **CU-14
(base) + CU-15/CU-16/CU-17 (`<<extend>>`)**. "Aprobar" y "Vetar" siguen
fundidas en CU-16 (mismo endpoint, booleano invertido): esa parte de la
decisión original sí se mantiene.

---

## Resumen: catálogo final (17 casos de uso, tras la corrección)

| # | Caso de uso | Relación | Tipo |
|---|---|---|---|
| CU-01 | Registrarse | — | Secundario |
| CU-02 | Iniciar sesión | — | Secundario |
| CU-03 | Cerrar sesión | — | Secundario |
| CU-04 | Cambiar mi contraseña | — | Secundario |
| CU-05 | Descargar horarios desde una fuente web | — | **Principal** |
| CU-06 | Consultar documentos descargados | — | Secundario |
| CU-07 | Extraer y normalizar los horarios de los PDF | — | **Principal** |
| CU-08 | Consultar y revisar los horarios extraídos | base | **Principal** |
| CU-08X | Corregir un registro extraído | `<<extend>>` de CU-08 | **Principal** |
| CU-09 | Resolver con IA las asignaturas mezcladas | `<<extend>>` de CU-07 y de CU-08X | **Principal** |
| CU-10 | Configurar y verificar el servicio de IA | — | Secundario |
| CU-11 | Volcar los horarios revisados a la base de datos | — | **Principal** |
| CU-12 | Consultar horarios | base | **Principal** |
| CU-12X | Construir el horario semanal | `<<extend>>` de CU-12 | **Principal** |
| CU-13 | Detectar solapamientos | `<<extend>>` de CU-12X | **Principal** |
| CU-14 | Consultar usuarios | base | Secundario |
| CU-15 | Dar de alta un usuario | `<<extend>>` de CU-14 | Secundario |
| CU-16 | Modificar un usuario (rol, contraseña, aprobación/veto) | `<<extend>>` de CU-14 | Secundario |
| CU-17 | Dar de baja un usuario | `<<extend>>` de CU-14 | Secundario |

`casos-de-uso-general.puml` se actualizó a esta versión (v2). No aparece
ningún actor ni objetivo de negocio adicional a los ya cubiertos.

// =========================================================
// GESTOR DE HORARIOS
// Los datos vienen de la BD (volcado) vía el backend:
//   GET /schedule-degrees        -> [{id, name, session_count}]
//   GET /schedule-subjects       -> [{id, subject_id, degree_id, degree_name,
//                                     name, group, semester, sessions[],
//                                     unscheduled_count}]
// Cada entrada de /schedule-subjects es una (asignatura + grupo) ya
// condensada: sessions[] = [{day, start, end, classroom, status}].
// Cualquier campo puede ser null; las sesiones sin día u hora completa no
// llegan en sessions[] (van contadas en unscheduled_count).
// =========================================================

let degrees  = [];
let subjects = [];

const days  = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes"];
// Filas de la rejilla: 08:00–09:00 ... 20:00–21:00 (max hora fin en BD = 21:00)
const hours = [
    "08:00", "09:00", "10:00", "11:00", "12:00", "13:00", "14:00",
    "15:00", "16:00", "17:00", "18:00", "19:00", "20:00",
];

// =========================================================
// ESTADO
// =========================================================

let selectedSubjects = [];
const colorMap = {};
let colorCounter = 0;

// =========================================================
// COLORES POR ASIGNATURA
// =========================================================

function getColor(subjectId) {
    if (colorMap[subjectId] === undefined) {
        colorMap[subjectId] = colorCounter % 8;
        colorCounter++;
    }
    return colorMap[subjectId];
}

// =========================================================
// ELEMENTOS
// =========================================================

const degreeSelect         = document.getElementById("degreeSelect");
const subjectSearch        = document.getElementById("subjectSearch");
const subjectSelect        = document.getElementById("subjectSelect");
const addSubjectBtn        = document.getElementById("addSubjectBtn");
const clearBtn             = document.getElementById("clearBtn");
const scheduleBody         = document.getElementById("scheduleBody");
const selectedSubjectsList = document.getElementById("selectedSubjectsList");
const messageBox           = document.getElementById("messageBox");
const conflictInfo         = document.getElementById("conflictInfo");

// =========================================================
// INICIO
// =========================================================

async function init() {
    degreeSelect.addEventListener("change", onDegreeChange);
    subjectSearch.addEventListener("input", filterSubjects);
    addSubjectBtn.addEventListener("click", addSelectedSubject);
    clearBtn.addEventListener("click", clearSchedule);
    subjectSelect.addEventListener("dblclick", addSelectedSubject);

    renderSelectedSubjects();
    renderSchedule();

    await loadDegrees();
    await loadSubjects();
}

// =========================================================
// FETCH HELPERS
// =========================================================

async function fetchJson(url) {
    const response = await fetch(url);
    let data = null;
    try { data = await response.json(); } catch (e) { /* cuerpo no-JSON */ }

    if (!response.ok) {
        const detail = (data && (data.detail || data.message)) ||
                       `Error ${response.status}`;
        const err = new Error(detail);
        err.status = response.status;
        throw err;
    }
    return data;
}

// =========================================================
// CARRERAS
// =========================================================

async function loadDegrees() {
    degreeSelect.innerHTML = "";

    const allOption = document.createElement("option");
    allOption.value = "";
    allOption.textContent = "Todas las carreras";
    degreeSelect.appendChild(allOption);

    try {
        degrees = await fetchJson("/schedule-degrees");
    } catch (e) {
        degrees = [];
        showMessage(
            e.status === 404
                ? "No hay horarios en la base de datos. Ejecuta el volcado primero."
                : `No se pudieron cargar las titulaciones: ${e.message}`,
            "error"
        );
        return;
    }

    degrees.forEach(degree => {
        const option = document.createElement("option");
        option.value = degree.id;
        option.textContent = degree.name;
        degreeSelect.appendChild(option);
    });
}

// =========================================================
// ASIGNATURAS
// =========================================================

async function onDegreeChange() {
    await loadSubjects();
}

async function loadSubjects() {
    const degreeId = degreeSelect.value;
    const url = degreeId
        ? `/schedule-subjects?degree_id=${encodeURIComponent(degreeId)}`
        : "/schedule-subjects";

    subjectSelect.innerHTML = "";
    const loading = document.createElement("option");
    loading.textContent = "Cargando asignaturas…";
    loading.disabled = true;
    subjectSelect.appendChild(loading);

    let raw;
    try {
        raw = await fetchJson(url);
    } catch (e) {
        subjects = [];
        renderSubjectOptions([]);
        showMessage(
            e.status === 404
                ? "No hay asignaturas para esa selección en la base de datos."
                : `No se pudieron cargar las asignaturas: ${e.message}`,
            "error"
        );
        return;
    }

    // Normaliza al modelo que usa el resto del gestor.
    subjects = raw.map(entry => ({
        id:          entry.id,                        // "<subject_id>|<grupo>"
        degreeId:    entry.degree_id,
        degreeName:  entry.degree_name || "",
        name:        entry.name || "(sin nombre)",
        group:       entry.group || "Sin grupo",
        semester:    entry.semester,
        unscheduled: entry.unscheduled_count || 0,
        sessions:    Array.isArray(entry.sessions) ? entry.sessions : [],
    }));

    filterSubjects();
}

// =========================================================
// FILTRADO (en cliente, sobre lo ya cargado)
// =========================================================

function filterSubjects() {
    const searchText = normalizeText(subjectSearch.value);

    const filtered = subjects.filter(subject => {
        if (searchText === "") return true;
        const fullText = normalizeText(
            `${subject.name} ${subject.group} ${subject.degreeName} ${subject.semester ?? ""}`
        );
        return fullText.includes(searchText);
    });

    renderSubjectOptions(filtered);
}

function renderSubjectOptions(filtered) {
    subjectSelect.innerHTML = "";

    if (filtered.length === 0) {
        const option = document.createElement("option");
        option.textContent = subjects.length === 0
            ? "No hay asignaturas cargadas"
            : "No hay asignaturas que coincidan con la búsqueda";
        option.disabled = true;
        subjectSelect.appendChild(option);
        return;
    }

    filtered.forEach(subject => {
        const option = document.createElement("option");
        option.value = subject.id;
        const sem = subject.semester != null ? ` · sem. ${subject.semester}` : "";
        const noSlot = subject.unscheduled > 0 ? ` · ⚠ ${subject.unscheduled} sin horario` : "";
        option.textContent = `${subject.name} · ${subject.group}${sem}${noSlot}`;
        subjectSelect.appendChild(option);
    });
}

// =========================================================
// AÑADIR / ELIMINAR / LIMPIAR
// =========================================================

function addSelectedSubject() {
    const subjectId = subjectSelect.value;

    if (!subjectId) {
        showMessage("Selecciona una asignatura válida.", "error");
        return;
    }

    const subject = subjects.find(s => s.id === subjectId);

    if (!subject) {
        showMessage("No se ha encontrado la asignatura.", "error");
        return;
    }

    if (selectedSubjects.some(s => s.id === subject.id)) {
        showMessage("Esta asignatura ya está seleccionada.", "error");
        return;
    }

    getColor(subject.id);
    selectedSubjects.push(subject);

    const conflicts = detectConflicts();

    let msg;
    if (subject.sessions.length === 0) {
        msg = subject.unscheduled > 0
            ? "Asignatura añadida, pero sus sesiones no tienen día u hora en la BD: no se pueden pintar."
            : "Asignatura añadida, pero no tiene ninguna sesión registrada.";
        showMessage(msg, "error");
    } else {
        showMessage(
            conflicts.length > 0
                ? "Hay solapes en el horario. Revisa las celdas marcadas."
                : "Asignatura añadida correctamente. Sin solapes.",
            conflicts.length > 0 ? "error" : "success"
        );
    }

    renderSelectedSubjects();
    renderSchedule();
}

function removeSubject(subjectId) {
    selectedSubjects = selectedSubjects.filter(subject => subject.id !== subjectId);

    const conflicts = detectConflicts();
    showMessage(
        conflicts.length > 0
            ? "Asignatura eliminada, pero quedan solapes."
            : "Asignatura eliminada. Sin solapes.",
        conflicts.length > 0 ? "error" : "success"
    );

    renderSelectedSubjects();
    renderSchedule();
}

function clearSchedule() {
    selectedSubjects = [];
    renderSelectedSubjects();
    renderSchedule();
    showMessage("Horario limpiado.", "success");
}

// =========================================================
// RENDER ASIGNATURAS SELECCIONADAS
// =========================================================

function renderSelectedSubjects() {
    selectedSubjectsList.innerHTML = "";

    if (selectedSubjects.length === 0) {
        const li = document.createElement("li");
        li.className = "s-item";
        const em = document.createElement("span");
        em.className = "empty-text";
        em.textContent = "No hay asignaturas seleccionadas.";
        li.appendChild(em);
        selectedSubjectsList.appendChild(li);
        return;
    }

    selectedSubjects.forEach(subject => {
        const ci = getColor(subject.id);

        const li = document.createElement("li");
        li.className = `s-item c${ci}`;

        const dot = document.createElement("span");
        dot.className = "s-dot";

        const name = document.createElement("span");
        name.className = "s-name";
        name.textContent = subject.name;

        const meta = document.createElement("span");
        meta.className = "s-meta";
        const sem = subject.semester != null ? ` · sem. ${subject.semester}` : "";
        const noSlot = subject.unscheduled > 0
            ? ` · ⚠ ${subject.unscheduled} sesión(es) sin horario`
            : "";
        meta.textContent = `${subject.group}${sem}${noSlot}`;

        const btn = document.createElement("button");
        btn.className = "remove-btn";
        btn.textContent = "Quitar";
        btn.addEventListener("click", () => removeSubject(subject.id));

        li.append(dot, name, meta, btn);
        selectedSubjectsList.appendChild(li);
    });
}

// =========================================================
// RENDER HORARIO
// =========================================================

function renderSchedule() {
    scheduleBody.innerHTML = "";

    const conflicts = detectConflicts();

    hours.forEach(hour => {
        const row = document.createElement("tr");

        const timeCell = document.createElement("td");
        timeCell.className = "time-cell";
        timeCell.textContent = `${hour}–${getNextHour(hour)}`;
        row.appendChild(timeCell);

        days.forEach(day => {
            const cell = document.createElement("td");
            const sessions = getSessionsForCell(day, hour);

            sessions.forEach(({ subject, session }) => {
                const ci = getColor(subject.id);
                const block = document.createElement("div");
                block.className = `class-block c${ci}`;

                const hasConflict = conflicts.some(c =>
                    c.subjectId === subject.id &&
                    c.day === session.day &&
                    c.start === session.start &&
                    c.end === session.end
                );

                if (hasConflict) {
                    block.classList.add("conflict");
                    cell.classList.add("conflict-cell");
                }

                if (session.status === "WARNING") {
                    block.classList.add("warning-src");
                }

                const room = session.classroom
                    ? `<span style="opacity:.8">${session.classroom}</span>`
                    : "";

                block.innerHTML =
                    `<strong>${subject.name}</strong>` +
                    `${subject.group} · ${session.start}–${session.end}<br>` +
                    room;

                cell.appendChild(block);
            });

            row.appendChild(cell);
        });

        scheduleBody.appendChild(row);
    });

    renderConflicts();
}

// =========================================================
// SOLAPES (INFO LEGIBLE)
// =========================================================

function getConflictPairs() {
    const pairs = [];

    for (let i = 0; i < selectedSubjects.length; i++) {
        for (let j = i + 1; j < selectedSubjects.length; j++) {
            const A = selectedSubjects[i];
            const B = selectedSubjects[j];

            A.sessions.forEach(sa => {
                B.sessions.forEach(sb => {
                    if (sa.day === sb.day && sessionsOverlap(sa, sb)) {
                        pairs.push({
                            day: sa.day,
                            a: { name: A.name, group: A.group, start: sa.start, end: sa.end },
                            b: { name: B.name, group: B.group, start: sb.start, end: sb.end },
                        });
                    }
                });
            });
        }
    }

    return pairs;
}

function renderConflicts() {
    conflictInfo.innerHTML = "";

    if (selectedSubjects.length === 0) return;

    const pairs = getConflictPairs();

    if (pairs.length === 0) {
        const ok = document.createElement("p");
        ok.className = "conflict-ok";
        ok.textContent = "✓ Sin solapes entre las asignaturas seleccionadas.";
        conflictInfo.appendChild(ok);
        return;
    }

    const title = document.createElement("p");
    title.className = "conflict-title";
    title.textContent = `${pairs.length} solape${pairs.length > 1 ? "s" : ""} detectado${pairs.length > 1 ? "s" : ""}`;
    conflictInfo.appendChild(title);

    const ul = document.createElement("ul");
    ul.className = "conflict-list";

    pairs.forEach(p => {
        const li = document.createElement("li");
        li.innerHTML =
            `<span class="c-when">${p.day} ${p.a.start}–${p.a.end}</span> · ` +
            `<span class="c-subj">${p.a.name}</span> (${p.a.group}) ` +
            `choca con <span class="c-subj">${p.b.name}</span> (${p.b.group}, ${p.b.start}–${p.b.end})`;
        ul.appendChild(li);
    });

    conflictInfo.appendChild(ul);
}

// =========================================================
// LÓGICA
// =========================================================

function getSessionsForCell(day, hour) {
    const result = [];
    const cs = timeToMinutes(hour);
    const ce = timeToMinutes(getNextHour(hour));

    selectedSubjects.forEach(subject => {
        subject.sessions.forEach(session => {
            const ss = timeToMinutes(session.start);
            const se = timeToMinutes(session.end);
            if (ss === null || se === null) return;
            if (session.day === day && ss < ce && se > cs) {
                result.push({ subject, session });
            }
        });
    });

    return result;
}

function detectConflicts() {
    const conflicts = [];

    for (let i = 0; i < selectedSubjects.length; i++) {
        for (let j = i + 1; j < selectedSubjects.length; j++) {
            const subjectA = selectedSubjects[i];
            const subjectB = selectedSubjects[j];

            subjectA.sessions.forEach(sessionA => {
                subjectB.sessions.forEach(sessionB => {
                    if (sessionA.day === sessionB.day && sessionsOverlap(sessionA, sessionB)) {
                        conflicts.push({ subjectId: subjectA.id, day: sessionA.day, start: sessionA.start, end: sessionA.end });
                        conflicts.push({ subjectId: subjectB.id, day: sessionB.day, start: sessionB.start, end: sessionB.end });
                    }
                });
            });
        }
    }

    return conflicts;
}

function sessionsOverlap(a, b) {
    const as = timeToMinutes(a.start), ae = timeToMinutes(a.end);
    const bs = timeToMinutes(b.start), be = timeToMinutes(b.end);
    if (as === null || ae === null || bs === null || be === null) return false;
    return as < be && bs < ae;
}

function timeToMinutes(time) {
    if (!time) return null;
    const parts = time.split(":");
    if (parts.length < 2) return null;
    const h = Number(parts[0]);
    const m = Number(parts[1]);
    if (Number.isNaN(h) || Number.isNaN(m)) return null;
    return h * 60 + m;
}

function getNextHour(hour) {
    const [h, m] = hour.split(":").map(Number);
    return `${String(h + 1).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
}

function normalizeText(text) {
    return text
        .toString()
        .toLowerCase()
        .normalize("NFD")
        .replace(/[̀-ͯ]/g, "")
        .trim();
}

function showMessage(text, type) {
    messageBox.textContent = text;
    messageBox.className = `msg ${type}`;
}

// =========================================================
// ARRANQUE
// =========================================================

init();

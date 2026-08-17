// =========================================================
// DATA (estática — se reemplazará con datos del backend)
// =========================================================

const degrees = [
    { id: "GII",   name: "Grado en Ingeniería Informática" },
    { id: "GIEAI", name: "Grado en Ingeniería Electrónica y Automática Industrial" },
    { id: "GIT",   name: "Grado en Ingeniería de Telecomunicaciones" }
];

const subjects = [
    {
        id: 101, degreeId: "GII", name: "Programación", group: "Grupo A",
        sessions: [
            { day: "Lunes",  start: "10:00", end: "12:00", classroom: "Lab 1" },
            { day: "Jueves", start: "09:00", end: "11:00", classroom: "Lab 1" }
        ]
    },
    {
        id: 102, degreeId: "GII", name: "Bases de Datos", group: "Grupo A",
        sessions: [
            { day: "Martes", start: "10:00", end: "12:00", classroom: "Lab 2" },
            { day: "Jueves", start: "12:00", end: "14:00", classroom: "Lab 2" }
        ]
    },
    {
        id: 103, degreeId: "GII", name: "Sistemas Operativos", group: "Grupo B",
        sessions: [
            { day: "Lunes",     start: "11:00", end: "13:00", classroom: "Aula 204" },
            { day: "Miércoles", start: "09:00", end: "11:00", classroom: "Aula 204" }
        ]
    },
    {
        id: 201, degreeId: "GIEAI", name: "Matemáticas I", group: "Grupo A",
        sessions: [
            { day: "Lunes",     start: "09:00", end: "10:00", classroom: "Aula 101" },
            { day: "Miércoles", start: "11:00", end: "12:00", classroom: "Aula 101" }
        ]
    },
    {
        id: 202, degreeId: "GIEAI", name: "Física", group: "Grupo B",
        sessions: [
            { day: "Lunes",   start: "09:30", end: "10:30", classroom: "Aula 202" },
            { day: "Viernes", start: "12:00", end: "13:00", classroom: "Aula 202" }
        ]
    },
    {
        id: 203, degreeId: "GIEAI", name: "Electrónica Industrial", group: "Grupo A",
        sessions: [
            { day: "Martes", start: "08:00", end: "10:00", classroom: "Lab Electrónica" },
            { day: "Jueves", start: "10:00", end: "12:00", classroom: "Lab Electrónica" }
        ]
    },
    {
        id: 301, degreeId: "GIT", name: "Redes de Comunicaciones", group: "Grupo A",
        sessions: [
            { day: "Martes",  start: "09:00", end: "11:00", classroom: "Aula 301" },
            { day: "Viernes", start: "10:00", end: "12:00", classroom: "Aula 301" }
        ]
    },
    {
        id: 302, degreeId: "GIT", name: "Señales y Sistemas", group: "Grupo B",
        sessions: [
            { day: "Miércoles", start: "10:00", end: "12:00", classroom: "Aula 302" },
            { day: "Jueves",    start: "12:00", end: "14:00", classroom: "Aula 302" }
        ]
    }
];

const days  = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes"];
const hours = ["08:00","09:00","10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00"];

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

// =========================================================
// INICIO
// =========================================================

function init() {
    loadDegrees();
    filterSubjects();
    renderSelectedSubjects();
    renderSchedule();

    degreeSelect.addEventListener("change", filterSubjects);
    subjectSearch.addEventListener("input", filterSubjects);
    addSubjectBtn.addEventListener("click", addSelectedSubject);
    clearBtn.addEventListener("click", clearSchedule);
    subjectSelect.addEventListener("dblclick", addSelectedSubject);
}

// =========================================================
// CARRERAS
// =========================================================

function loadDegrees() {
    degreeSelect.innerHTML = "";

    const allOption = document.createElement("option");
    allOption.value = "";
    allOption.textContent = "Todas las carreras";
    degreeSelect.appendChild(allOption);

    degrees.forEach(degree => {
        const option = document.createElement("option");
        option.value = degree.id;
        option.textContent = `${degree.id} — ${degree.name}`;
        degreeSelect.appendChild(option);
    });
}

// =========================================================
// FILTRADO
// =========================================================

function filterSubjects() {
    const selectedDegreeId = degreeSelect.value;
    const searchText = normalizeText(subjectSearch.value);

    const filteredSubjects = subjects.filter(subject => {
        const degree = degrees.find(d => d.id === subject.degreeId);
        const fullText = normalizeText(
            `${subject.id} ${subject.name} ${subject.group} ${subject.degreeId} ${degree ? degree.name : ""}`
        );
        const matchesDegree = selectedDegreeId === "" || subject.degreeId === selectedDegreeId;
        const matchesSearch = searchText === "" || fullText.includes(searchText);
        return matchesDegree && matchesSearch;
    });

    renderSubjectOptions(filteredSubjects);
}

function renderSubjectOptions(filteredSubjects) {
    subjectSelect.innerHTML = "";

    if (filteredSubjects.length === 0) {
        const option = document.createElement("option");
        option.textContent = "No hay asignaturas encontradas";
        option.disabled = true;
        subjectSelect.appendChild(option);
        return;
    }

    filteredSubjects.forEach(subject => {
        const degree = degrees.find(d => d.id === subject.degreeId);
        const option = document.createElement("option");
        option.value = subject.id;
        option.textContent = `${subject.id} — ${subject.name} · ${subject.group} · ${degree?.id}`;
        subjectSelect.appendChild(option);
    });
}

// =========================================================
// AÑADIR / ELIMINAR / LIMPIAR
// =========================================================

function addSelectedSubject() {
    const subjectId = Number(subjectSelect.value);

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

    getColor(subjectId);
    selectedSubjects.push(subject);

    const conflicts = detectConflicts();
    showMessage(
        conflicts.length > 0
            ? "Hay solapes en el horario. Revisa las celdas marcadas."
            : "Asignatura añadida correctamente. Sin solapes.",
        conflicts.length > 0 ? "error" : "success"
    );

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
        const degree = degrees.find(d => d.id === subject.degreeId);
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
        meta.textContent = `${subject.group} · ${degree?.id} · ID ${subject.id}`;

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

                block.innerHTML =
                    `<strong>${subject.name}</strong>` +
                    `${subject.group} · ${session.start}–${session.end}<br>` +
                    `<span style="opacity:.8">${session.classroom}</span>`;

                cell.appendChild(block);
            });

            row.appendChild(cell);
        });

        scheduleBody.appendChild(row);
    });
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
    return timeToMinutes(a.start) < timeToMinutes(b.end) &&
           timeToMinutes(b.start) < timeToMinutes(a.end);
}

function timeToMinutes(time) {
    const [h, m] = time.split(":").map(Number);
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
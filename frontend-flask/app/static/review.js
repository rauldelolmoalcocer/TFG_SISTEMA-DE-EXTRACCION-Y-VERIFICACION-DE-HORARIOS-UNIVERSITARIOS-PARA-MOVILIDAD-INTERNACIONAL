// =========================================================
// ESTADO
// =========================================================

const state = {
    page: 1,
    pageSize: 50,
    filters: {
        status: "",
        degree: "",
        course_year: "",
        semester: "",
        group: "",
        day: "",
        pdf: "",
        issue: "",
        search: "",
    },
    currentItemId: null,
    currentItem: null,
};

const STATUS_ICON = { VALID: "✓", WARNING: "⚠", INVALID: "✕" };
const STATUS_LABEL = { VALID: "Válido", WARNING: "Warning", INVALID: "Incorrecto" };

// =========================================================
// ELEMENTOS
// =========================================================

const noDataBox = document.getElementById("noDataBox");
const reviewContent = document.getElementById("reviewContent");

const chipTotal = document.getElementById("chipTotal");
const chipValid = document.getElementById("chipValid");
const chipWarning = document.getElementById("chipWarning");
const chipInvalid = document.getElementById("chipInvalid");
const chipReviewed = document.getElementById("chipReviewed");
const chipLlmBlock = document.getElementById("chipLlmBlock");
const chipLlmReviewed = document.getElementById("chipLlmReviewed");

const treeContainer = document.getElementById("treeContainer");

const searchInput = document.getElementById("searchInput");
const statusTabs = document.getElementById("statusTabs");
const statusTabButtons = statusTabs.querySelectorAll(".summary-chip.tab");
const degreeFilter = document.getElementById("degreeFilter");
const yearFilter = document.getElementById("yearFilter");
const semesterFilter = document.getElementById("semesterFilter");
const groupFilter = document.getElementById("groupFilter");
const dayFilter = document.getElementById("dayFilter");
const pdfFilter = document.getElementById("pdfFilter");
const issueFilter = document.getElementById("issueFilter");
const clearFiltersBtn = document.getElementById("clearFiltersBtn");

const resultsBody = document.getElementById("resultsBody");
const resultsCount = document.getElementById("resultsCount");
const pagination = document.getElementById("pagination");

const detailModalOverlay = document.getElementById("detailModalOverlay");
const detailCloseX = document.getElementById("detailCloseX");
const detailTitle = document.getElementById("detailTitle");
const detailBadges = document.getElementById("detailBadges");
const detailFeedback = document.getElementById("detailFeedback");

const roFile = document.getElementById("roFile");
const roPage = document.getElementById("roPage");
const roRawText = document.getElementById("roRawText");
const roStrategy = document.getElementById("roStrategy");
const roIssues = document.getElementById("roIssues");
const roAnnotations = document.getElementById("roAnnotations");
const roLlmBlock = document.getElementById("roLlmBlock");
const roLlm = document.getElementById("roLlm");
const roRawTableBlock = document.getElementById("roRawTableBlock");
const roRawTable = document.getElementById("roRawTable");

const fDegree = document.getElementById("fDegree");
const fCourseYear = document.getElementById("fCourseYear");
const fSemester = document.getElementById("fSemester");
const fGroup = document.getElementById("fGroup");
const fDay = document.getElementById("fDay");
const fTimeStart = document.getElementById("fTimeStart");
const fTimeEnd = document.getElementById("fTimeEnd");
const fSubjectName = document.getElementById("fSubjectName");
const fRooms = document.getElementById("fRooms");
const fMultipleEntries = document.getElementById("fMultipleEntries");
const fNotes = document.getElementById("fNotes");

const duplicateBtn = document.getElementById("duplicateBtn");
const llmReviewOneBtn = document.getElementById("llmReviewOneBtn");
const llmReviewAllBtn = document.getElementById("llmReviewAllBtn");
const llmReviewStatusText = document.getElementById("llmReviewStatusText");
const restoreBtn = document.getElementById("restoreBtn");
const markInvalidBtn = document.getElementById("markInvalidBtn");
const markValidBtn = document.getElementById("markValidBtn");
const saveEditBtn = document.getElementById("saveEditBtn");

// =========================================================
// INICIO
// =========================================================

async function init() {
    try {
        const summaryRes = await fetch("/review-summary");

        if (!summaryRes.ok) {
            noDataBox.hidden = false;
            reviewContent.hidden = true;
            return;
        }

        reviewContent.hidden = false;
        noDataBox.hidden = true;

        await loadFilterOptions();
        await refreshAll();

    } catch (e) {
        console.error("Error inicializando revisión:", e);
        noDataBox.hidden = false;
        reviewContent.hidden = true;
    }
}

async function refreshAll() {
    await Promise.all([loadSummary(), loadTree(), loadRecords()]);
}

// =========================================================
// RESUMEN
// =========================================================

async function loadSummary() {
    const res = await fetch("/review-summary");
    if (!res.ok) return;
    const data = await res.json();

    chipTotal.textContent = data.total_records;
    chipValid.textContent = data.accepted_records - data.records_with_warnings - data.invalid_records;
    chipWarning.textContent = data.records_with_warnings;
    chipInvalid.textContent = data.invalid_records + data.skipped_records;
    chipReviewed.textContent = data.reviewed_records;

    if (data.llm_review && data.llm_review.enabled && !data.llm_review.error) {
        chipLlmBlock.hidden = false;
        chipLlmReviewed.textContent = data.llm_review.reviewed_count ?? 0;
    } else {
        chipLlmBlock.hidden = true;
    }
}

// =========================================================
// FILTROS
// =========================================================

function fillSelect(select, options, currentValue) {
    const placeholder = select.querySelector("option[value='']");
    select.innerHTML = "";
    if (placeholder) select.appendChild(placeholder);

    options.forEach(opt => {
        const option = document.createElement("option");
        option.value = opt.value;
        option.textContent = opt.label;
        select.appendChild(option);
    });

    select.value = currentValue || "";
}

async function loadFilterOptions() {
    const res = await fetch("/review-filters");
    if (!res.ok) return;
    const data = await res.json();

    fillSelect(
        degreeFilter,
        data.degrees.map(d => ({ value: d, label: d })),
        state.filters.degree
    );

    fillSelect(
        yearFilter,
        data.course_years.map(y => ({ value: y, label: y })),
        state.filters.course_year
    );

    fillSelect(
        semesterFilter,
        data.semesters.map(s => ({ value: s, label: `Semestre ${s}` })),
        state.filters.semester
    );

    fillSelect(
        groupFilter,
        data.groups.map(g => ({ value: g, label: g })),
        state.filters.group
    );

    fillSelect(
        dayFilter,
        data.days.map(d => ({ value: d, label: d })),
        state.filters.day
    );

    fillSelect(
        pdfFilter,
        data.pdfs.map(p => ({ value: p, label: p })),
        state.filters.pdf
    );

    fillSelect(
        issueFilter,
        data.issues.map(i => ({ value: i, label: i })),
        state.filters.issue
    );
}

function buildQueryParams(extra) {
    const params = new URLSearchParams();
    Object.entries(state.filters).forEach(([key, value]) => {
        if (value) params.set(key, value);
    });
    if (extra) {
        Object.entries(extra).forEach(([key, value]) => params.set(key, value));
    }
    return params;
}

function onFilterChange() {
    state.filters.degree = degreeFilter.value;
    state.filters.course_year = yearFilter.value;
    state.filters.semester = semesterFilter.value;
    state.filters.group = groupFilter.value;
    state.filters.day = dayFilter.value;
    state.filters.pdf = pdfFilter.value;
    state.filters.issue = issueFilter.value;
    state.page = 1;
    refreshAll();
}

let searchDebounce = null;
searchInput.addEventListener("input", () => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => {
        state.filters.search = searchInput.value;
        state.page = 1;
        refreshAll();
    }, 350);
});

[degreeFilter, yearFilter, semesterFilter, groupFilter, dayFilter, pdfFilter, issueFilter]
    .forEach(select => select.addEventListener("change", onFilterChange));

statusTabButtons.forEach(tab => {
    tab.addEventListener("click", () => {
        state.filters.status = tab.dataset.status;
        state.page = 1;
        syncStatusTabs();
        refreshAll();
    });
});

function syncStatusTabs() {
    statusTabButtons.forEach(tab => {
        tab.classList.toggle("active", tab.dataset.status === (state.filters.status || ""));
    });
}

clearFiltersBtn.addEventListener("click", () => {
    state.filters = {
        status: "", degree: "", course_year: "", semester: "", group: "",
        day: "", pdf: "", issue: "", search: "",
    };
    state.page = 1;
    searchInput.value = "";
    [degreeFilter, yearFilter, semesterFilter, groupFilter, dayFilter, pdfFilter, issueFilter]
        .forEach(select => select.value = "");
    syncStatusTabs();
    refreshAll();
});

// =========================================================
// ÁRBOL DE AGRUPACIÓN
// =========================================================

const NULL_LABELS = {
    degree: "Sin titulación",
    year: "Sin año académico",
    semester: "Sin cuatrimestre",
    group: "Sin grupo",
};

function treeValueOrNull(raw, kind) {
    return raw === NULL_LABELS[kind] ? "__NULL__" : raw;
}

async function loadTree() {
    const params = buildQueryParams();
    const res = await fetch(`/review-tree?${params.toString()}`);
    if (!res.ok) return;
    const data = await res.json();
    renderTree(data.tree);
}

function renderTree(tree) {
    treeContainer.innerHTML = "";

    if (!tree.length) {
        treeContainer.innerHTML = '<div class="empty-state">Sin registros para estos filtros.</div>';
        return;
    }

    tree.forEach(degreeNode => {
        const degreeValue = treeValueOrNull(degreeNode.degree_name, "degree");

        const degreeDetails = document.createElement("details");
        degreeDetails.open = tree.length <= 3;

        const degreeSummary = document.createElement("summary");
        degreeSummary.innerHTML =
            `<span class="tree-node-label">${escapeHtml(degreeNode.degree_name)}</span>` +
            `<span class="tree-node-count">${degreeNode.count}</span>`;
        degreeDetails.appendChild(degreeSummary);

        degreeNode.years.forEach(yearNode => {
            const yearValue = treeValueOrNull(yearNode.course_year, "year");

            const yearDetails = document.createElement("details");
            const yearSummary = document.createElement("summary");
            yearSummary.innerHTML =
                `<span class="tree-node-label">${escapeHtml(yearNode.course_year)}</span>` +
                `<span class="tree-node-count">${yearNode.count}</span>`;
            yearDetails.appendChild(yearSummary);

            yearNode.semesters.forEach(semesterNode => {
                const semesterValue = treeValueOrNull(semesterNode.semester, "semester");

                const semesterDetails = document.createElement("details");
                const semesterSummary = document.createElement("summary");
                semesterSummary.innerHTML =
                    `<span class="tree-node-label">Semestre ${escapeHtml(semesterNode.semester)}</span>` +
                    `<span class="tree-node-count">${semesterNode.count}</span>`;
                semesterDetails.appendChild(semesterSummary);

                semesterNode.groups.forEach(groupNode => {
                    const groupValue = treeValueOrNull(groupNode.group, "group");

                    const leaf = document.createElement("div");
                    leaf.className = "tree-leaf";
                    if (
                        state.filters.degree === degreeValue &&
                        state.filters.course_year === String(yearValue) &&
                        state.filters.semester === String(semesterValue) &&
                        state.filters.group === String(groupValue)
                    ) {
                        leaf.classList.add("active");
                    }

                    leaf.innerHTML =
                        `<span class="tree-node-label">${escapeHtml(groupNode.group)}</span>` +
                        `<span class="tree-node-count">${groupNode.count}</span>`;

                    leaf.addEventListener("click", () => {
                        selectTreeLeaf(degreeValue, yearValue, semesterValue, groupValue);
                    });

                    semesterDetails.appendChild(leaf);
                });

                yearDetails.appendChild(semesterDetails);
            });

            degreeDetails.appendChild(yearDetails);
        });

        treeContainer.appendChild(degreeDetails);
    });
}

function selectTreeLeaf(degree, year, semester, group) {
    state.filters.degree = String(degree);
    state.filters.course_year = String(year);
    state.filters.semester = String(semester);
    state.filters.group = String(group);
    state.page = 1;

    syncFilterSelects();
    refreshAll();
}

function syncFilterSelects() {
    degreeFilter.value = state.filters.degree || "";
    yearFilter.value = state.filters.course_year || "";
    semesterFilter.value = state.filters.semester || "";
    groupFilter.value = state.filters.group || "";
    dayFilter.value = state.filters.day || "";
    pdfFilter.value = state.filters.pdf || "";
    issueFilter.value = state.filters.issue || "";
}

// =========================================================
// TABLA DE RESULTADOS
// =========================================================

async function loadRecords() {
    resultsBody.innerHTML = '<tr><td colspan="8" class="empty-state">Cargando…</td></tr>';

    const params = buildQueryParams({ page: state.page, page_size: state.pageSize });
    const res = await fetch(`/review-records?${params.toString()}`);

    if (!res.ok) {
        resultsBody.innerHTML = '<tr><td colspan="8" class="empty-state">Error al cargar los registros.</td></tr>';
        return;
    }

    const data = await res.json();
    renderTable(data.items);
    renderPagination(data.total, data.page, data.page_size, data.total_pages);

    resultsCount.textContent = `${data.total} registro${data.total === 1 ? "" : "s"}`;
}

function escapeHtml(value) {
    if (value === null || value === undefined) return "";
    return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
}

function renderTable(items) {
    resultsBody.innerHTML = "";

    if (!items.length) {
        resultsBody.innerHTML = '<tr><td colspan="8" class="empty-state">No hay registros para estos filtros.</td></tr>';
        return;
    }

    items.forEach(item => {
        const c = item.current;
        const tr = document.createElement("tr");
        tr.addEventListener("click", () => openDetail(item.id));

        const originTag = item.origin === "skipped"
            ? '<span class="origin-tag origin-skipped">Tabla omitida</span>'
            : c.multiple_entries_suspected
                ? '<span class="origin-tag origin-mixed">Asignaturas mezcladas</span>'
                : item.origin === "manual"
                    ? '<span class="origin-tag origin-manual">Manual</span>'
                    : "";

        tr.innerHTML = `
            <td>
                <span class="status-icon ${item.status}" title="${STATUS_LABEL[item.status]}">${STATUS_ICON[item.status]}</span>
                ${item.manually_modified ? '<span class="modified-flag" title="Modificado manualmente">✎</span>' : ""}
            </td>
            <td>
                <div class="subject-cell">
                    <span class="subject-name">${item.origin === "skipped" ? "— tabla omitida, sin datos —" : (escapeHtml(c.subject_name) || "(sin nombre)")}</span>
                    ${originTag}
                </div>
            </td>
            <td>${escapeHtml(c.degree) || "—"}</td>
            <td>${escapeHtml(c.course_year) || "—"} / ${c.semester ?? "—"}</td>
            <td>${escapeHtml(c.group) || "—"}</td>
            <td>${escapeHtml(c.day) || "—"}<br>${escapeHtml(c.time_start) || "?"}–${escapeHtml(c.time_end) || "?"}</td>
            <td>${(c.rooms && c.rooms.length) ? escapeHtml(c.rooms.join(", ")) : "—"}</td>
            <td class="pdf-cell">${escapeHtml(item.source.file)}<br>pág. ${item.source.page}</td>
        `;

        resultsBody.appendChild(tr);
    });
}

function renderPagination(total, page, pageSize, totalPages) {
    pagination.innerHTML = "";
    if (totalPages <= 1) return;

    const prevBtn = document.createElement("button");
    prevBtn.textContent = "‹ Anterior";
    prevBtn.disabled = page <= 1;
    prevBtn.addEventListener("click", () => { state.page = page - 1; loadRecords(); });
    pagination.appendChild(prevBtn);

    const info = document.createElement("span");
    info.className = "pagination-info";
    info.textContent = `Página ${page} de ${totalPages}`;
    pagination.appendChild(info);

    const nextBtn = document.createElement("button");
    nextBtn.textContent = "Siguiente ›";
    nextBtn.disabled = page >= totalPages;
    nextBtn.addEventListener("click", () => { state.page = page + 1; loadRecords(); });
    pagination.appendChild(nextBtn);
}

// =========================================================
// DETALLE / EDICIÓN
// =========================================================

async function openDetail(id) {
    const res = await fetch(`/review-records/${encodeURIComponent(id)}`);
    if (!res.ok) return;

    const item = await res.json();
    state.currentItemId = id;
    state.currentItem = item;

    populateDetail(item);
    detailModalOverlay.classList.add("open");
}

function populateDetail(item) {
    const c = item.current;
    const ex = item.extraction || {};

    detailTitle.textContent = item.origin === "skipped"
        ? "Tabla omitida (sin datos extraídos)"
        : (c.subject_name || "Registro sin nombre de asignatura");
    detailFeedback.textContent = "";
    detailFeedback.className = "detail-feedback";

    const originBadge = item.origin === "skipped"
        ? '<span class="badge origin-skipped">📋 Tabla omitida — el extractor no consiguió leer nada de esta página</span>'
        : c.multiple_entries_suspected
            ? '<span class="badge origin-mixed">⚠ Esta celda mezcla más de una asignatura — sepáralas con "Duplicar" abajo</span>'
            : item.origin === "manual"
                ? '<span class="badge origin-manual">✎ Registro creado a mano</span>'
                : "";

    detailBadges.innerHTML = `
        <span class="badge ${item.status}">${STATUS_ICON[item.status]} ${STATUS_LABEL[item.status]}</span>
        ${originBadge}
        ${item.manually_modified ? '<span class="badge modified">✎ Modificado manualmente</span>' : ""}
        ${item.reviewed ? '<span class="badge reviewed">Revisado</span>' : ""}
    `;

    roFile.textContent = item.source.file || "—";
    roPage.textContent = item.source.page ?? "—";
    roRawText.textContent = item.source.raw_text || "—";
    roStrategy.textContent = (ex.strategy || ex.table_index !== undefined)
        ? `${ex.strategy || "—"} (tabla ${ex.table_index ?? "—"})`
        : "—";
    roIssues.textContent = (item.original_reason && item.original_reason.length)
        ? item.original_reason.join(", ")
        : "Sin problemas detectados.";
    roAnnotations.textContent = (ex.non_room_annotations && ex.non_room_annotations.length)
        ? ex.non_room_annotations.join(", ")
        : "—";

    if (ex.llm_reviewed) {
        roLlmBlock.hidden = false;
        roLlm.textContent = `Confianza: ${ex.llm_confidence || "—"}. ${ex.llm_note || ""}`.trim();
    } else {
        roLlmBlock.hidden = true;
    }

    if (ex.rows && ex.rows.length) {
        roRawTableBlock.hidden = false;
        const tableHtml = ex.rows.map(row =>
            "<tr>" + row.map(cell => `<td>${escapeHtml(cell)}</td>`).join("") + "</tr>"
        ).join("");
        roRawTable.innerHTML = `<div class="raw-table-wrap"><table class="raw-table"><tbody>${tableHtml}</tbody></table></div>`;
    } else {
        roRawTableBlock.hidden = true;
        roRawTable.innerHTML = "";
    }

    fDegree.value = c.degree || "";
    fCourseYear.value = c.course_year || "";
    fSemester.value = c.semester ?? "";
    fGroup.value = c.group || "";
    fDay.value = c.day || "";
    fTimeStart.value = c.time_start || "";
    fTimeEnd.value = c.time_end || "";
    fSubjectName.value = c.subject_name || "";
    fRooms.value = (c.rooms || []).join(", ");
    fMultipleEntries.checked = !!c.multiple_entries_suspected;
    fNotes.value = (c.notes || []).join("\n");
}

function readFormPayload() {
    const semester = fSemester.value.trim();

    return {
        degree: fDegree.value.trim() || null,
        course_year: fCourseYear.value.trim() || null,
        semester: semester === "" ? null : Number(semester),
        group: fGroup.value.trim() || null,
        day: fDay.value.trim() || null,
        time_start: fTimeStart.value.trim() || null,
        time_end: fTimeEnd.value.trim() || null,
        subject_name: fSubjectName.value.trim() || null,
        rooms: fRooms.value.split(",").map(r => r.trim()).filter(Boolean),
        multiple_entries_suspected: fMultipleEntries.checked,
        notes: fNotes.value.split("\n").map(n => n.trim()).filter(Boolean),
    };
}

function showFeedback(message, type) {
    detailFeedback.textContent = message;
    detailFeedback.className = `detail-feedback ${type || ""}`;
}

async function saveEdit() {
    if (!state.currentItemId) return;

    saveEditBtn.disabled = true;
    try {
        const res = await fetch(`/review-records/${encodeURIComponent(state.currentItemId)}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(readFormPayload()),
        });

        const data = await res.json();

        if (!res.ok || !data.success) {
            showFeedback(data.message || "No se pudieron guardar los cambios.", "error");
            return;
        }

        state.currentItem = data.item;
        populateDetail(data.item);
        showFeedback("Cambios guardados.", "success");

        await Promise.all([loadRecords(), loadTree(), loadSummary()]);

    } catch (e) {
        showFeedback("No se pudo conectar con el servidor.", "error");
    } finally {
        saveEditBtn.disabled = false;
    }
}

async function markStatus(status) {
    if (!state.currentItemId) return;

    try {
        const res = await fetch(`/review-records/${encodeURIComponent(state.currentItemId)}/status`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ status }),
        });

        const data = await res.json();

        if (!res.ok || !data.success) {
            showFeedback(data.message || "No se pudo actualizar el estado.", "error");
            return;
        }

        state.currentItem = data.item;
        populateDetail(data.item);
        showFeedback("Estado actualizado.", "success");

        await Promise.all([loadRecords(), loadTree(), loadSummary()]);

    } catch (e) {
        showFeedback("No se pudo conectar con el servidor.", "error");
    }
}

async function restoreCurrent() {
    if (!state.currentItemId) return;

    try {
        const res = await fetch(`/review-records/${encodeURIComponent(state.currentItemId)}/restore`, {
            method: "POST",
        });

        const data = await res.json();

        if (!res.ok || !data.success) {
            showFeedback(data.message || "No se pudo restaurar el registro.", "error");
            return;
        }

        state.currentItem = data.item;
        populateDetail(data.item);
        showFeedback("Registro restaurado al original.", "success");

        await Promise.all([loadRecords(), loadTree(), loadSummary()]);

    } catch (e) {
        showFeedback("No se pudo conectar con el servidor.", "error");
    }
}

async function duplicateRecord() {
    if (!state.currentItemId) return;

    duplicateBtn.disabled = true;
    try {
        const res = await fetch(`/review-records/${encodeURIComponent(state.currentItemId)}/duplicate`, {
            method: "POST",
        });

        const data = await res.json();

        if (!res.ok || !data.success) {
            showFeedback(data.message || "No se pudo duplicar el registro.", "error");
            return;
        }

        await Promise.all([loadRecords(), loadTree(), loadSummary()]);

        // Abre directamente la copia nueva para seguir editándola (p.ej.
        // para meter la segunda asignatura de una celda mezclada).
        state.currentItemId = data.item.id;
        state.currentItem = data.item;
        populateDetail(data.item);
        showFeedback("Registro duplicado. Edita esta copia con los datos que faltaban y guarda.", "success");

    } catch (e) {
        showFeedback("No se pudo conectar con el servidor.", "error");
    } finally {
        duplicateBtn.disabled = false;
    }
}

// =========================================================
// REVISIÓN CON IA (bajo demanda, uno o todos los registros)
// =========================================================

let llmReviewPollTimer = null;

async function pollLlmReviewStatus() {
    try {
        const res = await fetch("/review-llm-review-status");
        if (!res.ok) return;
        const status = await res.json();

        if (status.running) {
            llmReviewStatusText.textContent = status.total
                ? `Revisando con IA… ${status.attempted}/${status.total}`
                : "Revisando con IA…";
            llmReviewPollTimer = setTimeout(pollLlmReviewStatus, 1500);
            return;
        }

        llmReviewAllBtn.disabled = false;
        llmReviewOneBtn.disabled = false;

        const errorCount = (status.errors || []).length;
        llmReviewStatusText.textContent =
            `IA: ${status.resolved || 0} resueltos, ${status.split || 0} separados` +
            (errorCount ? `, ${errorCount} con error` : "");

        await Promise.all([loadRecords(), loadTree(), loadSummary()]);

        // Si el detalle abierto es uno de los que se acaban de procesar,
        // refresca su contenido para ver el resultado sin tener que cerrarlo.
        if (state.currentItemId) {
            const itemRes = await fetch(`/review-records/${encodeURIComponent(state.currentItemId)}`);
            if (itemRes.ok) {
                const item = await itemRes.json();
                state.currentItem = item;
                populateDetail(item);
            }
        }

    } catch (e) {
        llmReviewStatusText.textContent = "No se pudo consultar el estado de la revisión con IA.";
        llmReviewAllBtn.disabled = false;
        llmReviewOneBtn.disabled = false;
    }
}

async function startLlmReview(itemIds) {
    clearTimeout(llmReviewPollTimer);
    llmReviewAllBtn.disabled = true;
    llmReviewOneBtn.disabled = true;
    llmReviewStatusText.textContent = "Iniciando revisión con IA…";

    try {
        const res = await fetch("/review-llm-review", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ item_ids: itemIds }),
        });
        const data = await res.json();

        if (!res.ok || !data.success) {
            llmReviewStatusText.textContent = data.message || "No se pudo iniciar la revisión con IA.";
            llmReviewAllBtn.disabled = false;
            llmReviewOneBtn.disabled = false;
            return;
        }

        pollLlmReviewStatus();

    } catch (e) {
        llmReviewStatusText.textContent = "No se pudo conectar con el servidor.";
        llmReviewAllBtn.disabled = false;
        llmReviewOneBtn.disabled = false;
    }
}

function closeDetail() {
    detailModalOverlay.classList.remove("open");
    state.currentItemId = null;
    state.currentItem = null;
}

detailCloseX.addEventListener("click", closeDetail);
detailModalOverlay.addEventListener("click", (e) => {
    if (e.target === detailModalOverlay) closeDetail();
});

saveEditBtn.addEventListener("click", saveEdit);
markValidBtn.addEventListener("click", () => markStatus("VALID"));
markInvalidBtn.addEventListener("click", () => markStatus("INVALID"));
restoreBtn.addEventListener("click", restoreCurrent);
duplicateBtn.addEventListener("click", duplicateRecord);
llmReviewOneBtn.addEventListener("click", () => {
    if (state.currentItemId) startLlmReview([state.currentItemId]);
});
llmReviewAllBtn.addEventListener("click", () => startLlmReview(null));

// =========================================================
// ARRANQUE
// =========================================================

init();

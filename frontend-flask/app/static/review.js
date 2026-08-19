// =========================================================
// ESTADO
// =========================================================

const state = {
    page: 1,
    pageSize: 50,
    filters: {
        status: "",
        degree: "",
        year: "",
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
const roConfidence = document.getElementById("roConfidence");
const roIssues = document.getElementById("roIssues");

const fDegreeCode = document.getElementById("fDegreeCode");
const fDegreeName = document.getElementById("fDegreeName");
const fAcademicYear = document.getElementById("fAcademicYear");
const fSemester = document.getElementById("fSemester");
const fYear = document.getElementById("fYear");
const fGroup = document.getElementById("fGroup");
const fDay = document.getElementById("fDay");
const fStartTime = document.getElementById("fStartTime");
const fEndTime = document.getElementById("fEndTime");
const fSubjectCode = document.getElementById("fSubjectCode");
const fSubjectName = document.getElementById("fSubjectName");
const fSubgroup = document.getElementById("fSubgroup");
const fClassroom = document.getElementById("fClassroom");
const fNotes = document.getElementById("fNotes");

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
    chipValid.textContent = data.accepted_records - data.records_with_warnings;
    chipWarning.textContent = data.records_with_warnings;
    chipInvalid.textContent = data.rejected_records;
    chipReviewed.textContent = data.reviewed_records;
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
        data.degrees.map(d => ({ value: d.code || d.name, label: d.name || d.code })),
        state.filters.degree
    );

    fillSelect(
        yearFilter,
        data.years.map(y => ({ value: y, label: `Curso ${y}` })),
        state.filters.year
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
    state.filters.year = yearFilter.value;
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
        status: "", degree: "", year: "", semester: "", group: "",
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
    year: "Sin curso",
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
        const realDegreeValue = degreeNode.degree_code || degreeValue;

        const degreeDetails = document.createElement("details");
        degreeDetails.open = tree.length <= 3;

        const degreeSummary = document.createElement("summary");
        degreeSummary.innerHTML =
            `<span class="tree-node-label">${escapeHtml(degreeNode.degree_name)}</span>` +
            `<span class="tree-node-count">${degreeNode.count}</span>`;
        degreeDetails.appendChild(degreeSummary);

        degreeNode.years.forEach(yearNode => {
            const yearValue = treeValueOrNull(yearNode.year, "year");

            const yearDetails = document.createElement("details");
            const yearSummary = document.createElement("summary");
            yearSummary.innerHTML =
                `<span class="tree-node-label">Curso ${escapeHtml(yearNode.year)}</span>` +
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
                        state.filters.degree === realDegreeValue &&
                        state.filters.year === String(yearValue) &&
                        state.filters.semester === String(semesterValue) &&
                        state.filters.group === String(groupValue)
                    ) {
                        leaf.classList.add("active");
                    }

                    leaf.innerHTML =
                        `<span class="tree-node-label">${escapeHtml(groupNode.group)}</span>` +
                        `<span class="tree-node-count">${groupNode.count}</span>`;

                    leaf.addEventListener("click", () => {
                        selectTreeLeaf(realDegreeValue, yearValue, semesterValue, groupValue);
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
    state.filters.year = String(year);
    state.filters.semester = String(semester);
    state.filters.group = String(group);
    state.page = 1;

    syncFilterSelects();
    refreshAll();
}

function syncFilterSelects() {
    degreeFilter.value = state.filters.degree || "";
    yearFilter.value = state.filters.year || "";
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

        tr.innerHTML = `
            <td>
                <span class="status-icon ${item.status}" title="${STATUS_LABEL[item.status]}">${STATUS_ICON[item.status]}</span>
                ${item.manually_modified ? '<span class="modified-flag" title="Modificado manualmente">✎</span>' : ""}
            </td>
            <td>
                <div class="subject-cell">
                    <span class="subject-name">${escapeHtml(c.subject.name) || "(sin nombre)"}</span>
                    ${c.subject.code ? `<span class="subject-code">${escapeHtml(c.subject.code)}</span>` : ""}
                </div>
            </td>
            <td>${escapeHtml(c.degree.code) || escapeHtml(c.degree.name) || "—"}</td>
            <td>${c.year ?? "—"} / ${c.semester ?? "—"}</td>
            <td>${escapeHtml(c.group) || "—"}${c.subgroup ? " · " + escapeHtml(c.subgroup) : ""}</td>
            <td>${escapeHtml(c.day) || "—"}<br>${escapeHtml(c.start_time) || "?"}–${escapeHtml(c.end_time) || "?"}</td>
            <td>${escapeHtml(c.classroom) || "—"}</td>
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

    detailTitle.textContent = c.subject.name || "Registro sin nombre de asignatura";
    detailFeedback.textContent = "";
    detailFeedback.className = "detail-feedback";

    detailBadges.innerHTML = `
        <span class="badge ${item.status}">${STATUS_ICON[item.status]} ${STATUS_LABEL[item.status]}</span>
        ${item.manually_modified ? '<span class="badge modified">✎ Modificado manualmente</span>' : ""}
        ${item.reviewed ? '<span class="badge reviewed">Revisado</span>' : ""}
    `;

    roFile.textContent = item.source.file || "—";
    roPage.textContent = item.source.page ?? "—";
    roRawText.textContent = item.source.raw_text || "—";
    roConfidence.textContent = item.confidence !== null && item.confidence !== undefined
        ? `${Math.round(item.confidence * 100)}%`
        : "—";
    roIssues.textContent = (item.original_reason && item.original_reason.length)
        ? item.original_reason.join(", ")
        : "Sin problemas detectados.";

    fDegreeCode.value = c.degree.code || "";
    fDegreeName.value = c.degree.name || "";
    fAcademicYear.value = c.academic_year || "";
    fSemester.value = c.semester ?? "";
    fYear.value = c.year ?? "";
    fGroup.value = c.group || "";
    fDay.value = c.day || "";
    fStartTime.value = c.start_time || "";
    fEndTime.value = c.end_time || "";
    fSubjectCode.value = c.subject.code || "";
    fSubjectName.value = c.subject.name || "";
    fSubgroup.value = c.subgroup || "";
    fClassroom.value = c.classroom || "";
    fNotes.value = (c.notes || []).join("\n");
}

function readFormPayload() {
    const semester = fSemester.value.trim();
    const year = fYear.value.trim();

    return {
        degree: {
            code: fDegreeCode.value.trim() || null,
            name: fDegreeName.value.trim() || null,
        },
        academic_year: fAcademicYear.value.trim() || null,
        semester: semester === "" ? null : Number(semester),
        year: year === "" ? null : Number(year),
        group: fGroup.value.trim() || null,
        day: fDay.value || null,
        start_time: fStartTime.value.trim() || null,
        end_time: fEndTime.value.trim() || null,
        subject: {
            code: fSubjectCode.value.trim() || null,
            name: fSubjectName.value.trim() || null,
        },
        subgroup: fSubgroup.value.trim() || null,
        classroom: fClassroom.value.trim() || null,
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

// =========================================================
// ARRANQUE
// =========================================================

init();

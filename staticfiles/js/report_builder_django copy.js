/**
 * Django Report Builder JavaScript
 * Adapted from Flask version for Django MIS App
 */

document.addEventListener("DOMContentLoaded", function () {
    // ================================================================
    // 1. GLOBAL STATE AND CONFIGURATION
    // ================================================================
    
    // Django CSRF token handling
    function getCSRFToken() {
        return document.querySelector('[name=csrfmiddlewaretoken]')?.value || csrfToken;
    }

    // Enhanced modal handling for Django
    const Modal = {
        show(id, opts) {
            const el = document.getElementById(id);
            if (!el) return;
            const m = bootstrap.Modal.getOrCreateInstance(el, opts || {});
            m.show();
        },
        hide(id) {
            const el = document.getElementById(id);
            if (!el) return;
            const m = bootstrap.Modal.getOrCreateInstance(el);
            m.hide();
        },
    };

    // UUID helper
    function getUUID() {
        if (window.crypto && typeof window.crypto.randomUUID === "function") {
            return window.crypto.randomUUID();
        }
        return "uuid-" + ([1e7] + -1e3 + -4e3 + -8e3 + -1e11).replace(/[018]/g, (c) =>
            (c ^ (crypto.getRandomValues(new Uint8Array(1))[0] & (15 >> (c / 4)))).toString(16)
        );
    }

    // Application State
    const AppState = {
        currentReportId: null,
        currentReportName: "New Report",
        canEditCurrentReport: true,
        dataSourceSchema: {},
        reportConfig: {
            type: "advanced",
            connection_id: null,
            columns: [],
            filters: [],
            groups: [],
            sorts: [],
            joins: [],
            column_aliases: {},
            column_formats: {},
            calculated_fields: [],
            formats: {},
            user_filters: [],
            data_prep_recipe: [],
        },
        reportData: {
            headers: [],
            rows: [],
            total_rows: 0,
            current_page: 1
        },
        drillDown: {
            active: false,
            path: [],
            currentField: null,
            currentValue: null,
        },
    };

    // DOM Cache
    const DOM = {
        connectionSelect: document.getElementById("connectionSelect"),
        dataSourceAccordion: document.getElementById("dataSourceAccordion"),
        dataSourceSearch: document.getElementById("dataSourceSearch"),
        columnsBox: document.getElementById("columnsBox"),
        filtersBox: document.getElementById("filtersBox"),
        groupBox: document.getElementById("groupBox"),
        sortsBox: document.getElementById("sortsBox"),
        joinsBox: document.getElementById("joinsBox"),
        resultsContainer: document.getElementById("resultsContainer"),
        reportNameDisplay: document.getElementById("reportNameDisplay"),
        updateReportBtn: document.getElementById("updateReportBtn"),
        saveAsReportBtn: document.getElementById("saveAsReportBtn"),
        loadReportBtn: document.getElementById("loadReportBtn"),
        exportMenuBtn: document.getElementById("exportMenuBtn"),
        refreshReportBtn: document.getElementById("refreshReportBtn"),
        addCalculatedFieldBtn: document.getElementById("addCalculatedFieldBtn"),
        configHeader: document.getElementById("configHeader"),
        toggleConfigBtn: document.getElementById("toggleConfigBtn"),
        configChevron: document.getElementById("configChevron"),
        queryBuilder: document.getElementById("queryBuilder"),
    };

    // Toast notification
    const toastEl = document.getElementById("appToast");
    const toastBootstrap = toastEl ? bootstrap.Toast.getOrCreateInstance(toastEl) : null;

    // Helper functions
    const showToast = (message, isError = false) => {
        if (!toastBootstrap) {
            const notifier = isError ? window.showError : window.showSuccess;
            if (typeof notifier === 'function') {
                notifier(message);
            } else {
                console.error('Notification:', message);
            }
            return;
        }
        document.getElementById("toastTitle").textContent = isError ? "Error" : "Success";
        document.getElementById("toastBody").textContent = message;
        toastEl.classList.remove("bg-success", "bg-danger", "text-white");
        toastEl.classList.add(isError ? "bg-danger" : "bg-success", "text-white");
        toastBootstrap.show();
    };

    const sanitizeForId = (name) => String(name).replace(/[^a-zA-Z0-9\-_]/g, "-");
    
    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    // Django-specific fetch with CSRF token
    function fetchWithCSRF(url, options = {}) {
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCSRFToken(),
            },
        };
        
        return fetch(url, {
            ...defaultOptions,
            ...options,
            headers: {
                ...defaultOptions.headers,
                ...(options.headers || {}),
            },
        });
    }

    // ================================================================
    // 2. INITIALIZATION
    // ================================================================

    function initializeDropZones() {
        const dropZoneIds = ["columnsBox", "filtersBox", "groupBox", "sortsBox"];
        dropZoneIds.forEach((id) => {
            const element = document.getElementById(id);
            if (element) {
                new Sortable(element, {
                    group: "fields",
                    animation: 150,
                    onAdd: handleFieldDropped,
                    onEnd: syncConfigAndState,
                });
            }
        });
    }

    function initializeEventListeners() {
        // Main action buttons
        DOM.updateReportBtn?.addEventListener("click", () => saveOrUpdateReport(true));
        DOM.saveAsReportBtn?.addEventListener("click", openSaveAsModal);
        DOM.loadReportBtn?.addEventListener("click", openLoadModal);
        DOM.refreshReportBtn?.addEventListener("click", () => generateReport(1));

        // Connection and data source events
        DOM.connectionSelect?.addEventListener("change", loadDataSources);
        DOM.dataSourceSearch?.addEventListener("input", debounce(filterDataSources, 300));

        // Calculated fields
        DOM.addCalculatedFieldBtn?.addEventListener("click", openCalculatedFieldModal);

        // Export buttons
        document.getElementById("exportExcelBtn")?.addEventListener("click", () => exportReport('excel'));
        document.getElementById("exportCsvBtn")?.addEventListener("click", () => exportReport('csv'));
        document.getElementById("exportPdfBtn")?.addEventListener("click", () => exportReport('pdf'));

        // Modal buttons
        document.getElementById("confirmSaveReport")?.addEventListener("click", () => saveOrUpdateReport(false));
        document.getElementById("saveCalculatedField")?.addEventListener("click", saveCalculatedField);

        // Configuration toggle
        DOM.toggleConfigBtn?.addEventListener("click", toggleConfigVisibility);
        DOM.configHeader?.addEventListener("click", toggleConfigVisibility);

        // Delegated event listeners for dynamic content
        document.getElementById("queryBuilder")?.addEventListener("click", (e) => {
            const removeIcon = e.target.closest(".remove-icon");
            if (removeIcon) {
                removeIcon.closest(".pill, .filter-pill, .join-pill")?.remove();
                syncConfigAndState();
            }

            const formatIcon = e.target.closest(".format-trigger");
            if (formatIcon) {
                openFormattingModal(formatIcon);
            }
        });

        DOM.resultsContainer?.addEventListener("click", (e) => {
            const drillElement = e.target.closest(".drillable-value");
            if (drillElement) {
                handleDrillDown(drillElement.dataset.field, drillElement.dataset.value);
            }
        });
    }

    async function initializeApp() {
        initializeDropZones();
        initializeEventListeners();
        await populateConnectionsDropdown();
    }

    // ================================================================
    // 3. DATA LOADING & POPULATION
    // ================================================================

    async function populateConnectionsDropdown() {
        try {
            const response = await fetchWithCSRF(URLS.connections);
            if (!response.ok) throw new Error("Failed to fetch connections");
            
            const connections = await response.json();
            DOM.connectionSelect.innerHTML = '<option value="">Select connection...</option>';
            
            connections.forEach((conn) => {
                const option = new Option(conn.nickname, conn.id);
                DOM.connectionSelect.add(option);
            });
        } catch (error) {
            console.error(error);
            showToast("Failed to load connections", true);
            DOM.connectionSelect.innerHTML = '<option value="">Error loading connections</option>';
        }
    }

    async function loadDataSources() {
        const connectionId = DOM.connectionSelect.value;
        AppState.reportConfig.connection_id = connectionId;
        AppState.dataSourceSchema = {};

        if (!connectionId) {
            DOM.dataSourceAccordion.innerHTML = '<div class="text-center text-muted p-3">Select a connection to view tables</div>';
            return;
        }

        try {
            DOM.dataSourceAccordion.innerHTML = '<div class="text-center p-3"><div class="spinner-border spinner-border-sm" role="status"></div> Loading tables...</div>';
            
            const response = await fetchWithCSRF(URLS.tables(connectionId));
            if (!response.ok) throw new Error("Failed to fetch tables");
            
            const tables = await response.json();
            
            if (!Array.isArray(tables) || tables.length === 0) {
                DOM.dataSourceAccordion.innerHTML = '<div class="text-center text-muted p-3">No tables found</div>';
                return;
            }

            await renderDataSources(tables);
        } catch (error) {
            console.error(error);
            showToast("Failed to load data sources", true);
            DOM.dataSourceAccordion.innerHTML = '<div class="text-center text-danger p-3">Error loading tables</div>';
        }
    }

    async function renderDataSources(tables) {
        let accordionHTML = '';
        
        for (let i = 0; i < tables.length; i++) {
            const table = tables[i];
            const sanitizedId = sanitizeForId(table);
            
            accordionHTML += `
                <div class="accordion-item">
                    <h6 class="accordion-header" id="heading-${sanitizedId}">
                        <button class="accordion-button collapsed small py-2" type="button" 
                                data-bs-toggle="collapse" data-bs-target="#collapse-${sanitizedId}">
                            <i class="fas fa-table me-2"></i>${table}
                        </button>
                    </h6>
                    <div id="collapse-${sanitizedId}" class="accordion-collapse collapse" 
                         data-bs-parent="#dataSourceAccordion">
                        <div class="accordion-body p-2">
                            <div class="text-center p-2">
                                <div class="spinner-border spinner-border-sm" role="status"></div>
                                <div class="small text-muted mt-1">Loading columns...</div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }
        
        DOM.dataSourceAccordion.innerHTML = accordionHTML;

        // Add event listeners for accordion expansion
        tables.forEach(table => {
            const sanitizedId = sanitizeForId(table);
            const accordionBtn = document.querySelector(`#heading-${sanitizedId} .accordion-button`);
            accordionBtn?.addEventListener('click', () => loadTableColumns(table, sanitizedId));
        });
    }

    async function loadTableColumns(tableName, sanitizedId) {
        const connectionId = AppState.reportConfig.connection_id;
        if (!connectionId) return;

        const collapseElement = document.getElementById(`collapse-${sanitizedId}`);
        if (!collapseElement) return;

        // Check if already loaded
        if (AppState.dataSourceSchema[tableName]) {
            renderTableColumns(tableName, sanitizedId, AppState.dataSourceSchema[tableName]);
            return;
        }

        try {
            const response = await fetchWithCSRF(URLS.tableColumns(connectionId, tableName));
            if (!response.ok) throw new Error("Failed to fetch columns");
            
            const data = await response.json();
            const columns = data.columns || [];
            
            AppState.dataSourceSchema[tableName] = columns;
            renderTableColumns(tableName, sanitizedId, columns);
        } catch (error) {
            console.error(error);
            collapseElement.querySelector('.accordion-body').innerHTML = 
                '<div class="text-center text-danger p-2">Error loading columns</div>';
        }
    }

    function renderTableColumns(tableName, sanitizedId, columns) {
        const collapseElement = document.getElementById(`collapse-${sanitizedId}`);
        if (!collapseElement) return;

        let columnsHTML = '';
        columns.forEach(col => {
            const columnName = col.name || col;
            const columnType = col.type || 'unknown';
            
            columnsHTML += `
                <div class="field-item" draggable="true" 
                     data-field="${tableName}.${columnName}" 
                     data-table="${tableName}" 
                     data-column="${columnName}"
                     data-type="${columnType}">
                    <i class="fas fa-grip-vertical me-1 text-muted" style="font-size: 0.7rem;"></i>
                    <small>${columnName}</small>
                    <small class="text-muted ms-auto">${columnType}</small>
                </div>
            `;
        });

        collapseElement.querySelector('.accordion-body').innerHTML = columnsHTML;

        // Add drag event listeners
        collapseElement.querySelectorAll('.field-item').forEach(item => {
            item.addEventListener('dragstart', (e) => {
                e.dataTransfer.setData('text/plain', JSON.stringify({
                    field: item.dataset.field,
                    table: item.dataset.table,
                    column: item.dataset.column,
                    type: item.dataset.type
                }));
            });
        });
    }

    function filterDataSources() {
        const searchTerm = DOM.dataSourceSearch.value.toLowerCase();
        const accordionItems = DOM.dataSourceAccordion.querySelectorAll('.accordion-item');
        
        accordionItems.forEach(item => {
            const tableName = item.querySelector('.accordion-button').textContent.toLowerCase();
            const columns = item.querySelectorAll('.field-item');
            
            let hasMatch = tableName.includes(searchTerm);
            
            columns.forEach(col => {
                const columnText = col.textContent.toLowerCase();
                const columnMatch = columnText.includes(searchTerm);
                col.style.display = columnMatch ? 'flex' : 'none';
                if (columnMatch) hasMatch = true;
            });
            
            item.style.display = hasMatch ? 'block' : 'none';
        });
    }

    // ================================================================
    // 4. DRAG & DROP HANDLING
    // ================================================================

    function handleFieldDropped(evt) {
        const item = evt.item;
        const targetContainer = evt.to;
        const fieldData = item.dataset.field;
        
        if (!fieldData) return;

        const containerType = targetContainer.dataset.type;
        
        // Create appropriate pill based on container type
        switch (containerType) {
            case 'columns':
                createColumnPill(item, fieldData);
                break;
            case 'filters':
                createFilterPill(item, fieldData);
                break;
            case 'groups':
                createGroupPill(item, fieldData);
                break;
            case 'sorts':
                createSortPill(item, fieldData);
                break;
        }

        syncConfigAndState();
    }

    function createColumnPill(item, fieldData) {
        const [table, column] = fieldData.split('.');
        const columnType = item.dataset.type || 'text';
        
        item.outerHTML = `
            <div class="pill" data-field="${fieldData}" data-type="column">
                <i class="fas fa-columns me-1"></i>
                <span>${column}</span>
                <select class="form-select form-select-sm d-inline-block ms-2" style="width: auto;" onchange="syncConfigAndState()">
                    <option value="NONE">No Aggregation</option>
                    ${columnType.includes('int') || columnType.includes('float') || columnType.includes('decimal') ? `
                        <option value="SUM">Sum</option>
                        <option value="AVG">Average</option>
                        <option value="MIN">Minimum</option>
                        <option value="MAX">Maximum</option>
                    ` : ''}
                    <option value="COUNT">Count</option>
                </select>
                <i class="fas fa-palette format-trigger ms-2" title="Format Column"></i>
                <i class="fas fa-times remove-icon"></i>
            </div>
        `;
    }

    function createFilterPill(item, fieldData) {
        const [table, column] = fieldData.split('.');
        
        item.outerHTML = `
            <div class="filter-pill" data-field="${fieldData}" data-type="filter">
                <i class="fas fa-filter me-1"></i>
                <span>${column}</span>
                <select class="form-select form-select-sm d-inline-block ms-2" style="width: 80px;" onchange="updateFilterOperator(this)">
                    <option value="=">=</option>
                    <option value="!=">≠</option>
                    <option value=">">&gt;</option>
                    <option value="<">&lt;</option>
                    <option value=">=">&ge;</option>
                    <option value="<=">&le;</option>
                    <option value="LIKE">Contains</option>
                    <option value="IN">In List</option>
                </select>
                <input type="text" class="form-control form-control-sm d-inline-block ms-2" 
                       style="width: 120px;" placeholder="Value" onchange="syncConfigAndState()">
                <i class="fas fa-times remove-icon"></i>
            </div>
        `;
    }

    function createGroupPill(item, fieldData) {
        const [table, column] = fieldData.split('.');
        
        item.outerHTML = `
            <div class="pill" data-field="${fieldData}" data-type="group">
                <i class="fas fa-layer-group me-1"></i>
                <span>${column}</span>
                <select class="form-select form-select-sm d-inline-block ms-2" style="width: auto;" onchange="syncConfigAndState()">
                    <option value="exact">Exact</option>
                    <option value="date">Date</option>
                    <option value="month">Month</option>
                    <option value="quarter">Quarter</option>
                    <option value="year">Year</option>
                    <option value="range">Range</option>
                </select>
                <i class="fas fa-times remove-icon"></i>
            </div>
        `;
    }

    function createSortPill(item, fieldData) {
        const [table, column] = fieldData.split('.');
        
        item.outerHTML = `
            <div class="pill" data-field="${fieldData}" data-type="sort">
                <i class="fas fa-sort me-1"></i>
                <span>${column}</span>
                <select class="form-select form-select-sm d-inline-block ms-2" style="width: auto;" onchange="syncConfigAndState()">
                    <option value="ASC">Ascending</option>
                    <option value="DESC">Descending</option>
                </select>
                <i class="fas fa-times remove-icon"></i>
            </div>
        `;
    }

    // ================================================================
    // 5. CONFIG SYNCHRONIZATION
    // ================================================================

    function syncConfigAndState() {
        // Sync columns
        AppState.reportConfig.columns = [];
        DOM.columnsBox.querySelectorAll('.pill[data-type="column"]').forEach(pill => {
            const field = pill.dataset.field;
            const aggSelect = pill.querySelector('select');
            const aggregation = aggSelect ? aggSelect.value : 'NONE';
            
            AppState.reportConfig.columns.push({
                field: field,
                agg: aggregation,
                aggregation: aggregation
            });
        });

        // Sync filters
        AppState.reportConfig.filters = [];
        DOM.filtersBox.querySelectorAll('.filter-pill[data-type="filter"]').forEach(pill => {
            const field = pill.dataset.field;
            const opSelect = pill.querySelector('select');
            const valueInput = pill.querySelector('input');
            
            if (opSelect && valueInput && valueInput.value) {
                AppState.reportConfig.filters.push({
                    field: field,
                    operator: opSelect.value,
                    op: opSelect.value,
                    value: valueInput.value,
                    val: valueInput.value
                });
            }
        });

        // Sync groups
        AppState.reportConfig.groups = [];
        DOM.groupBox.querySelectorAll('.pill[data-type="group"]').forEach(pill => {
            const field = pill.dataset.field;
            const methodSelect = pill.querySelector('select');
            const method = methodSelect ? methodSelect.value : 'exact';
            
            AppState.reportConfig.groups.push({
                field: field,
                method: method
            });
        });

        // Sync sorts
        AppState.reportConfig.sorts = [];
        DOM.sortsBox.querySelectorAll('.pill[data-type="sort"]').forEach(pill => {
            const field = pill.dataset.field;
            const dirSelect = pill.querySelector('select');
            const direction = dirSelect ? dirSelect.value : 'ASC';
            
            AppState.reportConfig.sorts.push({
                field: field,
                dir: direction
            });
        });

        updateDropZonePlaceholders();
    }

    function updateDropZonePlaceholders() {
        ['columnsBox', 'filtersBox', 'groupBox', 'sortsBox'].forEach(boxId => {
            const box = document.getElementById(boxId);
            if (!box) return;
            
            const placeholder = box.querySelector('.drop-zone-placeholder');
            if (box.children.length > 1) {
                placeholder?.style.setProperty('display', 'none');
            } else {
                placeholder?.style.removeProperty('display');
            }
        });
    }

    // ================================================================
    // 6. REPORT GENERATION
    // ================================================================

    async function generateReport(page = 1) {
        if (!AppState.reportConfig.connection_id) {
            showToast("Please select a connection first", true);
            return;
        }

        if (!AppState.reportConfig.columns.length && !AppState.reportConfig.groups.length) {
            showToast("Please add at least one column or group", true);
            return;
        }

        try {
            DOM.resultsContainer.innerHTML = `
                <div class="text-center py-5">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">Generating report...</span>
                    </div>
                    <div class="mt-3">Building your report...</div>
                </div>
            `;

            const reportConfig = {
                ...AppState.reportConfig,
                page: page,
                page_size: 100
            };

            const response = await fetchWithCSRF(URLS.executeReport, {
                method: 'POST',
                body: JSON.stringify({ report_config: reportConfig }),
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Report generation failed');
            }

            const result = await response.json();
            
            if (!result.success) {
                throw new Error(result.error || 'Report generation failed');
            }

            AppState.reportData = {
                headers: result.data.headers,
                rows: result.data.rows,
                total_rows: result.total_rows,
                current_page: page
            };

            renderReportResults();
            collapseConfig(); // Hide config panel after successful generation
            
        } catch (error) {
            console.error('Report generation error:', error);
            showToast(error.message || "Failed to generate report", true);
            DOM.resultsContainer.innerHTML = `
                <div class="text-center py-5 text-danger">
                    <i class="fas fa-exclamation-circle fa-3x mb-3"></i>
                    <h5>Report Generation Failed</h5>
                    <p>${error.message || "An unexpected error occurred"}</p>
                    <button class="btn btn-primary" onclick="generateReport()">
                        <i class="fas fa-redo"></i> Try Again
                    </button>
                </div>
            `;
        }
    }

    function renderReportResults() {
        const { headers, rows, total_rows, current_page } = AppState.reportData;
        
        if (!headers || !headers.length) {
            DOM.resultsContainer.innerHTML = `
                <div class="text-center py-5 text-muted">
                    <i class="fas fa-table fa-3x mb-3"></i>
                    <h5>No Data</h5>
                    <p>The report returned no results</p>
                </div>
            `;
            return;
        }

        let tableHTML = `
            <div class="d-flex justify-content-between align-items-center mb-3">
                <h6 class="mb-0">Report Results</h6>
                <small class="text-muted">
                    Showing ${rows.length} of ${total_rows} rows
                </small>
            </div>
            <table class="table table-sm table-hover results-table">
                <thead>
                    <tr>
                        ${headers.map(header => `
                            <th>
                                ${header}
                                <i class="fas fa-palette format-trigger ms-2" data-column="${header}" title="Format Column"></i>
                            </th>
                        `).join('')}
                    </tr>
                </thead>
                <tbody>
        `;

        rows.forEach(row => {
            tableHTML += '<tr>';
            headers.forEach(header => {
                const value = row[header];
                const displayValue = value !== null && value !== undefined ? value : '';
                
                // Make certain values drillable
                const isDrillable = typeof value === 'string' || typeof value === 'number';
                const cellClass = isDrillable ? 'drillable-value' : '';
                const cellAttrs = isDrillable ? `data-field="${header}" data-value="${value}"` : '';
                
                tableHTML += `<td><span class="${cellClass}" ${cellAttrs}>${displayValue}</span></td>`;
            });
            tableHTML += '</tr>';
        });

        tableHTML += `
                </tbody>
            </table>
        `;

        // Add pagination if needed
        if (total_rows > 100) {
            const totalPages = Math.ceil(total_rows / 100);
            tableHTML += renderPagination(current_page, totalPages);
        }

        DOM.resultsContainer.innerHTML = tableHTML;
    }

    function renderPagination(currentPage, totalPages) {
        let paginationHTML = `
            <nav aria-label="Report pagination" class="mt-4">
                <ul class="pagination pagination-sm justify-content-center">
        `;

        // Previous button
        paginationHTML += `
            <li class="page-item ${currentPage === 1 ? 'disabled' : ''}">
                <a class="page-link" href="#" onclick="generateReport(${currentPage - 1})" 
                   ${currentPage === 1 ? 'tabindex="-1"' : ''}>
                    <i class="fas fa-chevron-left"></i>
                </a>
            </li>
        `;

        // Page numbers
        const startPage = Math.max(1, currentPage - 2);
        const endPage = Math.min(totalPages, currentPage + 2);

        if (startPage > 1) {
            paginationHTML += `<li class="page-item"><a class="page-link" href="#" onclick="generateReport(1)">1</a></li>`;
            if (startPage > 2) {
                paginationHTML += `<li class="page-item disabled"><span class="page-link">...</span></li>`;
            }
        }

        for (let i = startPage; i <= endPage; i++) {
            paginationHTML += `
                <li class="page-item ${i === currentPage ? 'active' : ''}">
                    <a class="page-link" href="#" onclick="generateReport(${i})">${i}</a>
                </li>
            `;
        }

        if (endPage < totalPages) {
            if (endPage < totalPages - 1) {
                paginationHTML += `<li class="page-item disabled"><span class="page-link">...</span></li>`;
            }
            paginationHTML += `<li class="page-item"><a class="page-link" href="#" onclick="generateReport(${totalPages})">${totalPages}</a></li>`;
        }

        // Next button
        paginationHTML += `
            <li class="page-item ${currentPage === totalPages ? 'disabled' : ''}">
                <a class="page-link" href="#" onclick="generateReport(${currentPage + 1})" 
                   ${currentPage === totalPages ? 'tabindex="-1"' : ''}>
                    <i class="fas fa-chevron-right"></i>
                </a>
            </li>
        `;

        paginationHTML += `
                </ul>
            </nav>
        `;

        return paginationHTML;
    }

    // ================================================================
    // 7. MODAL FUNCTIONS
    // ================================================================

    function openSaveAsModal() {
        const modal = document.getElementById('saveReportModal');
        const nameInput = document.getElementById('reportName');
        const descInput = document.getElementById('reportDescription');
        
        nameInput.value = '';
        descInput.value = '';
        
        Modal.show('saveReportModal');
    }

    async function openLoadModal() {
        Modal.show('loadReportModal');
        const modalBody = document.getElementById('loadReportModalBody');
        
        modalBody.innerHTML = `
            <div class="text-center py-3">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Loading reports...</span>
                </div>
            </div>
        `;

        try {
            const response = await fetchWithCSRF(URLS.loadReports);
            const result = await response.json();
            
            if (result.success && result.reports.length > 0) {
                let reportsHTML = `
                    <div class="list-group">
                        ${result.reports.map(report => `
                            <a href="#" class="list-group-item list-group-item-action" 
                               onclick="loadReport('${report.id}', '${report.name}')">
                                <div class="d-flex justify-content-between align-items-center">
                                    <div>
                                        <h6 class="mb-1">${report.name}</h6>
                                        <small class="text-muted">Created: ${new Date(report.created_at).toLocaleDateString()}</small>
                                    </div>
                                    <i class="fas fa-folder-open text-primary"></i>
                                </div>
                            </a>
                        `).join('')}
                    </div>
                `;
                modalBody.innerHTML = reportsHTML;
            } else {
                modalBody.innerHTML = `
                    <div class="text-center py-5 text-muted">
                        <i class="fas fa-folder-open fa-3x mb-3"></i>
                        <h5>No Saved Reports</h5>
                        <p>You haven't saved any reports yet.</p>
                    </div>
                `;
            }
        } catch (error) {
            modalBody.innerHTML = `
                <div class="text-center py-5 text-danger">
                    <i class="fas fa-exclamation-circle fa-3x mb-3"></i>
                    <h5>Error Loading Reports</h5>
                    <p>${error.message}</p>
                </div>
            `;
        }
    }

    function openCalculatedFieldModal() {
        Modal.show('calculatedFieldModal');
        // Populate available fields list
        populateAvailableFields();
    }

    function populateAvailableFields() {
        const fieldsList = document.getElementById('availableFieldsList');
        if (!fieldsList) return;

        let fieldsHTML = '';
        Object.keys(AppState.dataSourceSchema).forEach(tableName => {
            const columns = AppState.dataSourceSchema[tableName];
            fieldsHTML += `<div class="fw-bold small text-primary mb-1">${tableName}</div>`;
            
            columns.forEach(col => {
                const columnName = col.name || col;
                fieldsHTML += `
                    <div class="field-reference-item small mb-1 p-1 rounded" 
                         style="cursor: pointer; background: #f8f9fa;"
                         onclick="insertFieldReference('${tableName}', '${columnName}')">
                        ${columnName} <small class="text-muted">(${col.type || 'unknown'})</small>
                    </div>
                `;
            });
            fieldsHTML += '<div class="mb-2"></div>';
        });

        fieldsList.innerHTML = fieldsHTML;
    }

    function insertFieldReference(tableName, columnName) {
        const textarea = document.getElementById('calcFieldFormula');
        if (!textarea) return;

        const reference = `[${tableName}.${columnName}]`;
        const start = textarea.selectionStart;
        const end = textarea.selectionEnd;
        const currentValue = textarea.value;

        textarea.value = currentValue.slice(0, start) + reference + currentValue.slice(end);
        textarea.setSelectionRange(start + reference.length, start + reference.length);
        textarea.focus();
    }

    async function saveCalculatedField() {
        const nameInput = document.getElementById('calcFieldName');
        const formulaInput = document.getElementById('calcFieldFormula');
        const descInput = document.getElementById('calcFieldDescription');

        if (!nameInput.value.trim() || !formulaInput.value.trim()) {
            showToast("Name and formula are required", true);
            return;
        }

        const calculatedField = {
            name: nameInput.value.trim(),
            formula: formulaInput.value.trim(),
            description: descInput.value.trim(),
            type: 'calculated'
        };

        // Add to app state
        AppState.reportConfig.calculated_fields.push(calculatedField);
        
        // Clear form
        nameInput.value = '';
        formulaInput.value = '';
        descInput.value = '';

        Modal.hide('calculatedFieldModal');
        showToast("Calculated field added successfully");
        
        // Re-render calculated fields list
        renderCalculatedFields();
    }

    function renderCalculatedFields() {
        const container = document.getElementById('calculatedFieldsList');
        if (!container) return;

        const fields = AppState.reportConfig.calculated_fields || [];
        
        if (fields.length === 0) {
            container.innerHTML = '<div class="text-muted small text-center">No calculated fields</div>';
            return;
        }

        let fieldsHTML = '';
        fields.forEach((field, index) => {
            fieldsHTML += `
                <div class="field-item d-flex justify-content-between align-items-center" 
                     draggable="true" 
                     data-field="calculated.${field.name}" 
                     data-type="calculated">
                    <div>
                        <i class="fas fa-calculator me-1 text-primary"></i>
                        <small>${field.name}</small>
                    </div>
                    <button class="btn btn-sm btn-outline-danger" 
                            onclick="removeCalculatedField(${index})"
                            title="Remove">
                        <i class="fas fa-times"></i>
                    </button>
                </div>
            `;
        });

        container.innerHTML = fieldsHTML;
    }

    function removeCalculatedField(index) {
        AppState.reportConfig.calculated_fields.splice(index, 1);
        renderCalculatedFields();
        showToast("Calculated field removed");
    }

    // ================================================================
    // 8. SAVE/LOAD FUNCTIONALITY
    // ================================================================

    async function saveOrUpdateReport(isUpdate = false) {
        const reportName = isUpdate ? 
            AppState.currentReportName : 
            document.getElementById('reportName').value.trim();

        if (!reportName) {
            showToast("Report name is required", true);
            return;
        }

        try {
            const payload = {
                report_name: reportName,
                report_config: AppState.reportConfig
            };

            const response = await fetchWithCSRF(URLS.saveReport, {
                method: 'POST',
                body: JSON.stringify(payload),
            });

            const result = await response.json();

            if (result.success) {
                AppState.currentReportId = result.report_id;
                AppState.currentReportName = reportName;
                DOM.reportNameDisplay.textContent = reportName;
                
                Modal.hide('saveReportModal');
                showToast(`Report ${isUpdate ? 'updated' : 'saved'} successfully`);
            } else {
                throw new Error(result.error || 'Save failed');
            }
        } catch (error) {
            showToast(error.message || "Failed to save report", true);
        }
    }

    async function loadReport(reportId, reportName) {
        // This would load a specific report configuration
        // Implementation depends on your backend API
        Modal.hide('loadReportModal');
        showToast("Report loading functionality to be implemented");
    }

    // ================================================================
    // 9. EXPORT FUNCTIONALITY
    // ================================================================

    async function exportReport(format) {
        if (!AppState.reportData.headers.length) {
            showToast("No report data to export. Generate a report first.", true);
            return;
        }

        try {
            const payload = {
                report_config: AppState.reportConfig,
                format: format,
                data: AppState.reportData
            };

            const response = await fetchWithCSRF('/api/export-report/', {
                method: 'POST',
                body: JSON.stringify(payload),
            });

            if (!response.ok) throw new Error('Export failed');

            // Handle file download
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = `report-${Date.now()}.${format}`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            
            showToast(`Report exported as ${format.toUpperCase()}`);
        } catch (error) {
            showToast(`Export failed: ${error.message}`, true);
        }
    }

    // ================================================================
    // 10. UI UTILITY FUNCTIONS
    // ================================================================

    function toggleConfigVisibility() {
        const queryBuilder = DOM.queryBuilder;
        const chevron = DOM.configChevron;
        
        if (queryBuilder.classList.contains('show')) {
            queryBuilder.classList.remove('show');
            chevron.classList.remove('fa-chevron-up');
            chevron.classList.add('fa-chevron-down');
        } else {
            queryBuilder.classList.add('show');
            chevron.classList.remove('fa-chevron-down');
            chevron.classList.add('fa-chevron-up');
        }
    }

    function collapseConfig() {
        const queryBuilder = DOM.queryBuilder;
        const chevron = DOM.configChevron;
        
        queryBuilder.classList.remove('show');
        chevron.classList.remove('fa-chevron-up');
        chevron.classList.add('fa-chevron-down');
    }

    function handleDrillDown(field, value) {
        // Drill-down functionality
        AppState.drillDown.active = true;
        AppState.drillDown.currentField = field;
        AppState.drillDown.currentValue = value;
        
        // Add filter for drill-down
        AppState.reportConfig.filters.push({
            field: field,
            operator: '=',
            value: value,
            isDrillDown: true
        });

        generateReport(1);
        showToast(`Drilling down on ${field}: ${value}`);
    }

    // ================================================================
    // 11. INITIALIZATION
    // ================================================================

    // Initialize the application
    initializeApp();

    // Make certain functions globally available for onclick handlers
    window.generateReport = generateReport;
    window.loadReport = loadReport;
    window.insertFieldReference = insertFieldReference;
    window.removeCalculatedField = removeCalculatedField;
    window.syncConfigAndState = syncConfigAndState;
});


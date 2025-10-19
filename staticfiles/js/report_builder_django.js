// mis_app/static/js/report_builder_django.js

document.addEventListener("DOMContentLoaded", function () {
    // ================================================================
    // 1. APPLICATION STATE & CONFIGURATION
    // ================================================================

    const AppState = {
    dataSourceSchema: {},
    reportConfig: {
        connection_id: null,
        columns: [],
        filters: [],
        groups: [],
        sorts: [],
        formats: {},
        page: 1,
        page_size: 100,
        calculated_fields: [], // <-- ADD THIS LINE
    },
    reportData: { headers: [], rows: [] }, // Add this line
    drillDown: { // Add this entire object
        active: false,
        path: [], // Stores the stack of drill levels
    },
};

    const DOM = {
        connectionSelect: document.getElementById("connectionSelect"),
        dataSourceAccordion: document.getElementById("dataSourceAccordion"),
        dataSourceSearch: document.getElementById("dataSourceSearch"),
        columnsBox: document.getElementById("columnsBox"),
        filtersBox: document.getElementById("filtersBox"),
        groupBox: document.getElementById("groupBox"),
        sortsBox: document.getElementById("sortsBox"),
        queryBuilder: document.getElementById('queryBuilder'),
    };

    let currentFormatColumn = null;
    let currentShareReportId = null;
    let shares = [];

    // ================================================================
    // 2. INITIALIZATION
    // ================================================================

    function initializeApp() {
        initializeEventListeners();
        initializeDropZones();
    }

    function initializeEventListeners() {
    // --- Main Sidebar & Top Buttons ---
    DOM.connectionSelect?.addEventListener("change", handleConnectionChange);
    DOM.dataSourceSearch?.addEventListener("input", debounce(filterDataSources, 300));
    document.getElementById('prepareDataBtn')?.addEventListener('click', openDataPrepModal);
    document.getElementById('addCalculatedFieldBtn')?.addEventListener('click', openCalculatedFieldModal);
     document.getElementById('newReportBtn')?.addEventListener('click', startNewReport);
     document.getElementById('configHeader')?.addEventListener('click', toggleConfigPanel);
     document.getElementById('addUserShareBtn')?.addEventListener('click', addShare);
    document.getElementById('saveSharesBtn')?.addEventListener('click', saveShares);
    document.getElementById('exportExcelBtn')?.addEventListener('click', () => exportReport('excel'));
    document.getElementById('exportCsvBtn')?.addEventListener('click', () => exportReport('csv'));
    
    // --- Report Action Buttons ---
    document.getElementById('refreshReportBtn')?.addEventListener('click', generateReport);
    document.getElementById('loadReportBtn')?.addEventListener('click', openLoadModal);
    document.getElementById('saveAsReportBtn')?.addEventListener('click', openSaveAsModal);
    document.getElementById('updateReportBtn')?.addEventListener('click', updateReport);
    document.getElementById('drillBackBtn')?.addEventListener('click', handleDrillBack);

    // --- Modal "Confirm" Buttons ---
    document.getElementById('confirmSaveReport')?.addEventListener('click', saveReport);
    document.getElementById('saveCalculatedField')?.addEventListener('click', saveCalculatedField);
    document.getElementById('applyFormattingBtn')?.addEventListener('click', applyFormatting);
    
    // --- Interactive Filter Buttons ---
    document.getElementById('showFiltersBtn')?.addEventListener('click', createInteractiveFilters);
    document.getElementById('hideFiltersBtn')?.addEventListener('click', hideInteractiveFilters); // Add this
    document.getElementById('applyFiltersBtn')?.addEventListener('click', applyInteractiveFilters);
    document.getElementById('pageSizeSelect')?.addEventListener('change', () => generateReport(1));

    // --- Event Delegation for Dynamic Content ---

    // Handles clicks inside the main query builder (remove icons, etc.)
    if (DOM.queryBuilder) {
        DOM.queryBuilder.addEventListener('click', function(event) {
            const removeIcon = event.target.closest('.remove-icon');
            if (removeIcon) {
                removeIcon.closest('.pill, .filter-pill, .join-pill').remove();
                syncConfigAndState();
                validateJoinPath(); // Re-validate joins after removing a field
            }
        });
        // Syncs state anytime a dropdown or input inside a pill is changed
        DOM.queryBuilder.addEventListener('change', function(event) {
            if (event.target.tagName === 'SELECT' || event.target.tagName === 'INPUT') {
                syncConfigAndState();
            }
        });
    }

    // Handles clicks inside the results table (formatting and drill-down)
    document.getElementById('resultsContainer')?.addEventListener('click', function(event) {
        const formatTrigger = event.target.closest('.format-trigger');
        if (formatTrigger) {
            openFormattingModal(formatTrigger.dataset.columnName);
            return;
        }
        const drillTrigger = event.target.closest('.drillable-value');
        if (drillTrigger) {
            handleDrillDown(drillTrigger.dataset.field, drillTrigger.dataset.value);
        }
    });

    document.querySelectorAll('.action-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        toggleQuerySection(btn.dataset.section);
    });
});

    // Handles clicks inside the calculated field modal (to add fields to formula)
    document.getElementById('calculatedFieldModal')?.addEventListener('click', function(event) {
        const fieldRef = event.target.closest('[data-field-ref]');
        if (fieldRef) {
            const formulaInput = document.getElementById('calcFieldFormula');
            formulaInput.value += fieldRef.dataset.fieldRef;
            formulaInput.focus();
        }
    });
}

    function initializeDropZones() {
        const dropZoneIds = ["columnsBox", "filtersBox", "groupBox", "sortsBox"];
        dropZoneIds.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                new Sortable(element, {
                    group: "fields",
                    animation: 150,
                    onAdd: handleFieldDropped,
                    onEnd: syncConfigAndState // Sync state after dragging/reordering
                });
            }
        });
    }

    function startNewReport() {
    // 1. Reset the core application state
    AppState.currentReportId = null;
    AppState.currentReportName = 'New Report';
    AppState.reportConfig = {
        connection_id: null,
        columns: [],
        filters: [],
        groups: [],
        sorts: [],
        formats: {},
        calculated_fields: []
    };
    AppState.reportData = { headers: [], rows: [] };
    AppState.drillDown = { active: false, path: [] };

    // 2. Update the UI to reflect the new, empty state
    document.getElementById('reportNameDisplay').textContent = 'New Report';

    // 3. Clear all the pill containers
    const containers = ['columnsBox', 'filtersBox', 'groupBox', 'sortsBox', 'joinsBox'];
    containers.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.innerHTML = `<div class="drop-zone-placeholder"><i class="fas fa-columns"></i><span>Drag items here</span></div>`;
        }
    });

    // 4. Clear the results table
    document.getElementById('resultsContainer').innerHTML = `
        <div class="text-center py-5 text-muted">
            <h5>New Report</h5>
            <p>Select a connection and drag fields to begin.</p>
        </div>`;

    // 5. Reset the connection dropdown and clear the data source list
    if (DOM.connectionSelect) {
        DOM.connectionSelect.value = '';
    }
    if (DOM.dataSourceAccordion) {
        DOM.dataSourceAccordion.innerHTML = '<div class="text-center text-muted p-3 small">Select a connection to view tables.</div>';
    }

    // 6. Disable the update button as there is no saved report
    document.getElementById('updateReportBtn').disabled = true;

    console.log("New report state initialized.");
}

    // ================================================================
    // 3. STATE SYNCHRONIZATION (The Fix)
    // ================================================================
    
    /**
     * Reads all pills from the DOM and updates AppState.reportConfig.
     * This ensures the internal state always matches the UI.
     */
    function syncConfigAndState() {
    // Helper function to read data from a pill container
    const getPillData = (selector, builderFn) => {
        const container = document.querySelector(selector);
        if (!container) return [];
        return Array.from(container.children).map(pill => {
            // Ensure we don't process the placeholder text element
            if (pill.classList.contains('drop-zone-placeholder')) return null;
            try {
                return builderFn(pill);
            } catch (e) {
                console.error(`Error processing pill in ${selector}:`, pill, e);
                return null;
            }
        }).filter(Boolean); // Filter out any nulls from placeholders or errors
    };

    // Read the configuration from all the UI pill boxes
    const configFromDOM = {
        columns: getPillData("#columnsBox", pill => ({
            field: JSON.parse(pill.dataset.fieldJson).fullName,
            agg: pill.querySelector('select')?.value.toUpperCase() || 'NONE',
        })),
        filters: getPillData("#filtersBox", pill => ({
            field: JSON.parse(pill.dataset.fieldJson).fullName,
            op: pill.querySelector('.filter-op')?.value || '=',
            val: pill.querySelector('.filter-val')?.value || '',
        })),
        groups: getPillData("#groupBox", pill => ({
            field: JSON.parse(pill.dataset.fieldJson).fullName,
            // Add method if your group pills have options, e.g., for dates
            method: pill.querySelector('.group-method-select')?.value || 'exact'
        })),
        sorts: getPillData("#sortsBox", pill => ({
            field: JSON.parse(pill.dataset.fieldJson).fullName,
            dir: pill.querySelector('select')?.value.toUpperCase() || 'ASC',
        })),
        joins: getPillData("#joinsBox", pill => ({
            left_col: pill.querySelector('.join-left-col')?.value,
            type: pill.querySelector('.join-type')?.value,
            right_col: pill.querySelector('.join-right-col')?.value
        }))
    };

    // Merge the collected UI state into the global AppState.reportConfig object
    Object.assign(AppState.reportConfig, configFromDOM);

    console.log("State synchronized:", AppState.reportConfig); // For debugging
    // You can add calls to update button states or other UI elements here if needed
}

/**
 * Helper to detect field type for providing smart grouping options.
 * @param {object} field - The field data object from the schema.
 * @returns {string} 'date', 'numeric', or 'text'
 */
function getFieldType(field) {
    const dbType = (field.type || "").toUpperCase();
    const fieldName = field.name.toLowerCase();

    if (dbType.includes("DATE") || dbType.includes("TIMESTAMP") || fieldName.includes("date")) {
        return "date";
    }
    if (dbType.includes("INT") || dbType.includes("FLOAT") || dbType.includes("DECIMAL") || dbType.includes("NUMERIC")) {
        return "numeric";
    }
    return "text";
}

function createDateGroupPill(field) {
    return `
        <span>Group by <strong>${field.fullName}</strong> as </span>
        <select class="form-select form-select-sm group-method-select">
            <option value="exact">Exact Date</option>
            <option value="year">Year</option>
            <option value="quarter">Quarter</option>
            <option value="month">Month</option>
        </select>
        <div class="pill-controls">
            <i class="fas fa-times-circle remove-icon"></i>
        </div>
    `;
}

function createTextGroupPill(field) {
    return `
        <span>Group by <strong>${field.fullName}</strong></span>
        <input type="hidden" class="group-method-select" value="exact">
        <div class="pill-controls">
            <i class="fas fa-times-circle remove-icon"></i>
        </div>
    `;
}

/**
 * Checks the backend to see if a valid join path exists for the tables currently in use.
 */
async function validateJoinPath() {
    const runButton = document.getElementById('refreshReportBtn');
    const joinAlert = document.getElementById('joinStatusAlert');

    // Get all unique table names, excluding calculated fields
    const tablesInUse = [...new Set(
        Array.from(document.querySelectorAll('.pill[data-field-json], .filter-pill[data-field-json]'))
            .map(pill => {
                const fieldData = JSON.parse(pill.dataset.fieldJson);
                return fieldData.fullName.startsWith('calc.') ? null : fieldData.fullName.split('.')[0];
            })
            .filter(table => table !== null && table !== 'calc')
    )];

    // If less than two tables, no join needed
    if (tablesInUse.length < 2) {
        joinAlert.style.display = 'none';
        runButton.disabled = false;
        return;
    }

    joinAlert.style.display = 'block';
    joinAlert.className = 'alert alert-secondary small p-2 mt-2';
    joinAlert.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Checking data model for joins...`;

    try {
        const response = await fetch(URLS.checkJoinPath, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken },
            body: JSON.stringify({
                connection_id: AppState.reportConfig.connection_id,
                tables: tablesInUse
            })
        });
        
        const result = await response.json();
        
        if (!result.success) {
            throw new Error(result.error);
        }

        if (result.path_exists) {
            // Joins found in data model
            joinAlert.className = 'alert alert-success small p-2 mt-2';
            joinAlert.innerHTML = `<i class="fas fa-check-circle me-2"></i>${result.message}`;
            runButton.disabled = false;
        } else {
            // No joins found - try auto-join
            joinAlert.className = 'alert alert-warning small p-2 mt-2';
            joinAlert.innerHTML = `
                <i class="fas fa-exclamation-triangle me-2"></i>${result.message}
                <button class="btn btn-sm btn-outline-primary ms-2" onclick="attemptAutoJoin()">Try Auto-Join</button>
                <button class="btn btn-sm btn-outline-secondary ms-1" onclick="addJoinPill()">Add Manual Join</button>
            `;
            runButton.disabled = true;
        }

    } catch (error) {
        joinAlert.className = 'alert alert-danger small p-2 mt-2';
        joinAlert.innerHTML = `<i class="fas fa-times-circle me-2"></i>Error checking joins: ${error.message}`;
        runButton.disabled = true;
    }
}

// Add this function to handle auto-join attempts
window.attemptAutoJoin = async function() {
    const joinAlert = document.getElementById('joinStatusAlert');
    const tablesInUse = [...new Set(
        Array.from(document.querySelectorAll('.pill[data-field-json], .filter-pill[data-field-json]'))
            .map(pill => JSON.parse(pill.dataset.fieldJson).fullName.split('.')[0])
            .filter(table => table !== 'calc')
    )];

    joinAlert.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Attempting auto-join...`;

    try {
        const response = await fetch('/api/auto_find_joins/', {  // Add this URL to your urls.py
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken },
            body: JSON.stringify({
                connection_id: AppState.reportConfig.connection_id,
                tables: tablesInUse
            })
        });
        
        const result = await response.json();
        
        if (result.success && result.joins.length > 0) {
            // Add auto-detected joins to UI
            result.joins.forEach(join => {
                // You'll need to implement this function to add joins to the UI
                addAutoDetectedJoin(join);
            });
            
            joinAlert.className = 'alert alert-info small p-2 mt-2';
            joinAlert.innerHTML = `<i class="fas fa-robot me-2"></i>Added ${result.joins.length} auto-detected joins`;
        } else {
            joinAlert.className = 'alert alert-warning small p-2 mt-2';
            joinAlert.innerHTML = `<i class="fas fa-exclamation-triangle me-2"></i>No auto-joins detected. Please add manual joins.`;
        }
    } catch (error) {
        joinAlert.className = 'alert alert-danger small p-2 mt-2';
        joinAlert.innerHTML = `<i class="fas fa-times-circle me-2"></i>Auto-join failed: ${error.message}`;
    }
};

    // ================================================================
    // 4. DATA LOADING & RENDERING
    // ================================================================

    async function handleConnectionChange() {
    const connectionId = DOM.connectionSelect.value;
    AppState.reportConfig.connection_id = connectionId;
    AppState.dataSourceSchema = {}; // Clear previous schema

    if (!connectionId) {
        DOM.dataSourceAccordion.innerHTML = '<div class="text-center text-muted p-3 small">Select a connection to view tables.</div>';
        return;
    }

    DOM.dataSourceAccordion.innerHTML = `<div class="text-center p-3"><div class="spinner-border spinner-border-sm"></div> <span class="ms-2">Loading schema...</span></div>`;

    try {
        // Step 1: Fetch the list of visible table names
        const tablesUrl = URLS.tables(connectionId);
        const tablesResponse = await fetch(tablesUrl);
        if (!tablesResponse.ok) throw new Error('Failed to fetch tables.');
        const result = await tablesResponse.json();
        const tables = result.tables || result; // Handle both response formats
        if (!Array.isArray(tables) || tables.length === 0) {
            DOM.dataSourceAccordion.innerHTML = '<div class="text-center text-muted p-3 small">No tables found.</div>';
            return;
        }

        // Step 2: Fetch all columns for ALL visible tables in a single request
        const columnsResponse = await fetch(URLS.tableColumns, {
             method: 'POST',
             headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken },
             body: JSON.stringify({
                 connection_id: connectionId,
                 tables: tables
             })
        });
        if (!columnsResponse.ok) throw new Error('Failed to fetch column details.');
        const columnsResult = await columnsResponse.json();
        
        // Step 3: Populate the AppState schema object
        const schema = {};
        columnsResult.columns.forEach(col => {
            if (!schema[col.source]) {
                schema[col.source] = [];
            }
            schema[col.source].push(col);
        });
        AppState.dataSourceSchema = schema;

        // Step 4: Render the entire sidebar with the fully populated schema
        renderDataSources(Object.keys(schema));

    } catch (error) {
        console.error("Error loading data sources:", error);
        DOM.dataSourceAccordion.innerHTML = `<div class="alert alert-danger p-2 small m-2"><strong>Failed to load schema:</strong><br>${error.message}</div>`;
    }
}


    function renderDataSources(tables) {
        if (!tables || tables.length === 0) {
            DOM.dataSourceAccordion.innerHTML = '<div class="text-center text-muted p-3 small">No tables found.</div>';
            return;
        }
        const accordionHTML = tables.map(tableName => {
            const sanitizedId = tableName.replace(/[^a-zA-Z0-9]/g, '');
            return `
                <div class="accordion-item">
                    <h2 class="accordion-header" id="heading-${sanitizedId}">
                        <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" 
                                data-bs-target="#collapse-${sanitizedId}" aria-expanded="false" 
                                data-table-name="${tableName}">
                            <i class="fas fa-table me-2"></i>${tableName}
                        </button>
                    </h2>
                    <div id="collapse-${sanitizedId}" class="accordion-collapse collapse" data-bs-parent="#dataSourceAccordion">
                        <div class="accordion-body p-1"><div class="text-center p-2"><div class="spinner-border spinner-border-sm"></div></div></div>
                    </div>
                </div>`;
        }).join('');
        DOM.dataSourceAccordion.innerHTML = accordionHTML;
        DOM.dataSourceAccordion.querySelectorAll('.accordion-collapse').forEach(el => {
            el.addEventListener('show.bs.collapse', handleTableExpand);
        });
    }

    async function handleTableExpand(event) {
    // This function now becomes much simpler. The data is already loaded in AppState.
    // Its only job is to render the columns when the accordion expands.
    const accordionBody = event.target.querySelector('.accordion-body');
    const button = document.querySelector(`[data-bs-target="#${event.target.id}"]`);
    const tableName = button.dataset.tableName;

    // Check if columns have already been rendered to avoid re-rendering
    if (accordionBody.querySelector('.field-list')) {
        return; 
    }
    
    // Render columns from the pre-loaded schema
    renderTableColumns(tableName, accordionBody);
}

    function renderTableColumns(tableName, container) {
        const columns = AppState.dataSourceSchema[tableName];
        if (!columns) return;
        const columnsHTML = columns.map(col => `
            <div class="field-item" draggable="true" 
                 data-field-json='${JSON.stringify({ fullName: `${tableName}.${col.name}`, ...col })}'>
                <span>${col.name}</span><small class="text-muted ms-auto">${String(col.type).split('(')[0]}</small>
            </div>`).join('');
        container.innerHTML = `<div class="field-list list-group list-group-flush">${columnsHTML}</div>`;
        const fieldList = container.querySelector('.field-list');
        if (fieldList) {
            new Sortable(fieldList, {
                group: { name: 'fields', pull: 'clone', put: false },
                sort: false,
                animation: 150,
            });
        }
    }


// mis_app/static/js/report_builder_django.js

async function generateReport(page = 1) {
    // First, ensure the AppState is up-to-date with the UI
    syncConfigAndState();

    AppState.reportConfig.page = page;
    AppState.reportConfig.page_size = parseInt(document.getElementById('pageSizeSelect').value, 10) || 100;

    const reportConfig = AppState.reportConfig;

    if (!reportConfig.connection_id) {
        showError("Please select a connection first.");
        return;
    }
    if (reportConfig.columns.length === 0 && reportConfig.groups.length === 0) {
        showError("Please add at least one column or group to your report.");
        return;
    }

    const runButton = document.getElementById('refreshReportBtn');
    const resultsContainer = document.getElementById('resultsContainer');

    // Show a loading state
    runButton.disabled = true;
    runButton.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Running...`;
    resultsContainer.innerHTML = `
        <div class="text-center py-5">
            <div class="spinner-border text-primary" style="width: 3rem; height: 3rem;"></div>
            <p class="mt-3">Fetching data from server...</p>
        </div>`;

    try {
        const response = await fetch(URLS.executeReport, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken
            },
            body: JSON.stringify(reportConfig)
        });

        const result = await response.json();

        if (!response.ok || result.error) {
            throw new Error(result.error || `Server returned status ${response.status}`);
        }

        // On success, render the data table
        AppState.reportData = {
            headers: result.data.headers,
            rows: result.data.rows
        };

        renderTable(AppState.reportData.headers, AppState.reportData.rows);
        renderPagination(result.pagination, result.total_rows);

        // Auto-collapse the query builder panel
        if (DOM.queryBuilder) {
            const collapseElement = bootstrap.Collapse.getOrCreateInstance(DOM.queryBuilder);
            collapseElement.hide();
        }

    } catch (error) {
        console.error("Error generating report:", error);
        resultsContainer.innerHTML = `<div class="alert alert-danger m-3"><strong>Report Failed:</strong> ${error.message}</div>`;
    } finally {
        // Restore the button to its normal state
        runButton.disabled = false;
        runButton.innerHTML = `<i class="fas fa-play"></i> Run Report`;
    }
}


function renderTable(headers, rows) {
    const resultsContainer = document.getElementById('resultsContainer');
    AppState.reportData = { headers, rows }; // Store the latest data

    if (!headers || headers.length === 0 || !rows || rows.length === 0) {
        resultsContainer.innerHTML = `
            <div class="text-center py-5 text-muted">
                <i class="fas fa-table fa-3x mb-3"></i>
                <h5>Query returned no results.</h5>
            </div>`;
        return;
    }

    const formats = AppState.reportConfig.formats || {};
    // Identify which columns are part of a 'group by'
    const groupedColumns = new Set((AppState.reportConfig.groups || []).map(g => g.field.replace('.', '_') + '_' + (g.method || 'exact')));

    let tableHTML = '<table class="table table-striped table-sm table-hover">';
    tableHTML += '<thead class="table-light"><tr>';
    headers.forEach(header => {
        const format = formats[header] || {};
        const displayName = format.alias || header;
        tableHTML += `<th><span class="format-trigger" data-column-name="${header}" style="cursor: pointer;">${displayName} <i class="fas fa-cog fa-xs text-muted"></i></span></th>`;
    });
    tableHTML += '</tr></thead>';

    tableHTML += '<tbody>';
    rows.forEach(row => {
        tableHTML += '<tr>';
        headers.forEach(header => {
            const format = formats[header] || {};
            let value = row[header];
            let cellContent = value === null || value === undefined ? '' : value;
            let cellStyle = '';

            // Apply formatting (from previous step)
            if (format.type === 'number' || format.type === 'currency' || format.type === 'percent') {
                 // ... existing formatting logic ...
                 cellStyle = 'text-align: right;';
            }

            // --- DRILL-DOWN LOGIC ---
            // If the current header is one of the grouped columns, make it clickable
            if (groupedColumns.has(header)) {
                cellContent = `<span class="drillable-value text-primary" style="cursor: pointer; text-decoration: underline;" data-field="${header}" data-value="${value}">${cellContent}</span>`;
            }
            
            tableHTML += `<td style="${cellStyle}">${cellContent}</td>`;
        });
        tableHTML += '</tr>';
    });
    tableHTML += '</tbody></table>';
    resultsContainer.innerHTML = tableHTML;
}

function handleDrillDown(field, value) {
    // Save the current filter state so we can return to it
    const previousFilters = JSON.parse(JSON.stringify(AppState.reportConfig.filters));
    AppState.drillDown.path.push({ field, value, previousFilters });
    AppState.drillDown.active = true;

    // Add a new filter for the value that was clicked
    // Note: The field name from the table header might be aliased, e.g., "products_category_exact"
    // We need to find the original field name, e.g., "products.category"
    const originalGroup = (AppState.reportConfig.groups || []).find(g => field.startsWith(g.field.replace('.', '_')));
    
    if (originalGroup) {
        AppState.reportConfig.filters.push({
            field: originalGroup.field,
            op: '=',
            val: value,
        });
    }

    updateDrillUI();
    generateReport(); // Re-run the report with the new filter
}

function handleDrillBack() {
    if (AppState.drillDown.path.length === 0) return;

    // Restore the filters from the previous level
    const lastLevel = AppState.drillDown.path.pop();
    AppState.reportConfig.filters = lastLevel.previousFilters;

    if (AppState.drillDown.path.length === 0) {
        AppState.drillDown.active = false;
    }

    updateDrillUI();
    generateReport(); // Re-run the report with the restored filters
}// mis_app/static/js/report_builder_django.js

async function createInteractiveFilters() {
    const container = document.getElementById('dynamicFilterContainer');
    const controlsContainer = document.getElementById('userFilterControls');
    if (!container || !controlsContainer) return;

    controlsContainer.style.display = 'block';
    container.innerHTML = '<div class="text-center"><div class="spinner-border spinner-border-sm"></div></div>';

    const headers = AppState.reportData ? AppState.reportData.headers : [];
    const schema = AppState.dataSourceSchema || {};
    const allSchemaFields = Object.values(schema).flat();

    if (allSchemaFields.length === 0) {
        container.innerHTML = '<p class="text-danger small">Error: Data source schema not loaded.</p>';
        return;
    }

    const filterableFields = headers.map(header => {
        const originalField = allSchemaFields.find(f => {
            const schemaFieldName = f.fullName.replace('.', '_');
            return header === schemaFieldName || header.startsWith(schemaFieldName + '_');
        });

        // --- THIS IS THE FIX ---
        // We now check the 'is_numeric' flag from the schema, which is more reliable.
        if (originalField && !originalField.is_numeric) {
            return originalField.fullName;
        }
        return null;
    }).filter(Boolean);

    if (filterableFields.length === 0) {
        container.innerHTML = '<p class="text-muted small">No filterable (text-based) columns in this report.</p>';
        return;
    }

    try {
        const response = await fetch(URLS.getFilterValues, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken },
            body: JSON.stringify({
                connection_id: AppState.reportConfig.connection_id,
                fields: filterableFields
            })
        });
        const result = await response.json();
        if (!result.success) throw new Error(result.error);
        
        let controlsHTML = '';
        for (const fieldName in result.data) {
            const fieldData = result.data[fieldName];
            if (fieldData.values) {
                const displayName = fieldName.split('.')[1] || fieldName;
                // Note the change to data-field-name
                controlsHTML += `
                    <div class="me-3 mb-2">
                        <label class="form-label small fw-bold">${displayName}</label>
                        <select class="form-select form-select-sm interactive-filter" data-field-name="${fieldName}">
                            <option value="">All</option>
                            ${fieldData.values.map(v => `<option value="${v}">${v}</option>`).join('')}
                        </select>
                    </div>`;
            }
        }
        container.innerHTML = controlsHTML;
    } catch (error) {
        container.innerHTML = `<p class="text-danger small">Error: ${error.message}</p>`;
    }
}

function hideInteractiveFilters() {
    const controlsContainer = document.getElementById('userFilterControls');
    if (controlsContainer) {
        controlsContainer.style.display = 'none';
    }
}


function applyInteractiveFilters() {
    // 1. Gather the selected values from the interactive filter dropdowns.
    const userFilters = [];
    document.querySelectorAll('.interactive-filter').forEach(select => {
        if (select.value) {
            userFilters.push({
                field: select.dataset.fieldName,
                op: '=', // The operation is always 'equals' for these dropdowns
                val: select.value
            });
        }
    });

    // 2. Add these filters to a special 'user_filters' key in our main report configuration.
    // This keeps them separate from the filters in the main query builder.
    AppState.reportConfig.user_filters = userFilters;

    console.log("Applying interactive filters by re-running report with config:", AppState.reportConfig);

    // 3. Re-run the entire report on the server.
    // The backend will now see the 'user_filters' and apply them to the database query.
    generateReport();
}

function updateDrillUI() {
    const drillBackBtn = document.getElementById('drillBackBtn');
    if (!drillBackBtn) return;

    if (AppState.drillDown.active) {
        drillBackBtn.style.display = 'inline-block';
    } else {
        drillBackBtn.style.display = 'none';
    }
}


/**
 * Opens the "Load Report" modal and populates it with the user's saved reports.
 */
async function openLoadModal() {
    const modalBody = document.getElementById('loadReportModalBody');
    if (!modalBody) return;

    modalBody.innerHTML = `<div class="text-center p-3"><div class="spinner-border spinner-border-sm"></div></div>`;
    const loadReportModal = new bootstrap.Modal(document.getElementById('loadReportModal'));
    loadReportModal.show();

    try {
        const response = await fetch(URLS.loadReports);
        const result = await response.json();
        if (!result.success) throw new Error(result.error);

        if (result.reports.length === 0) {
            modalBody.innerHTML = `<div class="text-muted text-center p-3">You have no saved reports.</div>`;
            return;
        }

        const reportsHTML = result.reports.map(report => `
            <div class="list-group-item d-flex justify-content-between align-items-center">
                <div>
                    <strong>${report.name}</strong>
                    <br>
                    <small class="text-muted">Owner: ${report.owner} | Permission: ${report.permission}</small>
                </div>
                <div class="btn-group">
                    <button class="btn btn-primary btn-sm" onclick="loadReport('${report.id}')">Load</button>
                    
                    ${report.permission === 'owner' ? `
                    <button class="btn btn-outline-secondary btn-sm" onclick="window.openShareModal('${report.id}', '${report.name.replace(/'/g, "\\'")}')" title="Share">
                        <i class="fas fa-share-alt"></i>
                    </button>
                    <button class="btn btn-outline-danger btn-sm" onclick="deleteReport('${report.id}', this)" title="Delete">
                        <i class="fas fa-trash"></i>
                    </button>
                    ` : ''}
                </div>
            </div>
        `).join('');
        modalBody.innerHTML = `<div class="list-group">${reportsHTML}</div>`;

    } catch (error) {
        modalBody.innerHTML = `<div class="alert alert-danger">${error.message}</div>`;
    }
}

/**
 * Fetches a specific report's config and populates the UI.
 * Made globally accessible for the onclick attribute.
 * @param {string} reportId - The UUID of the report to load.
 */
window.loadReport = async function(reportId) {
    const loadReportModal = bootstrap.Modal.getInstance(document.getElementById('loadReportModal'));
    
    try {
        const url = URLS.reportDetail(reportId);
        const response = await fetch(url);
        if (!response.ok) throw new Error(`Server returned status ${response.status}`);
        
        const result = await response.json();
        if (!result.success) throw new Error(result.error);
        
        // --- THIS IS THE CORRECTED LOGIC ---

        // 1. Set the application state from the loaded report
        AppState.currentReportId = result.id;
        AppState.currentReportName = result.name;
        AppState.reportConfig = result.config;

        // 2. Update the UI to match the loaded report's state
        document.getElementById('reportNameDisplay').textContent = result.name;
        document.getElementById('updateReportBtn').disabled = false;
        
        // 3. Set the connection dropdown to the report's connection
        DOM.connectionSelect.value = result.config.connection_id;

        // 4. IMPORTANT: Reload the tables and columns for that connection
        await handleConnectionChange();
        
        // 5. Now that the schema is loaded, build the pills in the query builder
        populateUIFromConfig();

        // 6. Hide the "Load" modal
        if (loadReportModal) {
            loadReportModal.hide();
        }

        // 7. Finally, run the report
        await generateReport();

    } catch (error) {
        showError(`Error loading report: ${error.message}`);
        // You might want to show this error inside the modal instead of an alert
        const modalBody = document.getElementById('loadReportModalBody');
        if (modalBody) {
            modalBody.innerHTML = `<div class="alert alert-danger">${error.message}</div>`;
        }
    }
}

function toggleBuilderSection(section, forceShow = false) {
    // This function might not exist in your file, please add it.
    const sectionElement = document.getElementById(`${section}Section`); // e.g. filtersSection
    const btn = document.querySelector(`.action-btn[data-section="${section}"]`);
    
    if (!sectionElement || !btn) return;

    const isVisible = sectionElement.style.display !== 'none';
    
    if (forceShow || !isVisible) {
        sectionElement.style.display = 'block';
        btn.classList.add('active');
    } else {
        sectionElement.style.display = 'none';
        btn.classList.remove('active');
    }
}


function populateUIFromConfig() {
    const { columns, filters, groups, sorts, joins } = AppState.reportConfig;

    // --- 1. Clear all existing pills and de-activate section buttons ---
    const containers = [
        document.getElementById("columnsBox"),
        document.getElementById("filtersBox"),
        document.getElementById("groupBox"),
        document.getElementById("sortsBox"),
        document.getElementById("joinsBox")
    ];
    containers.forEach(box => {
        if (box) {
            // Add back the placeholder text
            const placeholder = box.querySelector('.drop-zone-placeholder');
            box.innerHTML = ''; // Clear all pills
            if (placeholder) {
                box.appendChild(placeholder);
            }
        }
    });

    document.querySelectorAll(".action-btn").forEach(btn => {
        const sectionId = `${btn.dataset.section}Section`;
        const sectionElement = document.getElementById(sectionId);
        // Hide all sections except for the 'columns' section
        if (sectionElement && btn.dataset.section !== 'columns') {
            sectionElement.style.display = 'none';
        }
        // Deactivate all buttons except for the 'columns' button
        btn.classList.toggle('active', btn.dataset.section === 'columns');
    });

    // --- 2. Helper to find field metadata from the loaded schema ---
    const findFieldInSchema = (fieldFullName) => {
        if (typeof fieldFullName !== 'string') return { fullName: '', name: '' };
        if (fieldFullName.startsWith('calc.')) {
            return { fullName: fieldFullName, name: fieldFullName.split('.')[1] };
        }
        for (const tableName in AppState.dataSourceSchema) {
            const found = AppState.dataSourceSchema[tableName].find(f => f.fullName === fieldFullName);
            if (found) return found;
        }
        return { fullName: fieldFullName, name: fieldFullName.split('.')[1] || fieldFullName };
    };

    // --- 3. Rebuild the UI from the loaded AppState.reportConfig ---
    
    // Populate Columns
    if (columns && columns.length > 0) {
        columns.forEach(c => {
            const fieldObject = findFieldInSchema(c.field);
            document.getElementById("columnsBox").insertAdjacentHTML('beforeend', createColumnPill(fieldObject));
            const newPill = document.getElementById("columnsBox").lastElementChild;
            if (newPill && newPill.querySelector('select')) {
                newPill.querySelector('select').value = (c.agg || 'none').toUpperCase();
            }
        });
    }

    // Populate Filters (into the hidden container)
    if (filters && filters.length > 0) {
        filters.forEach(f => {
            const fieldObject = findFieldInSchema(f.field);
            document.getElementById("filtersBox").insertAdjacentHTML('beforeend', createFilterPill(fieldObject));
            const newPill = document.getElementById("filtersBox").lastElementChild;
            if (newPill) {
                newPill.querySelector('.filter-op').value = f.op;
                newPill.querySelector('.filter-val').value = f.val;
            }
        });
    }

    // Populate Groups (into the hidden container)
    if (groups && groups.length > 0) {
        groups.forEach(g => {
            const fieldObject = findFieldInSchema(g.field);
            document.getElementById("groupBox").insertAdjacentHTML('beforeend', createGroupPill(fieldObject));
            const newPill = document.getElementById("groupBox").lastElementChild;
            if (newPill && newPill.querySelector('.group-method-select')) {
                newPill.querySelector('.group-method-select').value = g.method || 'exact';
            }
        });
    }

    // Populate Sorts (into the hidden container)
    if (sorts && sorts.length > 0) {
        sorts.forEach(s => {
            const fieldObject = findFieldInSchema(s.field);
            document.getElementById("sortsBox").insertAdjacentHTML('beforeend', createSortPill(fieldObject));
            const newPill = document.getElementById("sortsBox").lastElementChild;
            if (newPill && newPill.querySelector('select')) {
                newPill.querySelector('select').value = s.dir || 'ASC';
            }
        });
    }
}
/**
 * Handles the "Save As..." logic by showing the modal.
 */
function openSaveAsModal() {
    document.getElementById('reportName').value = '';
    document.getElementById('reportDescription').value = '';
    const saveModal = new bootstrap.Modal(document.getElementById('saveReportModal'));
    saveModal.show();
}

/**
 * Called by the "Save" button in the modal to send the report to the server.
 */
async function saveReport() {
    const reportName = document.getElementById('reportName').value.trim();
    if (!reportName) {
        showError('Report Name is required.');
        return;
    }
    syncConfigAndState();

    try {
        const response = await fetch(URLS.saveReport, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken },
            body: JSON.stringify({
                report_name: reportName,
                report_config: AppState.reportConfig
            })
        });
        const result = await response.json();
        if (!result.success) throw new Error(result.error);

        AppState.currentReportId = result.report_id;
        AppState.currentReportName = reportName;

        document.getElementById('reportNameDisplay').textContent = reportName;
        document.getElementById('updateReportBtn').disabled = false;
        
        const saveModal = bootstrap.Modal.getInstance(document.getElementById('saveReportModal'));
        saveModal.hide();
        showSuccess('Report saved successfully!');

    } catch (error) {
        showError(`Error saving report: ${error.message}`);
    }
}

/**
 * Updates an existing report with the current configuration.
 */
async function updateReport() {
    if (!AppState.currentReportId) {
        showError("No report is currently loaded. Please use 'Save As' to create a new report.");
        return;
    }

    syncConfigAndState(); // Ensure the config is up-to-date

    try {
        const url = URLS.updateReport(AppState.currentReportId);
        const response = await fetch(url, {
            method: 'POST', // Or 'PUT', depending on your urls.py setup
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken
            },
            body: JSON.stringify({
                report_config: AppState.reportConfig
                // You can also send 'report_name' and 'description' if you allow editing them
            })
        });

        const result = await response.json();
        if (!result.success) throw new Error(result.error);

        showSuccess('Report updated successfully!');

    } catch (error) {
        showError(`Error updating report: ${error.message}`);
    }
}

/**
 * Opens the calculated field modal and populates it with available fields.
 */
function openCalculatedFieldModal() {
    // Clear previous state
    document.getElementById('calcFieldName').value = '';
    document.getElementById('calcFieldFormula').value = '';

    const availableFieldsContainer = document.getElementById('availableFieldsList');
    const schema = AppState.dataSourceSchema;

    // Check if any tables have been loaded for the connection
    if (!schema || Object.keys(schema).length === 0) {
        availableFieldsContainer.innerHTML = `<div class="text-muted small p-2">Please select a connection and load tables first.</div>`;
    } else {
        // Build an accordion, just like the main sidebar
        const accordionHTML = Object.keys(schema).map(tableName => {
            const sanitizedId = `calc-field-table-${tableName.replace(/[^a-zA-Z0-9]/g, '')}`;
            const columnsHTML = schema[tableName].map(col => `
                <div class="list-group-item list-group-item-action p-1 small" style="cursor: pointer;" data-field-ref="[${tableName}.${col.name}]">
                    ${col.name} <span class="text-muted small">(${String(col.type).split('(')[0]})</span>
                </div>
            `).join('');

            return `
                <div class="accordion-item">
                    <h2 class="accordion-header">
                        <button class="accordion-button collapsed py-2" type="button" data-bs-toggle="collapse" data-bs-target="#${sanitizedId}">
                            ${tableName}
                        </button>
                    </h2>
                    <div id="${sanitizedId}" class="accordion-collapse collapse">
                        <div class="list-group list-group-flush">${columnsHTML}</div>
                    </div>
                </div>
            `;
        }).join('');
        availableFieldsContainer.innerHTML = `<div class="accordion">${accordionHTML}</div>`;
    }

    const modal = new bootstrap.Modal(document.getElementById('calculatedFieldModal'));
    modal.show();
}

/**
 * Saves the new calculated field to the AppState and refreshes the sidebar.
 */
function saveCalculatedField() {
    const name = document.getElementById('calcFieldName').value.trim();
    const formula = document.getElementById('calcFieldFormula').value.trim();

    if (!name || !formula) {
        showError("Field Name and Formula are both required.");
        return;
    }

    if (!AppState.reportConfig.calculated_fields) {
        AppState.reportConfig.calculated_fields = [];
    }

    // Add or update the field definition
    const existingIndex = AppState.reportConfig.calculated_fields.findIndex(f => f.name === name);
    if (existingIndex > -1) {
        AppState.reportConfig.calculated_fields[existingIndex].formula = formula;
    } else {
        AppState.reportConfig.calculated_fields.push({ name, formula });
    }

    renderCalculatedFieldsSidebar();
    syncConfigAndState(); // Important to update the main state object

    const modal = bootstrap.Modal.getInstance(document.getElementById('calculatedFieldModal'));
    modal.hide();
}

/**
 * Renders the list of created calculated fields in the left sidebar.
 */
function renderCalculatedFieldsSidebar() {
    const container = document.getElementById('calculatedFieldsList');
    const fields = AppState.reportConfig.calculated_fields || [];

    if (fields.length === 0) {
        container.innerHTML = '';
        return;
    }

    const fieldsHTML = fields.map(field => {
        // This creates the JSON data that makes the field draggable
        const fieldJson = JSON.stringify({
            fullName: `calc.${field.name}`,
            name: field.name,
            type: 'calculated',
            formula: field.formula,
            is_numeric: true // Assume numeric for now for aggregations
        });
        return `
            <div class="field-item" draggable="true" data-field-json='${fieldJson}'>
                <span><i class="fas fa-calculator text-success me-2"></i>${field.name}</span>
            </div>`;
    }).join('');

    container.innerHTML = `<div class="field-list list-group list-group-flush">${fieldsHTML}</div>`;
    
    // Make the new fields draggable
    const fieldList = container.querySelector('.field-list');
    if (fieldList) {
        new Sortable(fieldList, {
            group: { name: 'fields', pull: 'clone', put: false },
            sort: false
        });
    }
}

/**
 * Opens the formatting modal for a specific column.
 * @param {string} columnName - The full name of the column, e.g., 'cpm_data.Total_Cost'.
 */
function openFormattingModal(columnName) {
    const modalEl = document.getElementById('formattingModal');
    if (!modalEl) return;

    // --- THIS IS THE FIX ---
    // Use getOrCreateInstance to prevent creating multiple modal objects
    const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
    
    const format = AppState.reportConfig.formats[columnName] || {};
    
    // Set the modal title and store the column name
    modalEl.querySelector('#formatColumnName').textContent = columnName;
    modalEl.dataset.columnName = columnName;

    // Populate the form with existing values or defaults
    modalEl.querySelector('#columnAlias').value = format.alias || '';
    modalEl.querySelector('#numberFormatType').value = format.type || 'none';
    modalEl.querySelector('#decimalPlaces').value = format.decimals ?? 2;
    modalEl.querySelector('#currencySymbol').value = format.symbol || '$';

    // Show/hide options based on the selected format type
    toggleFormatOptions();

    modal.show();
}

/**
 * Saves the formatting rules from the modal to the AppState.
 */
function applyFormatting() {
    const modal = document.getElementById('formattingModal');
    const columnName = modal.dataset.columnName;
    if (!columnName) return;

    const format = {
        alias: document.getElementById('columnAlias').value.trim(),
        type: document.getElementById('numberFormatType').value,
        decimals: parseInt(document.getElementById('decimalPlaces').value, 10),
        symbol: document.getElementById('currencySymbol').value.trim(),
    };

    AppState.reportConfig.formats[columnName] = format;
    syncConfigAndState();

    const formattingModal = bootstrap.Modal.getInstance(modal);
    formattingModal.hide();
    
    // THIS IS THE FIX:
    // Only re-render the table if there is already data present.
    if (AppState.reportData && AppState.reportData.headers) {
        renderTable(AppState.reportData.headers, AppState.reportData.rows);
    }
}

/**
 * Shows or hides the number/currency options in the formatting modal.
 */
function toggleFormatOptions() {
    const formatType = document.getElementById('numberFormatType').value;
    const numberOptions = document.getElementById('numberOptionsContainer');
    const currencyOptions = document.getElementById('currencySymbolContainer');

    if (formatType === 'number' || formatType === 'currency') {
        numberOptions.style.display = 'block';
        currencyOptions.style.display = formatType === 'currency' ? 'block' : 'none';
    } else {
        numberOptions.style.display = 'none';
    }
}

    function filterDataSources() {
        // Implementation from previous steps...
    }

    // ================================================================
    // 5. DRAG-DROP & PILL CREATION
    // ================================================================

    function handleFieldDropped(event) {
    const itemEl = event.item; // The temporary element created by Sortable.js
    const targetZone = event.to; // The drop zone (e.g., columnsBox)
    const fieldData = JSON.parse(itemEl.dataset.fieldJson || '{}');
    const zoneType = targetZone.dataset.type;
    
    let pillHTML = '';
    if (zoneType === 'columns') {
        pillHTML = createColumnPill(fieldData);
    } else if (zoneType === 'filters') {
        pillHTML = createFilterPill(fieldData);
    } else if (zoneType === 'groups') {
        pillHTML = createGroupPill(fieldData);
    } else if (zoneType === 'sorts') {
        pillHTML = createSortPill(fieldData);
    }
    
    // Create the new pill element from the HTML string
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = pillHTML.trim();
    const pillNode = tempDiv.firstChild;

    // --- THIS IS THE FIX ---
    // Instead of swapping, we explicitly remove the old item and add the new one.
    if (pillNode) {
        itemEl.remove(); // Remove the temporary item
        targetZone.appendChild(pillNode); // Add our new, styled pill
    } else {
        itemEl.remove(); // Fallback to just remove the item if no pill was created
    }
    validateJoinPath();
}

    function createColumnPill(field) {
        const isNumeric = String(field.type).toLowerCase().includes('int') || String(field.type).toLowerCase().includes('float') || String(field.type).toLowerCase().includes('dec');
        const aggOptions = isNumeric ? ['NONE', 'SUM', 'COUNT', 'AVG', 'MIN', 'MAX'] : ['NONE', 'COUNT'];
        return `
            <div class="pill" data-field-json='${JSON.stringify(field)}'>
                <i class="fas fa-columns me-2"></i><span>${field.fullName}</span>
                <select class="form-select form-select-sm ms-2" style="width: auto;">${aggOptions.map(opt => `<option value="${opt}">${opt}</option>`).join('')}</select>
                <i class="fas fa-times remove-icon ms-2" style="cursor: pointer;"></i>
            </div>`;
    }

    function createFilterPill(field) {
        return `
            <div class="filter-pill pill" data-field-json='${JSON.stringify(field)}' style="display: flex; align-items: center; width: 100%;">
                <span>${field.fullName}</span>
                <select class="form-select form-select-sm mx-2 filter-op" style="width: 120px;">
                    <option value="=">=</option><option value="!=">!=</option><option value=">">&gt;</option><option value="<">&lt;</option><option value=">=">&gt;=</option><option value="<=">&lt;=</option><option value="LIKE">contains</option><option value="IN">in list</option>
                </select>
                <input type="text" class="form-control form-control-sm filter-val" placeholder="Value...">
                <i class="fas fa-times remove-icon ms-2" style="cursor: pointer;"></i>
            </div>`;
    }

    function createGroupPill(field) {
    const fieldType = getFieldType(field);
    let pillContent = "";

    switch (fieldType) {
        case "date":
            pillContent = createDateGroupPill(field);
            break;
        // Numeric grouping can be added as a future enhancement
        // case "numeric":
        //     pillContent = createNumericGroupPill(field);
        //     break;
        default: // 'text'
            pillContent = createTextGroupPill(field);
    }

    return `<div class="pill" data-field-json='${JSON.stringify(field)}'>${pillContent}</div>`;
}

    function createSortPill(field) {
        return `
            <div class="pill" data-field-json='${JSON.stringify(field)}'>
                <i class="fas fa-sort me-2"></i><span>${field.fullName}</span>
                <select class="form-select form-select-sm ms-2" style="width: auto;"><option value="ASC">Asc</option><option value="DESC">Desc</option></select>
                <i class="fas fa-times remove-icon ms-2" style="cursor: pointer;"></i>
            </div>`;
    }

    /**
 * Creates and appends a new join configuration pill to the UI.
 */
function addJoinPill() {
    const joinsBox = document.getElementById('joinsBox');
    if (!joinsBox) return;

    // 1. Get all fields currently used in the report from all boxes
    const fieldsInUse = Array.from(document.querySelectorAll('.pill[data-field-json], .filter-pill[data-field-json]'))
        .map(pill => JSON.parse(pill.dataset.fieldJson).fullName)
        .filter((value, index, self) => self.indexOf(value) === index) // Get unique values
        .sort();

    if (fieldsInUse.length < 1) {
        showError("Please add at least one column from a table before creating a join.");
        return;
    }

    // 2. Create the HTML for the dropdown options
    const optionsHTML = fieldsInUse.map(field => `<option value="${field}">${field}</option>`).join('');

    // 3. Create the HTML for the entire join pill
    const joinPillHTML = `
        <div class="join-pill" style="display: flex; align-items: center; width: 100%; padding: 8px; border-bottom: 1px solid #eee;">
            <select class="form-select form-select-sm join-left-col">${optionsHTML}</select>
            <select class="form-select form-select-sm join-type mx-2" style="width: 150px;">
                <option value="INNER">Inner Join</option>
                <option value="LEFT">Left Join</option>
            </select>
            <select class="form-select form-select-sm join-right-col">${optionsHTML}</select>
            <i class="fas fa-times remove-icon ms-2" style="cursor: pointer; color: #dc3545;"></i>
        </div>
    `;

    // 4. Add the new pill to the DOM and sync the state
    joinsBox.insertAdjacentHTML('beforeend', joinPillHTML);
    syncConfigAndState();
}

    // ================================================================
    // 6. UTILITY & LAUNCH
    // ================================================================
    
    // mis_app/static/js/report_builder_django.js
// mis_app/static/js/report_builder_django.js

function renderPagination(pagination, totalRows) {
    const { current_page, page_size, total_pages } = pagination;
    const infoEl = document.getElementById('resultsInfo');
    const listEl = document.getElementById('paginationList');

    // Update the "Showing X - Y of Z" text
    if (!totalRows || totalRows === 0) {
        infoEl.textContent = 'No results';
        listEl.innerHTML = '';
        return;
    }
    const startRow = (current_page - 1) * page_size + 1;
    const endRow = Math.min(startRow + page_size - 1, totalRows);
    infoEl.textContent = `Showing ${startRow} - ${endRow} of ${totalRows}`;

    // If there's only one page, don't show the page number controls
    if (total_pages <= 1) {
        listEl.innerHTML = '';
        return;
    }

    let paginationHTML = '';
    const pageContext = 2; // How many pages to show on each side of the current page

    // --- Previous Button ---
    paginationHTML += `<li class="page-item ${current_page === 1 ? 'disabled' : ''}">
        <a class="page-link" href="#" onclick="event.preventDefault(); generateReport(${current_page - 1})">Prev</a>
    </li>`;

    // --- Page Number Logic ---
    let lastPageShown = 0;
    for (let i = 1; i <= total_pages; i++) {
        // Conditions to determine if a page number link should be shown
        const showPage = (
            i === 1 || // Always show the first page
            i === total_pages || // Always show the last page
            (i >= current_page - pageContext && i <= current_page + pageContext) // Show pages around the current one
        );

        if (showPage) {
            if (i > lastPageShown + 1) {
                // If there's a gap between the last shown page and this one, add an ellipsis
                paginationHTML += `<li class="page-item disabled"><span class="page-link">...</span></li>`;
            }

            if (i === current_page) {
                paginationHTML += `<li class="page-item active" aria-current="page"><span class="page-link">${i}</span></li>`;
            } else {
                // Use event.preventDefault() to stop the page from jumping to the top
                paginationHTML += `<li class="page-item"><a class="page-link" href="#" onclick="event.preventDefault(); generateReport(${i})">${i}</a></li>`;
            }
            lastPageShown = i;
        }
    }

    // --- Next Button ---
    paginationHTML += `<li class="page-item ${current_page >= total_pages ? 'disabled' : ''}">
        <a class="page-link" href="#" onclick="event.preventDefault(); generateReport(${current_page + 1})">Next</a>
    </li>`;

    listEl.innerHTML = paginationHTML;
}

function toggleConfigPanel() {
    const queryBuilder = document.getElementById('queryBuilder');
    const chevron = document.getElementById('configChevron');
    if (!queryBuilder || !chevron) return;

    const bsCollapse = bootstrap.Collapse.getOrCreateInstance(queryBuilder);
    bsCollapse.toggle();

    // Listen for the panel to be shown or hidden to update the arrow icon
    queryBuilder.addEventListener('shown.bs.collapse', () => {
        chevron.classList.remove('fa-chevron-down');
        chevron.classList.add('fa-chevron-up');
    });
    queryBuilder.addEventListener('hidden.bs.collapse', () => {
        chevron.classList.remove('fa-chevron-up');
        chevron.classList.add('fa-chevron-down');
    });
}

function toggleQuerySection(sectionName) {
    const sectionEl = document.getElementById(sectionName + 'Section');
    const buttonEl = document.querySelector(`.action-btn[data-section="${sectionName}"]`);
    if (!sectionEl || !buttonEl) return;

    const isVisible = sectionEl.style.display !== 'none';
    sectionEl.style.display = isVisible ? 'none' : 'block';
    buttonEl.classList.toggle('active', !isVisible);
}

function renderShares() {
    const userListDiv = document.getElementById('shareUserList'); // Ensure this ID exists in your share modal
    userListDiv.innerHTML = shares.length === 0
        ? '<p class="text-muted text-center small">Not shared with anyone yet.</p>'
        : shares.map((share, index) => `
            <div class="d-flex justify-content-between align-items-center p-2 border-bottom">
                <span>${share.username} - <strong>${share.permission}</strong></span>
                <button class="btn btn-sm btn-outline-danger" onclick="removeShare(${index})">&times;</button>
            </div>`).join('');
}

function addShare() {
    const userSelect = document.getElementById('userToShare'); // Ensure this ID exists
    const userId = userSelect.value;
    if (!userId || shares.some(s => s.user_id == userId)) {
        return; // Don't add if no user is selected or if user is already in the list
    }
    shares.push({
        user_id: parseInt(userId),
        username: userSelect.options[userSelect.selectedIndex].text,
        permission: document.getElementById('sharePermission').value, // Ensure this ID exists
    });
    renderShares();
}

// Make this function globally accessible so the onclick can find it
window.removeShare = (index) => {
    shares.splice(index, 1);
    renderShares();
};

async function saveShares() {
    if (!currentShareReportId) return;
    try {
        const response = await fetch(URLS.updateReportShares(currentShareReportId), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken },
            body: JSON.stringify({ shares: shares }),
        });
        const result = await response.json();
        if (!result.success) throw new Error(result.error);
        showSuccess('Sharing settings saved!');
        bootstrap.Modal.getInstance(document.getElementById('shareReportModal')).hide();
    } catch (error) {
        showError(`Error saving shares: ${error.message}`);
    }
}

// Make this function globally accessible for the onclick attribute
window.openShareModal = async function(reportId, reportName) {
    currentShareReportId = reportId;
    document.getElementById('shareReportName').textContent = reportName; // Ensure this ID exists
    const shareModal = bootstrap.Modal.getOrCreateInstance(document.getElementById('shareReportModal'));

    try {
        const [usersRes, sharesRes] = await Promise.all([
            fetch(URLS.listUsers),
            fetch(URLS.getReportShares(reportId))
        ]);
        const usersData = await usersRes.json();
        const sharesData = await sharesRes.json();
        
        if (!usersData.success || !sharesData.success) throw new Error("Could not load sharing data.");
        
        document.getElementById('userToShare').innerHTML = '<option value="">Select a user...</option>' + usersData.users
            .map(u => `<option value="${u.id}">${u.username}</option>`).join('');
        
        shares = sharesData.shares || [];
        renderShares();
        shareModal.show();
    } catch (error) {
        showError(`Could not open sharing modal: ${error.message}`);
    }
}

async function exportReport(format) {
    if (!AppState.reportData || !AppState.reportData.rows || AppState.reportData.rows.length === 0) {
        showError("No data available to export. Please run a report first.");
        return;
    }

    let url;
    if (format === 'excel') {
        url = URLS.exportReport;
    } else if (format === 'csv') {
        url = URLS.exportCsv;
    } else {
        showError("Unsupported format");
        return;
    }

    try {
        const payload = {
            headers: AppState.reportData.headers,
            rows: AppState.reportData.rows,
            report_config: { name: AppState.currentReportName }
        };

        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrfToken },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`Export failed with status: ${response.status}`);
        }

        // Handle file download
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = downloadUrl;
        
        const contentDisposition = response.headers.get('content-disposition');
        let filename = `report.${format}`;
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="(.+)"/);
            if (filenameMatch.length > 1) {
                filename = filenameMatch[1];
            }
        }
        a.download = filename;
        
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(downloadUrl);
        a.remove();

    } catch (error) {
        showError(`An error occurred during export: ${error.message}`);
    }
}

    function debounce(func, delay) {
        let timeout;
        return function(...args) {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    }

    function openDataPrepModal() {
        syncConfigAndState();
        const config = AppState.reportConfig;
        if (!config.connection_id || (!config.columns?.length && !config.groups?.length)) {
            showError("Please select a connection and add at least one column or group before preparing data.");
            return;
        }
        const dataPrepModal = new bootstrap.Modal(document.getElementById('dataPrepModal'));
        dataPrepModal.show();
        if (window.initializeDataPrepWorkflow) {
            window.initializeDataPrepWorkflow(config);
        } else {
            console.error("Data Prep Workflow is not initialized. Is data_prep.js loaded?");
        }
    }
    
    initializeApp();
});

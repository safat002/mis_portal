// In static/js/dashboard_designer.js

// Helper function to get the CSRF token required by Django for POST requests
function getCsrfToken() {
    const csrfInput = document.querySelector('[name=csrfmiddlewaretoken]');
    return csrfInput ? csrfInput.value : '';
}

document.addEventListener('DOMContentLoaded', function () {
    const dashboardContainer = document.querySelector('.dashboard-container');
    if (!dashboardContainer) {
        console.error('Dashboard container not found!');
        return;
    }

    // --- 1. STATE & CONFIGURATION ---

    const WIDGET_LIBRARY = {
        bar: { name: "Bar Chart", icon: "bi-bar-chart-line", defaultSize: { w: 6, h: 4 } },
        line: { name: "Line Chart", icon: "bi-graph-up", defaultSize: { w: 6, h: 4 } },
        pie: { name: "Pie Chart", icon: "bi-pie-chart", defaultSize: { w: 4, h: 4 } },
        doughnut: { name: "Doughnut Chart", icon: "bi-pie-chart-fill", defaultSize: { w: 4, h: 4 } }, // ADD THIS
        kpi: { name: "KPI / Card", icon: "bi-speedometer2", defaultSize: { w: 3, h: 2 } },
        table: { name: "Data Table", icon: "bi-table", defaultSize: { w: 6, h: 5 } },
    };
    window.AppState = {
        dashboardId: dashboardContainer.dataset.dashboardId,
        grid: null,
        config: {},
        saveTimeout: null,
        isLoading: true,
        availableFields: [],
        activeFilters: [] // Also track available fields globally
    };

    // --- 2. CORE FUNCTIONS (LOAD & SAVE) ---

    async function loadDashboardConfig() {
        if (!AppState.dashboardId) return;
        try {
            const response = await fetch(`/api/dashboard/${AppState.dashboardId}/config/`);
            if (!response.ok) throw new Error('Network response was not ok');
            AppState.config = await response.json();
            console.log('Dashboard config loaded:', AppState.config);
            renderAllWidgets();
        } catch (error) {
            console.error('Failed to load dashboard config:', error);
        } finally {
            AppState.isLoading = false;
        }
    }

    function initializeFiltering() {
    const chipsContainer = document.getElementById('global-filter-chips');

    // Listen for when a chart wants to apply a filter
    eventBus.on('filter:apply', (filter) => {
        // For now, we only support one active filter at a time for simplicity
        AppState.activeFilters = [filter];

        console.log('Applying filter:', filter);
        renderActiveFilters(); // Update the UI to show the new chip
        renderAllWidgets();   // Refresh the dashboard widgets
    });

    // Use event delegation to handle clicks on the "remove" button for any chip
    if (chipsContainer) {
        chipsContainer.addEventListener('click', (event) => {
            const removeBtn = event.target.closest('.remove-filter-btn');
            if (removeBtn) {
                const indexToRemove = parseInt(removeBtn.dataset.index, 10);
                // Remove the filter from our state
                AppState.activeFilters.splice(indexToRemove, 1);

                renderActiveFilters(); // Update the UI to remove the chip
                renderAllWidgets();   // Refresh the dashboard widgets
            }
        });
    }
}

    // In static/js/dashboard_designer.js

function saveDashboardConfig() {
    if (AppState.isLoading || !AppState.grid) return;

    clearTimeout(AppState.saveTimeout);

    AppState.saveTimeout = setTimeout(async () => {
        console.log('%c--- Starting Autosave ---', 'color: blue; font-weight: bold;');
        
        // 1. Get the current layout from GridStack
        const gridData = AppState.grid.save();
        console.log('Step 1: Layout from GridStack:', gridData);

        // 2. Get the current list of widget configurations from our state
        const oldWidgets = AppState.config.pages?.[0]?.widgets || [];
        console.log('Step 2: Widget configs currently in state:', oldWidgets);

        // 3. Build a new, synced list of widgets
        const newWidgets = gridData.map(gridNode => {
            // Find the full configuration for this widget from our old state using its ID
            const fullWidgetConfig = oldWidgets.find(w => w.id === gridNode.id);
            
            // --- THIS IS THE MOST IMPORTANT LOG ---
            // It tells us if we successfully found the full config for each widget on the grid.
            console.log(`Step 3: Matching widget with ID [${gridNode.id}]. Found full config:`, fullWidgetConfig);
            
            // Merge the layout data from the grid with the detailed configuration
            return {
                ...(fullWidgetConfig || {}), // Keep all the old config (dataConfig, displayOptions, etc.)
                id: gridNode.id,
                x: gridNode.x,
                y: gridNode.y,
                w: gridNode.w,
                h: gridNode.h,
            };
        });

        // 4. Replace the old widgets array with our new, correct one
        if (AppState.config.pages && AppState.config.pages[0]) {
            AppState.config.pages[0].widgets = newWidgets;
        }

        console.log('%cStep 4: Final config object being sent to server:', 'color: green; font-weight: bold;', AppState.config);
        
        try {
            const response = await fetch(`/api/dashboard/${AppState.dashboardId}/config/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': getCsrfToken()
                },
                body: JSON.stringify(AppState.config)
            });

            if (!response.ok) throw new Error('Save request failed');
            
            const result = await response.json();
            console.log('%cStep 5: Save successful!', 'color: green; font-weight: bold;', result.message);
            
        } catch (error) {
            console.error('Failed to save dashboard:', error);
        }
    }, 1500);
}

    // --- 3. GRID & WIDGET LIFECYCLE ---

    function initializeGrid() {
        AppState.grid = GridStack.init({
            float: false,
            cellHeight: 80,
            minRow: 1,
            acceptWidgets: '.palette-item'
        });

        AppState.grid.on('dropped', (event, previousWidget, newGridItem) => {
            const item = newGridItem.el;
            const payload = JSON.parse(item.dataset.gsPayload);
            AppState.grid.removeWidget(item);
            addNewWidget({ type: payload.type, x: newGridItem.x, y: newGridItem.y, w: newGridItem.w, h: newGridItem.h });
        });

        AppState.grid.on('change', saveDashboardConfig);
        AppState.grid.on('added', saveDashboardConfig);
        AppState.grid.on('removed', saveDashboardConfig);
    }

    function addNewWidget(options) {
    const widgetInfo = WIDGET_LIBRARY[options.type];
    if (!widgetInfo) {
        console.error(`Unknown widget type: ${options.type}`);
        return;
    }

    const newWidget = {
        // --- THIS IS THE CRITICAL FIX ---
        // Generate a more unique random ID instead of using the timestamp
        id: 'widget_' + Math.random().toString(36).substring(2, 11),
        // -----------------------------
        type: options.type,
        x: options.x,
        y: options.y,
        w: options.w || widgetInfo.defaultSize.w,
        h: options.h || widgetInfo.defaultSize.h,
        dataConfig: {},
        displayOptions: {
            title: `New ${widgetInfo.name}`
        }
    };

    if (AppState.config.pages && AppState.config.pages[0]) {
        AppState.config.pages[0].widgets.push(newWidget);
    }
    
    const widgetEl = AppState.grid.addWidget(createWidgetHTML(newWidget), newWidget);
    const widgetBody = widgetEl.querySelector('.widget-body');
    if (widgetBody) {
        widgetRenderer.renderWidget(widgetBody, newWidget, AppState.dashboardId);
    }
    saveDashboardConfig();
}

    function deleteWidget(widgetId) {
    if (!confirm('Are you sure you want to delete this widget?')) {
        return;
    }

    // Find the widget's DOM element on the grid
    const widgetEl = document.querySelector(`.grid-stack-item[gs-id="${widgetId}"]`);
    if (widgetEl) {
        // Remove the widget from the GridStack layout
        AppState.grid.removeWidget(widgetEl);
    }

    // Remove the widget from our state object
    const page = AppState.config.pages[0];
    if (page && page.widgets) {
        page.widgets = page.widgets.filter(w => w.id !== widgetId);
    }

    // Destroy any associated chart instance to prevent memory leaks
    widgetRenderer.destroyWidget(widgetId);

    // Save the change to the backend
    saveDashboardConfig();
}

    // --- 4. DATA CONTEXT MODAL ---

    function initializeDataContextModal() {
    // --- Existing variables ---
    const manageDataContextBtn = document.getElementById('manageDataContextBtn');
    const dataContextModalEl = document.getElementById('dataContextModal');
    const connectionSelect = document.getElementById('data-context-connection-select');
    const saveBtn = document.getElementById('saveDataContextBtn');
    
    // --- New variables for the join form ---
    const addJoinBtn = document.getElementById('add-join-btn');
    const newJoinForm = document.getElementById('new-join-form');
    const saveJoinBtn = document.getElementById('save-join-btn');
    const cancelJoinBtn = document.getElementById('cancel-join-btn');
    const leftTableSelect = document.getElementById('join-left-table');
    const leftColumnSelect = document.getElementById('join-left-column');
    const rightTableSelect = document.getElementById('join-right-table');
    const rightColumnSelect = document.getElementById('join-right-column');

    // Helper function to populate a column dropdown
    const populateJoinColumns = async (tableSelect, columnSelect) => {
        const tableName = tableSelect.value;
        const connectionId = connectionSelect.value;
        if (!tableName || !connectionId) {
            columnSelect.innerHTML = '<option>Select table first</option>';
            return;
        }
        try {
            const response = await fetch(`/api/connections/${connectionId}/tables/${tableName}/columns/`);
            const data = await response.json();
            if (data.columns) {
                columnSelect.innerHTML = data.columns.map(col => `<option value="${col.name}">${col.name}</option>`).join('');
            }
        } catch (error) {
            console.error('Failed to fetch columns:', error);
            columnSelect.innerHTML = '<option>Error loading</option>';
        }
    };

    // --- Event Listeners ---
    manageDataContextBtn.addEventListener('click', () => {
        const modal = new bootstrap.Modal(dataContextModalEl);
        loadDataContextIntoModal();
        modal.show();
    });

    connectionSelect.addEventListener('change', (event) => {
        const connectionId = event.target.value;
        if (connectionId) loadTablesForConnection(connectionId);
    });

    saveBtn.addEventListener('click', saveDataContext);

    // Show and populate the "Add Join" form
    addJoinBtn.addEventListener('click', () => {
        newJoinForm.classList.remove('d-none');
        const selectedTables = Array.from(document.querySelectorAll('#data-context-tables-list input:checked')).map(input => input.value);
        const optionsHTML = selectedTables.map(t => `<option value="${t}">${t}</option>`).join('');
        leftTableSelect.innerHTML = optionsHTML;
        rightTableSelect.innerHTML = optionsHTML;
        // Trigger column load for the default selected tables
        populateJoinColumns(leftTableSelect, leftColumnSelect);
        populateJoinColumns(rightTableSelect, rightColumnSelect);
    });

    // Add event listeners to update columns when tables change
    leftTableSelect.addEventListener('change', () => populateJoinColumns(leftTableSelect, leftColumnSelect));
    rightTableSelect.addEventListener('change', () => populateJoinColumns(rightTableSelect, rightColumnSelect));

    // Hide the form on cancel
    cancelJoinBtn.addEventListener('click', () => newJoinForm.classList.add('d-none'));

    // Save the new join to our AppState
    saveJoinBtn.addEventListener('click', () => {
        if (!AppState.savedContext.joins) AppState.savedContext.joins = [];
        const newJoin = {
            left_table: leftTableSelect.value,
            left_column: leftColumnSelect.value,
            right_table: rightTableSelect.value,
            right_column: rightColumnSelect.value
        };
        // Basic validation
        if (!newJoin.left_column || !newJoin.right_column) {
            showError('Please select columns for the join.');
            return;
        }
        AppState.savedContext.joins.push(newJoin);
        renderJoins(AppState.savedContext.joins);
        newJoinForm.classList.add('d-none');
    });

    // Handle removing a join
    document.getElementById('data-context-joins-list').addEventListener('click', (e) => {
        if (e.target.classList.contains('remove-join-btn')) {
            const indexToRemove = parseInt(e.target.dataset.index, 10);
            AppState.savedContext.joins.splice(indexToRemove, 1);
            renderJoins(AppState.savedContext.joins);
        }
    });
}

    async function loadDataContextIntoModal() {
        const connSelect = document.getElementById('data-context-connection-select');
        try {
            const response = await fetch('/api/connections/');
            const connections = await response.json();
            connSelect.innerHTML = '<option selected disabled>Choose a connection...</option>';
            connections.forEach(conn => {
                connSelect.innerHTML += `<option value="${conn.id}">${conn.nickname}</option>`;
            });

            const contextResponse = await fetch(`/api/dashboard/${AppState.dashboardId}/data_context/`);
            const savedContext = await contextResponse.json();

            if (savedContext.connection_id) {
                connSelect.value = savedContext.connection_id;
                await loadTablesForConnection(savedContext.connection_id, savedContext.selected_tables);
            }
            renderJoins(savedContext.joins);
        } catch (error) {
            console.error('Error loading data context:', error);
        }
    }

    async function loadTablesForConnection(connectionId, selectedTables = []) {
        const tablesListDiv = document.getElementById('data-context-tables-list');
        tablesListDiv.innerHTML = '<p class="text-muted small">Loading tables...</p>';
        try {
            const response = await fetch(`/api/connections/${connectionId}/tables/`);
            const result = await response.json();
            if (!result.success) throw new Error(result.error);

            if (result.tables.length === 0) {
                tablesListDiv.innerHTML = '<p class="text-muted small">No tables found.</p>';
                return;
            }

            tablesListDiv.innerHTML = result.tables.map(table => `
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" value="${table}" id="table-${table}" ${selectedTables.includes(table) ? 'checked' : ''}>
                    <label class="form-check-label" for="table-${table}">${table}</label>
                </div>
            `).join('');
        } catch (error) {
            tablesListDiv.innerHTML = `<p class="text-danger small">Error: ${error.message}</p>`;
        }
    }

    async function saveDataContext() {
        const connectionId = document.getElementById('data-context-connection-select').value;
        const selectedTables = Array.from(document.querySelectorAll('#data-context-tables-list input:checked')).map(input => input.value);

        if (!connectionId || selectedTables.length === 0) {
            showError('Please select a connection and at least one table.');
            return;
        }

        const joins = AppState.savedContext.joins || [];

        try {
            const response = await fetch(`/api/dashboard/${AppState.dashboardId}/data_context/`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-CSRFToken': getCsrfToken() },
                body: JSON.stringify({ connection_id: connectionId, selected_tables: selectedTables, joins: joins })
            });
            const result = await response.json();
        if (result.success) {
            showSuccess('Data context saved!');
            const modal = bootstrap.Modal.getInstance(document.getElementById('dataContextModal'));
            modal.hide();

            updateAvailableFields(); 

        } else {
            throw new Error(result.error);
        }
    } catch (error) {
        showError(`Failed to save: ${error.message}`);
    }
}

    async function updateAvailableFields() {
    try {
        const response = await fetch(`/api/dashboard/${AppState.dashboardId}/available_fields/`);
        const result = await response.json();
        if (!result.success) throw new Error(result.error);
        
        renderAvailableFields(result.fields);
    } catch (error) {
        console.error("Failed to update available fields:", error);
        const fieldsList = document.getElementById('available-fields-list');
        fieldsList.innerHTML = `<div class="text-danger small p-3 text-center">Could not load fields.</div>`;
    }
}

function renderAvailableFields(fields, containerId = 'available-fields-list', isDraggable = false) {
    const fieldsList = document.getElementById(containerId);
    if (!fields || fields.length === 0) {
        fieldsList.innerHTML = `<div class="text-muted small p-3 text-center">No fields found. Configure the data source.</div>`;
        return;
    }

    const fieldsByTable = fields.reduce((acc, field) => {
        if (!acc[field.table]) {
            acc[field.table] = [];
        }
        acc[field.table].push(field);
        return acc;
    }, {});

    let html = '<div class="accordion" id="fieldsAccordion">';
    let index = 0;
    for (const [table, tableFields] of Object.entries(fieldsByTable)) {
        const collapseId = `collapse-fields-${index}`;
        // The "collapsed" class is added to the button when the target is not shown by default
        html += `
            <div class="accordion-item">
                <h2 class="accordion-header" id="header-${collapseId}">
                    <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#${collapseId}" aria-expanded="false" aria-controls="${collapseId}">
                        ${table}
                    </button>
                </h2>
                <div id="${collapseId}" class="accordion-collapse collapse" aria-labelledby="header-${collapseId}">
                    <div class="accordion-body">
        `;
        
        tableFields.forEach(field => {
        html += `<div class="field-item p-2 mb-1 border rounded small" 
                      ${isDraggable ? `draggable="true" data-field-name="${field.table}.${field.name}"` : ''}>
                      ${field.name} <span class="text-muted">(${field.type})</span>
                 </div>`;
    });

        html += `   </div>
                </div>
            </div>`;
        index++;
    }
    html += '</div>';
    fieldsList.innerHTML = html;
}

    // --- 5. RENDERING & UI ---

    function createWidgetHTML(widget) {
        const title = widget.displayOptions?.title || 'Untitled Widget';
        return `
            <div class="grid-stack-item-content">
                <div class="widget-header">
                    <span class="widget-title">${title}</span>
                    <div class="widget-controls">
                        <button class="btn-widget-settings" title="Settings" data-widget-id="${widget.id}"><i class="bi bi-gear-fill"></i></button>
                        <button class="btn-widget-delete" title="Delete" data-widget-id="${widget.id}"><i class="bi bi-trash-fill"></i></button>
                    </div>
                </div>
                <div class="widget-body" data-widget-id="${widget.id}"></div>
            </div>`;
    }

    function renderAllWidgets() {
    const grid = AppState.grid;
    const configWidgets = AppState.config?.pages?.[0]?.widgets || [];
    const activeFilters = AppState.activeFilters || [];
    const dashboardId = AppState.dashboardId;

    if (!grid) return;

    const configWidgetMap = new Map(configWidgets.map(w => [w.id, w]));
    const existingItems = grid.getGridItems();

    // Batch update for performance
    grid.batchUpdate();

    // --- 1. Remove stale widgets ---
    existingItems.forEach(item => {
        const widgetId = item.getAttribute('gs-id');
        if (!configWidgetMap.has(widgetId)) {
            grid.removeWidget(item, false); // false = silent removal
        }
    });

    // --- 2. Add/update widgets ---
    configWidgets.forEach(widget => {
        let widgetEl = document.querySelector(`.grid-stack-item[gs-id="${widget.id}"]`);

        // Add widget if missing
        if (!widgetEl) {
            grid.addWidget(createWidgetHTML(widget), widget);
            widgetEl = document.querySelector(`.grid-stack-item[gs-id="${widget.id}"]`);
        }

        // Render widget content
        const widgetBody = widgetEl?.querySelector('.widget-body');
        if (widgetBody) {
            // Avoid self-filtering loops
            const filtersToApply = activeFilters.some(f => f.sourceWidgetId === widget.id)
                ? []
                : activeFilters;

            // Defer rendering to avoid layout thrashing
            setTimeout(() => {
                widgetRenderer.renderWidget(widgetBody, widget, dashboardId, filtersToApply);
            }, 0);
        }
    });

    // Finalize DOM updates
    grid.commit();
}

function renderActiveFilters() {
    const chipsContainer = document.getElementById('global-filter-chips');
    if (!chipsContainer) return;

    if (AppState.activeFilters.length === 0) {
        chipsContainer.innerHTML = '';
        return;
    }

    chipsContainer.innerHTML = AppState.activeFilters.map((filter, index) => {
        // Display the field name without the table prefix for a cleaner look
        const fieldName = filter.field.split('.').pop();
        return `
            <div class="filter-chip badge bg-primary text-wrap d-flex align-items-center">
                <span><strong>${fieldName}:</strong> ${filter.value}</span>
                <button class="btn-close btn-close-white ms-2 remove-filter-btn" data-index="${index}" aria-label="Remove filter"></button>
            </div>
        `;
    }).join('');
}

    function rebuildWidgetPalette() {
        const paletteContainer = document.getElementById('widget-palette');
        if (!paletteContainer) return;
        let paletteHTML = '';
        for (const [type, config] of Object.entries(WIDGET_LIBRARY)) {
            const payload = JSON.stringify({ type: type, w: config.defaultSize.w, h: config.defaultSize.h });
            paletteHTML += `<div class="palette-item" data-gs-payload='${payload}'><i class="bi ${config.icon}"></i><span>${config.name}</span></div>`;
        }
        paletteContainer.innerHTML = paletteHTML;
        GridStack.setupDragIn('.palette-item', { revert: 'invalid', scroll: false, appendTo: 'body', helper: 'clone' });
    }

    // --- 6. WIDGET SETTINGS MODAL ---

function initializeWidgetSettingsModal() {
    const dashboardGrid = document.getElementById('dashboard-grid');
    const saveBtn = document.getElementById('saveWidgetSettingsBtn');
    const modalBody = document.querySelector('#widgetSettingsModal .modal-body'); // Get the modal body

    // Use event delegation for settings buttons on widgets
    dashboardGrid.addEventListener('click', (event) => {
        const settingsBtn = event.target.closest('.btn-widget-settings');
        const deleteBtn = event.target.closest('.btn-widget-delete'); // <-- ADD THIS

        if (settingsBtn) {
            const widgetId = settingsBtn.dataset.widgetId;
            openWidgetSettingsModal(widgetId);
        } else if (deleteBtn) { // <-- ADD THIS ELSE-IF BLOCK
            const widgetId = deleteBtn.dataset.widgetId;
            deleteWidget(widgetId);
        }
    });

    saveBtn.addEventListener('click', saveWidgetSettings);

    // --- THIS IS THE NEW PART ---
    // Add a single event listener to the modal body to handle remove clicks
    if (modalBody) {
        modalBody.addEventListener('click', (event) => {
            handleRemoveField(event);
            handleAggregationChange(event); // <-- ADD THIS CALL
        });
    }
}

function initializeGlobalFilterModal() {
    const addFilterBtn = document.getElementById('add-filter-btn');
    const modalEl = document.getElementById('globalFilterModal');
    const applyBtn = document.getElementById('apply-global-filter-btn');
    const fieldSelect = document.getElementById('filter-field-select');

    if (!addFilterBtn || !modalEl || !applyBtn || !fieldSelect) return;

    const modal = new bootstrap.Modal(modalEl);

    // Make the event listener async to use 'await'
    addFilterBtn.addEventListener('click', async () => {
        
        // --- THIS IS THE FIX ---
        // First, ensure the available fields are up-to-date by fetching them again.
        await updateAvailableFields();
        // --------------------

        // Now, populate the fields dropdown with the fresh data
        if (AppState.availableFields.length === 0) {
            fieldSelect.innerHTML = '<option disabled selected>No fields available. Configure data context first.</option>';
        } else {
            fieldSelect.innerHTML = AppState.availableFields.map(field => {
                const fieldName = `${field.table}.${field.name}`;
                return `<option value="${fieldName}">${fieldName}</option>`;
            }).join('');
        }
        
        modal.show();
    });

    // When the "Apply Filter" button inside the modal is clicked
    applyBtn.addEventListener('click', () => {
        const newFilter = {
            sourceWidgetId: null, 
            field: document.getElementById('filter-field-select').value,
            operator: document.getElementById('filter-operator-select').value,
            value: document.getElementById('filter-value-input').value
        };

        if (!newFilter.field || !newFilter.value) {
            showError('Please select a field and enter a value.');
            return;
        }

        AppState.activeFilters.push(newFilter);

        renderActiveFilters();
        renderAllWidgets();
        modal.hide();
        document.getElementById('filter-value-input').value = ''; // Clear input for next time
    });
}

function initializeExportModal() {
    const modalEl = document.getElementById('exportModal');
    if (!modalEl) return;

    const formatSelect = document.getElementById('export-format-select');
    const widgetSelectContainer = document.getElementById('export-widget-select-container');
    const widgetSelect = document.getElementById('export-widget-select');
    const confirmBtn = document.getElementById('confirm-export-btn');

    // Show/hide the widget selector based on the chosen format
    formatSelect.addEventListener('change', () => {
        if (formatSelect.value === 'csv') {
            widgetSelectContainer.classList.remove('d-none');
        } else {
            widgetSelectContainer.classList.add('d-none');
        }
    });

    // Populate the widget dropdown when the modal is about to be shown
    modalEl.addEventListener('show.bs.modal', () => {
        const widgets = AppState.config?.pages?.[0]?.widgets || [];
        widgetSelect.innerHTML = widgets.map(w => 
            `<option value="${w.id}">${w.displayOptions.title || w.id}</option>`
        ).join('');
    });

    // Handle the final export button click
    confirmBtn.addEventListener('click', () => {
        const format = formatSelect.value;
        const filename = document.getElementById('export-filename-input').value;
        
        if (format === 'png') {
            exportService.exportGridToPNG(filename);
        } else if (format === 'csv') {
            const widgetId = widgetSelect.value;
            if (!widgetId) {
                showError('Please select a widget to export.');
                return;
            }
            exportService.exportWidgetToCSV(widgetId, filename);
        }
        
        const modal = bootstrap.Modal.getInstance(modalEl);
        modal.hide();
    });
}

function initializeThemeSelector() {
    const themeSelector = document.getElementById('colorThemeSelector');
    if (!themeSelector) return;

    // Set initial value from saved config
    themeSelector.value = AppState.config.themePalette || 'Tableau.Classic10';

    themeSelector.addEventListener('change', (event) => {
        const newPalette = event.target.value;
        AppState.config.themePalette = newPalette;
        
        // Re-render all widgets to apply the new theme
        renderAllWidgets(); 
        
        // Save the change
        saveDashboardConfig();
    });
}

async function openWidgetSettingsModal(widgetId) {
    const modalEl = document.getElementById('widgetSettingsModal');
    const modal = new bootstrap.Modal(modalEl);
    document.getElementById('editingWidgetId').value = widgetId;
    
    const widgetConfig = AppState.config.pages[0].widgets.find(w => w.id === widgetId);
    if (!widgetConfig) return;

    // Populate general options
    document.getElementById('widgetTitleInput').value = widgetConfig.displayOptions.title;
    document.getElementById('showLegendSwitch').checked = widgetConfig.displayOptions.showLegend !== false;

    // Populate the modal with available fields from our saved context
    await populateAvailableFieldsInModal();
    
    // Render the fields that are already assigned to the widget
    renderAssignedFields(widgetConfig);

    // Setup Drag and Drop
    setupDragAndDrop();

    modal.show();
}

async function populateAvailableFieldsInModal() {
    const fieldsContainer = document.getElementById('modal-available-fields');
    try {
        const response = await fetch(`/api/dashboard/${AppState.dashboardId}/available_fields/`);
        const result = await response.json();
        if (!result.success) throw new Error(result.error);
        
        AppState.availableFields = result.fields; // Store fields in state for later use

        // The rendering logic is the same as the sidebar, so we can reuse it!
        renderAvailableFields(result.fields, 'modal-available-fields', true); // `true` to make them draggable
    } catch (error) {
        fieldsContainer.innerHTML = `<div class="text-danger small p-2">Could not load fields.</div>`;
    }
}

function renderAssignedFields(widgetConfig) {
    const dimensionsZone = document.getElementById('widget-dropzone-dimensions');
    const measuresZone = document.getElementById('widget-dropzone-measures');
    dimensionsZone.innerHTML = '';
    measuresZone.innerHTML = '';

    (widgetConfig.dataConfig.dimensions || []).forEach(dim => {
        dimensionsZone.innerHTML += `<div class="field-pill" data-field-name="${dim.field}">${dim.field} <i class="bi bi-x-circle remove-field-btn"></i></div>`;
    });

    // New version for measures with a dropdown
    (widgetConfig.dataConfig.measures || []).forEach(measure => {
        const agg = measure.agg || 'SUM';
        measuresZone.innerHTML += `
            <div class="field-pill d-flex align-items-center" data-field-name="${measure.field}">
                <div class="dropdown">
                    <button class="btn btn-sm btn-light dropdown-toggle me-2" type="button" data-bs-toggle="dropdown">${agg}</button>
                    <ul class="dropdown-menu">
                        <li><a class="dropdown-item agg-select-btn" href="#" data-agg="SUM">SUM</a></li>
                        <li><a class="dropdown-item agg-select-btn" href="#" data-agg="AVG">AVG</a></li>
                        <li><a class="dropdown-item agg-select-btn" href="#" data-agg="COUNT">COUNT</a></li>
                        <li><a class="dropdown-item agg-select-btn" href="#" data-agg="MIN">MIN</a></li>
                        <li><a class="dropdown-item agg-select-btn" href="#" data-agg="MAX">MAX</a></li>
                    </ul>
                </div>
                <span>${measure.field}</span>
                <i class="bi bi-x-circle ms-auto remove-field-btn"></i>
            </div>`;
    });
}

// Add this new handler function after handleRemoveField
function handleAggregationChange(event) {
    const aggBtn = event.target.closest('.agg-select-btn');
    if (!aggBtn) return;

    const widgetId = document.getElementById('editingWidgetId').value;
    const widgetConfig = AppState.config.pages[0].widgets.find(w => w.id === widgetId);
    if (!widgetConfig) return;

    const fieldPill = aggBtn.closest('.field-pill');
    const fieldName = fieldPill.dataset.fieldName;
    const newAgg = aggBtn.dataset.agg;

    // Find the measure in the config and update its aggregation
    const measure = widgetConfig.dataConfig.measures.find(m => m.field === fieldName);
    if (measure) {
        measure.agg = newAgg;
    }

    // Re-render the pills to show the updated aggregation
    renderAssignedFields(widgetConfig);
}


function setupDragAndDrop() {
    const draggables = document.querySelectorAll('#modal-available-fields .field-item[draggable="true"]');
    const dropzones = document.querySelectorAll('.widget-dropzone');

    draggables.forEach(draggable => {
        draggable.addEventListener('dragstart', (e) => {
            e.dataTransfer.setData('text/plain', draggable.dataset.fieldName);
            setTimeout(() => draggable.classList.add('dragging'), 0);
        });
        draggable.addEventListener('dragend', () => draggable.classList.remove('dragging'));
    });

    dropzones.forEach(zone => {
        zone.addEventListener('dragover', (e) => {
            e.preventDefault();
            zone.classList.add('drag-over');
        });
        zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
        zone.addEventListener('drop', (e) => {
            e.preventDefault();
            zone.classList.remove('drag-over');
            const fieldName = e.dataTransfer.getData('text/plain');
            const dropType = zone.id.includes('dimensions') ? 'dimensions' : 'measures';
            
            addFieldToWidget(fieldName, dropType);
        });
    });
}

function addFieldToWidget(fieldName, dropType) {
    const widgetId = document.getElementById('editingWidgetId').value;
    const widgetConfig = AppState.config.pages[0].widgets.find(w => w.id === widgetId);

    // Find the full field info from our availableFields state
    const fieldInfo = AppState.availableFields.find(f => `${f.table}.${f.name}` === fieldName);
    const isNumeric = fieldInfo ? /INT|FLOAT|DECIMAL|NUMBER/i.test(fieldInfo.type) : false;

    if (!widgetConfig.dataConfig) widgetConfig.dataConfig = {};
    if (!widgetConfig.dataConfig[dropType]) widgetConfig.dataConfig[dropType] = [];

    if (widgetConfig.dataConfig[dropType].some(f => f.field === fieldName)) return;

    if (dropType === 'dimensions') {
        widgetConfig.dataConfig.dimensions.push({ field: fieldName });
    } else {
        // THE FIX: If the field is not numeric, default to COUNT. Otherwise, default to SUM.
        widgetConfig.dataConfig.measures.push({
            field: fieldName,
            agg: isNumeric ? 'SUM' : 'COUNT'
        });
    }
    
    renderAssignedFields(widgetConfig);
}

function handleRemoveField(event) {
    const removeBtn = event.target.closest('.remove-field-btn');
    if (!removeBtn) return; // Exit if the click wasn't on a remove button

    const widgetId = document.getElementById('editingWidgetId').value;
    const widgetConfig = AppState.config.pages[0].widgets.find(w => w.id === widgetId);
    if (!widgetConfig) return;

    const fieldPill = removeBtn.closest('.field-pill');
    const fieldName = fieldPill.dataset.fieldName;
    const dropzone = fieldPill.closest('.widget-dropzone');
    const fieldType = dropzone.id.includes('dimensions') ? 'dimensions' : 'measures';

    // Filter the array to remove the selected field
    widgetConfig.dataConfig[fieldType] = widgetConfig.dataConfig[fieldType].filter(f => f.field !== fieldName);

    // Re-render the assigned fields to reflect the change
    renderAssignedFields(widgetConfig);
}

function renderJoins(joins = []) {
    const joinsListDiv = document.getElementById('data-context-joins-list');
    if (joins.length === 0) {
        joinsListDiv.innerHTML = '<p class="text-muted small">No joins defined.</p>';
        return;
    }
    joinsListDiv.innerHTML = joins.map((join, index) => `
        <div class="field-pill d-flex align-items-center justify-content-between mb-1">
            <span><code>${join.left_table}.${join.left_column}</code> = <code>${join.right_table}.${join.right_column}</code></span>
            <button class="btn-close btn-sm remove-join-btn" data-index="${index}"></button>
        </div>
    `).join('');
}

function saveWidgetSettings() {
    const widgetId = document.getElementById('editingWidgetId').value;
    const widgetConfig = AppState.config.pages[0].widgets.find(w => w.id === widgetId);
    if (!widgetConfig) return;

    // Save display options from the modal
    widgetConfig.displayOptions.title = document.getElementById('widgetTitleInput').value;
    widgetConfig.displayOptions.showLegend = document.getElementById('showLegendSwitch').checked;

    // The dataConfig (dimensions/measures) was already updated by the drag-drop and aggregation-change actions

    // Hide the modal
    const modalEl = document.getElementById('widgetSettingsModal');
    const modal = bootstrap.Modal.getInstance(modalEl);
    if (modal) {
        modal.hide();
    }

    // --- THIS IS THE KEY FIX ---
    // Find the widget's element on the grid and re-render it
    const widgetEl = document.querySelector(`.grid-stack-item[gs-id="${widgetId}"]`);
    if (widgetEl) {
        // Update the title in the header immediately
        const titleEl = widgetEl.querySelector('.widget-title');
        if (titleEl) {
            titleEl.textContent = widgetConfig.displayOptions.title;
        }
        
        // Find the widget's body and tell the renderer to fetch new data and draw the chart
        const widgetBody = widgetEl.querySelector('.widget-body');
        if (widgetBody) {
            widgetRenderer.renderWidget(widgetBody, widgetConfig, AppState.dashboardId);
        }
    }

    // Save the entire dashboard state to the backend
    saveDashboardConfig();

    
}

// --- 7. INITIALIZATION ---

initializeGrid();
rebuildWidgetPalette();
initializeDataContextModal();
initializeWidgetSettingsModal();
initializeThemeSelector();
initializeFiltering();
initializeGlobalFilterModal();
initializeExportModal();
loadDashboardConfig();
updateAvailableFields();
});

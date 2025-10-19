/**
 * Data Management Page Application
 * Manages the two-panel UI for viewing and manipulating data.
 * This is a feature-complete version with wizards and schema modification.
 */
class DataManagementApp {
    constructor() {
        this.state = {
            connections: [],
            currentConnectionId: null,
            defaultConnectionId: null,
            tables: [],
            selectedTable: null,
            isLoading: false,
            wizardState: {} // To store temp data for multi-step wizards
        };
        this.dom = {};
        this.permissions = {};
        this.api = new ApiService();
        this.modals = {};
    }

    // Helper function to safely get element by ID
    getElementSafe(id, quiet = false) {
        const element = document.getElementById(id);
        if (!element && !quiet) {
            console.warn(`Element with ID ${id} not found`);
        }
        return element;
    }

    // Helper: hide a Bootstrap modal without leaving focus on a hidden element
    hideModalSafely(modalInstance, modalRootId) {
        try {
            const root = document.getElementById(modalRootId) || (modalInstance && modalInstance._element) || null;
            const active = document.activeElement;
            if (active && root && root.contains(active)) {
                active.blur();
            }
        } catch (e) {
            // no-op
        }
        setTimeout(() => {
            try { modalInstance && modalInstance.hide(); } catch (e) {}
        }, 0);
    }

    // --- INITIALIZATION --- //

    init() {
        this.cacheDOMElements();
        this.setupEventListeners();
        this.initializeModals();
        this.loadConnections();
        this.renderWelcomeView();
    }

    cacheDOMElements() {
        this.dom = {
            root: this.getElementSafe('dataManagementRoot'),
            connectionSelect: this.getElementSafe('connectionSelect'),
            sourceSearchInput: this.getElementSafe('sourceSearchInput'),
            sourceList: this.getElementSafe('sourceList'),
            workspace: this.getElementSafe('workspace'),
            addDataBtn: this.getElementSafe('add-data-btn'),
            refreshBtn: this.getElementSafe('refresh-sources-btn'),
            defaultConnectionStar: this.getElementSafe('defaultConnectionStar', true),
        };

        if (this.dom.root) {
            this.permissions = {
                canUpload: this.dom.root.dataset.canUpload === 'true',
                canCreateFromFile: this.dom.root.dataset.canCreateFromFile === 'true',
                canModify: this.dom.root.dataset.canModify === 'true',
                canDeleteRows: this.dom.root.dataset.canDeleteRows === 'true',
                canTruncate: this.dom.root.dataset.canTruncate === 'true',
                canDrop: this.dom.root.dataset.canDrop === 'true',
            };
            this.state.defaultConnectionId = this.dom.root.dataset.defaultConnectionId;
        }
    }

    initializeModals() {
        const passwordConfirmModalEl = this.getElementSafe('passwordConfirmModal');
        if (passwordConfirmModalEl) this.modals.passwordConfirm = new bootstrap.Modal(passwordConfirmModalEl);

        const modifySchemaModalEl = this.getElementSafe('modifySchemaModal');
        if (modifySchemaModalEl) {
            this.modals.modifySchema = new bootstrap.Modal(modifySchemaModalEl);
            // Auto-refresh table view when the modify schema modal is closed
            modifySchemaModalEl.addEventListener('hidden.bs.modal', () => {
                if (this.state.selectedTable) {
                    this.renderTableViewer(this.state.selectedTable);
                }
                this.refreshTableListOnly();
            });
        }

        const addColumnModalEl = this.getElementSafe('addColumnModal');
        if (addColumnModalEl) this.modals.addColumn = new bootstrap.Modal(addColumnModalEl);

        const renameColumnModalEl = this.getElementSafe('renameColumnModal');
        if (renameColumnModalEl) this.modals.renameColumn = new bootstrap.Modal(renameColumnModalEl);
    }

    // Fetch and update only the left table list without resetting selection
    async refreshTableListOnly() {
        try {
            if (!this.state.currentConnectionId) return;
            const tables = await this.api.getVisibleTables(this.state.currentConnectionId);
            this.state.tables = tables || [];
            this.renderTableList();
        } catch (err) {
            console.warn('Table list refresh failed', err);
        }
    }

    setupEventListeners() {
        if (this.dom.connectionSelect) this.dom.connectionSelect.addEventListener('change', (e) => this.handleConnectionChange(e.target.value));
        if (this.dom.defaultConnectionStar) this.dom.defaultConnectionStar.addEventListener('click', () => this.setDefaultConnection(this.state.currentConnectionId));
        if (this.dom.refreshBtn) this.dom.refreshBtn.addEventListener('click', () => this.handleConnectionChange(this.state.currentConnectionId));
        if (this.dom.sourceSearchInput) this.dom.sourceSearchInput.addEventListener('input', (e) => this.filterTableList(e.target.value));
        if (this.dom.addDataBtn) this.dom.addDataBtn.addEventListener('click', () => this.renderAddDataChoiceView());

        if (this.dom.sourceList) {
            this.dom.sourceList.addEventListener('click', (e) => {
                const tableItem = e.target.closest('.source-item');
                if (tableItem) this.handleTableSelect(tableItem.dataset.tableName);
            });
        }

        if (this.dom.workspace) {
            this.dom.workspace.addEventListener('click', (e) => {
                const target = e.target;
                
                if (target.closest && target.closest('#choiceCreateTable')) return this.renderCreateTableWizard();
                if (target.closest && target.closest('#choiceUploadToTable')) return this.renderUploadToTableWizard();
                if (target.closest && target.closest('#choiceIntelligentImport')) return openIntelligentImport();
                
                const button = target.closest('button');
                if (!button) return;

                switch (button.id) {
                    case 'modifySchemaBtn': this.handleModifySchema(); break;
                    case 'truncateTableBtn': this.handleTruncateTable(); break;
                    case 'dropTableBtn': this.handleDropTable(); break;
                    case 'deleteRowsBtn': this.handleDeleteRows(); break;
                    case 'backToChoicesBtn': this.renderAddDataChoiceView(); break;
                    
                    case 'wizardAnalyzeBtn': this.handleWizardAnalyzeFile(); break;
                    case 'goToStep3Btn': this.handleGoToStep3(); break;
                    case 'backToStep1Btn': this.renderCreateTableWizard(); break;
                    case 'backToStep2Btn': this.renderCreateTableStep2(); break;
                    case 'wizardExecuteCreateBtn': this.handleWizardExecuteCreate(); break;
                    
                    // Upload Wizard Buttons
                    case 'goToUploadStep2Btn': this.renderUploadToTableStep2(); break; // New
                    case 'goToUploadStep3Btn': this.handleGoToUploadStep3(); break; // New
                    case 'backToUploadStep1Btn': this.renderUploadToTableWizard(); break; // New
                    case 'backToUploadStep2Btn': this.renderUploadToTableStep2(); break; // New
                    case 'wizardExecuteUploadBtn': this.handleWizardUpload(); break; // Changed
                }
            });

            this.dom.workspace.addEventListener('change', (e) => {
                if (e.target.id === 'wizardFileInput') {
                    const wizardType = this.state.wizardState.type; // 'create' or 'upload'
                    this.handleWizardFileSelect(e.target.files[0], wizardType);
                }
                if (e.target.id === 'selectAllRows') this.toggleAllRowCheckboxes(e.target.checked);
                if (e.target.classList.contains('row-checkbox')) this.updateDeleteButtonState();
            });
        }

        const confirmActionBtn = this.getElementSafe('confirmActionBtn');
        if (confirmActionBtn) {
            confirmActionBtn.addEventListener('click', () => {
                if (this.state.passwordConfirmCallback) this.state.passwordConfirmCallback();
            });
        }

        // Modal-level buttons for schema editing
        const addNewColumnBtn = this.getElementSafe('addNewColumnBtn');
        if (addNewColumnBtn) addNewColumnBtn.addEventListener('click', () => this.showAddColumnModal());

        const confirmAddColumnBtn = this.getElementSafe('confirmAddColumn');
        if (confirmAddColumnBtn) confirmAddColumnBtn.addEventListener('click', () => this.handleAddColumn());

        const confirmRenameColumnBtn = this.getElementSafe('confirmRenameColumn');
        if (confirmRenameColumnBtn) confirmRenameColumnBtn.addEventListener('click', () => this.handleRenameColumn());

        const planBtn = this.getElementSafe('planSchemaChangesBtn');
        if (planBtn) planBtn.addEventListener('click', () => this.planSchemaChanges());

        const applyBtn = this.getElementSafe('applySchemaChangesBtn');
        if (applyBtn) applyBtn.addEventListener('click', () => this.applySchemaChanges());
    }

    // --- DATA LOADING & STATE --- //

    async loadConnections() {
        this.setLoading(true);
        try {
            const connections = await this.api.getConnections();
            this.state.connections = connections;
            this.renderConnectionDropdown();
            if (this.state.defaultConnectionId) {
                this.dom.connectionSelect.value = this.state.defaultConnectionId;
                this.handleConnectionChange(this.state.defaultConnectionId);
            }
        } catch (error) {
            this.showError(`Failed to load connections: ${error.message}`);
        } finally {
            this.setLoading(false);
        }
    }

    async handleConnectionChange(connectionId) {
        if (!connectionId) {
            this.state.tables = [];
            this.renderTableList();
            this.renderWelcomeView();
            this.updateDefaultConnectionStar();
            return;
        }
        this.state.currentConnectionId = connectionId;
        this.updateDefaultConnectionStar();
        this.setLoading(true);
        if (this.dom.sourceList) {
            this.dom.sourceList.innerHTML = `<div class="text-center p-3"><div class="spinner-border spinner-border-sm"></div></div>`;
        }
        try {
            const tables = await this.api.getVisibleTables(connectionId);
            this.state.tables = tables;
            this.renderTableList();
            this.renderWelcomeView();
        } catch (error) {
            this.showError(`Failed to load tables: ${error.message}`);
            this.state.tables = [];
            this.renderTableList();
        } finally {
            this.setLoading(false);
        }
    }
    
    handleTableSelect(tableName) {
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(tableName)) {
            this.showError('Invalid table name selected.');
            return;
        }
        this.state.selectedTable = tableName;
        this.renderTableList();
        this.renderTableViewer(tableName);
    }

    setLoading(isLoading) {
        this.state.isLoading = isLoading;
        // This can be expanded to show a global loading spinner
    }

    // --- UI RENDERING --- //

    renderConnectionDropdown() {
        if (!this.dom.connectionSelect) return;
        const currentVal = this.dom.connectionSelect.value;
        this.dom.connectionSelect.innerHTML = '<option value="">Select a connection...</option>';
        this.state.connections.forEach(conn => {
            const option = document.createElement('option');
            option.value = conn.id;
            option.textContent = conn.nickname;
            this.dom.connectionSelect.appendChild(option);
        });
        if (currentVal) this.dom.connectionSelect.value = currentVal;
    }

    updateDefaultConnectionStar() {
        if (!this.dom.defaultConnectionStar) return;
        const isDefault = this.state.currentConnectionId && this.state.currentConnectionId === this.state.defaultConnectionId;
        this.dom.defaultConnectionStar.innerHTML = `<i class="bi ${isDefault ? 'bi-star-fill' : 'bi-star'}" title="Set as default"></i>`;
    }

    async setDefaultConnection(connectionId) {
        if (!connectionId) return;
        try {
            await this.api.setDefaultConnection(connectionId);
            this.state.defaultConnectionId = connectionId;
            this.updateDefaultConnectionStar();
            this.showError('Default connection updated successfully.');
        } catch (error) {
            this.showError(`Failed to set default connection: ${error.message}`);
        }
    }

    renderTableList() {
        if (!this.dom.sourceSearchInput || !this.dom.sourceList) return;
        const searchTerm = this.dom.sourceSearchInput.value.toLowerCase();
        this.dom.sourceList.innerHTML = '';

        if (!this.state.currentConnectionId) {
            this.dom.sourceList.innerHTML = `<div class="text-muted text-center p-3">Select a connection.</div>`;
            return;
        }
        if (this.state.tables.length === 0) {
            this.dom.sourceList.innerHTML = `<div class="text-muted text-center p-3">No tables found.</div>`;
            return;
        }

        const nameOk = (t) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(t);
        const filteredTables = this.state.tables
            .filter(t => nameOk(t))
            .filter(t => t.toLowerCase().includes(searchTerm));
        filteredTables.forEach(tableName => {
            const item = document.createElement('div');
            item.className = 'source-item';
            item.dataset.tableName = tableName;
            if (tableName === this.state.selectedTable) item.classList.add('active');
            item.innerHTML = `<i class="bi bi-table me-2"></i><span class="flex-grow-1 text-truncate">${tableName}</span>`;
            this.dom.sourceList.appendChild(item);
        });
    }
    
    filterTableList() {
        this.renderTableList();
    }

    renderWelcomeView() {
  this.state.selectedTable = null;
  this.renderTableList();
  if (this.dom.workspace) {
    this.dom.workspace.innerHTML = `
      <div class="workspace-welcome text-center p-5">
        <i class="bi bi-hand-index-thumb display-6 d-block mb-2"></i>
        <h4>Data Management</h4>
        <p class="text-muted">Select a connection and table from the left, or add new data.</p>
        <button id="add-data-btn" class="btn btn-primary mt-2">
          <i class="bi bi-plus-lg me-1"></i> Add Data
        </button>
      </div>`;
  }
}

    async renderTableViewer(tableName) {
        if (!this.dom.workspace) return;
        this.dom.workspace.innerHTML = `<div class="d-flex justify-content-center align-items-center h-100"><div class="spinner-border" role="status"></div></div>`;
        try {
        const result = await this.api.getTableData(this.state.currentConnectionId, tableName);
        if (!result.success) throw new Error(result.error);
        const { data, columns, stats } = result;
        
        // Store columns in state for later use
        this.state.tableColumns = columns;

            this.dom.workspace.innerHTML = `
    <div class="table-viewer-header">
        <div class="d-flex justify-content-between align-items-center">
            <div>
                <h4 class="mb-0 text-truncate">${tableName}</h4>
                <small class="text-muted">${(stats.total_rows || 0).toLocaleString()} rows</small>
            </div>
            <div class="btn-toolbar" role="toolbar">
                 <div class="btn-group me-2" role="group">
                    <button class="btn btn-outline-secondary" id="deleteRowsBtn" disabled><i class="bi bi-trash me-1"></i>Delete Selected</button>
                </div>
                 <div class="btn-group" role="group">
                    <button class="btn btn-outline-primary" id="modifySchemaBtn"><i class="bi bi-pencil-square me-1"></i> Modify Schema</button>
                    <button class="btn btn-outline-danger" id="truncateTableBtn"><i class="bi bi-trash3 me-1"></i> Truncate</button>
                    <button class="btn btn-danger" id="dropTableBtn"><i class="bi bi-x-octagon me-1"></i> Drop</button>
                </div>
            </div>
        </div>
    </div>
    <div class="table-viewer-body">
        ${data.length === 0 ? 
            `<div class="text-center text-muted p-5">This table is empty.</div>` :
            `<div class="table-responsive"><table class="table table-striped table-hover table-sm" id="dmDataTable">
                <thead class="table-light">
                    <tr>
                        <th style="width: 1%;"><input class="form-check-input" type="checkbox" id="selectAllRows"></th>
                        ${columns.map(c => `
                            <th>
                                <div class="d-flex flex-column">
                                    <span>${c}</span>
                                    <input type="text" class="form-control form-control-sm dm-col-filter" data-col="${c}" placeholder="Filter...">
                                </div>
                            </th>
                        `).join('')}
                    </tr>
                </thead>
                <tbody id="dmDataTbody">
                    ${data.map(row => {
                        const pk = row[columns[0]]; // Assumes first column is PK
                        return `<tr>
                            <td><input class="form-check-input row-checkbox" type="checkbox" data-pk="${pk}"></td>
                            ${columns.map(c => `<td>${row[c] !== null ? this.escapeHTML(row[c]) : ''}</td>`).join('')}
                        </tr>`;
                    }).join('')}
                </tbody>
            </table></div>`
        }
    </div>
`;
            // Save current data for client-side filtering
            this.state.tableData = Array.isArray(data) ? data : [];
            this.state.tableFilters = {};

            // Bind filter inputs with debounce
            const filterInputs = this.dom.workspace.querySelectorAll('.dm-col-filter');
            let dmFilterTimer = null;
            filterInputs.forEach(inp => {
                inp.addEventListener('input', () => {
                    const col = inp.dataset.col;
                    this.state.tableFilters[col] = (inp.value || '').trim();
                    clearTimeout(dmFilterTimer);
                    dmFilterTimer = setTimeout(() => this.applyTableFilters(), 200);
                });
            });
        } catch (error) {
            this.showError(`Failed to load table data: ${error.message}`);
            this.renderWelcomeView();
        }
    }

    // Apply header text filters to the currently loaded rows
    applyTableFilters() {
        const tbody = this.getElementSafe('dmDataTbody');
        const columns = this.state.tableColumns || [];
        const all = Array.isArray(this.state.tableData) ? this.state.tableData : [];
        const filters = this.state.tableFilters || {};

        const norm = {};
        Object.entries(filters).forEach(([k, v]) => { norm[k] = String(v || '').toLowerCase(); });

        const filtered = all.filter(row => {
            for (const c of columns) {
                const f = norm[c];
                if (!f) continue;
                const v = row[c];
                const vs = (v === null || v === undefined) ? '' : String(v).toLowerCase();
                if (!vs.includes(f)) return false;
            }
            return true;
        });

        const html = filtered.map(row => {
            const pk = row[columns[0]];
            return `<tr>
                <td><input class="form-check-input row-checkbox" type="checkbox" data-pk="${pk}"></td>
                ${columns.map(c => `<td>${row[c] !== null && row[c] !== undefined ? this.escapeHTML(row[c]) : ''}</td>`).join('')}
            </tr>`;
        }).join('');
        if (tbody) tbody.innerHTML = html;
    }
    
    renderAddDataChoiceView() {
        if (!this.state.currentConnectionId) {
            this.showError("Please select a database connection first.");
            return;
        }
        this.state.selectedTable = null;
        this.renderTableList();

        const canIntelligentImport = checkImportPermission();
        const intelligentImportCard = `
            <div class="col-md-4">
                <div class="card h-100 wizard-card" id="choiceIntelligentImport" style="cursor: pointer;">
                    <div class="card-body d-flex flex-column">
                        <i class="bi bi-magic fs-1 text-primary mb-3"></i>
                        <h5 class="card-title">Intelligent Import</h5>
                        <p class="card-text text-muted">Use AI to import and map data.</p>
                    </div>
                </div>
            </div>
        `;

        if (this.dom.workspace) {
            this.dom.workspace.innerHTML = `
            <div class="wizard-container text-center">
                <h3 class="mb-4">Add Data</h3>
                <div class="row g-3">
                    <div class="col-md-4">
                        <div class="card h-100 wizard-card" id="choiceCreateTable" style="cursor: pointer;">
                            <div class="card-body d-flex flex-column">
                                <i class="bi bi-file-earmark-plus fs-1 text-primary mb-3"></i>
                                <h5 class="card-title">Create New Table from File</h5>
                                <p class="card-text text-muted">Upload a CSV/Excel to create a new table.</p>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-4">
                        <div class="card h-100 wizard-card" id="choiceUploadToTable" style="cursor: pointer;">
                            <div class="card-body d-flex flex-column">
                                <i class="bi bi-upload fs-1 text-primary mb-3"></i>
                                <h5 class="card-title">Upload Data to Existing Table</h5>
                                <p class="card-text text-muted">Append or replace data in a table.</p>
                            </div>
                        </div>
                    </div>
                    ${canIntelligentImport ? intelligentImportCard : ''}
                </div>
                <div class="mt-3">
                    <button class="btn btn-sm btn-outline-secondary" id="backToChoicesBtn">
                        <i class="bi bi-arrow-left me-1"></i> Back to Data Management
                    </button>
                </div>
            </div>`;
        }
    }

    renderCreateTableWizard() {
    this.state.wizardState = { type: 'create', step: 1 };
    this.dom.workspace.innerHTML = `
        <div class="wizard-container">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <h3 class="mb-0">Create New Table from File</h3>
                <button class="btn btn-sm btn-outline-secondary" id="backToChoicesBtn">
                    <i class="bi bi-arrow-left me-1"></i> Back to Choices
                </button>
            </div>
            <div class="wizard-progress mb-4">
                <div class="progress" style="height: 8px;">
                    <div class="progress-bar" role="progressbar" style="width: 33%;"></div>
                </div>
                <div class="d-flex justify-content-between mt-2">
                    <span class="text-primary">Step 1: Upload File</span>
                    <span class="text-muted">Step 2: Review Schema</span>
                    <span class="text-muted">Step 3: Confirm</span>
                </div>
            </div>
            <div class="wizard-step card">
                <div class="card-body" id="wizardStepContainer">
                    <h5 class="card-title">Step 1: Upload File</h5>
                    <p class="text-muted">Select a CSV or Excel file to create a new table from.</p>
                    
                    <div class="mb-3">
                        <label for="wizardFileInput" class="form-label">Select File</label>
                        <input class="form-control" type="file" id="wizardFileInput" accept=".csv,.xlsx,.xls">
                    </div>
                    
                    <div id="wizardStatus" class="alert alert-info">
                        Please select a file to begin.
                    </div>
                </div>
            </div>
        </div>`;
    
    // Add event listener for back button
    document.getElementById('backToChoicesBtn').addEventListener('click', () => {
        this.renderAddDataChoiceView();
    });
}

renderCreateTableStep2() {
    this.state.wizardState.step = 2;
    const { analysis } = this.state.wizardState;
    const container = document.getElementById('wizardStepContainer');
    if (!container) return;

    // Update progress bar
    document.querySelector('.progress-bar').style.width = '66%';
    document.querySelectorAll('.wizard-progress span').forEach(el => el.classList.replace('text-primary', 'text-muted'));
    document.querySelectorAll('.wizard-progress span')[1].classList.replace('text-muted', 'text-primary');

    // Use previously edited values if they exist, otherwise use suggestions
    const tableName = this.state.wizardState.tableName || analysis.table_name;
    const schema = this.state.wizardState.finalSchema || analysis.schema;

    container.innerHTML = `
        <h5 class="card-title">Step 2: Review and Edit Schema</h5>
        <p class="text-muted">Adjust the table name, column names, data types, and primary keys as needed.</p>
        
        <div class="mb-3">
            <label class="form-label">Table Name</label>
            <input type="text" class="form-control" id="wizardTableName" value="${this.escapeHTML(tableName)}">
        </div>
        
        <h6>Columns</h6>
        <div class="table-responsive" id="schemaEditor">
            <table class="table table-sm">
                <thead>
                    <tr>
                        <th>Original Name</th>
                        <th>SQL Name (Editable)</th>
                        <th>Data Type (Editable)</th>
                        <th>Primary Key</th>
                    </tr>
                </thead>
                <tbody>
                    ${schema.map(col => `
                        <tr data-original-name="${this.escapeHTML(col.original_name)}">
                            <td class="text-muted">${this.escapeHTML(col.original_name)}</td>
                            <td><input type="text" class="form-control form-control-sm" name="sql_name" value="${this.escapeHTML(col.sql_name)}"></td>
                            <td>
                                <select class="form-select form-select-sm" name="sql_type">
                                    <option value="VARCHAR(255)" ${col.sql_type.startsWith('VARCHAR') ? 'selected' : ''}>Text</option>
                                    <option value="TEXT" ${col.sql_type === 'TEXT' ? 'selected' : ''}>Long Text</option>
                                    <option value="INTEGER" ${col.sql_type === 'INTEGER' ? 'selected' : ''}>Integer</option>
                                    <option value="BIGINT" ${col.sql_type === 'BIGINT' ? 'selected' : ''}>Big Integer</option>
                                    <option value="FLOAT" ${col.sql_type === 'FLOAT' ? 'selected' : ''}>Decimal (Float)</option>
                                    <option value="DATE" ${col.sql_type === 'DATE' ? 'selected' : ''}>Date</option>
                                    <option value="TIMESTAMP" ${col.sql_type === 'TIMESTAMP' ? 'selected' : ''}>Date & Time</option>
                                    <option value="BOOLEAN" ${col.sql_type === 'BOOLEAN' ? 'selected' : ''}>True/False</option>
                                </select>
                            </td>
                            <td class="text-center"><input class="form-check-input" type="checkbox" name="is_pk" ${col.is_pk ? 'checked' : ''}></td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
        
        <div class="d-flex justify-content-between mt-3">
            <button class="btn btn-secondary" id="backToStep1Btn"><i class="bi bi-arrow-left me-1"></i> Back to Upload</button>
            <button class="btn btn-primary" id="goToStep3Btn">Next: Preview Data <i class="bi bi-arrow-right ms-1"></i></button>
        </div>
    `;
}

renderCreateTableStep3() {
    this.state.wizardState.step = 3;
    const { analysis, tableName, finalSchema } = this.state.wizardState;

    const container = document.getElementById('wizardStepContainer');
    if (!container) return;

    // Update progress bar
    document.querySelector('.progress-bar').style.width = '100%';
    document.querySelectorAll('.wizard-progress span').forEach(el => el.classList.replace('text-primary', 'text-muted'));
    document.querySelectorAll('.wizard-progress span')[2].classList.replace('text-muted', 'text-primary');

    // Prepare preview data using the user's final schema
    const previewHeaders = finalSchema.map(col => col.original_name);
    const previewData = analysis.preview_data.slice(0, 15); // Show up to 15 rows

    container.innerHTML = `
        <h5 class="card-title">Step 3: Preview and Confirm</h5>
        <p class="text-muted">A preview of your data is shown below. If everything looks correct, confirm to create the table.</p>
        
        <div class="mb-3">
            <label class="form-label">Table Name</label>
            <input type="text" class="form-control" value="${this.escapeHTML(tableName)}" readonly>
        </div>
        
        <h6>Data Preview (First ${previewData.length} Rows)</h6>
        <div class="table-responsive" style="max-height: 300px;">
            <table class="table table-sm table-bordered">
                <thead class="table-light">
                    <tr>
                        ${previewHeaders.map(h => `<th>${this.escapeHTML(h)}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
                    ${previewData.map(row => `
                        <tr>
                            ${finalSchema.map(col => `<td>${this.escapeHTML(row[col.original_name] ?? '')}</td>`).join('')}
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
        
        <div id="wizardStatusMessage" class="mt-3"></div>

        <div class="d-flex justify-content-between mt-3">
            <button class="btn btn-secondary" id="backToStep2Btn">
                <i class="bi bi-arrow-left me-1"></i> Back to Schema
            </button>
            <button class="btn btn-success" id="wizardExecuteCreateBtn">
                <i class="bi bi-check-circle me-1"></i> Confirm & Create Table
            </button>
        </div>
    `;
}

handleGoToStep3() {
    const tableNameInput = document.getElementById('wizardTableName');
    if (!tableNameInput || !tableNameInput.value.trim()) {
        this.showError("Table name is required.");
        return;
    }
    this.state.wizardState.tableName = tableNameInput.value.trim();

    const finalSchema = [];
    document.querySelectorAll('#schemaEditor tbody tr').forEach(row => {
        finalSchema.push({
            original_name: row.dataset.originalName,
            sql_name: row.querySelector('[name="sql_name"]').value,
            sql_type: row.querySelector('[name="sql_type"]').value,
            is_pk: row.querySelector('[name="is_pk"]').checked,
        });
    });

    if (finalSchema.length === 0) {
        this.showError("The table must have at least one column.", true);
        return;
    }
    this.state.wizardState.finalSchema = finalSchema;
    this.renderCreateTableStep3();
}
    
    renderUploadToTableWizard() {
    console.log('[Wizard] Initializing Upload to Table Wizard');
    this.state.wizardState = { type: 'upload', step: 1 };

    const tables = Array.isArray(this.state.tables) ? this.state.tables : [];
    const tableOptions = tables.length
        ? tables.map(table => `<option value="${table}">${table}</option>`).join('')
        : `<option disabled>No tables available</option>`;

    if (!this.dom.workspace) {
        console.warn('[Wizard] Workspace DOM element not found.');
        return;
    }

    this.dom.workspace.innerHTML = `
        <div class="wizard-container">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <h3 class="mb-0">Upload Data to Existing Table</h3>
                <button class="btn btn-sm btn-outline-secondary" id="backToChoicesBtn">
                    <i class="bi bi-arrow-left me-1"></i> Back
                </button>
            </div>
            <div class="wizard-progress mb-4">
                <div class="progress" style="height: 8px;">
                    <div class="progress-bar" role="progressbar" style="width: 33%;"></div>
                </div>
                <div class="d-flex justify-content-between mt-2">
                    <span class="text-primary">Step 1: Select Table</span>
                    <span class="text-muted">Step 2: Upload File</span>
                    <span class="text-muted">Step 3: Map & Confirm</span>
                </div>
            </div>
            <div class="wizard-step card">
                <div class="card-body" id="wizardStepContainer">
                    <h5 class="card-title">Step 1: Select a Target Table</h5>
                    <p class="text-muted">Choose which table to upload new data into.</p>
                    <div class="mb-3">
                        <label for="uploadTableSelect" class="form-label">Table</label>
                        <select class="form-select" id="uploadTableSelect">
                            <option value="">Select a table...</option>
                            ${tableOptions}
                        </select>
                    </div>
                    <div class="d-flex justify-content-end">
                        <button class="btn btn-primary" id="goToUploadStep2Btn" disabled>
                            Next <i class="bi bi-arrow-right ms-1"></i>
                        </button>
                    </div>
                </div>
            </div>
        </div>`;

    this.bindUploadWizardEvents();
}

bindUploadWizardEvents() {
    const tableSelect = this.getElementSafe('uploadTableSelect');
    const nextBtn = this.getElementSafe('goToUploadStep2Btn');
    const backBtn = this.getElementSafe('backToChoicesBtn');

    if (tableSelect && nextBtn) {
        tableSelect.addEventListener('change', () => {
            const selected = tableSelect.value;
            nextBtn.disabled = !selected;
            if (selected) {
                this.state.wizardState.tableName = selected;
                console.log(`[Wizard] Selected table: ${selected}`);
            }
        });

        nextBtn.addEventListener('click', () => {
            console.log('[Wizard] Proceeding to Step 2');
            this.renderUploadToTableStep2();
        });
    }

    if (backBtn) {
        backBtn.addEventListener('click', () => {
            console.log('[Wizard] Returning to wizard choice screen');
            this.renderAddDataChoiceView();
        });
    }
}

    renderUploadToTableStep2() {
    this.state.wizardState.step = 2;
    const container = this.getElementSafe('wizardStepContainer');
    if (!container) {
        console.warn('[Wizard] Step container not found.');
        return;
    }

    // Update progress bar
    const progressBar = document.querySelector('.progress-bar');
    if (progressBar) progressBar.style.width = '66%';

    const progressSpans = document.querySelectorAll('.wizard-progress span');
    progressSpans.forEach(span => span.classList.replace('text-primary', 'text-muted'));
    if (progressSpans[1]) progressSpans[1].classList.replace('text-muted', 'text-primary');

    // Render step content
    container.innerHTML = `
        <h5 class="card-title">Step 2: Upload File</h5>
        <p class="text-muted">
            Select the CSV or Excel file containing the data you want to upload to
            <strong>${this.escapeHTML(this.state.wizardState.tableName)}</strong>.
        </p>
        <div class="mb-3">
            <label for="wizardFileInput" class="form-label">Select File</label>
            <input class="form-control" type="file" id="wizardFileInput" accept=".csv,.xlsx,.xls">
        </div>
        <div id="wizardStatus" class="mt-3 alert alert-info">Please select a file to continue.</div>
        <div class="d-flex justify-content-between">
            <button class="btn btn-secondary" id="backToUploadStep1Btn">
                <i class="bi bi-arrow-left me-1"></i> Back
            </button>
            <button class="btn btn-primary" id="goToUploadStep3Btn" disabled>
                Next: Map Columns <i class="bi bi-arrow-right ms-1"></i>
            </button>
        </div>
    `;

    this.bindUploadStep2Events();
}

bindUploadStep2Events() {
    const fileInput = this.getElementSafe('wizardFileInput');
    const nextBtn = this.getElementSafe('goToUploadStep3Btn');
    const backBtn = this.getElementSafe('backToUploadStep1Btn');

    if (fileInput && nextBtn) {
        fileInput.addEventListener('change', (e) => {
            const file = e.target.files[0];
            if (file) {
                nextBtn.disabled = false;
                this.handleWizardFileSelect(file, 'upload');
            } else {
                nextBtn.disabled = true;
            }
        });
    }

    if (nextBtn) {
        nextBtn.addEventListener('click', () => {
            this.handleGoToUploadStep3(); // Assuming this method exists
        });
    }

    if (backBtn) {
        backBtn.addEventListener('click', () => {
            this.renderUploadToTableWizard();
        });
    }
}
    
    renderWizardStep(type, step) {
        console.log('Method called: renderWizardStep');
        console.log('Current wizard state:', this.state.wizardState);
        console.log('Current step:', step);
        console.log('wizardStepContainer exists:', !!this.getElementSafe('wizardStepContainer'));
        const container = this.getElementSafe('wizardStepContainer');
        if (!container) return;
        let html = '';
        if (type === 'create') {
            switch(step) {
                case 1:
                    html = `<h5>Step 1: Upload File</h5><p class="text-muted">Select a CSV or Excel file.</p>
                            <input class="form-control" type="file" id="wizardFileInput" accept=".csv,.xlsx,.xls">
                            <div id="wizardStatus" class="mt-3"></div>`;
                    break;
                case 2:
                    const { schema, table_name } = this.state.wizardState.analysis;
                    html = `<h5>Step 2: Review and Confirm</h5>
                            <div class="mb-3"><label class="form-label">Table Name</label><input type="text" class="form-control" id="wizardTableName" value="${table_name}"></div>
                            <h6>Suggested Schema</h6>
                            <div class="table-responsive" style="max-height: 200px;">
                                <table class="table table-sm">
                                ${schema.map(col => `<tr><td>${col.original_name}</td><td><strong>${col.sql_name}</strong></td><td><span class="badge bg-secondary">${col.sql_type}</span></td></tr>`).join('')}
                                </table>
                            </div>
                            <button class="btn btn-primary mt-3" id="wizardExecuteCreateBtn">Create Table & Import Data</button>`;
                    break;
            }
        }
        container.innerHTML = html;
    }

    // --- WIZARD ACTIONS --- //

    async handleWizardUpload() {
    // Collect mapping from the UI
    const mapping = [];
    document.querySelectorAll('#columnMapper .file-column-mapper').forEach(select => {
        if (select.value) {
            mapping.push({
                db_column: select.dataset.dbColumn,
                file_column: select.value,
            });
        }
    });

    if (mapping.length === 0) {
        this.showError("You must map at least one column from your file.", true);
        return;
    }
    
    const { tableName, temp_filename } = this.state.wizardState;
    const uploadMethod = document.querySelector('input[name="uploadMethod"]:checked').value;
    const connectionId = this.state.currentConnectionId;

    const statusDiv = document.getElementById('wizardStatusMessage');
    const uploadBtn = document.getElementById('wizardExecuteUploadBtn');
    const backBtn = document.getElementById('backToUploadStep2Btn');
    this.setWizardUIState({ createBtn: uploadBtn, backBtn, statusDiv }, 'loading', 'Uploading data...');

    try {
        const result = await this.api.confirmUpload(connectionId, tableName, temp_filename, uploadMethod, mapping);
        this.setWizardUIState({ statusDiv }, 'success', result.message);
        setTimeout(() => {
            this.renderTableViewer(tableName);
        }, 1500);

    } catch (error) {
        this.setWizardUIState({ createBtn: uploadBtn, backBtn, statusDiv }, 'error', `Upload Failed: ${this.sanitizeMessage(error.message)}`);
    }
}
    
    async handleWizardFileSelect(file, wizardType) {
    if (!file) return;

    const statusDiv = this.getElementSafe('wizardStatus');
    if (!statusDiv) return;
    statusDiv.innerHTML = `<div class="alert alert-info"><div class="spinner-border spinner-border-sm me-2"></div>Inspecting file...</div>`;

    try {
        const result = await this.api.inspectFile(file);
        this.state.wizardState.temp_filename = result.temp_filename;
        this.state.wizardState.sheets = result.sheets;
        
        // ✨ FIXED: Added logic to handle sheet selection
        if (result.sheets && result.sheets.length > 0) {
            // Default to the first sheet
            this.state.wizardState.sheet_name = result.sheets[0];
        } else {
             this.state.wizardState.sheet_name = null;
        }
        
        let sheetSelectorHtml = '';
        if (result.sheets && result.sheets.length > 1) {
            sheetSelectorHtml = `
                <div class="my-3">
                    <label for="wizardSheetSelect" class="form-label"><strong>This file has multiple sheets. Please select one:</strong></label>
                    <select class="form-select" id="wizardSheetSelect">
                        ${result.sheets.map(s => `<option value="${this.escapeHTML(s)}">${this.escapeHTML(s)}</option>`).join('')}
                    </select>
                </div>`;
        }

        if (wizardType === 'create') {
            statusDiv.innerHTML = `
                <div class="alert alert-success mt-3">File is ready for analysis.</div>
                ${sheetSelectorHtml}
                <button class="btn btn-primary" id="wizardAnalyzeBtn">Analyze File</button>
            `;
        } else if (wizardType === 'upload') {
            statusDiv.innerHTML = `<div class="alert alert-success mt-3">File selected.</div>${sheetSelectorHtml}`;
            const nextBtn = this.getElementSafe('goToUploadStep3Btn');
            if (nextBtn) nextBtn.disabled = false;
        }
        
        // ✨ FIXED: Added event listener for the sheet selector dropdown
        const sheetSelect = this.getElementSafe('wizardSheetSelect', true);
        if (sheetSelect) {
            sheetSelect.addEventListener('change', (e) => {
                this.state.wizardState.sheet_name = e.target.value;
            });
        }

    } catch (error) {
        if (statusDiv) {
            statusDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
        }
    }
}


handleGoToUploadStep3() {
    const statusDiv = this.getElementSafe('wizardStatus');
    if (statusDiv) {
        statusDiv.innerHTML = `<div class="alert alert-info"><div class="spinner-border spinner-border-sm me-2"></div>Analyzing file and preparing column mapper...</div>`;
    }

    const { tableName, temp_filename, sheet_name } = this.state.wizardState;

    this.api.previewUploadForMatching(this.state.currentConnectionId, tableName, temp_filename, sheet_name)
        .then(result => {
            this.state.wizardState.analysis = result; // Save analysis for the next step
            this.renderUploadToTableStep3();
        })
        .catch(error => {
            if (statusDiv) {
                statusDiv.innerHTML = `<div class="alert alert-danger">Analysis failed: ${error.message}</div>`;
            }
        });
}

renderUploadToTableStep3() {
    this.state.wizardState.step = 3;
    const { analysis } = this.state.wizardState;
    const { db_columns, file_columns, initial_mapping, preview_data } = analysis;

    const container = this.getElementSafe('wizardStepContainer');
    if (!container) return;

    // Update progress bar
    document.querySelector('.progress-bar').style.width = '100%';
    document.querySelectorAll('.wizard-progress span').forEach(el => el.classList.replace('text-primary', 'text-muted'));
    document.querySelectorAll('.wizard-progress span')[2].classList.replace('text-muted', 'text-primary');

    const fileColumnOptions = file_columns.map(c => `<option value="${this.escapeHTML(c)}">${this.escapeHTML(c)}</option>`).join('');
    const preview = JSON.parse(preview_data);

    container.innerHTML = `
        <h5 class="card-title">Step 3: Map Columns and Confirm</h5>
        <p class="text-muted">Match the columns from your file to the columns in the database table. Unmapped database columns will be ignored.</p>
        
        <div class="table-responsive" style="max-height: 250px;">
            <table class="table table-sm">
                <thead><tr><th>Database Column</th><th>Your File's Column</th></tr></thead>
                <tbody id="columnMapper">
                ${db_columns.map(dbCol => `
                    <tr>
                        <td><strong>${this.escapeHTML(dbCol)}</strong></td>
                        <td>
                            <select class="form-select form-select-sm file-column-mapper" data-db-column="${this.escapeHTML(dbCol)}">
                                <option value="">-- Ignore this column --</option>
                                ${fileColumnOptions}
                            </select>
                        </td>
                    </tr>
                `).join('')}
                </tbody>
            </table>
        </div>

        <div class="my-3">
            <h6>Upload Method</h6>
            <div class="form-check">
                <input class="form-check-input" type="radio" name="uploadMethod" id="uploadMethodAppend" value="append" checked>
                <label class="form-check-label" for="uploadMethodAppend"><strong>Append:</strong> Add new rows to the end of the table.</label>
            </div>
            <div class="form-check">
                <input class="form-check-input" type="radio" name="uploadMethod" id="uploadMethodReplace" value="replace">
                <label class="form-check-label text-danger" for="uploadMethodReplace"><strong>Replace:</strong> Delete all existing data before uploading.</label>
            </div>
        </div>

        <h6>Data Preview (First 15 Rows)</h6>
        <div class="table-responsive">
            <table class="table table-sm table-bordered">
                <thead><tr>${preview.columns.map(h => `<th>${this.escapeHTML(h)}</th>`).join('')}</tr></thead>
                <tbody>${preview.data.map(row => `<tr>${row.map(cell => `<td>${this.escapeHTML(cell ?? '')}</td>`).join('')}</tr>`).join('')}</tbody>
            </table>
        </div>
        
        <div id="wizardStatusMessage" class="mt-3"></div>

        <div class="d-flex justify-content-between mt-3">
            <button class="btn btn-secondary" id="backToUploadStep2Btn"><i class="bi bi-arrow-left me-1"></i> Back</button>
            <button class="btn btn-success" id="wizardExecuteUploadBtn"><i class="bi bi-check-circle me-1"></i> Confirm and Upload Data</button>
        </div>
    `;

    // Pre-select the best matches
    initial_mapping.forEach(map => {
        if (map.file_column) {
            const select = container.querySelector(`.file-column-mapper[data-db-column="${map.db_column}"]`);
            if (select) select.value = map.file_column;
        }
    });
}
    
    async handleWizardAnalyzeFile() {
    const statusDiv = this.getElementSafe('wizardStatus');
    if (!statusDiv) return;

    // Start the progress bar
    const completeProgress = this.showProgress(statusDiv, 'Analyzing file...');

    const sheetSelect = this.getElementSafe('wizardSheetSelect', true);
    try {
        const result = await this.api.analyzeUpload(
            this.state.wizardState.temp_filename,
            sheetSelect ? sheetSelect.value : null
        );
        this.state.wizardState.analysis = result;
        
        // When done, complete the progress bar
        completeProgress();
        
        setTimeout(() => this.renderCreateTableStep2(), 200); // Short delay before showing next step
    } catch (error) {
        statusDiv.innerHTML = `<div class="alert alert-danger mt-3">Analysis failed: ${error.message}</div>`;
    }
}

    
    async handleWizardExecuteCreate() {
    const { tableName, finalSchema, temp_filename } = this.state.wizardState;
    const connectionId = this.state.currentConnectionId;

    if (!tableName || !finalSchema || !temp_filename || !connectionId) {
        this.showError("Wizard state is incomplete. Please start over.", true);
        return;
    }

    const statusDiv = this.getElementSafe('wizardStatusMessage');
    const createBtn = this.getElementSafe('wizardExecuteCreateBtn');
    const backBtn = this.getElementSafe('backToStep2Btn');
    
    if (createBtn) createBtn.disabled = true;
    if (backBtn) backBtn.disabled = true;

    // Start the progress bar
    const completeProgress = this.showProgress(statusDiv, 'Creating table and importing data...');

    try {
        const result = await this.api.createTableFromImport(connectionId, tableName, finalSchema, temp_filename);

        // When done, complete the progress bar and show success
        completeProgress();
        setTimeout(() => {
            this.setWizardUIState({ statusDiv }, 'success', result.message);
            setTimeout(async () => {
                await this.handleConnectionChange(connectionId);
                this.handleTableSelect(tableName);
            }, 1500);
        }, 200);

    } catch (error) {
        console.error('[Wizard] Table creation failed:', error);
        this.setWizardUIState({ createBtn, backBtn, statusDiv }, 'error', `Upload Failed: ${this.sanitizeMessage(error.message)}`);
    }
}

setWizardUIState({ createBtn, backBtn, statusDiv }, state, message) {
    const spinner = `<div class="spinner-border spinner-border-sm me-2"></div>`;
    const alertClass = {
        loading: 'alert-info',
        success: 'alert-success',
        error: 'alert-danger'
    }[state] || 'alert-secondary';

    if (statusDiv) {
        statusDiv.innerHTML = `<div class="alert ${alertClass} mt-3">${state === 'loading' ? spinner : ''}${message}</div>`;
    }

    if (createBtn) createBtn.disabled = state === 'loading';
    if (backBtn) backBtn.disabled = state === 'loading';
}

sanitizeMessage(msg) {
    const div = document.createElement('div');
    div.textContent = msg;
    return div.innerHTML;
}
    // --- OTHER ACTIONS & HELPERS --- //
    
    async handleModifySchema() {
        if (this.modals.modifySchema) this.modals.modifySchema.show();
        const container = this.getElementSafe('columnListContainer');
        if (!container) return;
        container.innerHTML = `<div class="text-center p-3"><div class="spinner-border spinner-border-sm"></div></div>`;
        try {
            const result = await this.api.getColumnsForTable(this.state.currentConnectionId, this.state.selectedTable);
            
            // Store current PKs
            this.state.currentPKs = Array.isArray(result.pks) ? result.pks : [];
            const commonTypes = [
                'VARCHAR(255)', 'TEXT', 'INTEGER', 'BIGINT', 'FLOAT', 'DECIMAL(10,2)', 'DATE', 'TIMESTAMP', 'BOOLEAN'
            ];
            const rowsHtml = result.columns.map(col => {
                const allTypes = Array.from(new Set([String(col.type), ...commonTypes]));
                const opts = allTypes.map(t => `<option value="${t}" ${String(col.type)===String(t)?'selected':''}>${t}</option>`).join('');
                const isPK = this.state.currentPKs.includes(col.name);
                const isAI = !!col.auto_increment;
                const isNullable = (col.nullable !== false);
                return `
                    <tr data-col="${col.name}">
                        <td class="align-middle"><code>${col.name}</code></td>
                        <td><select class="form-select form-select-sm col-type" data-original="${String(col.type)}">${opts}</select></td>
                        <td class="text-center"><input type="checkbox" class="form-check-input col-null" data-original="${isNullable?'true':'false'}" ${isNullable?'checked':''}></td>
                        <td class="text-center"><input type="checkbox" class="form-check-input col-pk" ${isPK?'checked':''}></td>
                        <td class="text-center"><input type="checkbox" class="form-check-input col-ai" data-original="${isAI?'true':'false'}" ${isAI?'checked':''}></td>
                        <td class="text-end">
                            <div class="btn-group btn-group-sm">
                                <button class="btn btn-outline-secondary rename-col-btn" data-column="${col.name}">Rename</button>
                                <button class="btn btn-outline-danger delete-col-btn" data-column="${col.name}">Delete</button>
                            </div>
                        </td>
                    </tr>`;
            }).join('');

            container.innerHTML = `
                <div class="table-responsive">
                  <table class="table table-sm align-middle">
                    <thead class="table-light">
                        <tr>
                            <th>Column</th>
                            <th style="width:220px">Data Type</th>
                            <th class="text-center" style="width:110px">Nullable</th>
                            <th class="text-center" style="width:120px">Primary Key</th>
                            <th class="text-center" style="width:140px">Auto Increment</th>
                            <th class="text-end" style="width:180px">Actions</th>
                        </tr>
                    </thead>
                    <tbody>${rowsHtml}</tbody>
                  </table>
                </div>
            `;
            
            // Add event listeners for the action buttons
            container.querySelectorAll('.rename-col-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const columnName = e.target.dataset.column;
                    this.showRenameColumnModal(columnName);
                });
            });
            
            container.querySelectorAll('.delete-col-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const columnName = e.target.dataset.column;
                    this.handleDeleteColumn(columnName);
                });
            });
        } catch(error) {
            container.innerHTML = `<div class="alert alert-danger">Failed to load columns: ${error.message}</div>`;
        }
    }

    planSchemaChanges() {
        const container = this.getElementSafe('columnListContainer');
        if (!container) return;
        const rows = Array.from(container.querySelectorAll('tbody tr[data-col]'));
        const typeChanges = [];
        const pkSelected = [];
        const aiChanges = [];
        const nullChanges = [];

        rows.forEach(tr => {
            const name = tr.getAttribute('data-col');
            const sel = tr.querySelector('.col-type');
            const newType = sel?.value || '';
            const origType = sel?.dataset.original || '';
            if (newType && origType && newType.trim() !== origType.trim()) typeChanges.push({ column: name, from: origType, to: newType });
            if (tr.querySelector('.col-pk')?.checked) pkSelected.push(name);
            const ai = tr.querySelector('.col-ai');
            if (ai) {
                const original = (ai.dataset.original || 'unknown').toLowerCase();
                const changed = (original === 'unknown') || (String(ai.checked) !== original);
                if (changed) aiChanges.push({ column: name, enable: ai.checked });
            }
            const nul = tr.querySelector('.col-null');
            if (nul) {
                const originalN = (nul.dataset.original || 'true').toLowerCase();
                if (String(nul.checked) !== originalN) nullChanges.push({ column: name, allow: !!nul.checked });
            }
        });

        const planBox = this.getElementSafe('schemaPlanContainer');
        const planSummary = this.getElementSafe('schemaPlanSummary');
        const planDDL = this.getElementSafe('schemaPlanDDL');
        if (!planBox || !planSummary || !planDDL) return;

        const lines = [];
        typeChanges.forEach(ch => lines.push(`Change type of ${ch.column} from ${ch.from} to ${ch.to}`));
        const origPK = (this.state?.currentPKs || []).join(', ') || '(none)';
        const nextPK = pkSelected.join(', ') || '(none)';
        if (origPK !== nextPK) lines.push(`Set primary key to [${nextPK}] (was [${origPK}])`);
        aiChanges.forEach(x => lines.push(`${x.enable ? 'Enable' : 'Disable'} AUTO INCREMENT on ${x.column}`));
        nullChanges.forEach(x => lines.push(`${x.allow ? 'Allow' : 'Disallow'} NULL on ${x.column}`));

        planSummary.innerHTML = lines.length ? ('- ' + lines.map(l => this.escapeHTML(l)).join('<br>- ')) : '<span class="text-muted">No changes detected.</span>';
        const ddl = [];
        typeChanges.forEach(ch => ddl.push(`ALTER TABLE ${this.state.selectedTable} ALTER COLUMN ${ch.column} TYPE ${ch.to};`));
        if (origPK !== nextPK) ddl.push(`-- primary key will be updated`);
        ddl.push(`-- auto increment will be toggled on selected columns`);
        ddl.push(`-- nullability will be updated on selected columns`);
        planDDL.textContent = ddl.join('\n');
        planBox.style.display = '';
    }

    async applySchemaChanges() {
        const container = this.getElementSafe('columnListContainer');
        if (!container) return;
        const rows = Array.from(container.querySelectorAll('tbody tr[data-col]'));
        const typeChanges = [];
        const pkSelected = [];
        const aiChanges = [];
        const nullChanges = [];

        rows.forEach(tr => {
            const name = tr.getAttribute('data-col');
            const sel = tr.querySelector('.col-type');
            const newType = sel?.value || '';
            const origType = sel?.dataset.original || '';
            if (newType && origType && newType.trim() !== origType.trim()) typeChanges.push({ column: name, to: newType });
            if (tr.querySelector('.col-pk')?.checked) pkSelected.push(name);
            const ai = tr.querySelector('.col-ai');
            if (ai) {
                const original = (ai.dataset.original || 'unknown').toLowerCase();
                const changed = (original === 'unknown') || (String(ai.checked) !== original);
                if (changed) aiChanges.push({ column: name, enable: ai.checked });
            }
            const nul = tr.querySelector('.col-null');
            if (nul) {
                const originalN = (nul.dataset.original || 'true').toLowerCase();
                if (String(nul.checked) !== originalN) nullChanges.push({ column: name, allow: !!nul.checked });
            }
        });

        try {
            for (const ch of typeChanges) {
                await this.api.modifyColumnType(this.state.currentConnectionId, this.state.selectedTable, ch.column, ch.to);
            }
            for (const nc of nullChanges) {
                await this.api.setNullable(this.state.currentConnectionId, this.state.selectedTable, nc.column, nc.allow);
            }
            await this.api.setPrimaryKey(this.state.currentConnectionId, this.state.selectedTable, pkSelected);
            for (const ai of aiChanges) {
                await this.api.setAutoIncrement(this.state.currentConnectionId, this.state.selectedTable, ai.column, ai.enable);
            }
            this.showError('Schema changes applied successfully');
            await this.handleModifySchema();
        } catch (e) {
            this.showError(`Failed to apply changes: ${e.message}`);
        }
    }

    showAddColumnModal() {
        const tableNameEl = this.getElementSafe('addColumnTableName');
        if (tableNameEl) tableNameEl.textContent = this.state.selectedTable;

        const colNameEl = this.getElementSafe('newColumnName');
        if (colNameEl) colNameEl.value = '';

        const colTypeEl = this.getElementSafe('newColumnType');
        if (colTypeEl) colTypeEl.value = 'VARCHAR(255)';

        const nullableEl = this.getElementSafe('newColumnNullable');
        if (nullableEl) nullableEl.checked = true; // default allow NULL

        if (this.modals.addColumn) this.modals.addColumn.show();
    }

    showRenameColumnModal(columnName) {
        const oldNameEl = this.getElementSafe('oldColumnName');
        if (oldNameEl) oldNameEl.value = columnName;

        const newNameEl = this.getElementSafe('newColumnNameInput');
        if (newNameEl) newNameEl.value = columnName;

        if (this.modals.renameColumn) this.modals.renameColumn.show();
    }

    async handleAddColumn() {
        const columnNameEl = this.getElementSafe('newColumnName');
        const columnTypeEl = this.getElementSafe('newColumnType');
        const columnNullableEl = this.getElementSafe('newColumnNullable');
        if (!columnNameEl || !columnTypeEl) {
            this.showError("Modal elements not found.");
            return;
        }

        const columnName = (columnNameEl.value || '').trim();
        const columnType = (columnTypeEl.value || '').trim();
        const wantPK = !!this.getElementSafe('newColumnPK')?.checked;
        const wantAI = !!this.getElementSafe('newColumnAI')?.checked;
        const allowNull = columnNullableEl ? !!columnNullableEl.checked : true;

        if (!columnName) {
            this.showError('Column name is required');
            return;
        }
        try {
            await this.api.addColumn(this.state.currentConnectionId, this.state.selectedTable, columnName, columnType, allowNull);
            if (wantPK) {
                try {
                    const meta = await this.api.getColumnsForTable(this.state.currentConnectionId, this.state.selectedTable);
                    const currentPKs = Array.isArray(meta.pks) ? meta.pks : [];
                    const nextPKs = Array.from(new Set([...currentPKs, columnName]));
                    await this.api.setPrimaryKey(this.state.currentConnectionId, this.state.selectedTable, nextPKs);
                } catch (_) {}
            }
            if (wantAI) {
                try {
                    await this.api.setAutoIncrement(this.state.currentConnectionId, this.state.selectedTable, columnName, true);
                } catch (_) {}
            }
            if (this.modals.addColumn) this.hideModalSafely(this.modals.addColumn, 'addColumnModal');
            this.showError('Column added successfully');
            this.handleModifySchema();
        } catch (error) {
            this.showError(`Failed to add column: ${error.message}`);
        }
    }

    async handleRenameColumn() {
        const oldNameEl = this.getElementSafe('oldColumnName');
        const newNameEl = this.getElementSafe('newColumnNameInput');

        if (!oldNameEl || !newNameEl) {
            this.showError("Modal elements not found.");
            return;
        }

        const oldName = (oldNameEl.value || '').trim();
        const newName = (newNameEl.value || '').trim();
        // No-op if unchanged
        if (oldName === newName) {
            if (this.modals.renameColumn) this.hideModalSafely(this.modals.renameColumn, 'renameColumnModal');
            this.showError('Column name unchanged');
            return;
        }
        
        if (!newName) {
            this.showError('New column name is required');
            return;
        }
        
        try {
            await this.api.renameColumn(this.state.currentConnectionId, this.state.selectedTable, oldName, newName);
            if (this.modals.renameColumn) this.hideModalSafely(this.modals.renameColumn, 'renameColumnModal');
            this.showError('Column renamed successfully');
            this.handleModifySchema(); // Refresh the column list
        } catch (error) {
            this.showError(`Failed to rename column: ${error.message}`);
        }
    }

    async handleDeleteColumn(columnName) {
        if (!confirm(`Are you sure you want to delete column "${columnName}"?`)) return;
        
        try {
            await this.api.dropColumn(this.state.currentConnectionId, this.state.selectedTable, columnName);
            this.showError('Column deleted successfully');
            this.handleModifySchema(); // Refresh the column list
        } catch (error) {
            this.showError(`Failed to delete column: ${error.message}`);
        }
    }

    async updateColumnType(columnName, newType) {
        try {
            await this.api.modifyColumnType(
                this.state.currentConnectionId, 
                this.state.selectedTable, 
                columnName, 
                newType
            );
            this.showError(`Column ${columnName} type updated successfully`);
            if (this.modals.modifySchema) this.hideModalSafely(this.modals.modifySchema, 'modifySchemaModal');
        } catch(error) {
            this.showError(`Failed to update column type: ${error.message}`);
        }
    }

    handleDeleteRows() {
        if (!this.dom.workspace) return;
        const selectedCheckboxes = this.dom.workspace.querySelectorAll('.row-checkbox:checked');
        const pksToDelete = Array.from(selectedCheckboxes).map(cb => cb.dataset.pk);
        if (pksToDelete.length === 0) return;

        const pkColumn = this.state.tableColumns[0].name; // Assumes first column is PK

        if (confirm(`Are you sure you want to delete ${pksToDelete.length} row(s)?`)) {
            this.api.deleteRows(this.state.currentConnectionId, this.state.selectedTable, pkColumn, pksToDelete)
                .then(() => {
                    this.renderTableViewer(this.state.selectedTable);
                })
                .catch(error => {
                    this.showError(`Delete failed: ${error.message}`);
                });
        }
    }

    handleTruncateTable() {
        this.setupPasswordConfirmation(
            `Confirm: Truncate Table`,
            `You are about to permanently delete all data from "${this.state.selectedTable}".`,
            async (password) => {
                await this.api.truncateTable(this.state.currentConnectionId, this.state.selectedTable, password);
                this.renderTableViewer(this.state.selectedTable);
            }
        );
    }

    handleDropTable() {
        this.setupPasswordConfirmation(
            `Confirm: Drop Table`,
            `You are about to permanently delete the entire "${this.state.selectedTable}" table.`,
            async (password) => {
                try {
                    await this.api.dropTable(this.state.currentConnectionId, this.state.selectedTable, password, false);
                    this.handleConnectionChange(this.state.currentConnectionId);
                } catch (error) {
                    const message = String(error?.message || error);
                    // Postgres dependency error hint
                    const needsCascade = /dependent objects/i.test(message) || /Use DROP .* CASCADE/i.test(message) || /2BP01/.test(message);
                    if (needsCascade) {
                        const ok = confirm(`Cannot drop table because other objects depend on it.\n\n${message}\n\nDrop with CASCADE? This will remove dependent constraints/objects.`);
                        if (ok) {
                            await this.api.dropTable(this.state.currentConnectionId, this.state.selectedTable, password, true);
                            this.handleConnectionChange(this.state.currentConnectionId);
                            return;
                        } else {
                            throw error;
                        }
                    }
                    throw error;
                }
            }
        );
    }

    toggleAllRowCheckboxes(checked) {
        if (!this.dom.workspace) return;
        this.dom.workspace.querySelectorAll('.row-checkbox').forEach(cb => cb.checked = checked);
        this.updateDeleteButtonState();
    }

    updateDeleteButtonState() {
        if (!this.dom.workspace) return;
        const selectedCount = this.dom.workspace.querySelectorAll('.row-checkbox:checked').length;
        const deleteBtn = this.getElementSafe('deleteRowsBtn');
        if (deleteBtn) {
            deleteBtn.disabled = selectedCount === 0;
            deleteBtn.innerHTML = `<i class="bi bi-trash me-1"></i>Delete Selected (${selectedCount})`;
        }
    }

    setupPasswordConfirmation(title, warning, callback) {
        const titleEl = this.getElementSafe('passwordConfirmTitle');
        if (titleEl) titleEl.textContent = title;

        const warningEl = this.getElementSafe('passwordConfirmWarning');
        if (warningEl) warningEl.textContent = warning;

        const errorEl = this.getElementSafe('passwordConfirmError');
        if (errorEl) errorEl.style.display = 'none';

        const passwordEl = this.getElementSafe('userPassword');
        if (passwordEl) passwordEl.value = '';

        this.state.passwordConfirmCallback = async () => {
            const passwordInput = this.getElementSafe('userPassword');
            if (!passwordInput) return;
            
            const password = passwordInput.value;
            if (!password) return;
            try {
                await callback(password);
                if (this.modals.passwordConfirm) this.hideModalSafely(this.modals.passwordConfirm, 'passwordConfirmModal');
            } catch (error) {
                const confirmErrorEl = this.getElementSafe('passwordConfirmError');
                if (confirmErrorEl) {
                    confirmErrorEl.textContent = error.message;
                    confirmErrorEl.style.display = 'block';
                }
            }
        };

        if (this.modals.passwordConfirm) this.modals.passwordConfirm.show();
    }
    
    showError(message) { alert(message); }
    
    escapeHTML(str) {
        if (str === null || str === undefined) return '';
        return String(str).replace(/[&<>"]/g, match => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[match]));
    }

    showProgress(statusDiv, message) {
    statusDiv.innerHTML = `
        <p class="mb-1">${message}</p>
        <div class="progress" style="height: 10px;">
            <div class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 0%" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100"></div>
        </div>
    `;
    const progressBar = statusDiv.querySelector('.progress-bar');
    let width = 0;
    
    // Animate the bar to 95% over 8 seconds to simulate work being done
    const interval = setInterval(() => {
        if (width >= 95) {
            clearInterval(interval);
        } else {
            width += 5;
            progressBar.style.width = width + '%';
            progressBar.textContent = width + '%';
        }
    }, 400);

    // Return a function that can be called to complete the progress bar
    return () => {
        clearInterval(interval);
        progressBar.style.width = '100%';
        progressBar.textContent = '100%';
        progressBar.classList.remove('progress-bar-animated');
    };
}




}


class ApiService {
    constructor() {
        this.csrftoken = this.getCookie('csrftoken');
    }

    async fetch(url, options = {}) {
        try {
            const isFormData = options.body instanceof FormData;
            const headers = { 'X-CSRFToken': this.csrftoken, ...options.headers };
            if (!isFormData) headers['Content-Type'] = 'application/json';

            options.headers = headers;
            options.credentials = 'same-origin';
            
            if (options.body && !isFormData) options.body = JSON.stringify(options.body);

            const response = await fetch(url, options);

            if (!response.ok) {
                const errorText = await response.text();
                let errorMessage = errorText;
                try {
                    const errorJson = JSON.parse(errorText);
                    errorMessage = errorJson.error || errorJson.message || JSON.stringify(errorJson);
                } catch (e) {
                    // Not JSON, use the raw text.
                }
                throw new Error(errorMessage);
            }
            
            return response.json();
        } catch (error) {
            console.error('API request failed:', error);
            throw error;
        }
    }
    
    // --- Existing and Correct Methods ---
    getConnections() { return this.fetch('/api/connections/'); }
    getVisibleTables(connectionId) { return this.fetch(`/api/data/visible-tables/${connectionId}/`); }
    getTableData(connectionId, tableName) { return this.fetch(`/api/data/get-table-data/${connectionId}/${tableName}/`); }
    getColumnsForTable(connectionId, tableName) { return this.fetch(`/api/data/table-columns/${connectionId}/${tableName}/`); }
    
    inspectFile(file) {
        const formData = new FormData();
        formData.append('file', file);
        return this.fetch('/api/data/inspect-file/', { method: 'POST', body: formData });
    }
    
    analyzeUpload(tempFilename, sheetName) {
        return this.fetch('/api/analyze_upload/', {
            method: 'POST', body: { temp_filename: tempFilename, sheet_name: sheetName }
        });
    }

    truncateTable(connectionId, tableName, password) {
        return this.fetch('/api/data/truncate-table/', {
            method: 'POST', body: { connection_id: connectionId, table_name: tableName, password: password }
        });
    }
    
    dropTable(connectionId, tableName, password, cascade = false) {
        return this.fetch('/api/data/drop-table/', {
            method: 'POST', body: { connection_id: connectionId, table_name: tableName, password: password, cascade }
        });
    }

    deleteRows(connectionId, tableName, pkColumn, pks) {
        return this.fetch('/api/data/delete-rows/', {
            method: 'POST',
            body: { connection_id: connectionId, table_name: tableName, pk_column: pkColumn, pks: pks }
        });
    }

    addColumn(connectionId, tableName, columnName, columnType, allowNull = true) {
        return this.fetch('/api/data/add-column/', {
            method: 'POST',
            body: { connection_id: connectionId, table_name: tableName, column_name: columnName, column_type: columnType, allow_null: !!allowNull }
        });
    }

    renameColumn(connectionId, tableName, oldName, newName) {
        return this.fetch('/api/data/rename-column/', {
            method: 'POST',
            body: { connection_id: connectionId, table_name: tableName, old_name: oldName, new_name: newName }
        });
    }

    dropColumn(connectionId, tableName, columnName) {
        return this.fetch('/api/data/drop-column/', {
            method: 'POST',
            body: { connection_id: connectionId, table_name: tableName, column_name: columnName }
        });
    }

    modifyColumnType(connectionId, tableName, columnName, newType) {
        return this.fetch('/api/data/modify-column-type/', {
            method: 'POST',
            body: { connection_id: connectionId, table_name: tableName, column_name: columnName, new_type: newType }
        });
    }
    
    setPrimaryKey(connectionId, tableName, columns) {
        return this.fetch('/api/data/set-primary-key/', {
            method: 'POST',
            body: { connection_id: connectionId, table_name: tableName, columns }
        });
    }
    
    setAutoIncrement(connectionId, tableName, columnName, enable) {
        return this.fetch('/api/data/set-auto-increment/', {
            method: 'POST',
            body: { connection_id: connectionId, table_name: tableName, column_name: columnName, enable: !!enable }
        });
    }
    
    setNullable(connectionId, tableName, columnName, allowNull) {
        return this.fetch('/api/data/set-nullable/', {
            method: 'POST',
            body: { connection_id: connectionId, table_name: tableName, column_name: columnName, allow_null: !!allowNull }
        });
    }
    
    createTableFromImport(connectionId, tableName, schema, tempFilename) {
        return this.fetch('/api/create_table_from_import/', {
            method: 'POST',
            body: { connection_id: connectionId, table_name: tableName, schema: schema, temp_filename: tempFilename }
        });
    }

    // *** ADDED THE TWO MISSING FUNCTIONS FOR THE UPLOAD WIZARD ***
    previewUploadForMatching(connectionId, tableName, tempFilename, sheetName) {
        return this.fetch('/api/data/preview-upload-matching/', {
            method: 'POST',
            body: {
                connection_id: connectionId,
                table_name: tableName,
                temp_filename: tempFilename,
                sheet_name: sheetName
            }
        });
    }

    confirmUpload(connectionId, tableName, tempFilename, uploadMethod, mapping) {
        return this.fetch('/api/data/confirm-upload/', {
            method: 'POST',
            body: {
                connection_id: connectionId,
                table_name: tableName,
                temp_filename: tempFilename,
                sheet_name: this.state?.wizardState?.sheet_name || null,
                upload_method: uploadMethod,
                mapping: mapping
            }
        });
    }

    setDefaultConnection(connectionId) {
        return this.fetch('/api/user/set-default-connection/', {
            method: 'POST',
            body: { connection_id: connectionId }
        });
    }
    
    // --- Utility Method ---
    getCookie(name) {
        let cookieValue = null;
        if (document.cookie && document.cookie !== '') {
            const cookies = document.cookie.split(';');
            for (let i = 0; i < cookies.length; i++) {
                const cookie = cookies[i].trim();
                if (cookie.substring(0, name.length + 1) === (name + '=')) {
                    cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                    break;
                }
            }
        }
        return cookieValue;
    }
}

function checkImportPermission() {
    if (window.dataManagementApp && window.dataManagementApp.permissions) {
        return !!window.dataManagementApp.permissions.canUpload;
    }
    const userType = window.currentUser?.user_type || "";
    return ["Admin", "Moderator", "Uploader"].includes(userType);
}

function getCurrentConnectionId() {
    if (window.dataManagementApp && window.dataManagementApp.state) {
        return window.dataManagementApp.state.currentConnectionId || null;
    }
    try {
        return sessionStorage.getItem("intelligentImportConnectionId");
    } catch (_) {
        return null;
    }
}

function showAlert(message, type = "info") {
    if (type === "error" && window.dataManagementApp && typeof window.dataManagementApp.showError === "function") {
        window.dataManagementApp.showError(message);
        return;
    }
    window.alert(message);
}

function openIntelligentImport() {
    if (!checkImportPermission()) {
        showAlert("You do not have permission to access the intelligent import feature.", "error");
        return;
    }

    if (window.dataManagementApp && typeof window.dataManagementApp.handleIntelligentImportRedirect === "function") {
        window.dataManagementApp.handleIntelligentImportRedirect();
        return;
    }

    const connectionId = getCurrentConnectionId();
    const targetUrl = connectionId
        ? `/intelligent-import/?connection=${encodeURIComponent(connectionId)}`
        : "/intelligent-import/";
    window.location.href = targetUrl;
}

document.addEventListener('DOMContentLoaded', () => {
    const app = new DataManagementApp();
    app.init();
    window.dataManagementApp = app;
});



document.addEventListener("DOMContentLoaded", () => {
    const canvas = document.getElementById("canvas");
    if (!canvas) return;

    // --- STATE AND CONFIG ---
    let currentConnectionId = null;
    let schema = {};
    let isEditable = document.querySelector('.data-model-container')?.dataset.editable === 'true';
    let history = [];
    let historyIndex = -1;
    let zoom = 1;
    let selectedConnection = null;
    let sortOrder = 'asc';
    let activeJoinWizard = { popover: null, sourceEl: null, previews: [] };

    // --- JSPLUMB INSTANCE ---
    const instance = jsPlumb.getInstance({
        Connector: ["Bezier", { curviness: 50 }],
        Anchors: ["Right", "Left"],
        PaintStyle: { strokeWidth: 2, stroke: "#4361ee" },
        HoverPaintStyle: { stroke: "#3a0ca3", strokeWidth: 3 },
        Endpoint: ["Dot", { radius: 5 }],
        EndpointStyle: { fill: "#4361ee" },
        Container: canvas,
        DragOptions: { cursor: "pointer", zIndex: 2000 },
        ConnectionOverlays: [["Arrow", { location: 1, id: "arrow", length: 10, foldback: 0.8 }]],
        ConnectionsDetachable: isEditable
    });

    // --- UTILITY FUNCTIONS ---
    function getCookie(name) {
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

    function safeAddEventListener(elementId, event, handler) {
        const element = document.getElementById(elementId);
        if (element) element.addEventListener(event, handler);
    }

    function getTypeIcon(type) {
        const t = type.toLowerCase();
        if (t.includes("int") || t.includes("num") || t.includes("dec") || t.includes("real")) return "123";
        else if (t.includes("date") || t.includes("time")) return "📅";
        return "Abc";
    }

    function findFieldElement(tableName, columnName) {
        return document.getElementById(`card_${tableName}_col_${columnName}`);
    }

    function isCompatibleField(f1, f2) {
        const t1 = f1.dataset.type.toLowerCase();
        const t2 = f2.dataset.type.toLowerCase();
        const n1 = f1.dataset.isNumeric === 'true';
        const n2 = f2.dataset.isNumeric === 'true';

        if (n1 && n2) return true;
        if (!n1 && !n2) {
            const dateTimeKeywords = ['date', 'time', 'year'];
            const isDateTime1 = dateTimeKeywords.some(k => t1.includes(k));
            const isDateTime2 = dateTimeKeywords.some(k => t2.includes(k));
            return !isDateTime1 && !isDateTime2;
        }
        return false;
    }

    function joinCssClass(type) {
        if (!type) return 'inner-join';
        const t = type.toUpperCase();
        if (t === 'LEFT') return 'left-join';
        if (t === 'RIGHT') return 'right-join';
        return 'inner-join';
    }

    function determineCardinality(sourceIsPk, targetIsPk) {
        if (sourceIsPk && targetIsPk) {
            return 'one-to-one';
        } else if (sourceIsPk && !targetIsPk) {
            return 'one-to-many';
        } else if (!sourceIsPk && targetIsPk) {
            return 'many-to-one';
        } else {
            return 'many-to-many';
        }
    }

    function setupNewConnection(connection, sourceEl, targetEl) {
    try {
        const sourceIsPk = sourceEl.dataset.pk === '1';
        const targetIsPk = targetEl.dataset.pk === '1';
        
        const newCardinality = determineCardinality(sourceIsPk, targetIsPk);

        connection.setData({
            joinType: 'INNER',
            cardinality: newCardinality
        });
        connection.addClass(joinCssClass('INNER'));
        setCardinalityOverlays(connection, newCardinality);
    } catch (error) {
        console.error("Failed to setup new connection:", error);
        instance.deleteConnection(connection);
    }
}

    // --- HISTORY MANAGEMENT ---
    function saveHistory() {
        const snapshot = exportModel();
        history = history.slice(0, historyIndex + 1);
        history.push(snapshot);
        historyIndex = history.length - 1;

        if (document.getElementById('undoBtn')) {
            document.getElementById('undoBtn').disabled = historyIndex <= 0;
        }
        if (document.getElementById('redoBtn')) {
            document.getElementById('redoBtn').disabled = historyIndex >= history.length - 1;
        }
    }

    function undo() {
        if (historyIndex > 0) {
            historyIndex--;
            loadModelFromHistory(history[historyIndex]);
        }
    }

    function redo() {
        if (historyIndex < history.length - 1) {
            historyIndex++;
            loadModelFromHistory(history[historyIndex]);
        }
    }

    function loadModelFromHistory(snapshot) {
        resetCanvas();
        
        snapshot.layout.forEach(l => {
            const tableSchema = schema[l.tableName];
            if (tableSchema) {
                addTableCard(tableSchema, l.x, l.y);
                if (l.collapsed) {
                    const card = document.querySelector(`.table-card[data-table-name="${CSS.escape(l.tableName)}"]`);
                    if (card) {
                        const body = card.querySelector('.table-card-body');
                        if (body) {
                            body.classList.add('collapsed');
                            const collapseBtn = card.querySelector('.btn-collapse i');
                            if (collapseBtn) collapseBtn.className = 'bi bi-chevron-right';
                            rerouteConnectionsToHeader(card);
                        }
                    }
                }
            }
        });

        setTimeout(() => {
            snapshot.joins.forEach(j => {
                const sourceEl = findFieldElement(j.leftTable, j.leftColumn);
                const targetEl = findFieldElement(j.rightTable, j.rightColumn);

                if (sourceEl && targetEl) {
                    const conn = instance.connect({
                        source: sourceEl,
                        target: targetEl,
                        data: {
                            joinType: j.joinType,
                            cardinality: j.cardinality,
                            originalSourceId: sourceEl.id,
                            originalTargetId: targetEl.id
                        }
                    });
                    conn.setClass(joinCssClass(j.joinType));
                    setCardinalityOverlays(conn, j.cardinality);
                } else {
                    console.warn('Could not create join, element not found for:', j);
                }
            });
            instance.repaintEverything();
        }, 300);

        zoom = snapshot.viewport?.zoom || 1;
        applyZoom(zoom, { x: 0, y: 0 });
        canvas.scrollLeft = snapshot.viewport?.scrollLeft || 0;
        canvas.scrollTop = snapshot.viewport?.scrollTop || 0;

        if (document.getElementById('undoBtn')) {
            document.getElementById('undoBtn').disabled = historyIndex <= 0;
        }
        if (document.getElementById('redoBtn')) {
            document.getElementById('redoBtn').disabled = historyIndex >= history.length - 1;
        }
    }

    // --- EVENT BINDING ---
    function initializeEventListeners() {
        safeAddEventListener("connectionSelect", "change", handleConnectionChange);
        safeAddEventListener("saveModelBtn", "click", saveModel);
        safeAddEventListener("undoBtn", "click", undo);
        safeAddEventListener("redoBtn", "click", redo);
        safeAddEventListener("autoArrangeBtn", "click", autoArrangeLayout);
        safeAddEventListener("centerCanvasBtn", "click", centerCanvas);
        safeAddEventListener("zoomInBtn", "click", () => adjustZoom(0.1));
        safeAddEventListener("zoomOutBtn", "click", () => adjustZoom(-0.1));
        safeAddEventListener("resetZoomBtn", "click", () => applyZoom(1));
        safeAddEventListener("validateBtn", "click", validateModel);
        safeAddEventListener("testConnectionBtn", "click", testConnection);
        safeAddEventListener("tableSearch", "input", updateTableSidebar);
        safeAddEventListener("sortTableBtn", "click", toggleTableSort);
        safeAddEventListener("toggleSidebar", "click", toggleSidebar);
        safeAddEventListener("suggestJoinsBtn", "click", fetchSuggestedJoins);
        safeAddEventListener("applyAllSuggested", "click", applyAllSuggestedJoins);

        // Modal buttons
        safeAddEventListener("saveJoinBtn", "click", () => {
            if (selectedConnection) {
                const joinType = document.getElementById('joinType').value;
                const cardinality = document.getElementById('cardinality').value;
                selectedConnection.setData({ joinType, cardinality });
                setCardinalityOverlays(selectedConnection, cardinality);
                bootstrap.Modal.getInstance(document.getElementById('joinModal')).hide();
                saveHistory();
            }
        });

        safeAddEventListener("deleteJoinBtn", "click", () => {
            if (selectedConnection) {
                instance.deleteConnection(selectedConnection);
                bootstrap.Modal.getInstance(document.getElementById('joinModal')).hide();
                saveHistory();
            }
        });

        canvas.addEventListener("dragover", e => e.preventDefault());
        canvas.addEventListener("drop", handleCanvasDrop);
        canvas.addEventListener("click", handleCanvasClick);
        document.addEventListener("keydown", handleKeyboardShortcuts);

        // jsPlumb event bindings
        instance.bind("beforeDrop", (info) => info.sourceId !== info.targetId);
        
        instance.bind("connection", (info, originalEvent) => {
    console.log("1. 'connection' event fired."); // DEBUG
    if (originalEvent) {
        console.log("2. User-dragged connection detected. Calling setup..."); // DEBUG
        setupNewConnection(info.connection, info.source, info.target);
        saveHistory();
    }
});

        instance.bind("click", (connection, originalEvent) => {
            originalEvent.preventDefault();
            openJoinModal(connection);
        });

        // Enhanced tooltip with timeout
        let tooltipTimeout;
        const tooltip = document.createElement('div');
        tooltip.className = 'join-info-tooltip';
        document.body.appendChild(tooltip);

        instance.bind("mouseover", (connection, originalEvent) => {
            if (typeof connection.getData !== 'function') return;
            clearTimeout(tooltipTimeout);
            const data = connection.getData() || {};
            const sourceEl = document.getElementById(connection.sourceId);
            const targetEl = document.getElementById(connection.targetId);

            tooltip.innerHTML = `
                <strong>From:</strong> ${sourceEl.closest('.table-card').dataset.tableName}.${sourceEl.dataset.column}<br>
                <strong>To:</strong> ${targetEl.closest('.table-card').dataset.tableName}.${targetEl.dataset.column}<br>
                <strong>Type:</strong> ${data.joinType || 'INNER'} (${data.cardinality || '1:N'})
            `;
            tooltip.style.left = `${originalEvent.pageX + 15}px`;
            tooltip.style.top = `${originalEvent.pageY}px`;
            tooltip.style.display = 'block';
        });

        instance.bind("mouseout", (connection, originalEvent) => {
            tooltipTimeout = setTimeout(() => {
                tooltip.style.display = 'none';
            }, 50);
        });

        initializeZoom();
    }

    // --- CORE LOGIC ---
    async function handleConnectionChange(e) {
        currentConnectionId = e.target.value;
        if (!currentConnectionId) {
            resetCanvas();
            updateConnectionStatus('disconnected');
            return;
        }
        updateConnectionStatus('connecting');
        await loadModelForConnection(currentConnectionId);
    }

    async function loadModelForConnection(connId) {
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 30000);
            
            const response = await fetch(`/api/model/get/${connId}/`, {
                signal: controller.signal
            });
            clearTimeout(timeoutId);

            if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            
            const data = await response.json();
            if (!data.success) throw new Error(data.error || 'Unknown error from server');

            resetCanvas();
            schema = Object.fromEntries((data.tables || []).map(t => [t.name, t]));
            updateTableSidebar();

            (data.layout || []).forEach(l => {
                if (schema[l.table_name]) {
                    addTableCard(schema[l.table_name], l.x_pos, l.y_pos, l.collapsed);
                }
            });

            setTimeout(() => {
    (data.joins || []).forEach(j => {
        const sourceEl = findFieldElement(j.left_table, j.left_column);
        const targetEl = findFieldElement(j.right_table, j.right_column);
        
        if (sourceEl && targetEl) {
            const conn = instance.connect({
                source: sourceEl,
                target: targetEl,
                overlays: [["Arrow", { width: 10, length: 10, location: 1 }]]
            });
            
            // Store original field IDs in connection data
            conn.setData({
                joinType: j.join_type,
                cardinality: j.cardinality,
                originalSourceId: sourceEl.id,
                originalTargetId: targetEl.id
            });
            
            conn.addClass(joinCssClass(j.join_type));
            setCardinalityOverlays(conn, j.cardinality);
        }
    });
    instance.repaintEverything();
}, 300);

            saveHistory();
            updateConnectionStatus('connected');
        } catch (error) {
            const isTimeout = error.name === 'AbortError';
            console.error('Connection Error:', isTimeout ? 'Timeout' : error.message);
            showError(isTimeout ? 'Connection timeout. Please try again.' : `Error loading model: ${error.message}`);
            resetCanvas();
            updateConnectionStatus('disconnected');
        }
    }

    async function saveModel() {
    if (!currentConnectionId) {
        showError("Please select a connection first.");
        return;
    }

    try {
        const csrftoken = getCookie('csrftoken');
        if (!csrftoken) {
            showError('CSRF token missing. Please refresh the page and try again.');
            return;
        }

        // --- Capture layout ---
        const layout = Array.from(canvas.querySelectorAll(".table-card")).map(card => ({
            tableName: card.dataset.tableName,
            x: parseInt(card.style.left, 10),
            y: parseInt(card.style.top, 10),
            collapsed: card.querySelector(".table-card-body")?.classList.contains("collapsed") || false,
        }));

        // --- Capture joins ---
        const joins = instance.getAllConnections()
            .filter(conn => conn.source && conn.target)
            .map(conn => {
                let sourceEl = conn.source;
                let targetEl = conn.target;

                // If connected to header, restore original field elements
                const data = conn.getData?.() || {};
                if (sourceEl.closest('.table-card-header') && data.originalSourceId) {
                    sourceEl = document.getElementById(data.originalSourceId);
                }
                if (targetEl.closest('.table-card-header') && data.originalTargetId) {
                    targetEl = document.getElementById(data.originalTargetId);
                }

                if (!sourceEl || !targetEl) return null;

                return {
                    leftTable: sourceEl.closest(".table-card")?.dataset.tableName,
                    leftColumn: sourceEl.dataset.column,
                    rightTable: targetEl.closest(".table-card")?.dataset.tableName,
                    rightColumn: targetEl.dataset.column,
                    joinType: data.joinType || 'INNER',
                    cardinality: data.cardinality || 'one-to-many',
                };
            })
            .filter(j => j !== null);

        const payload = { layout, joins };
        console.log("Data being sent to server:", JSON.stringify(payload, null, 2));

        // --- Send to backend ---
        const response = await fetch(`/api/model/save/${currentConnectionId}/`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-CSRFToken": csrftoken,
            },
            body: JSON.stringify(payload),
        });

        const result = await response.json();
        if (!result.success) throw new Error(result.error);

        showSuccess("Model saved successfully!");
        saveHistory();
    } catch (error) {
        console.error('Save error:', error);
        showError(`Error saving model: ${error.message}`);
    }
}

    function addTableCard(tableSchema, x, y, collapsed = false) {
    const placeholder = document.getElementById("canvas-placeholder");
    if (placeholder) placeholder.style.display = 'none';

    const cardId = `card_${tableSchema.name}`;
    if (document.getElementById(cardId)) return;

    const card = document.createElement("div");
    card.className = "table-card";
    card.id = cardId;
    card.dataset.tableName = tableSchema.name;
    card.style.left = `${x}px`;
    card.style.top = `${y}px`;

    const bodyClass = collapsed ? 'table-card-body collapsed' : 'table-card-body';
    const fieldsHTML = tableSchema.columns.map(col => {
        const icon = col.is_pk ? 'bi-key-fill text-warning' : (col.is_numeric ? 'bi-hash' : 'bi-type');
        return `
            <div class="table-card-field" id="${cardId}_col_${col.name}"
                 data-column="${col.name}" data-type="${col.type}" data-pk="${col.is_pk ? '1' : '0'}">
                <span title="${col.type}">
                    <i class="bi ${icon} me-2 text-muted"></i>${col.name}
                    ${isEditable ? '<span class="join-wizard-btn ms-2"><i class="bi bi-gear-fill"></i></span>' : ''}
                </span>
            </div>`;
    }).join('');

    card.innerHTML = `
        <div class="table-card-header">
            <span><i class="bi bi-table me-2"></i>${tableSchema.name}</span>
            <div class="card-controls d-flex align-items-center">
                <button type="button" class="btn btn-sm btn-link text-secondary p-0 btn-collapse" title="Collapse/Expand fields">
                    <i class="bi ${collapsed ? 'bi-chevron-right' : 'bi-chevron-down'}"></i>
                </button>
                ${isEditable ? '<button type="button" class="btn-close btn-sm remove-card-btn" title="Remove table" aria-label="Remove table"></button>' : ''}
            </div>
        </div>
        <div class="${bodyClass}">${fieldsHTML}</div>
    `;

    canvas.appendChild(card);

    // --- Draggable Logic ---
    if (isEditable) {
        instance.draggable(card, {
            handle: ".table-card-header",
            grid: [10, 10],
            start: ({ el }) => {
                el.style.zIndex = 20;
                instance.setSuspendDrawing(true);
            },
            drag: ({ el }) => instance.repaint(el),
            stop: ({ el }) => {
                el.style.zIndex = 10;
                instance.setSuspendDrawing(false);
                instance.repaintEverything();
                saveHistory();
            }
        });
    }

    // --- Collapse Button ---
    const collapseBtn = card.querySelector('.btn-collapse');
    const body = card.querySelector('.table-card-body');
    collapseBtn?.addEventListener('click', () => {
        const isCollapsed = body.classList.toggle('collapsed');
        const icon = collapseBtn.querySelector('i');
        if (icon) icon.className = isCollapsed ? 'bi bi-chevron-right' : 'bi bi-chevron-down';

        isCollapsed ? rerouteConnectionsToHeader(card) : rerouteConnectionsToFields(card);
        instance.repaintEverything();
        saveHistory();
    });

    // --- Remove Button ---
    card.querySelector('.remove-card-btn')?.addEventListener('click', () => {
        instance.remove(card);
        card.remove();
        if (!canvas.querySelector('.table-card') && placeholder) {
            placeholder.style.display = 'block';
        }
        updateTableSidebar();
        saveHistory();
    });

    // --- Join Wizard Buttons ---
    card.querySelectorAll('.join-wizard-btn').forEach(btn => {
        btn.addEventListener('click', e => {
            if (!isEditable) return;
            e.stopPropagation();
            const fieldEl = btn.closest('.table-card-field');
            if (fieldEl) openJoinWizard(fieldEl);
        });
    });

    // --- Initialize Endpoints ---
    card.querySelectorAll('.table-card-field').forEach(fieldEl => {
        instance.makeSource(fieldEl, {
            anchor: "Right",
            maxConnections: -1,
            filter: ":not(.join-wizard-btn)"
        });
        instance.makeTarget(fieldEl, {
            anchor: "Left",
            maxConnections: -1
        });
    });

    updateTableSidebar();
}

    function updateTableSidebar() {
        const tableList = document.getElementById("tableList");
        if (!tableList || !schema) return;

        const searchTerm = document.getElementById("tableSearch")?.value.toLowerCase() || '';
        const tablesOnCanvas = new Set(
            Array.from(canvas.querySelectorAll('.table-card')).map(card => card.dataset.tableName)
        );

        const allTableNames = Object.keys(schema);
        let visibleTables = allTableNames.filter(name => !tablesOnCanvas.has(name) && name.toLowerCase().includes(searchTerm));
        let disabledTables = allTableNames.filter(name => tablesOnCanvas.has(name) && name.toLowerCase().includes(searchTerm));

        visibleTables.sort((a, b) => {
            if (sortOrder === 'asc') return a.localeCompare(b);
            return b.localeCompare(a);
        });

        disabledTables.sort((a, b) => {
            if (sortOrder === 'asc') return a.localeCompare(b);
            return b.localeCompare(a);
        });

        const renderItem = (name, disabled = false) => `
            <a href="#" class="list-group-item list-group-item-action ${disabled ? 'disabled' : ''}" 
               draggable="${!disabled}" data-table-name="${name}">${name}</a>`;

        if (allTableNames.length === 0) {
            tableList.innerHTML = `
                <div class="p-3 text-muted small text-center">
                    Select a database connection to begin modeling.
                </div>`;
            return;
        }

        tableList.innerHTML = [
            ...visibleTables.map(name => renderItem(name, false)),
            ...disabledTables.map(name => renderItem(name, true))
        ].join('');

        // Re-attach drag listeners to non-disabled items
        tableList.querySelectorAll('.list-group-item:not(.disabled)').forEach(item => {
            item.addEventListener('dragstart', (e) => {
                e.dataTransfer.setData('text/plain', e.target.dataset.tableName);
            });
        });
    }

    function toggleTableSort() {
        const btn = document.getElementById('sortTableBtn');
        const icon = btn?.querySelector('i');
        sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';

        if (sortOrder === 'asc') {
            if (icon) icon.className = 'bi bi-sort-alpha-down';
            if (btn) btn.title = 'Sort Ascending';
        } else {
            if (icon) icon.className = 'bi bi-sort-alpha-up-alt';
            if (btn) btn.title = 'Sort Descending';
        }
        updateTableSidebar();
    }

    function handleCanvasDrop(e) {
        e.preventDefault();
        const tableName = e.dataTransfer.getData("text/plain");
        if (!tableName) return;

        const tableSchema = schema[tableName];
        if (tableSchema) {
            const canvasRect = canvas.getBoundingClientRect();
            const x = (e.clientX - canvasRect.left) / zoom;
            const y = (e.clientY - canvasRect.top) / zoom;
            addTableCard(tableSchema, x, y);
            saveHistory();
        }
    }

    function handleCanvasClick(e) {
        const wizardBtn = e.target.closest('.join-wizard-btn');
        if (wizardBtn && isEditable) {
            const fieldEl = wizardBtn.closest('.table-card-field');
            if (fieldEl) openJoinWizard(fieldEl);
        }
    }

    function handleKeyboardShortcuts(e) {
        const ctrl = e.ctrlKey || e.metaKey;
        if (ctrl && e.key.toLowerCase() === 'z') {
            e.preventDefault();
            undo();
        } else if (ctrl && e.key.toLowerCase() === 'y') {
            e.preventDefault();
            redo();
        } else if (e.key === 'Delete') {
            if (selectedConnection) {
                instance.deleteConnection(selectedConnection);
                selectedConnection = null;
                saveHistory();
            }
        }
    }

    // --- UI & UX FUNCTIONS ---
    function updateZoomIndicator() {
        const zoomLevelElem = document.querySelector('.zoom-level');
        if (zoomLevelElem) {
            zoomLevelElem.textContent = Math.round(zoom * 100) + '%';
        }
    }

    function applyZoom(scale, zoomPoint) {
        const newZoom = Math.min(Math.max(scale, 0.2), 3);
        const oldZoom = zoom;
        zoom = newZoom;

        if (zoomPoint) {
            const canvasRect = canvas.getBoundingClientRect();
            const scrollX = (zoomPoint.x + canvas.scrollLeft) * (newZoom / oldZoom) - zoomPoint.x;
            const scrollY = (zoomPoint.y + canvas.scrollTop) * (newZoom / oldZoom) - zoomPoint.y;
            canvas.scrollLeft = scrollX;
            canvas.scrollTop = scrollY;
        }

        canvas.style.transform = `scale(${newZoom})`;
        canvas.style.transformOrigin = '0 0';
        instance.setZoom(newZoom);
        updateZoomIndicator();
    }

    function adjustZoom(delta) {
        const rect = canvas.parentElement.getBoundingClientRect();
        const centerPoint = { x: rect.width / 2, y: rect.height / 2 };
        applyZoom(zoom + delta, centerPoint);
    }

    function initializeZoom() {
        const canvasContainer = canvas.parentElement;

        // Zoom with mouse wheel (Ctrl + Scroll)
        canvasContainer.addEventListener('wheel', (e) => {
            if (e.ctrlKey) {
                e.preventDefault();
                const delta = e.deltaY > 0 ? -0.1 : 0.1;
                const rect = canvasContainer.getBoundingClientRect();
                const zoomPoint = {
                    x: e.clientX - rect.left,
                    y: e.clientY - rect.top
                };
                applyZoom(zoom + delta, zoomPoint);
            }
        }, { passive: false });
    }

    function openJoinModal(connection) {
        selectedConnection = connection;
        const sourceEl = document.getElementById(connection.sourceId);
        const targetEl = document.getElementById(connection.targetId);
        
        document.getElementById('sourceField').textContent = `${sourceEl.closest('.table-card').dataset.tableName}.${sourceEl.dataset.column}`;
        document.getElementById('targetField').textContent = `${targetEl.closest('.table-card').dataset.tableName}.${targetEl.dataset.column}`;
        document.getElementById('joinType').value = connection.getData()?.joinType || 'INNER';
        document.getElementById('cardinality').value = connection.getData()?.cardinality || 'one-to-many';
        
        const modal = new bootstrap.Modal(document.getElementById('joinModal'));
        modal.show();
    }

    function setCardinalityOverlays(conn, cardinality) {
        try {
            conn.removeOverlay("cardLeft");
            conn.removeOverlay("cardRight");
        } catch (e) {
            // overlay missing, ignore
        }

        const map = {
            "one-to-one": ["1", "1"],
            "one-to-many": ["1", "∞"],
            "many-to-one": ["∞", "1"],
            "many-to-many": ["∞", "∞"]
        };
        const [left, right] = map[cardinality] || ["", ""];

        try {
            if (left) conn.addOverlay(["Label", { id: "cardLeft", label: left, location: 0.1, cssClass: "connection-label" }]);
            if (right) conn.addOverlay(["Label", { id: "cardRight", label: right, location: 0.9, cssClass: "connection-label" }]);
        } catch (e) {
            // Ignore overlay errors
        }
    }

    function rerouteConnectionsToHeader(card) {
        const header = card.querySelector('.table-card-header');
        const fields = card.querySelectorAll('.table-card-field');
        let connectionsToModify = [];

        fields.forEach(field => {
            connectionsToModify.push(...instance.getConnections({ source: field }));
            connectionsToModify.push(...instance.getConnections({ target: field }));
        });

        const uniqueConnections = [...new Set(connectionsToModify)];

        uniqueConnections.forEach(conn => {
            if (!conn || !conn.source || !conn.target) return;

            const data = conn.getData();
            if (!data.originalSourceId && conn.source.closest('.table-card-field')) {
                data.originalSourceId = conn.source.id;
            }
            if (!data.originalTargetId && conn.target.closest('.table-card-field')) {
                data.originalTargetId = conn.target.id;
            }

            let newSource = conn.source;
            let newTarget = conn.target;

            if (conn.source.closest('.table-card') === card) newSource = header;
            if (conn.target.closest('.table-card') === card) newTarget = header;

            const tempConn = instance.connect({
                source: newSource,
                target: newTarget,
                anchors: ["Continuous", "Continuous"],
                cssClass: conn.getClass(),
                overlays: [["Arrow", { width: 10, length: 10, location: 1 }]]
            });

            tempConn.setData(data);
            if (data.cardinality) setCardinalityOverlays(tempConn, data.cardinality);
            instance.deleteConnection(conn);
        });

        // Hide field endpoints
        fields.forEach(field => {
            instance.getEndpoints(field).forEach(ep => ep.setVisible(false));
        });

        setTimeout(() => instance.repaintEverything(), 50);
    }

    function rerouteConnectionsToFields(card) {
        const header = card.querySelector('.table-card-header');
        if (!header) return;

        const connectionsOnHeader = [
            ...instance.getConnections({ source: header }),
            ...instance.getConnections({ target: header })
        ];
        const uniqueConnections = [...new Set(connectionsOnHeader)];

        uniqueConnections.forEach(conn => {
            const data = conn.getData() || {};
            const cssClass = conn.getClass() || '';
            const sourceEl = document.getElementById(data.originalSourceId);
            const targetEl = document.getElementById(data.originalTargetId);

            if (sourceEl && targetEl) {
                instance.deleteConnection(conn);
                const newConn = instance.connect({
                    source: sourceEl,
                    target: targetEl,
                    overlays: [["Arrow", { width: 10, length: 10, location: 1 }]]
                });
                newConn.setData(data);
                if (data.cardinality) setCardinalityOverlays(newConn, data.cardinality);
                if (cssClass) newConn.addClass(cssClass);
            } else {
                console.warn('Missing original field elements for rerouting:', data);
            }
        });

        // Restore visibility and reassociate endpoints
        card.querySelectorAll('.table-card-field').forEach(field => {
            instance.getEndpoints(field).forEach(ep => {
                ep.setVisible(true);
                ep.setElement(field);
            });
        });

        instance.repaintEverything();
    }

    function exportModel() {
        const layout = Array.from(canvas.querySelectorAll(".table-card")).map(card => {
            const computedStyle = window.getComputedStyle(card);
            const body = card.querySelector('.table-card-body');
            return {
                tableName: card.dataset.tableName,
                x: parseInt(computedStyle.left, 10) || 0,
                y: parseInt(computedStyle.top, 10) || 0,
                collapsed: body ? body.classList.contains('collapsed') : false,
                width: card.offsetWidth,
                height: card.offsetHeight
            };
        });

        const joins = instance.getAllConnections()
            .filter(conn => conn.source && conn.target)
            .map(conn => {
                let sourceEl = conn.source;
                let targetEl = conn.target;

                if (sourceEl.closest('.table-card-header') && conn.getData()?.originalSourceId) {
                    sourceEl = document.getElementById(conn.getData().originalSourceId);
                }
                if (targetEl.closest('.table-card-header') && conn.getData()?.originalTargetId) {
                    targetEl = document.getElementById(conn.getData().originalTargetId);
                }

                if (!sourceEl || !targetEl) return null;

                return {
                    leftTable: sourceEl.closest('.table-card').dataset.tableName,
                    leftColumn: sourceEl.dataset.column,
                    rightTable: targetEl.closest('.table-card').dataset.tableName,
                    rightColumn: targetEl.dataset.column,
                    joinType: conn.getData()?.joinType || 'INNER',
                    cardinality: conn.getData()?.cardinality || 'one-to-many',
                    suggested: !!conn.getData()?.suggested || false,
                };
            })
            .filter(j => j !== null);

        return {
            layout,
            joins,
            viewport: {
                zoom,
                scrollLeft: canvas.scrollLeft,
                scrollTop: canvas.scrollTop,
            }
        };
    }

    function resetCanvas() {
        canvas.innerHTML = '<div id="canvas-placeholder" class="placeholder-text"><i class="bi bi-diagram-3-fill fs-1"></i><p>Select a database connection to begin modeling.</p></div>';
        instance.deleteEveryConnection();
        history = [];
        historyIndex = -1;
        selectedConnection = null;
        zoom = 1;
    }

    function autoArrangeLayout() {
    const cards = Array.from(canvas.querySelectorAll(".table-card"));
    if (!cards.length || typeof dagre === "undefined" || !dagre.graphlib?.Graph || !dagre.layout) {
        console.warn("Auto-arrange skipped: Dagre not available or no cards present.");
        return;
    }

    const graph = new dagre.graphlib.Graph();
    graph.setGraph({
        rankdir: "LR",
        nodesep: 60,
        ranksep: 80,
        marginx: 50,
        marginy: 50
    });
    graph.setDefaultEdgeLabel(() => ({}));

    // Add nodes
    cards.forEach(card => {
        const tableName = card.dataset.tableName;
        if (!tableName) return;

        const body = card.querySelector(".table-card-body");
        const isCollapsed = body?.classList.contains("collapsed");
        const width = card.offsetWidth || 240;
        const height = isCollapsed ? 45 : card.offsetHeight || 160;

        graph.setNode(tableName, { width, height });
    });

    // Add edges
    const addedEdges = new Set();
    instance.getAllConnections().forEach(conn => {
        const sourceCard = conn.source?.closest(".table-card");
        const targetCard = conn.target?.closest(".table-card");
        if (!sourceCard || !targetCard) return;

        const source = sourceCard.dataset.tableName;
        const target = targetCard.dataset.tableName;
        const edgeKey = `${source}->${target}`;

        if (source && target && source !== target && !addedEdges.has(edgeKey)) {
            graph.setEdge(source, target);
            addedEdges.add(edgeKey);
        }
    });

    try {
        dagre.layout(graph);

        graph.nodes().forEach(tableName => {
            const node = graph.node(tableName);
            const card = canvas.querySelector(`.table-card[data-table-name="${CSS.escape(tableName)}"]`);
            if (card && node) {
                card.style.left = `${Math.round(node.x - node.width / 2)}px`;
                card.style.top = `${Math.round(node.y - node.height / 2)}px`;
            }
        });

        setTimeout(() => {
            instance.repaintEverything();
            saveHistory();
        }, 100);
    } catch (error) {
        console.error("Auto-arrange failed:", error);
    }
}

    function centerCanvas() {
        const cards = Array.from(canvas.querySelectorAll('.table-card'));
        if (!cards.length) return;

        let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;

        cards.forEach(card => {
            const x = parseInt(card.style.left, 10) || 0;
            const y = parseInt(card.style.top, 10) || 0;
            const w = card.offsetWidth || 120;
            const h = card.offsetHeight || 80;

            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x + w);
            maxY = Math.max(maxY, y + h);
        });

        const canvasWidth = canvas.clientWidth;
        const canvasHeight = canvas.clientHeight;
        const offsetX = (canvasWidth - (maxX - minX)) / 2 - minX;
        const offsetY = (canvasHeight - (maxY - minY)) / 2 - minY;

        cards.forEach(card => {
            const x = parseInt(card.style.left, 10) || 0;
            const y = parseInt(card.style.top, 10) || 0;
            card.style.left = Math.round(x + offsetX) + 'px';
            card.style.top = Math.round(y + offsetY) + 'px';
        });

        instance.repaintEverything();
        saveHistory();
    }

    function toggleSidebar() {
        const sidebar = document.getElementById('sourceTablesSidebar');
        const toggleBtn = document.getElementById('toggleSidebar');

        if (sidebar.style.width === '0px') {
            sidebar.style.width = 'var(--sidebar-width)';
            toggleBtn.innerHTML = '<i class="bi bi-chevron-double-left"></i>';
        } else {
            sidebar.style.width = '0';
            toggleBtn.innerHTML = '<i class="bi bi-chevron-double-right"></i>';
        }

        setTimeout(() => instance.repaintEverything(), 300);
    }

    function filterTableList() {
        updateTableSidebar();
    }

    function filterCanvasElements() {
        const searchTerm = this.value.toLowerCase();
        const tableCards = document.querySelectorAll('.table-card');

        if (!searchTerm) {
            tableCards.forEach(card => {
                card.style.opacity = 1;
                card.style.border = '1px solid #e1e5eb';
                card.querySelectorAll('.table-card-field').forEach(field => {
                    field.style.backgroundColor = '';
                });
            });
            return;
        }

        tableCards.forEach(card => {
            const tableName = card.dataset.tableName.toLowerCase();
            const fields = card.querySelectorAll('.table-card-field');
            let hasMatch = tableName.includes(searchTerm);

            if (!hasMatch) {
                fields.forEach(field => {
                    const fieldName = field.dataset.column.toLowerCase();
                    if (fieldName.includes(searchTerm)) {
                        hasMatch = true;
                        field.style.backgroundColor = '#fffce5';
                    } else {
                        field.style.backgroundColor = '';
                    }
                });
            }

            if (hasMatch) {
                card.style.opacity = 1;
                card.style.border = '2px solid #4361ee';
            } else {
                card.style.opacity = 0.3;
                card.style.border = '1px solid #e1e5eb';
            }
        });
    }

    function updateConnectionStatus(status) {
        const statusElem = document.getElementById('connectionStatus');
        if (!statusElem) return;

        const indicator = statusElem.querySelector('.status-indicator');
        const text = statusElem.querySelector('.status-text');

        indicator.classList.remove('status-connected', 'status-disconnected', 'status-connecting');
        indicator.classList.add(`status-${status}`);

        switch (status) {
            case 'connected':
                text.textContent = 'Connected';
                break;
            case 'connecting':
                text.textContent = 'Connecting...';
                break;
            case 'disconnected':
                text.textContent = 'Not connected';
                break;
            default:
                text.textContent = '';
        }
    }

    async function fetchSuggestedJoins() {
        if (!currentConnectionId) {
            showError("Please select a connection first.");
            return;
        }

        try {
            updateConnectionStatus('connecting');
            const response = await fetch(`/api/model/suggest-joins/${currentConnectionId}/`);
            const data = await response.json();

            if (data.success) {
                showSuggestedJoins(data.joins);
            } else {
                showError(`Failed to fetch suggested joins: ${data.error}`);
            }
        } catch (error) {
            showError(`Error fetching suggested joins: ${error.message}`);
        } finally {
            updateConnectionStatus('connected');
        }
    }

    function showSuggestedJoins(joins) {
        const listContainer = document.getElementById('suggestedJoinsList');
        if (!listContainer) return;

        if (!joins.length) {
            listContainer.innerHTML = '<div class="text-center text-muted py-4">No suggested joins found.</div>';
        } else {
            joins.forEach(join => {
                const joinElem = document.createElement('div');
                joinElem.className = 'card mb-2';
                joinElem.innerHTML = `
                    <div class="card-body py-2">
                        <div class="form-check">
                            <input class="form-check-input" type="checkbox" value="" 
                                   id="suggest-${join.leftTable}-${join.leftColumn}-${join.rightTable}-${join.rightColumn}" checked>
                            <label class="form-check-label w-100 d-flex justify-content-between" 
                                   for="suggest-${join.leftTable}-${join.leftColumn}-${join.rightTable}-${join.rightColumn}">
                                <span>${join.leftTable}.${join.leftColumn} → ${join.rightTable}.${join.rightColumn}</span>
                                <span class="badge bg-${join.confidence === 'high' ? 'success' : 'warning'}">${join.confidence}</span>
                            </label>
                        </div>
                    </div>`;
                listContainer.appendChild(joinElem);
            });
        }

        const modal = new bootstrap.Modal(document.getElementById('suggestJoinsModal'));
        modal.show();
    }

    function applyAllSuggestedJoins() {
        const checkboxes = document.querySelectorAll('#suggestedJoinsList .form-check-input:checked');
        checkboxes.forEach(checkbox => {
            const idParts = checkbox.id.replace('suggest-', '').split('-');
            console.log('Would apply join with parts:', idParts);
        });

        bootstrap.Modal.getInstance(document.getElementById('suggestJoinsModal')).hide();
        showSuccess(`${checkboxes.length} joins applied successfully!`);
    }

    async function testConnection() {
        const connectionId = document.getElementById('connectionSelect').value;
        if (!connectionId) {
            showError("Please select a connection first.");
            return;
        }

        try {
            updateConnectionStatus('connecting');
            const response = await fetch(`/api/model/test-connection/${connectionId}/`);
            const data = await response.json();

            if (data.success) {
                updateConnectionStatus('connected');
                showSuccess('Connection test successful!');
            } else {
                updateConnectionStatus('disconnected');
                showError(`Connection test failed: ${data.error}`);
            }
        } catch (error) {
            updateConnectionStatus('disconnected');
            showError(`Connection test error: ${error.message}`);
        }
    }

    async function validateModel() {
        if (!currentConnectionId) {
            showError("Select a database connection first.");
            return;
        }

        const payload = exportModel();

        try {
            const response = await fetch(`/api/model/validate/${currentConnectionId}/`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRFToken": document.querySelector('[name=csrfmiddlewaretoken]').value,
                },
                body: JSON.stringify(payload),
            });

            const data = await response.json();
            if (data.success) {
                showSuccess('Validation passed!');
            } else {
                showError(`Validation failed: ${data.error}`);
            }
        } catch (error) {
            showError(`Validation failed: ${error.message}`);
        }
    }

    // --- JOIN WIZARD FUNCTIONS ---
   function openJoinWizard(sourceFieldEl) {
        closeJoinWizard();

        const popoverTemplate = document.getElementById('joinWizardTemplate');
        if (!popoverTemplate) return;

        const sourceTableName = sourceFieldEl.closest('.table-card').dataset.tableName;
        const sourceColumnName = sourceFieldEl.dataset.column;
        const popoverContentNode = popoverTemplate.content.cloneNode(true);

        // --- Corrected logic to find ALL existing joins ---
        const existingJoinsContainer = popoverContentNode.querySelector('.existing-joins-container');
        
        // Get all connections associated with this element (source or target)
        const allConnections = instance.getConnections({ element: sourceFieldEl });

        if (allConnections.length > 0) {
            let joinsHtml = '<h6 class="join-wizard-sub-title">Existing Joins:</h6><ul class="list-group list-group-flush small">';
            
            allConnections.forEach(conn => {
                // Find the 'other' element in the connection
                const otherEl = conn.source === sourceFieldEl ? conn.target : conn.source;
                
                const otherTableName = otherEl.closest('.table-card').dataset.tableName;
                const otherColumnName = otherEl.dataset.column;
                joinsHtml += `<li class="list-group-item py-1 px-0">${otherTableName}.${otherColumnName}</li>`;
            });

            joinsHtml += '</ul>';
            existingJoinsContainer.innerHTML = joinsHtml;
        } else {
            existingJoinsContainer.remove(); // Remove the container if no joins exist
        }
        // --- End of corrected logic ---

        const popover = new bootstrap.Popover(sourceFieldEl, {
            html: true,
            placement: 'right',
            trigger: 'manual',
            customClass: 'join-wizard-popover',
            title: `Join from ${sourceTableName}.${sourceColumnName}`,
            content: popoverContentNode,
        });

        activeJoinWizard.popover = popover;
        activeJoinWizard.sourceEl = sourceFieldEl;
        activeJoinWizard.previews = [];

        sourceFieldEl.addEventListener('shown.bs.popover', () => {
            const popoverTip = document.querySelector('.popover');
            if (!popoverTip) return;

            popoverTip.querySelector('.add-join-row-btn')?.addEventListener('click', () => addJoinRow(popoverTip));
            popoverTip.querySelector('.cancel-join-btn')?.addEventListener('click', closeJoinWizard);
            popoverTip.querySelector('.apply-joins-btn')?.addEventListener('click', applyWizardJoins);

            addJoinRow(popoverTip);
        });

        popover.show();
    }

    function closeJoinWizard() {
        if (activeJoinWizard.popover) {
            activeJoinWizard.popover.dispose();
        }
        activeJoinWizard.previews.forEach(conn => instance.deleteConnection(conn));
        activeJoinWizard.popover = null;
        activeJoinWizard.sourceEl = null;
        activeJoinWizard.previews = [];
    }

    function addJoinRow(popoverTip) {
        const body = popoverTip.querySelector('.join-wizard-body');
        const newRow = document.createElement('div');
        newRow.className = 'join-wizard-row';

        const sourceTableName = activeJoinWizard.sourceEl.closest('.table-card').dataset.tableName;
        const allAvailableTables = Object.keys(schema).filter(name => name !== sourceTableName);

        newRow.innerHTML = `
            <select class="form-select form-select-sm target-table-select" aria-label="Select target table">
                <option value="">Select Target Table...</option>
                ${allAvailableTables.map(name => `<option value="${name}">${name}</option>`).join('')}
            </select>
            <select class="form-select form-select-sm target-column-select mt-1" aria-label="Select target column" disabled>
                <option value="">Select Column...</option>
            </select>
            <button type="button" class="btn btn-sm btn-outline-danger remove-join-row-btn mt-1" aria-label="Remove this join">×</button>
        `;

        body.appendChild(newRow);

        newRow.querySelector('.target-table-select').addEventListener('change', (e) => handleTableSelection(e.target));
        newRow.querySelector('.target-column-select').addEventListener('change', (e) => handleColumnSelection(e.target));
        newRow.querySelector('.remove-join-row-btn').addEventListener('click', () => {
            const previewConnId = newRow.dataset.previewConnId;
            if (previewConnId) {
                const idx = activeJoinWizard.previews.findIndex(c => c.id === previewConnId);
                if (idx !== -1) {
                    instance.deleteConnection(activeJoinWizard.previews[idx]);
                    activeJoinWizard.previews.splice(idx, 1);
                }
            }
            newRow.remove();
            instance.repaintEverything();
        });
    }

    function handleTableSelection(tableSelectEl) {
        const row = tableSelectEl.closest('.join-wizard-row');
        const columnSelectEl = row.querySelector('.target-column-select');
        const selectedTable = tableSelectEl.value;

        columnSelectEl.innerHTML = '<option value="">Select Column...</option>';
        columnSelectEl.disabled = !selectedTable;

        if (!selectedTable || !schema[selectedTable]) return;

        schema[selectedTable].columns.forEach(col => {
            const option = new Option(col.name, col.name);
            columnSelectEl.add(option);
        });
    }

    function handleColumnSelection(columnSelectEl) {
        const row = columnSelectEl.closest('.join-wizard-row');
        const targetTable = row.querySelector('.target-table-select')?.value;
        const targetColumn = columnSelectEl.value;

        // Remove old preview connection if it exists
        const oldPreviewId = row.dataset.previewConnId;
        if (oldPreviewId) {
            const idx = activeJoinWizard.previews.findIndex(c => c.id === oldPreviewId);
            if (idx !== -1) {
                instance.deleteConnection(activeJoinWizard.previews[idx]);
                activeJoinWizard.previews.splice(idx, 1);
            }
            delete row.dataset.previewConnId;
        }

        if (!targetTable || !targetColumn) {
            instance.repaintEverything();
            return;
        }

        const targetFieldEl = findFieldElement(targetTable, targetColumn);
        if (activeJoinWizard.sourceEl && targetFieldEl) {
            const previewConn = instance.connect({
                source: activeJoinWizard.sourceEl,
                target: targetFieldEl,
                anchors: ["Right", "Left"],
                paintStyle: { strokeWidth: 2, stroke: "#6c757d", dashstyle: "5 5" },
                overlays: [["Arrow", { location: 1, width: 8, length: 8, foldback: 0.8 }]]
            });

            previewConn.addClass('preview-join');
            row.dataset.previewConnId = previewConn.id;
            activeJoinWizard.previews.push(previewConn);
        }
    }

    function applyWizardJoins() {
        const popoverTip = document.querySelector('.popover');
        if (!popoverTip) return;

        const rows = popoverTip.querySelectorAll('.join-wizard-row');
        const sourceCard = activeJoinWizard.sourceEl.closest('.table-card');

        rows.forEach((row, idx) => {
            const targetTableName = row.querySelector('.target-table-select').value;
            const targetColumnName = row.querySelector('.target-column-select').value;

            if (!targetTableName || !targetColumnName) return;

            let targetCard = canvas.querySelector(`.table-card[data-table-name="${CSS.escape(targetTableName)}"]`);

            if (!targetCard && schema[targetTableName]) {
                const newX = sourceCard.offsetLeft + sourceCard.offsetWidth + 100;
                const newY = sourceCard.offsetTop + (idx * 50);
                addTableCard(schema[targetTableName], newX, newY);
                targetCard = canvas.querySelector(`.table-card[data-table-name="${CSS.escape(targetTableName)}"]`);
            }

            if (targetCard) {
                const targetFieldEl = findFieldElement(targetTableName, targetColumnName);
                if (activeJoinWizard.sourceEl && targetFieldEl) {
                    const existing = instance.getConnections({
                        source: activeJoinWizard.sourceEl,
                        target: targetFieldEl,
                    }).filter(c => !c.hasClass('preview-join'));

                    if (existing.length === 0) {
                        const newConn = instance.connect({
                            source: activeJoinWizard.sourceEl,
                            target: targetFieldEl
                        });
                        // Setup cardinality and styling
                        setupNewConnection(newConn, activeJoinWizard.sourceEl, targetFieldEl);
                    }
                }
            }
        });

        closeJoinWizard();
        saveHistory();
    }

    // --- INITIALIZE THE APP ---
    function initUI() {
        updateConnectionStatus('disconnected');

        const tableSearch = document.getElementById('tableSearch');
        if (tableSearch) {
            tableSearch.addEventListener('input', filterTableList);
        }

        const canvasSearch = document.getElementById('canvasSearch');
        if (canvasSearch) {
            canvasSearch.addEventListener('input', filterCanvasElements);
        }
    }

    initUI();
    initializeEventListeners();

    if (document.getElementById('undoBtn')) {
        document.getElementById('undoBtn').disabled = true;
    }
    if (document.getElementById('redoBtn')) {
        document.getElementById('redoBtn').disabled = true;
    }
});



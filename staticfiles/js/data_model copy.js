let container = null;
let isEditable = false;
let miniMap = null;

document.addEventListener("DOMContentLoaded", () => {
  // --- Permission check - read from data attribute
  container = document.querySelector(".data-model-container");
  isEditable = true;
  applyPermissions(); // Initial call

  // --- DOM refs
  const connectionSelect = document.getElementById("connectionSelect");
  const testBtn = document.getElementById("testConnectionBtn");
  const statusEl = document.getElementById("connectionStatus");
  const tableList = document.getElementById("tableList");
  const tableSearch = document.getElementById("tableSearch");
  const sortTablesBtn = document.getElementById("sortTablesBtn");
  const canvas = document.getElementById("canvas");
  const canvasPlaceholder = document.getElementById("canvas-placeholder");
  const undoBtn = document.getElementById("undoBtn");
  const redoBtn = document.getElementById("redoBtn");
  const saveBtn = document.getElementById("saveModelBtn");
  const zoomInBtn = document.getElementById("zoomInBtn");
  const zoomOutBtn = document.getElementById("zoomOutBtn");
  const resetZoomBtn = document.getElementById("resetZoomBtn");
  const autoArrangeBtn = document.getElementById("autoArrangeBtn");
  const toggleAllBtn = document.getElementById("toggleAllBtn");
  const exportPngBtn = document.getElementById("exportPngBtn");
  const exportPdfBtn = document.getElementById("exportPdfBtn");
  const canvasSearch = document.getElementById("canvasSearch");
  const validateBtn = document.getElementById("validateBtn");
  const autoSuggestToggle = document.getElementById("autoSuggestToggle");
  const centerCanvasBtn = document.getElementById("centerCanvasBtn");
  const joinModalEl = document.getElementById("joinModal");
  const joinModal = joinModalEl ? new bootstrap.Modal(joinModalEl) : null;
  const sourceFieldEl = document.getElementById("sourceField");
  const targetFieldEl = document.getElementById("targetField");
  const joinTypeSel = document.getElementById("joinType");
  const cardinalitySel = document.getElementById("cardinality");
  const saveJoinBtn = document.getElementById("saveJoinBtn");
  const deleteJoinBtn = document.getElementById("deleteJoinBtn");
  const ctxMenu = document.getElementById("connContextMenu");
  const ctxEditJoin = document.getElementById("ctxEditJoin");
  const ctxConfirmSuggested = document.getElementById("ctxConfirmSuggested");
  const ctxDeleteJoin = document.getElementById("ctxDeleteJoin");
  const toggleMiniMapBtn = document.getElementById("toggleMiniMapBtn");

  if (toggleMiniMapBtn) {
    toggleMiniMapBtn.addEventListener("click", () => {
      if (!miniMap) {
        miniMap = createMiniMap(); // Create it on first click
      }
      miniMap.toggle(); // Toggle its visibility
    });
  }

  // --- State
  let history = [];
  let historyIndex = -1;
  let tableSortDirection = "asc";
  let lastSchema = null;
  let zoom = 1;
  let selectedConnection = null;
  let suggestPool = [];
  let activeJoinWizard = { popover: null, sourceEl: null, previews: [] };

  // --- jsPlumb setup
  const instance = jsPlumb.getInstance({
    Connector: [
      "Flowchart",
      // Change the stub value below from 20 to 50
      { stub: 50, cornerRadius: 5, alwaysRespectStubs: true },
    ],
    Anchors: ["Right", "Left"],
    PaintStyle: { strokeWidth: 2, stroke: "#5a67d8" },
    Endpoint: ["Dot", { radius: 4 }],
    EndpointStyle: { fill: "#5a67d8" },
    Container: canvas,
    DragOptions: {
      cursor: "pointer",
      zIndex: 2000,
      containment: true,
      grid: [5, 5],
    },
  });

  let repaintTimeout = null;
  function scheduleRepaint() {
    if (repaintTimeout) clearTimeout(repaintTimeout);
    repaintTimeout = setTimeout(() => {
      if (instance) {
        instance.repaintEverything();
      }
    }, 100);
  }

  // Helper to map join type -> css class
  function joinCssClass(type) {
    console.log("DEBUG - joinCssClass received:", type);
    const t = (type || "INNER").toUpperCase();
    if (t === "LEFT") {
      console.log("DEBUG - Returning: left-join");
      return "left-join";
    }
    if (t === "RIGHT") {
      console.log("DEBUG - Returning: right-join");
      return "right-join";
    }
    console.log("DEBUG - Returning: inner-join");
    return "inner-join";
  }

  // Add cardinality labels (1, *) on both ends
  function setCardinalityOverlays(conn, cardinality) {
    try {
      conn.removeOverlay("cardLeft");
      conn.removeOverlay("cardRight");
    } catch (e) {
      console.log("Could not remove overlays:", e);
    }

    const map = {
      "one-to-one": ["1", "1"],
      "one-to-many": ["1", "*"],
      "many-to-one": ["*", "1"],
      "many-to-many": ["*", "*"],
    };
    const [left, right] = map[cardinality] || ["1", "*"];

    try {
      conn.addOverlay([
        "Label",
        {
          id: "cardLeft",
          label: left,
          location: 0.1,
          cssClass: "connection-label",
        },
      ]);
      conn.addOverlay([
        "Label",
        {
          id: "cardRight",
          label: right,
          location: 0.9,
          cssClass: "connection-label",
        },
      ]);
    } catch (e) {
      console.log("Could not add overlays:", e);
    }
  }

  function applyZoom() {
    canvas.style.transformOrigin = "0 0";
    canvas.style.transform = `scale(${zoom})`;
    instance.setZoom(zoom);
    jsPlumb.repaintEverything();
  }

  function saveHistory() {
    const snapshot = exportModel();
    history = history.slice(0, historyIndex + 1);
    history.push(snapshot);
    historyIndex = history.length - 1;
    undoBtn.disabled = historyIndex <= 0;
    redoBtn.disabled = historyIndex >= history.length - 1;
  }

  function resetCanvas() {
    tableList.innerHTML =
      '<div class="p-3 text-muted small text-center">Please select a connection.</div>';
    if (canvasPlaceholder) canvasPlaceholder.style.display = "block";
    canvas.innerHTML = "";
    instance.deleteEveryConnection();
    history = [];
    historyIndex = -1;
    undoBtn.disabled = true;
    redoBtn.disabled = true;
    selectedConnection = null;
    suggestPool = [];
    zoom = 1;
    applyZoom();
    canvas.scrollLeft = 0;
    canvas.scrollTop = 0;
  }

  // --- Small helpers
  function findFieldElement(tableName, colName) {
    console.log("Looking for field:", tableName, colName);
    const card = canvas.querySelector(
      `.table-card[data-table-name="${CSS.escape(tableName)}"]`
    );
    if (!card) {
      console.warn("Table card not found:", tableName);
      return null;
    }

    const field = Array.from(card.querySelectorAll(".table-card-field")).find(
      (el) => el.dataset.column === colName
    );

    if (!field) {
      console.warn("Field not found in table:", colName, "in", tableName);
      console.log(
        "Available fields:",
        Array.from(card.querySelectorAll(".table-card-field")).map(
          (f) => f.dataset.column
        )
      );
    }

    return field;
  }

  function makeFieldRow(col, tableName, cardId) {
    const field = document.createElement("div");
    // Create a unique and valid ID for the field
    const fieldId = `field_${cardId}_${col.name.replace(/[^a-zA-Z0-9_]/g, "")}`;
    field.id = fieldId; // Set the ID here
    field.className = "table-card-field";
    field.className = "table-card-field";
    field.dataset.column = col.name;
    field.dataset.type = col.type || "";
    field.dataset.isNumeric = String(!!col.is_numeric);
    field.dataset.isPk = String(!!col.is_pk);

    let typeIcon = "•";
    const t = (col.type || "").toLowerCase();
    if (
      t.includes("int") ||
      t.includes("num") ||
      t.includes("dec") ||
      t.includes("real")
    ) {
      typeIcon = "#";
    } else if (t.includes("date") || t.includes("time")) {
      typeIcon = "📅";
    } else {
      typeIcon = "Abc";
    }

    const left = document.createElement("span");
    const pkIcon = col.is_pk ? "🔑 " : "";
    left.innerHTML = `
      <span class="field-type-icon">${pkIcon}${typeIcon}</span>
      ${col.name}
      <span class="join-wizard-btn ms-2"><i class="bi bi-gear-fill"></i></span>
    `;

    const right = document.createElement("span");
    right.className = "text-muted small";
    right.textContent = col.type;

    field.appendChild(left);
    field.appendChild(right);
    return field;
  }

  // --- Setup permissions
  function applyPermissions() {
    if (!isEditable) {
      // Disable all editing controls
      const disabledButtons = [
        "undoBtn",
        "redoBtn",
        "autoArrangeBtn",
        "validateBtn",
        "saveModelBtn",
        "zoomInBtn",
        "zoomOutBtn",
        "resetZoomBtn",
        "exportPngBtn",
        "exportPdfBtn",
        "canvasSearch",
        "autoSuggestToggle",
      ];

      disabledButtons.forEach((id) => {
        const btn = document.getElementById(id);
        if (btn) btn.disabled = true;
      });

      // Hide the save button if not editable
      const saveBtn = document.getElementById("saveModelBtn");
      if (saveBtn) saveBtn.style.display = "none";

      // Make table list items not draggable
      document
        .querySelectorAll("#tableList .list-group-item")
        .forEach((item) => {
          item.draggable = false;
        });

      // Add view-only class to canvas
      canvas.classList.add("view-only");

      // Disable connection interactions for view-only users
      instance.setSuspendDrawing(true);
    }
  }

  // --- Table card creation with collapsible feature
  function addTableCard(tbl, x, y) {
    if (canvasPlaceholder) canvasPlaceholder.style.display = "none";

    const card = document.createElement("div");
    card.className = "table-card";
    card.id = `card_${tbl.name.replace(/[^a-zA-Z0-9]/g, "_")}_${Date.now()}`;
    card.style.left = `${x}px`;
    card.style.top = `${y}px`;
    card.dataset.tableName = tbl.name;

    const header = document.createElement("div");
    header.className = "table-card-header";
    header.innerHTML = `
      <span>${tbl.name}</span>
      <div class="card-controls">
        <button type="button" class="btn-collapse" title="Collapse/Expand fields">
          <i class="bi bi-chevron-down"></i>
        </button>
        <button type="button" class="btn-close remove-card-btn" title="Remove table from canvas" aria-label="Close"></button>
      </div>
    `;

    const body = document.createElement("div");
    body.className = "table-card-body";
    (tbl.columns || []).forEach((col) => {
      const field = makeFieldRow(col, tbl.name, card.id);
      body.appendChild(field);
    });

    card.appendChild(header);
    card.appendChild(body);
    canvas.appendChild(card);

    if (isEditable) {
      let originalPosition = null;
      instance.draggable(card, {
        handle: ".table-card-header",
        grid: [10, 10],
        start: function (params) {
          originalPosition = {
            left: params.el.style.left,
            top: params.el.style.top,
          };
          params.el.style.zIndex = 20;
        },
        drag: function (params) {
          instance.revalidate(card);
          let isInvalid = false;
          const allCards = canvas.querySelectorAll(".table-card");
          for (const otherCard of allCards) {
            if (otherCard === card) continue;
            if (isOverlapping(card, otherCard)) {
              isInvalid = true;
              break;
            }
          }
          if (isInvalid) {
            card.classList.add("card-invalid-pos");
          } else {
            card.classList.remove("card-invalid-pos");
          }
        },
        stop: function (params) {
          params.el.style.zIndex = 10;
          if (card.classList.contains("card-invalid-pos")) {
            card.style.left = originalPosition.left;
            card.style.top = originalPosition.top;
            card.classList.remove("card-invalid-pos");
            instance.revalidate(card);
          } else {
            saveHistory();
          }
        },
      });
    }

    card.addEventListener("click", (e) => {
      const removeBtn = e.target.closest(".remove-card-btn");
      if (removeBtn && isEditable) {
        removeTableCard(card);
        return;
      }

      // --- THIS BLOCK CONTAINS THE RESTORED LOGIC ---
      const collapseBtn = e.target.closest(".btn-collapse");
      if (collapseBtn) {
        const body = card.querySelector(".table-card-body");
        const isCollapsing = !body.classList.contains("collapsed");
        body.classList.toggle("collapsed");
        const icon = collapseBtn.querySelector("i");
        icon.className = isCollapsing
          ? "bi bi-chevron-right"
          : "bi bi-chevron-down";

        if (isCollapsing) {
          rerouteConnectionsToHeader(card);
        } else {
          rerouteConnectionsToFields(card);
        }

        instance.revalidate(card);
        instance.repaintEverything();
        saveHistory();
        return;
      }
      // --- END OF RESTORED LOGIC ---

      const wizardBtn = e.target.closest(".join-wizard-btn");
      if (wizardBtn && isEditable) {
        const fieldEl = wizardBtn.closest(".table-card-field");
        if (fieldEl) {
          openJoinWizard(fieldEl);
        }
        return;
      }
    });

    card.querySelectorAll(".table-card-field").forEach((field) => {
      if (isEditable) {
        instance.makeSource(field, {
          anchor: "Right",
          filter: function (e, el) {
            return !e.target.closest(".join-wizard-btn");
          },
        });
        instance.makeTarget(field, {
          anchor: "Left",
          allowLoopback: false,
        });
      }
    });

    updateSidebar();
    return card;
  }

  function removeTableCard(cardEl) {
    // Use instance.remove() to cleanly delete the element, its endpoints, and all connections.
    instance.remove(cardEl);

    if (!canvas.querySelector(".table-card")) {
      if (canvasPlaceholder) canvasPlaceholder.style.display = "block";
    }
    if (typeof saveHistory === "function") saveHistory();
    updateSidebar();
  }

  // --- Export & import snapshot
  function exportModel() {
    const tables = Array.from(canvas.querySelectorAll(".table-card")).map(
      (c) => ({
        table_name: c.dataset.tableName,
        x: parseInt(c.style.left || "0", 10),
        y: parseInt(c.style.top || "0", 10),
        collapsed: c
          .querySelector(".table-card-body")
          .classList.contains("collapsed"),
      })
    );

    // --- FIX: Filter out broken connections before mapping them ---
    const joins = instance
      .getAllConnections()
      .filter((conn) => conn && conn.source && conn.target) // This line prevents the crash
      .map((conn) => {
        const sourceIsHeader = conn.source.matches(".table-card-header");
        const targetIsHeader = conn.target.matches(".table-card-header");

        // Use stored original element IDs if the connection is on a collapsed header
        const originalSourceEl = sourceIsHeader
          ? document.getElementById(conn.data.originalSourceId)
          : conn.source;
        const originalTargetEl = targetIsHeader
          ? document.getElementById(conn.data.originalTargetId)
          : conn.target;

        // Ensure we have valid original elements before proceeding
        if (!originalSourceEl || !originalTargetEl) {
          return null; // This join can't be saved, so skip it
        }

        const sourceCard = originalSourceEl.closest(".table-card");
        const targetCard = originalTargetEl.closest(".table-card");

        return {
          left_table: sourceCard.dataset.tableName,
          left_column: originalSourceEl.dataset.column,
          right_table: targetCard.dataset.tableName,
          right_column: originalTargetEl.dataset.column,
          join_type: (conn.data && conn.data.join_type) || "INNER",
          cardinality: (conn.data && conn.data.cardinality) || "one-to-many",
          suggested: !!(conn.data && conn.data.suggested),
        };
      })
      .filter((j) => j !== null); // Clean up any nulls we may have returned

    return {
      layout: tables,
      joins,
      viewport: {
        zoom,
        scrollLeft: canvas.scrollLeft,
        scrollTop: canvas.scrollTop,
      },
    };
  }

  function loadModel(model) {
    canvas.innerHTML = "";
    instance.deleteEveryConnection();
    if (!model) return;

    if (model.layout) {
      model.layout.forEach((l) => {
        const tbl = lastSchema?.[l.table_name] || {
          name: l.table_name,
          columns: [],
        };
        const card = addTableCard(tbl, l.x, l.y);
        // ADD THIS BLOCK to restore the collapsed state
        if (l.collapsed) {
          const body = card.querySelector(".table-card-body");
          const icon = card.querySelector(".btn-collapse i");
          body.classList.add("collapsed");
          if (icon) icon.className = "bi bi-chevron-right";
        }
      });
    }

    if (model.joins) {
      // Defer join creation until all cards are drawn
      setTimeout(() => {
        model.joins.forEach((j) => {
          const s = findFieldElement(j.left_table, j.left_column);
          const t = findFieldElement(j.right_table, j.right_column);
          if (s && t) {
            // THIS IS WHERE THE CONNECTION CREATION CODE GOES
            const c = instance.connect({
              source: s,
              target: t,
              cssClass: joinCssClass(j.join_type), // Make sure this returns the correct class
              overlays: [["Arrow", { width: 10, length: 10, location: 1 }]],
            });
            c.data = {
              join_type: j.join_type,
              cardinality: j.cardinality,
              suggested: false,
            };
            setCardinalityOverlays(c, j.cardinality);
          }
        });

        // After loading, trigger the collapse logic for cards that were saved as collapsed
        model.layout.forEach((l) => {
          if (l.collapsed) {
            const card = canvas.querySelector(
              `.table-card[data-table-name="${CSS.escape(l.table_name)}"]`
            );
            if (card) {
              rerouteConnectionsToHeader(card);
            }
          }
        });
        instance.repaintEverything();
      }, 100);
    }

    if (model.viewport) {
      zoom = Math.min(2, Math.max(0.2, model.viewport.zoom || 1));
      applyZoom();
      canvas.scrollLeft = model.viewport.scrollLeft || 0;
      canvas.scrollTop = model.viewport.scrollTop || 0;
    }
  }

  // --- Connection interactions
  function openJoinModalFor(connection) {
    const sTable = connection.source.closest(".table-card").dataset.tableName;
    const sCol = connection.source.dataset.column;
    const tTable = connection.target.closest(".table-card").dataset.tableName;
    const tCol = connection.target.dataset.column;

    sourceFieldEl.textContent = `${sTable}.${sCol}`;
    targetFieldEl.textContent = `${tTable}.${tCol}`;
    joinTypeSel.value = connection.data?.join_type || "INNER";
    cardinalitySel.value = connection.data?.cardinality || "one-to-many";

    saveJoinBtn.onclick = () => {
      const jt = joinTypeSel.value;
      const card = cardinalitySel.value;

      connection.data = connection.data || {};
      connection.data.join_type = jt;
      connection.data.cardinality = card;
      connection.data.suggested = false;

      // Normalize classes
      connection.removeClass("inner-join");
      connection.removeClass("left-join");
      connection.removeClass("right-join");
      connection.removeClass("suggested-join");
      connection.addClass(joinCssClass(jt));

      setCardinalityOverlays(connection, card);
      if (joinModal) joinModal.hide();
      saveHistory();
    };

    deleteJoinBtn.onclick = () => {
      instance.deleteConnection(connection);
      if (joinModal) joinModal.hide();
      saveHistory();
    };

    if (joinModal) joinModal.show();
  }

  // Bind to newly created connections
  // --- Connection interactions
  instance.bind("connection", (info) => {
    // --- FIX: This logic now preserves existing data (like originalSourceId) ---
    // and only adds defaults for brand-new, user-drawn connections.
    const existingData = info.connection.data || {};

    if (!existingData.join_type || !existingData.cardinality) {
      const sourceIsPk = info.source.dataset.isPk === "true";
      const targetIsPk = info.target.dataset.isPk === "true";
      let detectedCardinality = "one-to-many";

      if (targetIsPk && !sourceIsPk) {
        detectedCardinality = "many-to-one";
      } else if (sourceIsPk && !targetIsPk) {
        detectedCardinality = "one-to-many";
      } else if (sourceIsPk && targetIsPk) {
        detectedCardinality = "one-to-one";
      } else {
        detectedCardinality = "many-to-many";
      }

      info.connection.data = {
        ...existingData, // Preserve existing data first
        join_type: existingData.join_type || "INNER",
        cardinality: existingData.cardinality || detectedCardinality,
      };

      setCardinalityOverlays(info.connection, info.connection.data.cardinality);
      saveHistory(); // Only save history for new connections
    }
    // --- End of Fix ---

    // These event bindings should be on ALL connections, new or recreated.
    info.connection.bind("click", (connectionParam) => {
      selectedConnection = connectionParam;
    });

    info.connection.bind("contextmenu", (conn, e) => {
      e.preventDefault();
      selectedConnection = info.connection;
      const isSuggested = !!(
        selectedConnection.data && selectedConnection.data.suggested
      );
      if (ctxConfirmSuggested)
        ctxConfirmSuggested.style.display = isSuggested ? "block" : "none";
      showCtxMenu(e.pageX, e.pageY);
      return false;
    });

    info.connection.bind("dblclick", () => {
      openJoinModalFor(info.connection);
    });
  });

  instance.bind("connectionDetached", () => saveHistory());

  // Drag feedback: highlight compatible fields during a connection drag
  instance.bind("connectionDrag", (p) => {
    const sourceField = p.source;
    const srcType = (sourceField.dataset.type || "").toLowerCase();
    const srcIsNumeric = sourceField.dataset.isNumeric === "true";
    const fields = canvas.querySelectorAll(".table-card-field");
    fields.forEach((f) => {
      const t = (f.dataset.type || "").toLowerCase();
      const isNum = f.dataset.isNumeric === "true";
      const compatible =
        (srcIsNumeric && isNum) ||
        (!srcIsNumeric && !t.includes("date") && !t.includes("time"));
      if (compatible) f.classList.add("compatible-field");
    });
  });

  instance.bind("connectionDragStop", () => {
    canvas
      .querySelectorAll(".table-card-field.compatible-field")
      .forEach((el) => {
        el.classList.remove("compatible-field");
      });
  });

  // --- Canvas drag & drop
  canvas.addEventListener("dragover", (e) => {
    e.preventDefault();
    canvas.classList.add("drag-over");
  });

  canvas.addEventListener("dragleave", () => {
    canvas.classList.remove("drag-over");
  });

  canvas.addEventListener("drop", (e) => {
    e.preventDefault();
    canvas.classList.remove("drag-over");
    const payload = e.dataTransfer.getData("application/json");
    if (!payload) return;

    try {
      const tbl = JSON.parse(payload);
      addTableCard(
        tbl,
        e.offsetX + canvas.scrollLeft,
        e.offsetY + canvas.scrollTop
      );
      saveHistory();
    } catch (error) {
      console.error("Error parsing dropped data:", error);
    }
  });

  // Sidebar interactions
  if (tableSearch) {
    tableSearch.addEventListener("input", () => {
      updateSidebar();
    });
  }

  // --- In your toolbar/sidebar event listeners section ---

  if (sortTablesBtn) {
    sortTablesBtn.addEventListener("click", () => {
      // Toggle direction
      tableSortDirection = tableSortDirection === "asc" ? "desc" : "asc";

      // Update button icon and title
      const icon = sortTablesBtn.querySelector("i");
      if (tableSortDirection === "asc") {
        icon.className = "bi bi-sort-alpha-down";
        sortTablesBtn.title = "Sort A-Z";
      } else {
        icon.className = "bi bi-sort-alpha-up";
        sortTablesBtn.title = "Sort Z-A";
      }

      // Redraw the sidebar with the new sort order
      updateSidebar();
    });
  }

  // Double-click to drop next grid slot
  tableList.addEventListener("dblclick", (e) => {
    const item = e.target.closest(".list-group-item");
    if (!item || !item.dataset.table) return;

    try {
      const tbl = JSON.parse(item.dataset.table);
      const gap = 40,
        w = 240;
      const cards = Array.from(canvas.querySelectorAll(".table-card"));
      const cols = Math.max(1, Math.floor(canvas.clientWidth / (w + gap)));
      const idx = cards.length,
        r = Math.floor(idx / cols),
        c = idx % cols;
      const x = c * (w + gap) + gap + canvas.scrollLeft;
      const y = r * (160 + gap) + gap + canvas.scrollTop;
      addTableCard(tbl, x, y);
      saveHistory();
    } catch (error) {
      console.error("Error parsing table data:", error);
    }
  });

  // --- Toolbar
  if (zoomInBtn) {
    zoomInBtn.addEventListener("click", () => {
      zoom = Math.min(2, zoom + 0.1);
      applyZoom();
    });
  }

  if (zoomOutBtn) {
    zoomOutBtn.addEventListener("click", () => {
      zoom = Math.max(0.2, zoom - 0.1);
      applyZoom();
    });
  }

  if (resetZoomBtn) {
    resetZoomBtn.addEventListener("click", () => {
      zoom = 1;
      applyZoom();
    });
  }

  if (autoArrangeBtn) {
    autoArrangeBtn.addEventListener("click", autoArrangeLayout);
  }

  // --- Toolbar Event Listeners ---

  if (toggleAllBtn) {
    toggleAllBtn.addEventListener("click", () => {
      const allCards = canvas.querySelectorAll(".table-card");
      if (allCards.length === 0) return;

      // Determine the action: If even one card is expanded, the goal is to collapse all.
      // If all are already collapsed, the goal is to expand all.
      const shouldCollapse = Array.from(allCards).some(
        (card) =>
          !card
            .querySelector(".table-card-body")
            .classList.contains("collapsed")
      );

      // Loop through each card and toggle it if it's not in the desired state.
      allCards.forEach((card) => {
        const body = card.querySelector(".table-card-body");
        const isCollapsed = body.classList.contains("collapsed");
        const collapseBtn = card.querySelector(".btn-collapse");

        // By triggering a click, we reuse all the existing connection rerouting logic!
        if (shouldCollapse && !isCollapsed) {
          collapseBtn?.click();
        } else if (!shouldCollapse && isCollapsed) {
          collapseBtn?.click();
        }
      });

      // After the action, update the main button's icon and title for the next click.
      const icon = toggleAllBtn.querySelector("i");
      if (shouldCollapse) {
        // We just collapsed everything, so the button should now offer to expand.
        toggleAllBtn.title = "Expand All";
        icon.className = "bi bi-arrows-expand";
      } else {
        // We just expanded everything, so the button should now offer to collapse.
        toggleAllBtn.title = "Collapse All";
        icon.className = "bi bi-arrows-collapse";
      }
    });
  }

  if (centerCanvasBtn) {
    centerCanvasBtn.addEventListener("click", centerCanvas);
  }

  if (exportPngBtn) {
    exportPngBtn.addEventListener("click", async () => {
      try {
        const old = canvas.style.transform;
        canvas.style.transform = "scale(1)";
        const rect = canvas.getBoundingClientRect();
        const image = await html2canvas(canvas, {
          backgroundColor: "#ffffff",
          x: 0,
          y: 0,
          width: rect.width,
          height: rect.height,
        });
        canvas.style.transform = old;
        const link = document.createElement("a");
        link.download = "data-model.png";
        link.href = image.toDataURL("image/png");
        link.click();
      } catch (e) {
        showError("Export requires html2canvas on the page.");
      }
    });
  }

  if (exportPdfBtn) {
    exportPdfBtn.addEventListener("click", async () => {
      if (!window.jspdf) {
        showError("PDF export requires jsPDF on the page.");
        return;
      }

      try {
        const { jsPDF } = window.jspdf;
        const old = canvas.style.transform;
        canvas.style.transform = "scale(1)";
        const rect = canvas.getBoundingClientRect();
        const image = await html2canvas(canvas, {
          backgroundColor: "#ffffff",
          width: rect.width,
          height: rect.height,
        });
        canvas.style.transform = old;
        const imgData = image.toDataURL("image/png");
        const pdf = new jsPDF({
          orientation: rect.width > rect.height ? "l" : "p",
          unit: "px",
          format: [rect.width, rect.height],
        });
        pdf.addImage(imgData, "PNG", 0, 0, rect.width, rect.height);
        pdf.save("data-model.pdf");
      } catch (error) {
        console.error("PDF export error:", error);
        showError("PDF export failed: " + error.message);
      }
    });
  }

  if (canvasSearch) {
    canvasSearch.addEventListener("input", () => {
      const q = canvasSearch.value.toLowerCase();
      const cards = canvas.querySelectorAll(".table-card");
      cards.forEach((card) => {
        const name = card.dataset.tableName.toLowerCase();
        const match = name.includes(q);
        card.style.outline = match && q ? "2px solid #0d6efd" : "none";
        card.style.opacity = q && !match ? 0.35 : 1;
      });
    });
  }

  if (validateBtn) {
    validateBtn.addEventListener("click", async () => {
      const connectionId = connectionSelect.value;
      if (!connectionId) {
        showError("Select a database connection first.");
        return;
      }
      const payload = exportModel();

      try {
        const res = await fetch(`/data-model/api/validate_model/${connectionId}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();

        const succ = document.getElementById("validationSuccess");
        const warn = document.getElementById("validationWarnings");
        const err = document.getElementById("validationErrors");
        const wList = document.getElementById("warningList");
        const eList = document.getElementById("errorList");

        if (succ) succ.style.display = "none";
        if (warn) warn.style.display = "none";
        if (err) err.style.display = "none";
        if (wList) wList.innerHTML = "";
        if (eList) eList.innerHTML = "";

        if (data.success && (!data.issues || data.issues.length === 0)) {
          if (succ) succ.style.display = "block";
        } else {
          (data.issues || []).forEach((item) => {
            const li = document.createElement("li");
            li.textContent = item.message || String(item);
            if (item.level === "warning" && wList) {
              wList.appendChild(li);
            } else if (eList) {
              eList.appendChild(li);
            }
          });
          if (wList && wList.children.length && warn)
            warn.style.display = "block";
          if (eList && eList.children.length && err)
            err.style.display = "block";
          if (
            wList &&
            eList &&
            !wList.children.length &&
            !eList.children.length &&
            succ
          ) {
            succ.style.display = "block";
          }
        }

        const validationModal = document.getElementById("validationModal");
        if (validationModal) {
          new bootstrap.Modal(validationModal).show();
        }
      } catch (e) {
        showError("Validation failed: " + e.message);
      }
    });
  }

  // --- Keyboard shortcuts
  document.addEventListener("keydown", (e) => {
    const ctrl = e.ctrlKey || e.metaKey;
    if (ctrl && e.key.toLowerCase() === "s") {
      e.preventDefault();
      if (saveBtn) saveBtn.click();
    }
    if (ctrl && e.key.toLowerCase() === "z") {
      e.preventDefault();
      if (historyIndex > 0) {
        historyIndex--;
        loadModel(history[historyIndex]);
        if (redoBtn) redoBtn.disabled = false;
        if (undoBtn) undoBtn.disabled = historyIndex <= 0;
      }
    }
    if (ctrl && e.key.toLowerCase() === "y") {
      e.preventDefault();
      if (historyIndex < history.length - 1) {
        historyIndex++;
        loadModel(history[historyIndex]);
        if (undoBtn) undoBtn.disabled = historyIndex <= 0;
        if (redoBtn) redoBtn.disabled = historyIndex >= history.length - 1;
      }
    }
    if (ctrl && (e.key === "+" || e.key === "=")) {
      e.preventDefault();
      zoom = Math.min(2, zoom + 0.1);
      applyZoom();
    }
    if (ctrl && (e.key === "-" || e.key === "_")) {
      e.preventDefault();
      zoom = Math.max(0.2, zoom - 0.1);
      applyZoom();
    }
    if (e.key === "Delete" && selectedConnection) {
      instance.deleteConnection(selectedConnection);
      selectedConnection = null;
      saveHistory();
    }
  });

  // --- Context menu
  function showCtxMenu(x, y) {
    if (!ctxMenu) return;

    // Clone the menu to ensure fresh positioning
    const menuClone = ctxMenu.cloneNode(true);
    ctxMenu.parentNode.replaceChild(menuClone, ctxMenu);
    ctxMenu = menuClone;

    // Calculate viewport-safe position
    const rect = ctxMenu.getBoundingClientRect();
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;

    const adjustedX = Math.min(x, viewportWidth - rect.width - 10);
    const adjustedY = Math.min(y, viewportHeight - rect.height - 10);

    ctxMenu.style.left = `${adjustedX}px`;
    ctxMenu.style.top = `${adjustedY}px`;
    ctxMenu.style.display = "block";
    ctxMenu.style.zIndex = "9999";

    setTimeout(() => {
      const hide = (ev) => {
        if (!ctxMenu.contains(ev.target)) {
          ctxMenu.style.display = "none";
          document.removeEventListener("click", hide);
          document.removeEventListener("contextmenu", hide);
        }
      };
      document.addEventListener("click", hide);
      document.addEventListener("contextmenu", hide);
    }, 0);
  }

  if (ctxEditJoin) {
    ctxEditJoin.addEventListener("click", () => {
      if (ctxMenu) ctxMenu.style.display = "none";
      if (selectedConnection) openJoinModalFor(selectedConnection);
    });
  }

  if (ctxConfirmSuggested) {
    ctxConfirmSuggested.addEventListener("click", () => {
      if (ctxMenu) ctxMenu.style.display = "none";
      if (selectedConnection) {
        selectedConnection.data.suggested = false;
        selectedConnection.removeClass("suggested-join");
        saveHistory();
      }
    });
  }

  if (ctxDeleteJoin) {
    ctxDeleteJoin.addEventListener("click", () => {
      if (ctxMenu) ctxMenu.style.display = "none";
      if (selectedConnection) {
        instance.deleteConnection(selectedConnection);
        selectedConnection = null;
        saveHistory();
      }
    });
  }

  // --- Change connection -> fetch schema + saved model
  // --- Change connection -> fetch schema + saved model
  connectionSelect.addEventListener("change", async function () {
    const connectionId = this.value;
    if (!connectionId) {
      lastSchema = null;
      resetCanvas();
      updateSidebar();
      return;
    }

    try {
      // 1. Fetch all model data from the server
      const res = await fetch(`/data-model/api/get_model/${connectionId}`);
      if (!res.ok) {
        throw new Error(
          `Failed to load model. Server responded with status ${res.status}`
        );
      }
      const data = await res.json();
      if (!data.success) {
        throw new Error(
          data.error || "Failed to load model from the database."
        );
      }

      // 2. Reset the UI and cache the new schema
      resetCanvas();
      lastSchema = Object.fromEntries(
        (data.tables || []).map((t) => [t.name, t])
      );

      // 3. Update the sidebar and draw the model on the canvas
      updateSidebar();
      drawModel(data);

      // 4. Finalize the setup
      saveHistory(); // Save the initial loaded state
      applyPermissions();
    } catch (err) {
      // If anything fails, reset the entire state to prevent errors
      console.error("Connection error:", err);
      lastSchema = null;
      resetCanvas();
      updateSidebar(); // This will show the "Please select a connection" message
      showError(`Error loading connection: ${err.message}`);
    }
  });

  // --- Test connection
  if (testBtn) {
    testBtn.addEventListener("click", async () => {
      const connectionId = connectionSelect.value;
      if (!connectionId) {
        showError("Select a connection first.");
        return;
      }

      try {
        const res = await fetch(`/data-model/api/test_connection/${connectionId}`);
        const data = await res.json();
        if (statusEl) {
          statusEl.textContent = data.success
            ? "✅ Connected"
            : "❌ " + (data.error || "Connection failed");
          statusEl.className = data.success
            ? "text-success"
            : "text-danger small";
        }
      } catch (err) {
        if (statusEl) {
          statusEl.textContent = "❌ " + err.message;
          statusEl.className = "text-danger small";
        }
      }
    });
  }

  // --- Save model
  if (saveBtn) {
    saveBtn.addEventListener("click", async () => {
      const connectionId = connectionSelect.value;
      if (!connectionId) {
        showError("Select a database connection first.");
        return;
      }
      const payload = exportModel();

      try {
        const res = await fetch(`/data-model/api/save_model/${connectionId}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (data.success) {
          showSuccess("Model saved successfully!");
        } else {
          showError("Save failed: " + (data.error || "Unknown error"));
        }
      } catch (e) {
        showError("Save failed: " + e.message);
      }
    });
  }

  // --- Undo/Redo
  if (undoBtn) {
    undoBtn.addEventListener("click", () => {
      if (historyIndex > 0) {
        historyIndex--;
        loadModel(history[historyIndex]);
        redoBtn.disabled = false;
        undoBtn.disabled = historyIndex <= 0;
      }
    });
  }

  if (redoBtn) {
    redoBtn.addEventListener("click", () => {
      if (historyIndex < history.length - 1) {
        historyIndex++;
        loadModel(history[historyIndex]);
        undoBtn.disabled = historyIndex <= 0;
        redoBtn.disabled = historyIndex >= history.length - 1;
      }
    });
  }

  // =================================================================
  // JOIN WIZARD LOGIC
  // =================================================================

  function openJoinWizard(sourceFieldEl) {
    closeJoinWizard();

    const popoverTemplate = document.getElementById("joinWizardTemplate");
    if (!popoverTemplate) {
      console.error("Join Wizard HTML template not found!");
      showError("Error: The join wizard template is missing from the HTML page.");
      return;
    }

    const popoverContentNode = popoverTemplate.content.cloneNode(true);
    const sourceTableName =
      sourceFieldEl.closest(".table-card").dataset.tableName;
    const sourceColumnName = sourceFieldEl.dataset.column;

    const popover = new bootstrap.Popover(sourceFieldEl, {
      html: true,
      placement: "right",
      trigger: "manual",
      customClass: "join-wizard-popover",
      title: `Join from: ${sourceTableName}.${sourceColumnName}`,
      content: popoverContentNode,
    });

    activeJoinWizard.popover = popover;
    activeJoinWizard.sourceEl = sourceFieldEl;

    sourceFieldEl.addEventListener(
      "shown.bs.popover",
      () => {
        const popoverTip = popover.tip;
        if (!popoverTip) return;

        popoverTip
          .querySelector(".add-join-row-btn")
          .addEventListener("click", () => addJoinRow(popoverTip));
        popoverTip
          .querySelector(".cancel-join-btn")
          .addEventListener("click", closeJoinWizard);
        popoverTip
          .querySelector(".apply-joins-btn")
          .addEventListener("click", applyWizardJoins);

        addJoinRow(popoverTip);
      },
      { once: true }
    );

    popover.show();
  }

  function closeJoinWizard() {
    if (activeJoinWizard.popover) {
      activeJoinWizard.popover.dispose();
    }

    // Its only job is to delete any preview lines that still exist.
    activeJoinWizard.previews.forEach((conn) => {
      instance.deleteConnection(conn);
    });

    // Finally, reset the state
    activeJoinWizard = { popover: null, sourceEl: null, previews: [] };
  }

  function addJoinRow(popoverTip) {
    const body = popoverTip.querySelector(".join-wizard-body");
    const newRow = document.createElement("div");
    newRow.className = "join-wizard-row";

    const sourceTableName =
      activeJoinWizard.sourceEl.closest(".table-card").dataset.tableName;

    const allAvailableTables = Object.keys(lastSchema || {}).filter(
      (name) => name !== sourceTableName
    );

    newRow.innerHTML = `
      <select class="form-select form-select-sm target-table-select" aria-label="Select target table">
        <option value="">Select Target Table...</option>
        ${allAvailableTables
          .map((name) => `<option value="${name}">${name}</option>`)
          .join("")}
      </select>
      <select class="form-select form-select-sm target-column-select" aria-label="Select target column" disabled>
        <option value="">Select Column...</option>
      </select>
      <button type="button" class="btn btn-sm btn-outline-danger remove-join-row-btn" aria-label="Remove this join">&times;</button>
    `;

    body.appendChild(newRow);

    newRow
      .querySelector(".target-table-select")
      .addEventListener("change", (e) => handleTableSelection(e.target));
    newRow
      .querySelector(".target-column-select")
      .addEventListener("change", (e) => handleColumnSelection(e.target));
    newRow
      .querySelector(".remove-join-row-btn")
      .addEventListener("click", () => {
        const previewConnId = newRow.dataset.previewConnId;
        if (previewConnId) {
          const connObj = activeJoinWizard.previews.find(
            (c) => c.id === previewConnId
          );
          if (connObj) instance.deleteConnection(connObj);
        }
        newRow.remove();
      });
  }

  function handleTableSelection(tableSelectEl) {
    const row = tableSelectEl.closest(".join-wizard-row");
    const columnSelectEl = row.querySelector(".target-column-select");
    const selectedTable = tableSelectEl.value;

    columnSelectEl.innerHTML = '<option value="">Select Column...</option>';
    columnSelectEl.disabled = true;
    handleColumnSelection(columnSelectEl);

    if (!selectedTable || !lastSchema) return;

    const tableSchema = lastSchema[selectedTable];
    if (tableSchema && tableSchema.columns) {
      tableSchema.columns.forEach((col) => {
        const option = new Option(col.name, col.name);
        columnSelectEl.add(option);
      });
      columnSelectEl.disabled = false;
    }
  }

  function handleColumnSelection(columnSelectEl) {
    const row = columnSelectEl.closest(".join-wizard-row");
    const targetTable = row.querySelector(".target-table-select").value;
    const targetColumn = columnSelectEl.value;

    const oldPreviewId = row.dataset.previewConnId;
    if (oldPreviewId) {
      const connIndex = activeJoinWizard.previews.findIndex(
        (c) => c.id === oldPreviewId
      );
      if (connIndex > -1) {
        instance.deleteConnection(activeJoinWizard.previews[connIndex]);
        activeJoinWizard.previews.splice(connIndex, 1);
      }
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
        cssClass: "preview-join",
        overlays: [["Arrow", { width: 8, length: 8, location: 1 }]],
      });
      row.dataset.previewConnId = previewConn.id;
      activeJoinWizard.previews.push(previewConn);
    }
  }

  function applyWizardJoins() {
    const popoverTip = activeJoinWizard.popover.tip;
    if (!popoverTip) return;

    const rows = popoverTip.querySelectorAll(".join-wizard-row");
    const sourceCard = activeJoinWizard.sourceEl.closest(".table-card");

    rows.forEach((row, index) => {
      const targetTableName = row.querySelector(".target-table-select").value;
      const targetColumnName = row.querySelector(".target-column-select").value;

      if (targetTableName && targetColumnName) {
        let targetCard = canvas.querySelector(
          `.table-card[data-table-name="${CSS.escape(targetTableName)}"]`
        );

        if (!targetCard) {
          const tableSchema = lastSchema[targetTableName];
          if (tableSchema) {
            const newX = sourceCard.offsetLeft + sourceCard.offsetWidth + 100;
            const newY = sourceCard.offsetTop + index * 50;
            targetCard = addTableCard(tableSchema, newX, newY);
          }
        }

        if (targetCard) {
          const targetFieldEl = findFieldElement(
            targetTableName,
            targetColumnName
          );
          if (activeJoinWizard.sourceEl && targetFieldEl) {
            // --- FIX: We no longer delete the preview line here. ---
            // We just create the permanent connection.
            const existing = instance
              .getConnections({
                source: activeJoinWizard.sourceEl,
                target: targetFieldEl,
              })
              .filter((c) => !c.hasClass("preview-join")); // Only check for non-preview duplicates

            if (existing.length === 0) {
              instance.connect({
                source: activeJoinWizard.sourceEl,
                target: targetFieldEl,
                overlays: [["Arrow", { width: 10, length: 10, location: 1 }]],
              });
            }
          }
        }
      }
    });

    closeJoinWizard(); // This will now handle all cleanup
    saveHistory();
  }

  function isCompatibleField(f1, f2) {
    const t1 = (f1.dataset.type || "").toLowerCase();
    const t2 = (f2.dataset.type || "").toLowerCase();
    const n1 = f1.dataset.isNumeric === "true";
    const n2 = f2.dataset.isNumeric === "true";

    // Numeric fields can join with other numeric fields
    if (n1 && n2) return true;

    // Text fields can join with other text fields (excluding dates/times)
    if (!n1 && !n2) {
      const dateTimeKeywords = ["date", "time", "year"];
      const isDateTime1 = dateTimeKeywords.some((k) => t1.includes(k));
      const isDateTime2 = dateTimeKeywords.some((k) => t2.includes(k));
      return !isDateTime1 && !isDateTime2;
    }

    return false;
  }

  // =================================================================
  // REROUTING LOGIC (Unchanged, works with the other fixes)
  // =================================================================
  function rerouteConnectionsToHeader(card) {
    const header = card.querySelector(".table-card-header");
    const fields = card.querySelectorAll(".table-card-field");

    const connectionsToModify = [];
    fields.forEach((field) => {
      connectionsToModify.push(...instance.getConnections({ source: field }));
      connectionsToModify.push(...instance.getConnections({ target: field }));
    });
    const uniqueConnections = [...new Set(connectionsToModify)];

    uniqueConnections.forEach((conn) => {
      if (!conn || !conn.source || !conn.target) return;

      if (!conn.data) conn.data = {};
      if (!conn.data.originalSourceId)
        conn.data.originalSourceId = conn.sourceId;
      if (!conn.data.originalTargetId)
        conn.data.originalTargetId = conn.targetId;

      const dataCopy = JSON.parse(JSON.stringify(conn.data));

      let newSource = conn.source;
      let newTarget = conn.target;

      if (conn.source.closest(".table-card") === card) {
        newSource = header;
      } else {
        newTarget = header;
      }

      // 1. Create the new temp connection WITHOUT passing data initially.
      const tempConn = instance.connect({
        source: newSource,
        target: newTarget,
        cssClass: conn.getClass(),
        anchor: "Continuous",
        overlays: [["Arrow", { width: 10, length: 10, location: 1 }]],
      });

      // 2. FORCEFULLY assign our complete data object to the new connection.
      // This bypasses the bug where jsPlumb strips the data.
      tempConn.data = dataCopy;

      if (tempConn.data.cardinality) {
        setCardinalityOverlays(tempConn, tempConn.data.cardinality);
      }

      instance.deleteConnection(conn);
    });

    fields.forEach((field) => {
      instance
        .getEndpoints(field)
        .forEach((endpoint) => endpoint.setVisible(false));
    });
  }

  function rerouteConnectionsToFields(card) {
    const header = card.querySelector(".table-card-header");
    const fields = card.querySelectorAll(".table-card-field");

    const connectionsOnHeader = [
      ...instance.getConnections({ source: header }),
      ...instance.getConnections({ target: header }),
    ];
    const uniqueConnections = [...new Set(connectionsOnHeader)];

    uniqueConnections.forEach((conn) => {
      const data = JSON.parse(JSON.stringify(conn.data || {}));
      const css = conn.getClass();
      instance.deleteConnection(conn);

      if (data && data.originalSourceId && data.originalTargetId) {
        const sourceEl = document.getElementById(data.originalSourceId);
        const targetEl = document.getElementById(data.originalTargetId);

        if (sourceEl && targetEl) {
          const newConn = instance.connect({
            source: sourceEl,
            target: targetEl,
            data: data,
            cssClass: css,
            overlays: [["Arrow", { width: 10, length: 10, location: 1 }]],
          });

          if (data.cardinality) {
            setCardinalityOverlays(newConn, data.cardinality);
          }
        }
      }
    });

    fields.forEach((field) => {
      instance
        .getEndpoints(field)
        .forEach((endpoint) => endpoint.setVisible(true));
    });
  }

  function rerouteConnectionsToFields(card) {
    const header = card.querySelector(".table-card-header");
    const fields = card.querySelectorAll(".table-card-field");

    console.log(
      `%c--- Expanding Card: ${card.dataset.tableName} ---`,
      "color: blue; font-weight: bold;"
    );

    const connectionsOnHeader = [
      ...instance.getConnections({ source: header }),
      ...instance.getConnections({ target: header }),
    ];
    const uniqueConnections = [...new Set(connectionsOnHeader)];

    console.log(
      `Found ${uniqueConnections.length} connection(s) on the header.`
    );

    uniqueConnections.forEach((conn) => {
      console.log("%cProcessing a connection...", "color: green;");

      const data = JSON.parse(JSON.stringify(conn.data || {}));
      const css = conn.getClass();

      console.log("  1. Copied data object from header connection:", data);

      instance.deleteConnection(conn);
      console.log("  2. Temporary connection from header deleted.");

      if (data && data.originalSourceId && data.originalTargetId) {
        console.log("  3. Found original IDs in data object:", {
          source: data.originalSourceId,
          target: data.originalTargetId,
        });

        const sourceEl = document.getElementById(data.originalSourceId);
        const targetEl = document.getElementById(data.originalTargetId);

        console.log("  4. Searching for original field elements in the DOM:", {
          sourceFound: !!sourceEl,
          targetFound: !!targetEl,
        });

        if (sourceEl && targetEl) {
          console.log(
            "%c  5. SUCCESS: Recreating connection on fields.",
            "font-weight: bold; color: purple;"
          );

          const newConn = instance.connect({
            source: sourceEl,
            target: targetEl,
            data: data,
            cssClass: css,
            overlays: [["Arrow", { width: 10, length: 10, location: 1 }]],
          });

          if (data.cardinality) {
            setCardinalityOverlays(newConn, data.cardinality);
          }
        } else {
          console.error(
            "  5. FAILURE: Could not find one or both original field elements in the DOM. Connection will not be redrawn."
          );
        }
      } else {
        console.error(
          "  3. FAILURE: Original source/target IDs were not found in the connection data. Cannot restore connection."
        );
      }
    });

    fields.forEach((field) => {
      instance
        .getEndpoints(field)
        .forEach((endpoint) => endpoint.setVisible(true));
    });
    console.log("--- Expansion complete ---");
  }

  // --- Auto arrange layout
  function autoArrangeLayout() {
    const cards = Array.from(canvas.querySelectorAll(".table-card"));
    if (cards.length === 0 || typeof dagre === "undefined") {
      return;
    }

    // 1. Create a new directed graph
    const g = new dagre.graphlib.Graph();

    // 2. Set layout options (e.g., layout from Left-to-Right, node spacing)
    g.setGraph({ rankdir: "LR", nodesep: 60, ranksep: 80 });
    g.setDefaultEdgeLabel(() => ({}));

    // 3. Add each table card as a "node" in the graph
    cards.forEach((card) => {
      const isCollapsed = card
        .querySelector(".table-card-body")
        .classList.contains("collapsed");
      const height = isCollapsed ? 40 : card.offsetHeight;
      const width = card.offsetWidth;
      g.setNode(card.dataset.tableName, { width: width, height: height });
    });

    // 4. Add each jsPlumb connection as an "edge" in the graph
    instance.getAllConnections().forEach((conn) => {
      // --- FIX: Corrected typo from 'targe' to 'target' ---
      if (!conn || !conn.source || !conn.target) {
        console.warn(
          "Auto-layout is skipping a connection with a missing endpoint."
        );
        return; // Move to the next connection
      }
      // --- End of Fix ---

      const sourceTable = conn.source.closest(".table-card").dataset.tableName;
      const targetTable = conn.target.closest(".table-card").dataset.tableName;
      if (!g.hasEdge(sourceTable, targetTable)) {
        g.setEdge(sourceTable, targetTable);
      }
    });

    // 5. Ask Dagre to calculate the layout
    dagre.layout(g);

    // 6. Apply the calculated positions to the actual card elements
    g.nodes().forEach((tableName) => {
      const node = g.node(tableName);
      if (node) {
        const card = canvas.querySelector(
          `.table-card[data-table-name="${CSS.escape(tableName)}"]`
        );
        if (card) {
          card.style.left = `${node.x - node.width / 2}px`;
          card.style.top = `${node.y - node.height / 2}px`;
        }
      }
    });

    // 7. Repaint all connections and save the new layout to history
    setTimeout(() => {
      instance.repaintEverything();
      saveHistory();
    }, 150);
  }

  function centerCanvas() {
    const cards = Array.from(canvas.querySelectorAll(".table-card"));
    if (cards.length === 0) return;

    // Calculate bounds of all cards
    let minX = Infinity,
      minY = Infinity,
      maxX = -Infinity,
      maxY = -Infinity;

    cards.forEach((card) => {
      const x = parseInt(card.style.left || "0", 10);
      const y = parseInt(card.style.top || "0", 10);
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x + card.offsetWidth);
      maxY = Math.max(maxY, y + card.offsetHeight);
    });

    // Calculate center offset
    const centerX = (canvas.clientWidth - (maxX - minX)) / 2 - minX;
    const centerY = (canvas.clientHeight - (maxY - minY)) / 2 - minY;

    // Apply offset to all cards
    cards.forEach((card) => {
      const x = parseInt(card.style.left || "0", 10) + centerX;
      const y = parseInt(card.style.top || "0", 10) + centerY;
      card.style.left = `${x}px`;
      card.style.top = `${y}px`;
    });

    instance.repaintEverything();
    saveHistory();
  }

  // --- Mini-map
  function initMiniMap() {
    if (miniMap) return;

    const mapEl = document.createElement("div");
    mapEl.className = "mini-map";
    mapEl.innerHTML = `
      <div class="mini-map-viewport"></div>
      <div class="mini-map-content"></div>
    `;
    document.body.appendChild(mapEl);
    miniMap = mapEl;

    updateMiniMap();
    mapEl.addEventListener("click", (e) => {
      const rect = mapEl.getBoundingClientRect();
      const x = ((e.clientX - rect.left) / rect.width) * canvas.scrollWidth;
      const y = ((e.clientY - rect.top) / rect.height) * canvas.scrollHeight;
      canvas.scrollLeft = x - canvas.clientWidth / 2;
      canvas.scrollTop = y - canvas.clientHeight / 2;
    });

    // Update on scroll and resize
    canvas.addEventListener("scroll", updateMiniMap);
    window.addEventListener("resize", updateMiniMap);
  }

  function createMiniMap() {
    const mapContainer = document.createElement("div");
    mapContainer.className = "minimap";
    // Start hidden, the toggle() will show it.
    mapContainer.style.display = "none";
    mapContainer.innerHTML = `
      <div class="minimap-header">
        <span>Overview</span>
        <button type="button" class="btn-close-minimap btn-close btn-close-sm"></button>
      </div>
      <div class="minimap-content">
        <div class="minimap-viewport"></div>
      </div>
    `;
    document.body.appendChild(mapContainer);

    const viewport = mapContainer.querySelector(".minimap-viewport");

    // Click on minimap to navigate main canvas
    mapContainer
      .querySelector(".minimap-content")
      .addEventListener("click", (e) => {
        if (e.target === viewport) return; // Ignore clicks on the viewport itself
        const rect = mapContainer
          .querySelector(".minimap-content")
          .getBoundingClientRect();
        const scale = parseFloat(mapContainer.dataset.scale || 1);
        const bounds = JSON.parse(mapContainer.dataset.bounds || "{}");

        const clickX = e.clientX - rect.left;
        const clickY = e.clientY - rect.top;

        const targetX = clickX / scale + bounds.minX - canvas.clientWidth / 2;
        const targetY = clickY / scale + bounds.minY - canvas.clientHeight / 2;

        canvas.scrollTo({ left: targetX, top: targetY, behavior: "smooth" });
      });

    // Close button
    mapContainer
      .querySelector(".btn-close-minimap")
      .addEventListener("click", () => (mapContainer.style.display = "none"));

    // Update the main canvas scroll position when dragging the viewport
    // (This part can be added later if needed to keep it simple)

    // Return the controller object
    return {
      element: mapContainer,
      update: () => updateMiniMap(mapContainer),
      toggle: () => {
        const isHidden = mapContainer.style.display === "none";
        mapContainer.style.display = isHidden ? "flex" : "none";
        if (isHidden) {
          updateMiniMap(mapContainer);
        }
      },
    };
  }

  function updateMiniMap(mapContainer) {
    if (!mapContainer || mapContainer.style.display === "none") return;

    const content = mapContainer.querySelector(".minimap-content");
    const viewport = mapContainer.querySelector(".minimap-viewport");
    content.innerHTML = '<div class="minimap-viewport"></div>'; // Clear old cards

    const cards = Array.from(canvas.querySelectorAll(".table-card"));
    if (cards.length === 0) return;

    // 1. Calculate the total bounds of all cards on the main canvas
    let minX = Infinity,
      minY = Infinity,
      maxX = -Infinity,
      maxY = -Infinity;
    cards.forEach((card) => {
      minX = Math.min(minX, card.offsetLeft);
      minY = Math.min(minY, card.offsetTop);
      maxX = Math.max(maxX, card.offsetLeft + card.offsetWidth);
      maxY = Math.max(maxY, card.offsetTop + card.offsetHeight);
    });
    const contentWidth = maxX - minX;
    const contentHeight = maxY - minY;

    // Store bounds for navigation calculations
    mapContainer.dataset.bounds = JSON.stringify({ minX, minY });

    // 2. Calculate scale to fit everything in the minimap
    const scale = Math.min(
      content.clientWidth / contentWidth,
      content.clientHeight / contentHeight
    );
    mapContainer.dataset.scale = scale;

    // 3. Draw mini-cards
    cards.forEach((card) => {
      const miniCard = document.createElement("div");
      miniCard.className = "table-card"; // Reuse style for consistency
      miniCard.style.position = "absolute";
      miniCard.style.left = `${(card.offsetLeft - minX) * scale}px`;
      miniCard.style.top = `${(card.offsetTop - minY) * scale}px`;
      miniCard.style.width = `${card.offsetWidth * scale}px`;
      miniCard.style.height = `${card.offsetHeight * scale}px`;
      content.appendChild(miniCard);
    });

    // 4. Update the viewport rectangle
    const vp = mapContainer.querySelector(".minimap-viewport");
    vp.style.left = `${(canvas.scrollLeft - minX) * scale}px`;
    vp.style.top = `${(canvas.scrollTop - minY) * scale}px`;
    vp.style.width = `${canvas.clientWidth * scale}px`;
    vp.style.height = `${canvas.clientHeight * scale}px`;
  }

  // Also, make sure the minimap updates when you scroll the main canvas
  canvas.addEventListener("scroll", () => {
    if (miniMap) {
      miniMap.update();
    }
  });

  function suggestJoins() {
    if (!lastSchema) return;

    // Clear previous suggestions
    instance.getAllConnections().forEach((conn) => {
      if (conn.data && conn.data.suggested) {
        instance.deleteConnection(conn);
      }
    });

    suggestPool = [];

    // Find potential joins based on column names and types
    const cards = Array.from(canvas.querySelectorAll(".table-card"));
    cards.forEach((sourceCard) => {
      const sourceTable = sourceCard.dataset.tableName;
      sourceCard
        .querySelectorAll(".table-card-field")
        .forEach((sourceField) => {
          const sourceCol = sourceField.dataset.column;
          const sourceType = sourceField.dataset.type;

          cards.forEach((targetCard) => {
            if (sourceCard === targetCard) return;

            const targetTable = targetCard.dataset.tableName;
            targetCard
              .querySelectorAll(".table-card-field")
              .forEach((targetField) => {
                const targetCol = targetField.dataset.column;
                const targetType = targetField.dataset.type;

                // Simple heuristic: same column name and compatible types
                if (
                  sourceCol === targetCol &&
                  isCompatibleField(sourceField, targetField)
                ) {
                  suggestPool.push({
                    source: sourceField,
                    target: targetField,
                    confidence: 0.8,
                  });
                }
              });
          });
        });
    });

    // Create suggested connections
    suggestPool.forEach((suggestion) => {
      const conn = instance.connect({
        source: suggestion.source,
        target: suggestion.target,
        cssClass: "suggested-join",
        overlays: [["Arrow", { width: 10, length: 10, location: 1 }]],
      });
      conn.data = {
        join_type: "INNER",
        cardinality: "one-to-many",
        suggested: true,
      };
      setCardinalityOverlays(conn, "one-to-many");
    });

    saveHistory();
  }

  // Initialize
  if (connectionSelect.value) {
    connectionSelect.dispatchEvent(new Event("change"));
  }

  /**
   * Central function to redraw the entire sidebar.
   * It handles searching, sorting, and disabling tables already on the canvas.
   */
  function updateSidebar() {
    if (!lastSchema) {
      tableList.innerHTML =
        '<div class="p-3 text-muted small text-center">Please select a connection.</div>';
      return;
    }

    const searchQuery = tableSearch.value.toLowerCase();
    const tablesOnCanvas = new Set(
      Array.from(canvas.querySelectorAll(".table-card")).map(
        (c) => c.dataset.tableName
      )
    );

    // 1. Filter all tables based on the search query
    const allTables = Object.values(lastSchema).filter((table) =>
      table.name.toLowerCase().includes(searchQuery)
    );

    // 2. Partition the list into active (draggable) and inactive (on-canvas) tables
    const activeTables = [];
    const inactiveTables = [];
    allTables.forEach((table) => {
      if (tablesOnCanvas.has(table.name)) {
        inactiveTables.push(table);
      } else {
        activeTables.push(table);
      }
    });

    // 3. Sort each partition alphabetically based on the current direction
    const sorter = (a, b) => {
      const nameA = a.name.toLowerCase();
      const nameB = b.name.toLowerCase();
      if (tableSortDirection === "asc") {
        return nameA.localeCompare(nameB);
      } else {
        return nameB.localeCompare(nameA);
      }
    };
    activeTables.sort(sorter);
    inactiveTables.sort(sorter);

    // 4. Rebuild the sidebar's HTML
    tableList.innerHTML = "";
    const combinedList = [...activeTables, ...inactiveTables];

    if (combinedList.length === 0) {
      tableList.innerHTML =
        '<div class="p-3 text-muted small text-center">No matching tables.</div>';
      return;
    }

    combinedList.forEach((tbl) => {
      const item = document.createElement("div");
      item.className = "list-group-item";
      item.textContent = tbl.name;
      item.dataset.table = JSON.stringify(tbl);

      const isActive = activeTables.includes(tbl);
      item.draggable = isEditable && isActive;

      if (!isActive) {
        item.classList.add("disabled"); // Visually grays out the item
      }

      if (isEditable && isActive) {
        item.addEventListener("dragstart", (e) => {
          e.dataTransfer.setData("application/json", JSON.stringify(tbl));
        });
      }
      tableList.appendChild(item);
    });
  }

  /**
   * Renders the tables and joins on the canvas based on the fetched model data.
   * @param {object} modelData - The data object from the /api/get_model endpoint.
   */
  function drawModel(modelData) {
    // 1. Draw table cards based on the layout
    (modelData.layout || []).forEach((l) => {
      // Ensure the table exists in the schema before trying to draw it
      const tbl = lastSchema[l.table_name];
      if (tbl) {
        const card = addTableCard(tbl, l.x, l.y);
        // Restore the collapsed state
        if (l.collapsed) {
          const body = card.querySelector(".table-card-body");
          const icon = card.querySelector(".btn-collapse i");
          body.classList.add("collapsed");
          if (icon) icon.className = "bi bi-chevron-right";
          // Reroute connections after the card has been drawn and added to the DOM
          setTimeout(() => rerouteConnectionsToHeader(card), 0);
        }
      }
    });

    // 2. Draw the joins between the cards
    (modelData.joins || []).forEach((j) => {
      const s = findFieldElement(j.left_table, j.left_column);
      const t = findFieldElement(j.right_table, j.right_column);
      if (s && t) {
        const c = instance.connect({
          source: s,
          target: t,
          cssClass: joinCssClass(j.join_type),
          overlays: [["Arrow", { width: 10, length: 10, location: 1 }]],
        });
        c.data = {
          join_type: j.join_type,
          cardinality: j.cardinality,
          suggested: false,
        };
        setCardinalityOverlays(c, j.cardinality);
      }
    });
  }

  function isOverlapping(el1, el2) {
    const rect1 = el1.getBoundingClientRect();
    const rect2 = el2.getBoundingClientRect();

    return !(
      rect1.right < rect2.left ||
      rect1.left > rect2.right ||
      rect1.bottom < rect2.top ||
      rect1.top > rect2.bottom
    );
  }
});


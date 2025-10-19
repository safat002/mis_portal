// static/js/dashborad_design.js

console.log("[Designer] JS loaded!");
window.showSuccess = window.showSuccess || ((m) => console.log("SUCCESS:", m));
window.showWarning = window.showWarning || ((m) => console.warn("WARN:", m));
window.showError = window.showError || ((m) => console.error("ERROR:", m));
window.showInfo = window.showInfo || ((m) => console.info("INFO:", m));
window.eventBus = window.eventBus || { on() {}, emit() {} };

(() => {
	"use strict";

	// -------------------------------
	// Guards (prevent hard crashes)
	// -------------------------------
	const hasGridStack = () => typeof window.GridStack !== "undefined";
	const hasWidgetRenderer = () => typeof window.WidgetRenderer !== "undefined";

	// -------------------------------
	// DOM helpers
	// -------------------------------
	const $ = (sel, root = document) => root.querySelector(sel);
	const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
	const on = (el, ev, cb) => el && el.addEventListener(ev, cb);
	const debounce = (fn, t = 600) => {
		let h;
		return (...a) => {
			clearTimeout(h);
			h = setTimeout(() => fn(...a), t);
		};
	};
	const uuid = () =>
		([1e7] + -1e3 + -4e3 + -8e3 + -1e11).replace(/[018]/g, (c) =>
			(
				c ^
				(crypto.getRandomValues(new Uint8Array(1))[0] & (15 >> (c / 4)))
			).toString(16)
		);

	function getCsrfToken() {
		return (
			document.querySelector('input[name="csrfmiddlewaretoken"]')?.value || ""
		);
	}

	async function saveDataContext() {
		const btn = document.getElementById("saveDataContextBtn");
		const root = document.getElementById("designer-root");
		const dashboardId = root?.dataset.dashboardId;

		if (!dashboardId) {
			showError("Dashboard ID missing.");
			return;
		}

		const connectionId =
			document.getElementById("data-context-connection-select")?.value || null;
		const tables = Array.from(
			document.querySelectorAll(
				'#data-context-tables-list input[type="checkbox"]:checked'
			)
		).map((i) => i.value);

		// Get joins from state or create empty array
		const joins = window.AppState?.dataContext?.joins || [];

		// If we have multiple tables but no joins, warn the user
		if (tables.length >= 2 && joins.length === 0) {
			const useDefault = confirm(
				"You have selected multiple tables but no joins are configured. " +
					"Would you like to use default join suggestions? " +
					"Otherwise, click 'Suggest Joins' first."
			);

			if (useDefault) {
				// This will trigger the backend to create default joins
				showInfo("Using default join configuration.");
			} else {
				showWarning(
					"Please configure joins between your tables for widgets to work properly."
				);
			}
		}

		window.AppState = window.AppState || {};
		AppState.dataContext = {
			connection_id: connectionId,
			tables: tables,
			joins: joins,
		};

		try {
			btn?.setAttribute("disabled", "disabled");

			const resp = await fetch(`/api/dashboard/${dashboardId}/data_context/`, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					"X-CSRFToken": getCsrfToken(),
				},
				body: JSON.stringify({
					connection_id: connectionId,
					selected_tables: tables,
					joins: joins,
				}),
			});

			let json = null;
			const ct = resp.headers.get("content-type") || "";
			if (ct.includes("application/json")) {
				try {
					json = await resp.json();
				} catch {
					/* ignore */
				}
			}

			if (!resp.ok || !json || json.success !== true) {
				const msg = json && json.error ? json.error : `HTTP ${resp.status}`;
				throw new Error(msg);
			}

			showSuccess("Data context saved.");

			// Refresh data-dependent UI
			if (typeof updateAvailableFields === "function") updateAvailableFields();
			if (window.eventBus?.emit)
				eventBus.emit("dataContext:changed", AppState.dataContext);
			if (typeof refreshAllWidgets === "function") refreshAllWidgets();
		} catch (e) {
			console.error(e);
			showError(`Save failed: ${e.message}`);
		} finally {
			btn?.removeAttribute("disabled");
			closeModalClean("dataContextModal");
		}
	}

	// ==== 1) Tiny toast fallback (put near top of file) ====
	function showToast(msg, type = "info") {
		try {
			// if you already have a real toast, no-op here
			console[type === "danger" ? "error" : "log"](`[${type}] ${msg}`);
		} catch (_) {}
	}

	// ==== 2) Helpers ====
	const UUID_RE =
		/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

	function normalizeConnectionId(raw) {
		const s = String(raw ?? "").trim();
		if (s === "true" || s === "false") return ""; // guard booleans coming from bad state
		if (UUID_RE.test(s)) return s; // OK
		return ""; // anything else = invalid
	}

	function getActiveConnectionId() {
		// prefer select element
		const sel = document.getElementById("data-context-connection-select");
		const fromSelect = sel?.value;
		const fromState =
			window.AppState?.dataContext?.connection_id ||
			window.AppState?.savedContext?.connection_id;
		return normalizeConnectionId(fromSelect || fromState);
	}

	// ==== 3) refreshCatalog: validate connectionId before fetching ====
	async function refreshCatalog(connectionId, tablesToLoad) {
		const connId = normalizeConnectionId(
			connectionId || getActiveConnectionId()
		);
		if (!connId) {
			DataPane.renderForWidget(null); // Clears the pane
			return;
		}

		try {
			const tables = Array.isArray(tablesToLoad) ? tablesToLoad : [];
			if (tables.length === 0) {
				// If no tables are selected, clear the fields panel
				window.AppState.dataSourceSchema = {};
				DataPane.renderForWidget(null);
				return;
			}

			const cRes = await fetch(`/api/table-columns/`, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					"X-CSRFToken": getCsrfToken(),
				},
				credentials: "same-origin",
				body: JSON.stringify({ connection_id: connId, tables: tables }),
			});

			if (!cRes.ok) throw new Error(`Columns batch failed (${cRes.status})`);
			const cJson = await cRes.json();
			if (!cJson.success) throw new Error(cJson.error || "Columns API error");

			// THIS IS THE CORE FIX:
			// We now correctly process the response and pass it to the DataPane object.
			const schema = {};
			(cJson.columns || []).forEach((col) => {
				(schema[col.source] ||= []).push({
					name: col.name,
					type: col.type,
					fullName: col.fullName || `${col.source}.${col.name}`,
				});
			});

			// Store the schema and trigger a re-render of the fields panel.
			window.AppState.dataSourceSchema = schema;
			DataPane.renderForWidget(window.AppState.currentWidgetId || null);
		} catch (err) {
			console.error("refreshCatalog error:", err);
			showToast(`Failed to load tables/columns: ${err.message}`, "danger");
			// Clear the panel on error
			window.AppState.dataSourceSchema = {};
			DataPane.renderForWidget(null);
		}
	}

	async function loadDataContextFromServer() {
		const root = document.getElementById("designer-root");
		const dashboardId = root?.dataset.dashboardId;
		if (!dashboardId) return;

		try {
			const resp = await fetch(`/api/dashboard/${dashboardId}/data_context/`, {
				method: "GET",
			});
			const json = await resp.json().catch(() => null);

			if (resp.ok && json?.success) {
				const ctx = json.data_context || {};
				const savedConn = normalizeConnectionId(ctx.connection_id);

				window.AppState = window.AppState || {};
				window.AppState.dataContext = {
					connection_id: savedConn,
					tables: ctx.tables || ctx.selected_tables || [],
					joins: ctx.joins || [],
				};

				// only call when valid
				if (savedConn) {
					await refreshCatalog(
						savedConn,
						window.AppState.dataContext.tables || []
					);
				}
			}
		} catch (e) {
			console.warn("Failed to load data_context:", e);
		}
	}

	// --- Data Context helpers: define at top-level (global scope) ---
	async function loadConnectionsIntoDropdown() {
		const sel = document.getElementById("data-context-connection-select");
		if (!sel) return; // modal not in DOM yet

		// UX: show loading and disable while fetching
		sel.innerHTML = `<option value="">Loading...</option>`;
		sel.disabled = true;

		try {
			const res = await fetch(`/api/connections/`, {
				credentials: "same-origin",
			});
			if (!res.ok) throw new Error(`HTTP ${res.status}`);

			const json = await res.json();

			// API may return either an array or an object with { success, connections }
			const list = Array.isArray(json)
				? json
				: Array.isArray(json?.connections)
				? json.connections
				: [];

			if (!list.length) {
				sel.innerHTML = `<option value="">(no connections available)</option>`;
				sel.disabled = false;
				return;
			}

			// Build options safely
			const makeLabel = (c) =>
				[c.nickname, c.db_type].filter(Boolean).join(" - ") || c.name || c.id;

			sel.innerHTML =
				`<option value="">-- Select a connection --</option>` +
				list
					.map(
						(c) => `<option value="${String(c.id)}">${makeLabel(c)}</option>`
					)
					.join("");

			// Try to restore previously saved selection
			const saved =
				(window.AppState &&
					window.AppState.dataContext &&
					window.AppState.dataContext.connection_id) ||
				(window.AppState &&
					window.AppState.savedContext &&
					window.AppState.savedContext.connection_id);

			if (saved && list.some((c) => String(c.id) === String(saved))) {
				sel.value = String(saved);
			}

			// If nothing selected yet and at least one connection exists, do not auto-select.
			// If you want auto-select first item, uncomment the next 2 lines:
			// if (!sel.value) {
			//   sel.value = String(list[0].id);
			// }

			// Fire change so downstream logic (e.g., load tables) runs if something is selected
			if (sel.value && UUID_RE.test(sel.value)) {
				sel.dispatchEvent(new Event("change", { bubbles: true }));
			}
		} catch (e) {
			console.error("[DataContext] loadConnectionsIntoDropdown error:", e);
			sel.innerHTML = `<option value="">(error loading)</option>`;
		} finally {
			sel.disabled = false;
		}
	}

	// Corrected & hardened
	async function loadTablesForSelectedConnection() {
		const connectionSel = document.getElementById(
			"data-context-connection-select"
		);
		const connectionId = connectionSel?.value;
		const target = document.getElementById("data-context-tables-list");
		if (!target) return;

		// Helper: safe ID for inputs/labels
		const safeId = (s) => `table-${String(s).replace(/[^a-zA-Z0-9_-]/g, "_")}`;

		// Nothing selected yet
		if (!connectionId) {
			target.innerHTML = `<div class="text-muted small">Choose a connection first.</div>`;
			return;
		}

		target.innerHTML = `<div class="text-muted small">Loading tables...</div>`;

		try {
			const res = await fetch(
				`/api/connections/${encodeURIComponent(connectionId)}/tables/`,
				{
					credentials: "same-origin",
				}
			);
			if (!res.ok) throw new Error(`HTTP ${res.status}`);

			const json = await res.json();
			// API shape: either {tables:[...]} or just [...]
			const tables = Array.isArray(json?.tables)
				? json.tables
				: Array.isArray(json)
				? json
				: [];
			tables.sort((a, b) => String(a).localeCompare(String(b))); // nicer UX

			// Previously selected tables from state (persisted or current)
			const previouslySelected =
				window.AppState?.dataContext?.tables ||
				window.AppState?.savedContext?.selected_tables ||
				[];

			const selected = new Set(
				previouslySelected.filter((t) => tables.includes(t))
			);

			// Render checkbox list
			if (!tables.length) {
				target.innerHTML = `<div class="text-muted small">No permitted tables.</div>`;
			} else {
				target.innerHTML = tables
					.map((t) => {
						const id = safeId(t);
						const checked = selected.has(t) ? "checked" : "";
						return `
          <div class="col">
            <div class="form-check">
              <input class="form-check-input" type="checkbox" id="${id}" value="${t}" ${checked}>
              <label class="form-check-label" for="${id}">${t}</label>
            </div>
          </div>
        `;
					})
					.join("");
			}

			// Save initial selection into AppState
			window.AppState = window.AppState || {};
			window.AppState.dataContext = window.AppState.dataContext || {};
			window.AppState.dataContext.tables = Array.from(selected);

			// Event delegation: keep AppState in sync on user changes
			if (!target._tablesChangeBound) {
				target.addEventListener("change", (e) => {
					if (!(e.target instanceof HTMLInputElement)) return;
					if (e.target.type !== "checkbox") return;

					const currSelected = new Set(
						Array.from(
							target.querySelectorAll('input[type="checkbox"]:checked')
						).map((el) => el.value)
					);
					window.AppState.dataContext.tables = Array.from(currSelected);

					// Optional: trigger a catalog refresh (batch columns) if available.
					// If your refreshCatalog signature accepts (connectionId, tables), pass them.
					if (typeof refreshCatalog === "function") {
						try {
							// Prefer selected tables if any, otherwise all tables
							const toLoad = window.AppState.dataContext.tables.length
								? window.AppState.dataContext.tables
								: tables;

							// Support both signatures: (conn) or (conn, tables)
							if (refreshCatalog.length >= 2) {
								refreshCatalog(connectionId, toLoad);
							} else {
								refreshCatalog(connectionId);
							}
						} catch (err) {
							console.warn("refreshCatalog call failed:", err);
						}
					}
				});
				target._tablesChangeBound = true;
			}
		} catch (e) {
			console.error("[DataContext] loadTablesForSelectedConnection error:", e);
			target.innerHTML = `<div class="text-danger small">Error: ${e.message}</div>`;
		}
	}

	async function suggestJoinsForSelectedTables() {
		const connectionId = document.getElementById(
			"data-context-connection-select"
		)?.value;
		const chosen = Array.from(
			document.querySelectorAll(
				'#data-context-tables-list input[type="checkbox"]:checked'
			)
		).map((i) => i.value);
		const panel = document.getElementById("join-suggestions");

		if (!connectionId) {
			showError("Choose a connection first.");
			return;
		}

		if (chosen.length < 2) {
			if (panel) {
				panel.innerHTML = `<div class="text-muted small">Select at least two tables to suggest joins.</div>`;
			}
			return;
		}

		if (panel) {
			panel.innerHTML = `<div class="text-muted small">Finding joins...</div>`;
		}

		try {
			const resp = await fetch(
				`/api/connections/${connectionId}/suggest_joins/`,
				{
					method: "POST",
					headers: {
						"Content-Type": "application/json",
						"X-CSRFToken": getCsrfToken(),
					},
					body: JSON.stringify({ tables: chosen }),
				}
			);
			const js = await resp.json();
			if (!js.success) throw new Error(js.error);

			window.AppState = window.AppState || {};
			AppState.dataContext = {
				...(AppState.dataContext || {}),
				connection_id: connectionId,
				tables: chosen,
				joins: js.chosen || [],
			};

			const all = js.edges || [];
			const chosenEdges = new Set(
				(js.chosen || []).map(
					(e) =>
						`${e.left_table}.${e.left_column}->${e.right_table}.${e.right_column}`
				)
			);

			if (panel) {
				panel.innerHTML = `
      <div class="small text-muted mb-2">
        ${
					js.needs_manual
						? "Could not fully connect all tables. Please use Visual Join Builder."
						: "All selected tables are connected."
				}
      </div>
      <div class="list-group">
        ${all
					.map((e) => {
						const key = `${e.left_table}.${e.left_column}->${e.right_table}.${e.right_column}`;
						const badge =
							e.source === "predefined"
								? "success"
								: e.source === "auto_fk"
								? "primary"
								: "secondary";
						const picked = chosenEdges.has(key);
						return `
            <div class="list-group-item d-flex justify-content-between align-items-center">
              <div>
                <span class="badge text-bg-${badge} me-2">${e.source}</span>
                <code>${e.left_table}.${e.left_column}</code>
                <span class="mx-1">=</span>
                <code>${e.right_table}.${e.right_column}</code>
                <span class="text-muted ms-2">(${e.join_type})</span>
              </div>
              ${picked ? '<span class="badge text-bg-info">used</span>' : ""}
            </div>
          `;
					})
					.join("")}
      </div>
    `;
			}
		} catch (e) {
			console.error("[DataContext] suggestJoinsForSelectedTables error:", e);
			if (panel) {
				panel.innerHTML = `<div class="text-danger small">Error: ${e.message}</div>`;
			}
		}
	}

	function wireDataContextModal() {
		const modalEl = document.getElementById("dataContextModal");
		if (!modalEl) return;

		modalEl.addEventListener("show.bs.modal", async () => {
			await loadConnectionsIntoDropdown();
			await loadTablesForSelectedConnection();
			const panel = document.getElementById("join-suggestions");
			if (panel) {
				panel.innerHTML = `<div class="text-muted small">Click "Suggest Joins" to auto-connect selected tables.</div>`;
			}
		});

		document
			.getElementById("data-context-connection-select")
			?.addEventListener("change", loadTablesForSelectedConnection);

		document
			.getElementById("btnSuggestJoins")
			?.addEventListener("click", suggestJoinsForSelectedTables);

		document
			.getElementById("saveDataContextBtn")
			?.addEventListener("click", saveDataContext);
	}

	function fieldTypeCategory(dbType = "") {
		const t = (dbType || "").toLowerCase();
		if (!t) return "dimension";
		if (
			t.includes("int") ||
			t.includes("num") ||
			t.includes("dec") ||
			t.includes("float") ||
			t.includes("real") ||
			t.includes("double") ||
			t.includes("numeric") ||
			t.includes("money")
		)
			return "measure";
		if (t.includes("bool")) return "dimension";
		if (t.includes("date") || t.includes("time")) return "dimension";
		return "dimension";
	}

	const MEASURE_AGG_OPTIONS = ["sum", "avg", "min", "max", "count"];
	const MEASURE_AGG_LABELS = {
		sum: "Sum",
		avg: "Average",
		min: "Min",
		max: "Max",
		count: "Count",
	};

	const DEFAULT_SLOT_BLUEPRINT = [
		{
			key: "dimensions",
			label: "Dimensions",
			accepts: ["dimension"],
			min: 0,
			max: 3,
			hint: "Grouping fields and future drill paths.",
		},
		{
			key: "measures",
			label: "Measures",
			accepts: ["measure"],
			min: 1,
			max: 3,
			hint: "Numeric values rendered in the widget.",
		},
	];

	const SERIES_CHART_BLUEPRINT = [
		{
			key: "category",
			label: "Category Axis",
			accepts: ["dimension"],
			min: 1,
			max: 1,
			hint: "Primary grouping axis (drill-ready).",
		},
		{
			key: "series",
			label: "Series (optional)",
			accepts: ["dimension"],
			min: 0,
			max: 2,
			hint: "Secondary grouping for multi-series and drill.",
		},
		{
			key: "measures",
			label: "Measures",
			accepts: ["measure"],
			min: 1,
			max: 4,
			hint: "Numeric values plotted on the chart.",
		},
	];

	const PIE_CHART_BLUEPRINT = [
		{
			key: "category",
			label: "Slices",
			accepts: ["dimension"],
			min: 1,
			max: 1,
			hint: "Grouping field for slices (drill supported later).",
		},
		{
			key: "measure",
			label: "Value",
			accepts: ["measure"],
			min: 1,
			max: 1,
			hint: "Numeric weight of each slice.",
		},
	];

	const KPI_BLUEPRINT = [
		{
			key: "primaryMeasure",
			label: "Value",
			accepts: ["measure"],
			min: 1,
			max: 1,
			hint: "Primary KPI metric.",
		},
		{
			key: "drillDimension",
			label: "Breakdown (optional)",
			accepts: ["dimension"],
			min: 0,
			max: 1,
			hint: "Dimension reserved for future drill paths.",
		},
	];

	const TABLE_BLUEPRINT = [
		{
			key: "row",
			label: "Row",
			accepts: ["dimension"],
			min: 1,
			max: 1,
			hint: "Primary grouping (rows).",
		},
		{
			key: "column",
			label: "Column",
			accepts: ["dimension"],
			min: 0,
			max: 1,
			hint: "Optional column grouping (pivot).",
		},
	];

	const SLICER_BLUEPRINT = [
		{
			key: "field",
			label: "Field",
			accepts: ["dimension"],
			min: 1,
			max: 1,
			hint: "Field users can slice/filter by.",
		},
	];

	const BUTTON_SLICER_BLUEPRINT = [
		{
			key: "field",
			label: "Field",
			accepts: ["dimension"],
			min: 1,
			max: 1,
			hint: "Field rendered as buttons for quick selection.",
		},
	];

	const WIDGET_SLOT_BLUEPRINTS = {
		kpi: KPI_BLUEPRINT,
		bar: SERIES_CHART_BLUEPRINT,
		line: SERIES_CHART_BLUEPRINT,
		area: SERIES_CHART_BLUEPRINT,
		pie: PIE_CHART_BLUEPRINT,
		doughnut: PIE_CHART_BLUEPRINT,
		table: TABLE_BLUEPRINT,
		slicer: SLICER_BLUEPRINT,
		button_slicer: BUTTON_SLICER_BLUEPRINT,
	};
	const DataPane = (() => {
		const state = {
			searchTerm: "",
			fieldCatalog: null,
			expandedTables: new Set(),
			dragPayload: null,
		};

		function cloneBlueprint(src) {
			return src.map((slot) => ({ ...slot }));
		}

		function getBlueprintForType(type) {
			return cloneBlueprint(
				WIDGET_SLOT_BLUEPRINTS[type] || DEFAULT_SLOT_BLUEPRINT
			);
		}

		function getFieldById(fieldId) {
			return state.fieldCatalog?.flat?.[fieldId] || null;
		}

		// And expose it in the return statement:
		return {
			refreshCatalog,
			renderForWidget,
			getFieldById, // Add this line
		};

		function ensurePaneSkeleton() {
			const group = document.getElementById("dd-data-fields-group");
			if (!group) return null;
			if (!group.dataset.enhanced) {
				group.dataset.enhanced = "1";
				group.innerHTML = `
      <div class="dd-group-title">Fields</div>
      <div class="dd-field-search">
        <input type="search" id="dd-field-search" placeholder="Search fields" autocomplete="off">
      </div>
      <div class="dd-field-library" id="dd-field-library"></div>
      <div class="dd-field-search-results" id="dd-field-search-results"></div>
    	`;
				const input = group.querySelector("#dd-field-search");
				input?.addEventListener("input", (ev) => {
					state.searchTerm = (ev.target.value || "").trim();
					renderFieldLibrary(window.AppState?.currentWidgetId || null);
				});
			}
			return {
				group,
				library: group.querySelector("#dd-field-library"),
				results: group.querySelector("#dd-field-search-results"),
				searchInput: group.querySelector("#dd-field-search"),
			};
		}

		function renderEmptyState(message) {
			const skeleton = ensurePaneSkeleton();
			if (!skeleton) return;
			const { library, results, searchInput } = skeleton;
			if (searchInput) {
				searchInput.value = "";
				searchInput.disabled = true;
			}
			state.searchTerm = "";
			library.style.display = "";
			library.innerHTML = `<div class="dd-field-search-empty">${message}</div>`;
			results.classList.remove("active");
			results.innerHTML = "";
		}

		function showLibraryLoading() {
			const skeleton = ensurePaneSkeleton();
			if (!skeleton) return;
			const { library, results, searchInput } = skeleton;
			if (searchInput) {
				searchInput.disabled = true;
			}
			library.style.display = "";
			library.innerHTML = `<div class="dd-field-search-empty">Loading fields...</div>`;
			results.classList.remove("active");
			results.innerHTML = "";
		}

		function buildFieldDescriptor(table, column, type) {
			const role = fieldTypeCategory(type);
			return {
				id: `${table}.${column}`,
				table,
				column,
				label: column,
				tableLabel: table,
				dataType: type,
				role,
				isNumeric: role === "measure",
			};
		}

		function buildCatalogFromContext(ctx) {
			return {
				signature: JSON.stringify({
					connection: ctx.connection_id,
					tables: [...ctx.tables].sort(),
				}),
				connectionId: ctx.connection_id,
				tables: {},
				flat: {},
			};
		}

		function getAssignedFieldIds(widgetId) {
			const widget = getWidgetConfig(widgetId);
			if (!widget?.dataConfig?.slots) return new Set();
			const result = new Set();
			Object.values(widget.dataConfig.slots).forEach((arr) => {
				(arr || []).forEach((entry) => result.add(entry.fieldId));
			});
			return result;
		}

		function createFieldPill(field) {
			const pill = document.createElement("button");
			pill.type = "button";
			pill.className = "dd-field-pill";
			pill.dataset.fieldId = field.id;
			pill.dataset.role = field.role;
			pill.dataset.table = field.table;
			pill.dataset.column = field.column;
			pill.setAttribute("draggable", "true");
			const roleLabel = field.role === "measure" ? "Measure" : "Dimension";
			pill.innerHTML = `<span>${field.column}</span><span class="dd-field-pill-meta">${field.table}</span><span class="dd-field-pill-badge">${roleLabel}</span>`;
			pill.addEventListener("click", () => {
				const widgetId = window.AppState?.currentWidgetId;
				if (!widgetId) {
					window.showWarning?.("Select a widget to apply fields.");
					return;
				}
				assignFieldFromLibrary(widgetId, field);
			});
			pill.addEventListener("dragstart", (ev) =>
				handleDragStart(ev, { source: "library", fieldId: field.id })
			);
			pill.addEventListener("dragend", handleDragEnd);
			return pill;
		}

		function renderFieldLibrary(widgetId) {
			const skeleton = ensurePaneSkeleton();
			if (!skeleton) return;
			const { library, results, searchInput } = skeleton;
			const catalog = state.fieldCatalog;
			if (!catalog || !Object.keys(catalog.tables).length) {
				renderEmptyState("No columns found for selected tables.");
				return;
			}
			if (searchInput) {
				searchInput.disabled = false;
				if (searchInput.value !== state.searchTerm)
					searchInput.value = state.searchTerm;
			}

			const assigned = widgetId ? getAssignedFieldIds(widgetId) : new Set();
			const term = (state.searchTerm || "").toLowerCase();

			if (term) {
				const matches = [];
				Object.values(catalog.tables).forEach((fields) => {
					fields.forEach((field) => {
						if (assigned.has(field.id)) return;
						if (field.column.toLowerCase().includes(term)) matches.push(field);
					});
				});
				library.style.display = "none";
				results.classList.add("active");
				results.innerHTML = "";
				if (!matches.length) {
					results.innerHTML = `<div class="dd-field-search-empty">No fields match "${state.searchTerm}".</div>`;
					return;
				}
				matches.forEach((field) => results.appendChild(createFieldPill(field)));
				return;
			}

			results.classList.remove("active");
			results.innerHTML = "";
			library.style.display = "";
			library.innerHTML = "";

			Object.entries(catalog.tables).forEach(([tableName, fields]) => {
				const wrapper = document.createElement("div");
				wrapper.className = "dd-field-table";
				if (state.expandedTables.has(tableName)) wrapper.classList.add("open");

				const header = document.createElement("div");
				header.className = "dd-field-table-header";
				header.innerHTML = `<span>${tableName}</span><i class="bi ${
					wrapper.classList.contains("open")
						? "bi-chevron-up"
						: "bi-chevron-down"
				}"></i>`;
				header.addEventListener("click", () => {
					if (state.expandedTables.has(tableName))
						state.expandedTables.delete(tableName);
					else state.expandedTables.add(tableName);
					renderFieldLibrary(widgetId);
				});
				wrapper.appendChild(header);

				const body = document.createElement("div");
				body.className = "dd-field-table-body";
				const available = fields.filter((field) => !assigned.has(field.id));
				if (!available.length) {
					const empty = document.createElement("div");
					empty.className = "dd-field-search-empty";
					empty.textContent = "All fields assigned to this widget.";
					body.appendChild(empty);
				} else {
					available.forEach((field) =>
						body.appendChild(createFieldPill(field))
					);
				}
				wrapper.appendChild(body);
				library.appendChild(wrapper);
			});
		}

		function ensureWidgetSlots(widget, blueprint) {
			widget.dataConfig = widget.dataConfig || {};
			const slots = widget.dataConfig.slots || {};
			const allowedKeys = new Set(blueprint.map((slot) => slot.key));
			Object.keys(slots).forEach((key) => {
				if (!allowedKeys.has(key)) delete slots[key];
			});
			blueprint.forEach((slot) => {
				if (!Array.isArray(slots[slot.key])) slots[slot.key] = [];
			});
			widget.dataConfig.slots = slots;
			return slots;
		}

		function isSlotFull(slotDef, items) {
			if (typeof slotDef.max !== "number" || slotDef.max <= 0) return false;
			return items.length >= slotDef.max;
		}

		function findSlotForField(widget, blueprint, field) {
			const slots = widget.dataConfig.slots;
			const priority =
				field.role === "measure"
					? ["measure", "dimension"]
					: ["dimension", "measure"];
			for (const desired of priority) {
				const candidate = blueprint.find(
					(slot) =>
						slot.accepts.includes(desired) &&
						!isSlotFull(slot, slots[slot.key] || [])
				);
				if (candidate) return candidate;
			}
			return null;
		}

		function getAggregationOptions(assignment) {
			if (assignment.appliedRole !== "measure") return [];
			if (assignment.originRole === "measure" || assignment.isNumeric) {
				return MEASURE_AGG_OPTIONS.slice();
			}
			return ["count"];
		}

		function buildAssignment(field, slotDef) {
			const acceptsMeasure = slotDef.accepts.includes("measure");
			const acceptsDimension = slotDef.accepts.includes("dimension");
			let appliedRole =
				acceptsMeasure && !acceptsDimension ? "measure" : "dimension";
			if (slotDef.accepts.length === 1) {
				appliedRole = slotDef.accepts[0];
			} else if (field.role === "measure" && acceptsMeasure) {
				appliedRole = "measure";
			} else if (field.role === "dimension" && acceptsDimension) {
				appliedRole = "dimension";
			} else if (acceptsMeasure) {
				appliedRole = "measure";
			}
			const assignment = {
				fieldId: field.id,
				table: field.table,
				column: field.column,
				label: field.column,
				tableLabel: field.table,
				dataType: field.dataType,
				originRole: field.role,
				appliedRole,
				isNumeric: field.isNumeric,
				aggregation: null,
			};
			if (assignment.appliedRole === "measure") {
				const options = getAggregationOptions(assignment);
				assignment.aggregation = options[0] || "count";
			}
			return assignment;
		}

		function isFieldAssigned(widget, fieldId) {
			if (!widget?.dataConfig?.slots) return false;
			return Object.values(widget.dataConfig.slots).some((arr) =>
				(arr || []).some((entry) => entry.fieldId === fieldId)
			);
		}

		function assignFieldFromLibrary(widgetId, field) {
			const widget = getWidgetConfig(widgetId);
			if (!widget) return;
			const blueprint = getBlueprintForType(widget.type);
			const slots = ensureWidgetSlots(widget, blueprint);
			if (isFieldAssigned(widget, field.id)) {
				window.showInfo?.("Field already assigned to this widget.");
				return;
			}
			const slotDef = findSlotForField(widget, blueprint, field);
			if (!slotDef) {
				window.showWarning?.("All slots that accept this field are full.");
				return;
			}
			const target = slots[slotDef.key];
			if (isSlotFull(slotDef, target)) {
				window.showWarning?.(
					`${slotDef.label} already has the maximum number of fields.`
				);
				return;
			}
			target.push(buildAssignment(field, slotDef));
			postAssignmentChange(widgetId);
		}

		function renderSlotAssignments(widgetId) {
			const group = document.getElementById("dd-data-config-group");
			if (!group) return;
			group.innerHTML = `<div class="dd-group-title">Widget Data</div>`;
			if (!widgetId) {
				const message = document.createElement("div");
				message.className = "small text-muted";
				message.textContent =
					"Select a widget on the canvas to configure its fields.";
				group.appendChild(message);
				return;
			}

			const widget = getWidgetConfig(widgetId);
			if (!widget) {
				const missing = document.createElement("div");
				missing.className = "small text-muted";
				missing.textContent = "Widget configuration not found.";
				group.appendChild(missing);
				return;
			}

			const info = document.createElement("div");
			info.className = "small text-muted mb-2";
			info.innerHTML = `Widget: <code>${
				widget.title || widget.type || widget.id
			}</code>`;
			group.appendChild(info);

			const blueprint = getBlueprintForType(widget.type);
			const slots = ensureWidgetSlots(widget, blueprint);

			const stack = document.createElement("div");
			stack.className = "dd-slot-stack";
			blueprint.forEach((slotDef) => {
				stack.appendChild(
					renderSlot(slotDef, slots[slotDef.key] || [], widgetId)
				);
			});
			group.appendChild(stack);

			const warnings = validateSlots(blueprint, slots);
			const footer = document.createElement("div");
			footer.className = "dd-slot-footer";
			footer.textContent = warnings.length
				? warnings.join(" ")
				: "Drag or click a field to fill these slots. Remove items to free them up.";
			group.appendChild(footer);
		}

		function formatSlotCount(count, max) {
			if (typeof max === "number" && max > 0) return `${count}/${max}`;
			return count === 1 ? "1 field" : `${count} fields`;
		}

		function renderSlot(slotDef, items, widgetId) {
			const slotEl = document.createElement("div");
			slotEl.className = "dd-slot";
			slotEl.dataset.slotKey = slotDef.key;
			slotEl.dataset.widgetId = widgetId;

			const header = document.createElement("div");
			header.className = "dd-slot-header";
			header.innerHTML = `<span>${
				slotDef.label
			}</span><span class="dd-slot-caption">${formatSlotCount(
				items.length,
				slotDef.max
			)}</span>`;
			slotEl.appendChild(header);

			if (slotDef.hint) {
				const hint = document.createElement("div");
				hint.className = "dd-slot-caption";
				hint.textContent = slotDef.hint;
				slotEl.appendChild(hint);
			}

			const body = document.createElement("div");
			body.className = "dd-slot-body";
			if (!items.length) {
				const placeholder = document.createElement("div");
				placeholder.className = "dd-slot-placeholder";
				placeholder.textContent = "Drag fields here";
				body.appendChild(placeholder);
			} else {
				items.forEach((assignment) =>
					body.appendChild(renderAssignedField(slotDef, assignment, widgetId))
				);
			}
			slotEl.appendChild(body);

			// Add drag event listeners
			slotEl.addEventListener("dragover", (ev) =>
				handleSlotDragOver(ev, slotDef, widgetId)
			);
			slotEl.addEventListener("dragleave", handleSlotDragLeave);
			slotEl.addEventListener("drop", (ev) =>
				handleSlotDrop(ev, slotDef, widgetId)
			);

			return slotEl;
		}

		function renderAssignedField(slotDef, assignment, widgetId) {
			const row = document.createElement("div");
			row.className = "dd-assigned-field";
			row.dataset.fieldId = assignment.fieldId;
			row.dataset.slotKey = slotDef.key;

			const label = document.createElement("div");
			label.className = "dd-assigned-label";
			label.innerHTML = `<span>${assignment.column}</span><span class="dd-field-pill-meta ms-2">${assignment.table}</span>`;
			row.appendChild(label);

			if (assignment.appliedRole === "measure") {
				const select = document.createElement("select");
				select.className = "dd-field-agg";
				const options = getAggregationOptions(assignment);
				options.forEach((agg) => {
					const opt = document.createElement("option");
					opt.value = agg;
					opt.textContent = MEASURE_AGG_LABELS[agg] || agg.toUpperCase();
					select.appendChild(opt);
				});
				select.value = assignment.aggregation || options[0];
				if (options.length === 1) select.disabled = true;
				select.addEventListener("change", (ev) => {
					assignment.aggregation = ev.target.value;
					syncWidgetDataConfig(widgetId);
					if (typeof refreshAllWidgets === "function") refreshAllWidgets();
					if (typeof window.requestSave === "function") window.requestSave();
					else if (typeof autosave === "function") autosave();
				});
				row.appendChild(select);
			}

			const actions = document.createElement("div");
			actions.className = "dd-assigned-actions";
			const removeBtn = document.createElement("button");
			removeBtn.type = "button";
			removeBtn.className = "dd-assigned-remove";
			removeBtn.setAttribute("title", "Remove field");
			removeBtn.innerHTML = '<i class="bi bi-x-lg"></i>';
			removeBtn.addEventListener("click", () =>
				removeAssignment(widgetId, slotDef.key, assignment.fieldId)
			);
			actions.appendChild(removeBtn);
			row.appendChild(actions);

			return row;
		}

		function removeAssignment(widgetId, slotKey, fieldId) {
			const widget = getWidgetConfig(widgetId);
			if (!widget?.dataConfig?.slots?.[slotKey]) return;
			const arr = widget.dataConfig.slots[slotKey];
			const idx = arr.findIndex((entry) => entry.fieldId === fieldId);
			if (idx === -1) return;
			arr.splice(idx, 1);
			postAssignmentChange(widgetId);
		}

		function handleDragStart(event, payload) {
			state.dragPayload = payload;
			try {
				event.dataTransfer.effectAllowed = "copy";
				event.dataTransfer.setData("application/json", JSON.stringify(payload));
			} catch (_) {
				/* ignore */
			}
		}

		function handleDragEnd() {
			state.dragPayload = null;
		}

		function resolveDragPayload(event) {
			if (state.dragPayload) return state.dragPayload;
			try {
				const raw =
					event.dataTransfer.getData("application/json") ||
					event.dataTransfer.getData("text/plain");
				return raw ? JSON.parse(raw) : null;
			} catch (_) {
				return null;
			}
		}

		function slotAcceptsField(slotDef, field) {
			return (
				slotDef.accepts.includes(field.role) ||
				(field.role === "dimension" && slotDef.accepts.includes("measure")) ||
				(field.role === "measure" &&
					slotDef.accepts.includes("dimension") &&
					slotDef.accepts.includes("measure"))
			);
		}

		function handleSlotDragLeave(event) {
			event.currentTarget.classList.remove("dd-slot-drop-active");
		}

		function handleSlotDragOver(event, slotDef, widgetIdArg) {
			event.preventDefault();

			const payload = resolveDragPayload(event);
			if (!payload || payload.source !== "library") return;

			const field = DataPane.getFieldById?.(payload.fieldId);
			if (!field) return;

			if (!slotAcceptsField(slotDef, field)) return;

			const widgetId = widgetIdArg || window.AppState?.currentWidgetId;
			if (!widgetId) return;

			const widget = getWidgetConfig(widgetId);
			if (!widget) return;

			const blueprint = getBlueprintForType(widget.type);
			const slots = ensureWidgetSlots(widget, blueprint);

			if (isFieldAssigned(widget, field.id)) return;
			if (isSlotFull(slotDef, slots[slotDef.key] || [])) return;

			event.dataTransfer.dropEffect = "copy";
			event.currentTarget.classList.add("dd-slot-drop-active");
		}

		function handleSlotDrop(event, slotDef, widgetIdFromArgs) {
			event.preventDefault();
			event.currentTarget.classList.remove("dd-slot-drop-active");

			const payload = resolveDragPayload(event);
			if (!payload || payload.source !== "library") return;

			const widgetId = widgetIdFromArgs || window.AppState?.currentWidgetId;
			if (!widgetId) {
				window.showWarning?.("Select a widget before dropping fields.");
				return;
			}

			const field = DataPane.getFieldById?.(payload.fieldId);
			if (!field) {
				console.warn("Field not found:", payload.fieldId);
				return;
			}

			assignFieldFromLibrary(widgetId, field);
		}

		function handleSlotDrop(event, slotDef, widgetIdFromArgs) {
			event.preventDefault();
			event.currentTarget.classList.remove("dd-slot-drop-active");

			const payload = resolveDragPayload(event);
			if (!payload || payload.source !== "library") return;

			const widgetId =
				widgetIdFromArgs ||
				event.currentTarget?.dataset?.widgetId ||
				window.AppState?.currentWidgetId;

			if (!widgetId) {
				window.showWarning?.(
					"Choose a widget (click its body or the gear) before dropping fields."
				);
				return;
			}

			const field =
				DataPane?.getFieldById?.(payload.fieldId) ||
				window?.AppState?.fieldCatalog?.flat?.[payload.fieldId];
			if (!field) return;

			assignFieldFromLibrary(widgetId, field);
		}

		function validateSlots(blueprint, slots) {
			const warnings = [];
			blueprint.forEach((slot) => {
				const count = (slots[slot.key] || []).length;
				if (slot.min && count < slot.min) {
					warnings.push(
						`${slot.label} requires at least ${slot.min} field${
							slot.min > 1 ? "s" : ""
						}.`
					);
				}
			});
			return warnings;
		}

		function syncWidgetDataConfig(widgetId) {
			const widget = getWidgetConfig(widgetId);
			if (!widget) return;
			const blueprint = getBlueprintForType(widget.type);
			const slots = ensureWidgetSlots(widget, blueprint);

			const flat = [];
			Object.values(slots).forEach((arr) => {
				(arr || []).forEach((item) => flat.push(item));
			});
			const firstField = flat[0] || null;

			widget.dataConfig = widget.dataConfig || {};
			widget.dataConfig.slots = slots;
			widget.dataConfig.table = firstField?.table || null;

			const dimensions = [];
			const measures = [];
			const measureAgg = {};

			Object.entries(slots).forEach(([key, arr]) => {
				(arr || []).forEach((entry) => {
					if (entry.appliedRole === "measure") {
						measures.push(entry.fieldId);
						measureAgg[entry.fieldId] = entry.aggregation || "count";
					} else {
						dimensions.push(entry.fieldId);
					}
				});
			});

			widget.dataConfig.dimensions = Array.from(new Set(dimensions));
			widget.dataConfig.measures = Array.from(new Set(measures));
			widget.dataConfig.measureAgg = measureAgg;
			widget.dataConfig.fields = blueprint.map((slot) => ({
				key: slot.key,
				items: (slots[slot.key] || []).map((entry) => ({
					fieldId: entry.fieldId,
					role: entry.appliedRole,
					aggregation:
						entry.appliedRole === "measure" ? entry.aggregation : null,
				})),
			}));

			widget.data = {
				dimensions: widget.dataConfig.dimensions.slice(),
				measures: widget.dataConfig.measures.slice(),
			};

			if (["bar", "line", "area"].includes(widget.type)) {
				widget.dataConfig.xField = slots.category?.[0]?.fieldId || null;
				widget.dataConfig.seriesFields = (slots.series || []).map(
					(entry) => entry.fieldId
				);
				const measureEntries = slots.measures || [];
				widget.dataConfig.yField = measureEntries[0]?.fieldId || null;
				widget.dataConfig.yFields = measureEntries.map((entry) => ({
					field: entry.fieldId,
					aggregation: entry.aggregation,
				}));
			} else if (["pie", "doughnut"].includes(widget.type)) {
				widget.dataConfig.sliceField = slots.category?.[0]?.fieldId || null;
				const measureEntry = slots.measure?.[0] || null;
				widget.dataConfig.valueField = measureEntry
					? {
							field: measureEntry.fieldId,
							aggregation: measureEntry.aggregation,
					  }
					: null;
			} else if (widget.type === "kpi") {
				const metric = slots.primaryMeasure?.[0] || null;
				widget.dataConfig.valueField = metric
					? { field: metric.fieldId, aggregation: metric.aggregation }
					: null;
				widget.dataConfig.breakdownField =
					slots.drillDimension?.[0]?.fieldId || null;
			} else if (widget.type === "table") {
				// Store Row/Column field ids in dataConfig (exactly two inputs as requested)
				widget.dataConfig.rowField = slots.row?.[0]?.fieldId || null;
				widget.dataConfig.columnField = slots.column?.[0]?.fieldId || null;
			} else if (widget.type === "slicer") {
				widget.dataConfig.field = slots.field?.[0]?.fieldId || null;
			} else if (widget.type === "button_slicer") {
				widget.dataConfig.field = slots.field?.[0]?.fieldId || null;
			}
		}

		function postAssignmentChange(widgetId) {
			renderFieldLibrary(widgetId);
			renderSlotAssignments(widgetId);
			syncWidgetDataConfig(widgetId);
			if (typeof refreshAllWidgets === "function") refreshAllWidgets();
			if (typeof window.requestSave === "function") window.requestSave();
			else if (typeof autosave === "function") autosave();
		}

		async function refreshCatalog() {
			const ctx = window.AppState?.dataContext || {};
			const connId = normalizeConnectionId(ctx.connection_id);
			const tables = Array.isArray(ctx.tables) ? ctx.tables : [];

			// If no connection or tables are selected, clear the panel and stop.
			if (!connId || tables.length === 0) {
				state.fieldCatalog = null;
				renderEmptyState(
					"Select a connection and tables in Data Context to see fields."
				);
				return;
			}

			showLibraryLoading(); // Display a "Loading..." message in the panel

			try {
				const cRes = await fetch(`/api/table-columns/`, {
					method: "POST",
					headers: {
						"Content-Type": "application/json",
						"X-CSRFToken": getCsrfToken(),
					},
					body: JSON.stringify({ connection_id: connId, tables: tables }),
				});

				if (!cRes.ok) throw new Error(`HTTP ${cRes.status}`);
				const cJson = await cRes.json();
				if (!cJson.success) throw new Error(cJson.error || "Columns API error");

				// Build the catalog of fields from the API response
				const catalog = buildCatalogFromContext(ctx);
				(cJson.columns || []).forEach((col) => {
					const field = buildFieldDescriptor(col.source, col.name, col.type);
					catalog.flat[field.id] = field;
					(catalog.tables[field.table] ||= []).push(field);
				});

				// Store the new catalog in our component's state
				state.fieldCatalog = catalog;

				// THIS IS THE KEY: Explicitly call the function to render the fields
				renderFieldLibrary(window.AppState.currentWidgetId);
			} catch (err) {
				console.error("DataPane.refreshCatalog error:", err);
				showToast(`Failed to load columns: ${err.message}`, "danger");
				state.fieldCatalog = null; // Clear catalog on error
				renderEmptyState(`Error: ${err.message}`); // Show the error in the panel
			}
		}

		function renderForWidget(widgetId) {
			console.log("--- DEBUG: DataPane.renderForWidget CALLED ---");
			console.log("Schema data to render:", window.AppState.dataSourceSchema);
			renderFieldLibrary(widgetId);
			renderSlotAssignments(widgetId);
		}

		return {
			refreshCatalog,
			renderForWidget,
			getFieldById: function (fieldId) {
				return state.fieldCatalog?.flat?.[fieldId] || null;
			},
		};
	})();

	async function updateAvailableFields(force = false) {
		return DataPane.refreshCatalog(force);
	}

	// -------------------------------
	// App State
	// -------------------------------
	const root = $("#designer-root");
	if (!root) {
		console.warn("designer-root not found; aborting designer boot.");
		return;
	}

	const CONFIG_URL = root.dataset.configUrl;
	const CONTEXT_URL = root.dataset.contextUrl;
	const WIDGET_URL = root.dataset.widgetUrl; // generic endpoint (optional)

	const dashboardId = root.dataset.dashboardId;

	// public (some libs rely on this)
	window.AppState = {
		dashboardId,
		config: null,
		activePageIndex: 0,
		dataContext: null,
		globalFilters: [],
		theme: "Tableau.Classic10",
		grids: {}, // pageId -> gridstack instance
		selectedWidgetIds: new Set(),
		currentWidgetId: null,
	};

	// -------------------------------
	// Save indicator
	// -------------------------------
	const saveIcon = $("#dd-save-icon");
	const saveMsg = $("#dd-save-msg");

	function setSaveState(state) {
		// states: "idle", "saving", "saved", "error"
		if (!saveIcon || !saveMsg) return;
		if (state === "saving") {
			saveIcon.style.background = "#F59E0B"; // amber
			saveMsg.textContent = "Saving…";
		} else if (state === "saved") {
			saveIcon.style.background = "#28a745"; // green
			saveMsg.textContent = "Saved";
		} else if (state === "error") {
			saveIcon.style.background = "#dc3545"; // red
			saveMsg.textContent = "Save failed";
		} else {
			saveIcon.style.background = "#28a745";
			saveMsg.textContent = "Saved";
		}
	}

	const autosave = debounce(async function autosaveImpl() {
		if (!CONFIG_URL || !window.AppState.config) return;
		setSaveState("saving");
		try {
			// bump updatedAt
			window.AppState.config.updatedAt = new Date().toISOString();
			const res = await fetch(CONFIG_URL, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					"X-CSRFToken": getCsrfToken(),
				},
				body: JSON.stringify(window.AppState.config),
			});
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const json = await res.json().catch(() => ({}));
			if (json && json.success === false)
				throw new Error(json.error || "Save failed");
			setSaveState("saved");
		} catch (err) {
			console.error("Autosave failed:", err);
			setSaveState("error");
			window.showWarning?.("Failed to save dashboard.");
		}
	}, 700);

	// -------------------------------
	// Pages (tabs)
	// -------------------------------
	const tabsUl = $("#dd-page-tabs");
	const addPageBtn = $("#dd-add-page");
	const pageContainer = $("#dd-page-container");
	const emptyHint = $("#dd-empty-hint");

	function ensureConfigPages(cfg) {
		if (!cfg || typeof cfg !== "object")
			return { title: "Untitled", pages: [] };
		if (!Array.isArray(cfg.pages))
			cfg.pages = [{ id: uuid(), title: "Page 1", widgets: [] }];
		return cfg;
	}

	function renderTabs() {
		if (!tabsUl || !window.AppState.config) return;
		tabsUl.innerHTML = "";
		const pages = window.AppState.config.pages || [];
		pages.forEach((p, idx) => {
			const li = document.createElement("li");
			const btn = document.createElement("button");
			btn.className = "dd-tab";
			btn.type = "button";
			btn.textContent = p.title || `Page ${idx + 1}`;
			btn.setAttribute("role", "tab");
			btn.setAttribute(
				"aria-selected",
				idx === window.AppState.activePageIndex ? "true" : "false"
			);
			if (idx === window.AppState.activePageIndex) btn.classList.add("active");
			on(btn, "click", () => switchPage(idx));
			li.appendChild(btn);
			tabsUl.appendChild(li);
		});
	}

	function switchPage(index) {
		window.AppState.activePageIndex = index;
		renderTabs();
		renderActivePage();
	}

	function addPage() {
		const cfg = window.AppState.config;
		const n = (cfg.pages || []).length + 1;
		const page = { id: uuid(), title: `Page ${n}`, widgets: [] };
		cfg.pages.push(page);
		switchPage(cfg.pages.length - 1);
		autosave();
	}

	on(addPageBtn, "click", () => {
		if (addPageBtn.disabled) return;
		addPage();
		window.showSuccess?.("Page added");
	});

	// -------------------------------
	// Grid / Widgets
	// -------------------------------
	function gridIdFor(page) {
		return `grid-${page.id}`;
	}

	function createGridShell(page) {
		pageContainer.innerHTML = ""; // wipe previous
		const wrap = document.createElement("div");
		// GridStack expects .grid-stack
		wrap.className = "grid-stack";
		// also provide legacy id for export_service
		wrap.id = "dashboard-grid";
		pageContainer.appendChild(wrap);
		return wrap;
	}

	function initGrid(page) {
		const holder = createGridShell(page);
		if (!hasGridStack()) {
			holder.innerHTML = `
        <div class="alert alert-warning m-2">
          GridStack library is not loaded. Drag & resize will be disabled.
        </div>`;
			return null;
		}

		const grid = GridStack.init(
			{
				float: true,
				cellHeight: 90,
				minRow: 1,
				disableOneColumnMode: false,
				resizable: { handles: "e, se, s, sw, w" },
				draggable: { handle: ".dd-widget-header" },
				margin: 8,
			},
			holder
		);

		// persist position/size on change
		grid.on("change", (_e, items) => {
			const page = getActivePage();
			if (!page) return;
			items.forEach((it) => {
				const id = it.el.dataset.wid;
				const w = page.widgets.find((x) => x.id === id);
				if (w) {
					w.x = it.x;
					w.y = it.y;
					w.w = it.w;
					w.h = it.h;
				}
			});
			autosave();
		});

		window.AppState.grids[page.id] = grid;
		return grid;
	}

	function getActivePage() {
		const cfg = window.AppState.config;
		if (!cfg) return null;
		const idx = window.AppState.activePageIndex || 0;
		return cfg.pages?.[idx] || null;
	}

	function defaultWidget(type) {
		// rough defaults per type
		const base = {
			id: uuid(),
			type,
			x: 0,
			y: 0,
			w: 4,
			h: 3,
			dataConfig: {},
			displayOptions: {},
		};
		if (type === "kpi") {
			base.w = 3;
			base.h = 2;
		}
		if (type === "table") {
			base.w = 6;
			base.h = 4;
		}
		return base;
	}

	function widgetShell(widget) {
		const el = document.createElement("div");
		el.className = "grid-stack-item";
		el.setAttribute("gs-w", widget.w || 4);
		el.setAttribute("gs-h", widget.h || 3);
		el.setAttribute("gs-x", widget.x || 0);
		el.setAttribute("gs-y", widget.y || 0);
		el.dataset.wid = widget.id;
		el.dataset.widgetId = widget.id;

		const content = document.createElement("div");
		content.className = "grid-stack-item-content";
		// --- MODIFICATION START ---
		content.innerHTML = `
      <div class="dd-widget" data-widget-id="${widget.id}">
        <div class="dd-widget-header">
          <div class="dd-widget-title">${
						widget.title || widget.type || "Widget"
					}</div>
          <div class="dd-widget-actions">
            <button class="btn btn-light btn-sm gs-no-drag"
                    title="Edit"
                    data-action="edit-widget"
                    data-widget-id="${
											widget.id
										}"><i class="bi bi-sliders2"></i></button>
            <button class="btn btn-light btn-sm gs-no-drag"
                    title="Duplicate"
                    data-action="duplicate-widget"
                    data-widget-id="${
											widget.id
										}"><i class="bi bi-files"></i></button>
            <button class="btn btn-light btn-sm gs-no-drag"
                    title="Export CSV"
                    data-action="export-widget"
                    data-widget-id="${
											widget.id
										}"><i class="bi bi-download"></i></button>
            <button class="btn btn-danger btn-sm gs-no-drag"
                    title="Delete"
                    data-action="delete-widget"
                    data-widget-id="${
											widget.id
										}"><i class="bi bi-trash"></i></button>
          </div>
        </div>
        <div class="dd-widget-body" id="wbody-${
					widget.id
				}" style="height: calc(100% - 42px); overflow:hidden;">
          <div class="p-2 text-muted small">Loading.</div>
        </div>
      </div>
    `;
		// --- MODIFICATION END ---
		el.appendChild(content);
		return el;
	}

	function renderWidget(renderer, widget) {
		try {
			// Ensure we're passing the complete widget object, not just type
			renderer.renderWidget(widget);
		} catch (e) {
			console.error("Widget render failed:", e);
			window.showError?.("Widget render failed.");
		}
	}

	function mountWidgetEvents(el, widget) {
		// Clicking anywhere on the widget sets it as current
		el.addEventListener("click", (e) => {
			if (e.target.closest(".dd-widget-actions")) return;

			// Set as current widget
			window.AppState.currentWidgetId = widget.id;

			// Single selection
			clearSelection();
			el.classList.add("selected");
			window.AppState.selectedWidgetIds.add(widget.id);
			updateSelectionUI();

			// Refresh inspector
			if (typeof DataPane?.renderForWidget === "function") {
				DataPane.renderForWidget(widget.id);
			}
		});
	}

	function toggleSelect(el) {
		const id = el.dataset.wid;
		const set = window.AppState.selectedWidgetIds;
		let nextCurrent = window.AppState.currentWidgetId || null;
		if (el.classList.contains("selected")) {
			el.classList.remove("selected");
			set.delete(id);
			if (set.size === 0) {
				nextCurrent = null;
			} else if (id === nextCurrent) {
				const arr = Array.from(set);
				nextCurrent = arr[arr.length - 1];
			}
		} else {
			el.classList.add("selected");
			set.add(id);
			nextCurrent = id;
		}
		window.AppState.currentWidgetId = nextCurrent;
		updateSelectionUI();
		if (typeof DataPane?.renderForWidget === "function") {
			DataPane.renderForWidget(nextCurrent || null);
		}
	}

	function clearSelection() {
		window.AppState.selectedWidgetIds.clear();
		$$(".grid-stack-item.selected").forEach((n) =>
			n.classList.remove("selected")
		);
		window.AppState.currentWidgetId = null;
		updateSelectionUI();
		if (typeof DataPane?.renderForWidget === "function") {
			DataPane.renderForWidget(null);
		}
	}

	function updateSelectionUI() {
		const count = window.AppState.selectedWidgetIds.size;
		$("#dd-duplicate")?.toggleAttribute("disabled", count === 0);
		$("#dd-delete")?.toggleAttribute("disabled", count === 0);
		$("#dd-align-left")?.toggleAttribute("disabled", count < 2);
		$("#dd-align-top")?.toggleAttribute("disabled", count < 2);
		$("#dd-distribute")?.toggleAttribute("disabled", count < 3);
	}

	function exportWidgetCSV(widgetId) {
		if (typeof window.exportService?.exportWidgetToCSV === "function") {
			window.exportService.exportWidgetToCSV(widgetId, "widget.csv");
		} else {
			window.showInfo?.(
				"CSV export will be available after export API is wired."
			);
		}
	}

	function deleteWidget(widgetId) {
		const page = getActivePage();
		if (!page) return;
		const idx = page.widgets.findIndex((w) => w.id === widgetId);
		if (idx < 0) return;
		page.widgets.splice(idx, 1);
		const grid = window.AppState.grids[page.id];
		const el = $(`.grid-stack-item[data-wid="${widgetId}"]`);
		if (grid && el) grid.removeWidget(el);
		window.AppState.selectedWidgetIds.delete(widgetId);
		if (window.AppState.currentWidgetId === widgetId) {
			window.AppState.currentWidgetId = null;
			if (typeof DataPane?.renderForWidget === "function") {
				DataPane.renderForWidget(null);
			}
		}
		updateSelectionUI();
		window.showSuccess?.("Widget deleted");
		autosave();
	}

	function duplicateWidget(widgetId) {
		const page = getActivePage();
		if (!page) return;
		const src = page.widgets.find((w) => w.id === widgetId);
		if (!src) return;
		const copy = JSON.parse(JSON.stringify(src));
		copy.id = uuid();
		copy.x = (src.x || 0) + 1;
		copy.y = (src.y || 0) + 1;
		page.widgets.push(copy);
		addToGrid(copy);
		window.showSuccess?.("Widget duplicated");
		autosave();
	}

	function addToGrid(widget) {
		const page = getActivePage();
		if (!page) return;
		const grid = window.AppState.grids[page.id];
		if (!grid) return;

		const el = widgetShell(widget);
		grid.addWidget(el, {
			x: widget.x || 0,
			y: widget.y || 0,
			w: widget.w || 4,
			h: widget.h || 3,
		});

		// Render content with WidgetRenderer
		const body = $(`#wbody-${widget.id}`);
		if (hasWidgetRenderer() && body) {
			const wr = new window.WidgetRenderer(body);
			// ensure global AppState has dashboardId
			renderWidget(wr, widget);
		} else if (body) {
			body.innerHTML = `<div class="p-2 small text-muted">WidgetRenderer not loaded.</div>`;
		}

		mountWidgetEvents(el, widget);
	}

	function renderActivePage() {
		const page = getActivePage();
		if (!page) return;

		clearSelection();

		// grid instance
		const grid = initGrid(page);

		// Widgets
		(page.widgets || []).forEach((w) => addToGrid(w));

		// empty state
		if (emptyHint) {
			const hasAny = (page.widgets || []).length > 0;
			emptyHint.style.display = hasAny ? "none" : "";
		}
	}

	function addWidget(type) {
		const page = getActivePage();
		if (!page) return;
		const w = defaultWidget(type);
		page.widgets.push(w);
		addToGrid(w);
		autosave();
	}

	// palette & quick-add
	$$(".dd-palette-item[data-widget]").forEach((btn) =>
		on(btn, "click", () => {
			if (btn.disabled) return;
			addWidget(btn.dataset.widget);
		})
	);
	$$(".dd-quick-add[data-widget]").forEach((btn) =>
		on(btn, "click", () => {
			if (btn.disabled) return;
			addWidget(btn.dataset.widget);
		})
	);

	// canvas toolbar buttons (basic)
	on($("#dd-delete"), "click", () => {
		const page = getActivePage();
		if (!page) return;
		const ids = Array.from(window.AppState.selectedWidgetIds);
		if (!ids.length) return;
		ids.forEach(deleteWidget);
		clearSelection();
		autosave();
	});
	on($("#dd-duplicate"), "click", () => {
		const ids = Array.from(window.AppState.selectedWidgetIds);
		ids.forEach(duplicateWidget);
		clearSelection();
		autosave();
	});

	// -------------------------------
	// Inspector (tabs only – content is filled by your logic)
	// -------------------------------
	const insTabs = $$(".dd-inspector-tabs button");
	insTabs.forEach((b) =>
		on(b, "click", () => {
			insTabs.forEach((x) => x.classList.remove("active"));
			b.classList.add("active");
			const pane = b.dataset.tab;
			$$(".dd-pane").forEach((p) => p.classList.add("d-none"));
			$(`.dd-pane[data-pane="${pane}"]`)?.classList.remove("d-none");
		})
	);

	function focusInspectorFor(widgetId) {
		return openWidgetInspector(widgetId);
	}

	async function openWidgetInspector(widgetId) {
		// Ensure context is loaded (no-ops if already present)
		if (!window.AppState?.dataContext) {
			await loadDataContextFromServer();
		}
		await updateAvailableFields();

		window.AppState = window.AppState || {};
		const state = window.AppState;
		state.currentWidgetId = widgetId;

		if (typeof clearSelection === "function") {
			clearSelection();
		}

		const gridItem = document
			.querySelector(`.grid-stack-item [data-widget-id="${widgetId}"]`)
			?.closest(".grid-stack-item");

		if (gridItem) {
			gridItem.classList.add("selected");
			if (
				state.selectedWidgetIds &&
				typeof state.selectedWidgetIds.add === "function"
			) {
				state.selectedWidgetIds.add(widgetId);
			}
		}

		if (typeof updateSelectionUI === "function") {
			updateSelectionUI();
		}

		setInspectorTab("data");
		renderInspectorForWidget(widgetId);
	}

	function setInspectorTab(name) {
		document
			.querySelectorAll(".dd-inspector .dd-inspector-tabs button")
			.forEach((b) => b.classList.toggle("active", b.dataset.tab === name));
		document
			.querySelectorAll(".dd-inspector .dd-pane")
			.forEach((p) => p.classList.toggle("d-none", p.dataset.pane !== name));
	}

	function renderInspectorForWidget(widgetId) {
		DataPane.renderForWidget(widgetId || null);
	}

	function getWidgetConfig(widgetId) {
		if (!widgetId) return null;

		const st = window.AppState || {};
		const pageIndex = st.activePageIndex || 0;
		const pages = st.config?.pages || [];
		const page = pages[pageIndex] || null;

		if (!page || !Array.isArray(page.widgets)) return null;

		return page.widgets.find((w) => w.id === widgetId) || null;
	}

	document.addEventListener("click", (ev) => {
		const btn = ev.target.closest("[data-action]");
		if (!btn) return;

		const action = btn.getAttribute("data-action");
		if (!action) return;

		const container = btn.closest("[data-widget-id]");
		const widgetId = btn.dataset.widgetId || container?.dataset.widgetId;
		if (!widgetId) return;

		ev.preventDefault();
		ev.stopPropagation();

		switch (action) {
			case "edit-widget":
				openWidgetInspector(widgetId);
				break;
			case "refresh-widget":
				if (window.eventBus?.emit)
					eventBus.emit("widget:refresh", { widgetId });
				break;
			case "delete-widget":
				if (typeof deleteWidget === "function") deleteWidget(widgetId);
				break;
			case "duplicate-widget":
				if (typeof duplicateWidget === "function") duplicateWidget(widgetId);
				break;
			case "export-widget":
				if (typeof exportWidgetCSV === "function") exportWidgetCSV(widgetId);
				break;
			default:
				break;
		}
	});

	// -------------------------------
	// Theme (ColorService)
	// -------------------------------
	function buildThemeMenu() {
		const menu = $("#dd-theme-menu");
		if (!menu) return;
		const palettes =
			(window.ColorService && window.ColorService.palettes) || {};
		const keys = Object.keys(palettes);
		if (!keys.length) {
			menu.innerHTML = `<li><span class="dropdown-item text-muted">No palettes</span></li>`;
			return;
		}
		menu.innerHTML = "";
		keys.forEach((k) => {
			const li = document.createElement("li");
			const btn = document.createElement("button");
			btn.className = "dropdown-item";
			btn.textContent = k;
			on(btn, "click", () => {
				window.AppState.theme = k;
				if (window.AppState.config?.theme) {
					window.AppState.config.theme.palette = k;
					autosave();
				}
				window.showInfo?.(`Theme applied: ${k}`);
				// If you have charting, re-render with new colors here.
			});
			li.appendChild(btn);
			menu.appendChild(li);
		});
	}

	// -------------------------------
	// Global filter chips (basic shell)
	// -------------------------------
	const chipRow = $("#dd-chip-row");
	function renderChips() {
		if (!chipRow) return;
		chipRow.innerHTML = "";
		(window.AppState.globalFilters || []).forEach((f, idx) => {
			const el = document.createElement("span");
			el.className = "dd-chip";
			el.innerHTML = `${f.field} ${f.op} ${f.value} <button class="btn btn-link p-0 ms-1" title="Remove"><i class="bi bi-x"></i></button>`;
			on(el.querySelector("button"), "click", () => {
				window.AppState.globalFilters.splice(idx, 1);
				renderChips();
				refreshAllWidgets();
				autosave();
			});
			chipRow.appendChild(el);
		});
	}

	// If a widget emits a filter (click on bar, etc.)
	window.eventBus?.on("filter:apply", (flt) => {
		window.AppState.globalFilters = window.AppState.globalFilters || [];
		window.AppState.globalFilters.push(flt);
		renderChips();
		refreshAllWidgets();
		autosave();
	});

	function refreshAllWidgets() {
		const page = getActivePage();
		if (!page) return;
		(page.widgets || []).forEach((w) => {
			const body = $(`#wbody-${w.id}`);
			if (hasWidgetRenderer() && body) {
				const wr = new window.WidgetRenderer(body);
				renderWidget(wr, w);
			}
		});
	}

	// // -------------------------------
	// // Data Context drawer (simple)
	// // -------------------------------
	function closeModalClean(id) {
		const el = document.getElementById(id);
		if (el) {
			const inst =
				bootstrap.Modal.getInstance(el) ||
				bootstrap.Modal.getOrCreateInstance(el);
			inst.hide();
		}
		// clean up any leftover backdrops (defensive)
		document.querySelectorAll(".modal-backdrop").forEach((b) => b.remove());
		document.body.classList.remove("modal-open");
		document.body.style.removeProperty("padding-right");
	}

	// -------------------------------
	// Title inline edit
	// -------------------------------
	const titleEl = $("#dd-title");
	if (titleEl && titleEl.getAttribute("contenteditable") === "true") {
		on(titleEl, "blur", () => {
			const t = (titleEl.textContent || "").trim() || "Untitled Dashboard";
			window.AppState.config.title = t;
			autosave();
		});
		on(titleEl, "keydown", (e) => {
			if (e.key === "Enter") {
				e.preventDefault();
				titleEl.blur();
			}
		});
	}

	// -------------------------------
	// Boot
	// -------------------------------
	async function boot() {
		// Load config
		try {
			const res = await fetch(CONFIG_URL, {
				headers: { "X-CSRFToken": getCsrfToken() },
			});
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const cfg = await res.json();
			window.AppState.config = ensureConfigPages(cfg);
			window.AppState.theme =
				window.AppState.config?.theme?.palette || window.AppState.theme;
		} catch (err) {
			console.error("Failed to load config:", err);
			window.showError?.("Failed to load dashboard config.");
			return;
		}

		buildThemeMenu();
		renderTabs();
		renderActivePage();

		// quick-add hooks live already
		on($("#dd-toggle-grid"), "click", () => {
			const cont = $(".dd-page-container");
			if (!cont) return;
			const current = getComputedStyle(cont).backgroundImage;
			cont.style.backgroundImage = current
				? "none"
				: "radial-gradient(var(--dd-grid-dot) 1px, transparent 1px)";
		});

		// basic keyboard: ESC clears selection
		on(document, "keydown", (e) => {
			if (e.key === "Escape") clearSelection();
		});

		// Chips initial
		renderChips();

		setSaveState("saved");
	}

	document.addEventListener("DOMContentLoaded", async () => {
		// 1) Load saved data context (connection, tables, joins)
		await loadDataContextFromServer();

		// 2) Populate the Fields panel from the loaded context
		await updateAvailableFields();

		if (typeof wireDataContextModal === "function") wireDataContextModal();
		if (typeof wireInspector === "function") wireInspector();
		if (typeof initGridStack === "function") initGridStack();
		if (typeof hydrateFromServer === "function") hydrateFromServer();
		if (typeof boot === "function") await boot();
	});

	// Temporary debug function - remove after testing
	function debugFieldDrag() {
		console.log("Field Drag Debug:");
		console.log("- Current Widget:", window.AppState?.currentWidgetId);
		console.log(
			"- Field Catalog:",
			window.AppState?.fieldCatalog ? "Loaded" : "Missing"
		);
		console.log("- Data Context:", window.AppState?.dataContext);

		// Test if fields are available
		const testField = Object.values(
			window.AppState?.fieldCatalog?.flat || {}
		)[0];
		console.log("- Sample Field:", testField);
	}

	// Call this in console if dragging still doesn't work
	window.debugFieldDrag = debugFieldDrag;
})();

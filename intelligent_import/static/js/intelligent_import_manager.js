/* intelligent_import/static/js/intelligent_import_manager.js
 * * Front-end controller for the intelligent import workflow.
 */

function getElementSafe(id) {
	return document.getElementById(id) || null;
}

function qs(selector, root = document) {
	return (root || document).querySelector(selector);
}

function qsa(selector, root = document) {
	return Array.from((root || document).querySelectorAll(selector));
}

class IntelligentImportManager {
	constructor() {
		this.csrfToken = this.getCsrfToken();
		this.connections = this.loadConnections();
		this.activeConnectionId =
			document.getElementById("active-connection-id")?.value || null;

		this.currentSession = null;
		this.currentStatus = null;
		this.analysisResults = {};
		this.targetColumns = {};
		this.columnMapping = {};
		this.suggestedMapping = {};
		this.templateOptions = [];
		this.selectedTemplateId = null;
		this.detectedTemplateId = null;
        this.detectedTemplateReason = null;

        // Relationship builder state (single relationship for simplicity)
        this.relationships = [];

		// Prefer template-defined target fields when a template is selected
		this.templateTargetFields = []; // ["field_name", ...]

		this.userType = window.currentUser?.user_type || "";
		this.canEditMapping = ["Admin", "Moderator"].includes(this.userType);

		this.stepOrder = ["upload", "mapping", "validate", "review", "done"];
		this.sectionMap = {
			upload: document.getElementById("upload-step"),
			mapping: document.getElementById("mapping-step"),
			validate: document.getElementById("validate-step"),
			review: document.getElementById("review-step"),
			done: document.getElementById("done-step"),
		};
		this.stepIndicatorMap = {
			upload: document.getElementById("step-upload"),
			mapping: document.getElementById("step-mapping"),
			validate: document.getElementById("step-validate"),
			review: document.getElementById("step-review"),
			done: document.getElementById("step-done"),
		};

		// Initialize DOM elements
		this.progressBanner = document.getElementById("progress-text");
		this.cancelButton = document.getElementById("cancel-import-btn");
		this.deleteButton = document.getElementById("delete-import-btn");
		this.mappingContainer = document.getElementById("column-mapping-container");
		this.mappingTableContainer =
			this.mappingContainer?.querySelector(".card-body") ||
			this.mappingContainer;
		this.saveMappingBtn = document.getElementById("save-mapping-btn");
		this.validationContainer = document.getElementById(
			"data-validation-results"
		);
		this.masterDataLink = document.getElementById("master-data-approval-link");
		this.previewTableHead = document.querySelector("#data-preview-table thead");
		this.previewTableBody = document.querySelector("#data-preview-table tbody");
		this.recentSessionsContainer = document.getElementById("recent-sessions");
		this.templateSelect = document.getElementById("report-template-select");
		this.templateDetectionMessage = document.getElementById(
			"template-detection-message"
		);
		this.mappingPermissionsInfo = document.getElementById(
			"mapping-permissions-info"
		);
		this.mappingReadonlyBanner = document.getElementById(
			"mapping-readonly-banner"
		);
		this.createTemplateContainer = document.getElementById(
			"create-template-container"
		);

		this.$templateToolbar = document.getElementById("template-toolbar");
		this.$addToTemplateBtn = document.getElementById("add-to-template-btn");
		this.$createTemplateBtn = document.getElementById("create-template-btn");
		this.$manageBtn = document.getElementById("manage-templates-btn");
		this.$mtModal = document.getElementById("manageTemplatesModal");
		this.$mtTbody = document.getElementById("mt-table-body");
		this.$mtNewName = document.getElementById("mt-new-name");
		this.$mtNewDesc = document.getElementById("mt-new-desc");
		this.$mtCreate = document.getElementById("mt-create-btn");

		this.builderState = { newTableName: null, newTableRole: "fact" };

		this.proposedTables = []; // [{ name, role, clientId }]
		this.templateMapping = this.templateMapping || {};
		this.builderState = this.builderState || {};

		// small utils
		this.normalizeSnake = (s) =>
			String(s || "")
				.trim()
				.toLowerCase()
				.replace(/[^a-z0-9]+/g, "_")
				.replace(/^_+|_+$/g, "");

		this.addProposedTable = (name, role = "fact") => {
			const n = this.normalizeSnake(name || "new_table");
			if (!this.proposedTables.some((t) => t.name === n)) {
				this.proposedTables.push({
					name: n,
					role: role === "ref" ? "ref" : "fact",
					clientId: `t_${Math.random().toString(36).slice(2, 8)}`,
				});
			}
			// inject the “[New] …” option into every table select
			document.querySelectorAll(".tm-table, .mtbl").forEach((sel) => {
				const value = `__new__:${n}`;
				if (![...sel.options].some((o) => o.value === value)) {
					const opt = document.createElement("option");
					opt.value = value;
					opt.textContent = `[New] ${n}`;
					const anchor = sel.querySelector('option[value="__new__"]');
					sel.insertBefore(opt, anchor ? anchor.nextSibling : sel.firstChild);
				}
			});
		};

		this.buildTableOptionsHTML = () => {
			const exist = (this.availableTables || [])
				.map((tbl) => `<option value="${tbl}">${tbl}</option>`)
				.join("");
			const proposed = this.proposedTables
				.map(
					(t) => `<option value="__new__:${t.name}">[New] ${t.name}</option>`
				)
				.join("");
			return `<option value="" selected disabled>Select table</option>
				  <option value="__new__">+ New Table…</option>
				  ${proposed}
				  ${exist}`;
		};

		this.buildColumnOptionsHTML = (columns = []) => {
			const opts = (columns || [])
				.map((c) => `<option value="${c}">${c}</option>`)
				.join("");
			return `<option value="" selected disabled>Select column</option>
				  <option value="__new__">+ New Column…</option>
				  ${opts}`;
		};

		// === Mapping UI state ===
		this.templateMapping = {}; // { "Uploaded Header": { table, column, create_table?, create_column?, type? } }
		this.availableTables = []; // ["public.orders", "public.users", ...]
		this.tableColumns = {}; // { "public.orders": ["id","order_no",...], ... }

		// Mounts
		this.$mappingCardBody = document.querySelector(
			"#column-mapping-container .card-body"
		);
		this.$saveMappingBtn = document.getElementById("save-mapping-btn");

		// Keep Save disabled until at least one mapping exists
		if (this.$saveMappingBtn) this.$saveMappingBtn.disabled = true;
		// Normalize initial visibility: remove inline display:none so JS can control it
		[
			this.$templateToolbar,
			this.$addToTemplateBtn,
			this.$createTemplateBtn,
		].forEach((el) => {
			if (!el) return;
			el.style.removeProperty("display"); // kill inline display:none
			el.classList.add("d-none"); // start hidden via class
		});

		this.bindEvents();

		this.updateTemplateButtonsVisibility = (forceShow = false) => {
			const hasAnalysis = !!(
				this.analysisResults && Object.keys(this.analysisResults).length
			);
			const hasSession = !!this.currentSession;
			const show = forceShow || hasAnalysis || hasSession;
			[
				this.$templateToolbar,
				this.$addToTemplateBtn,
				this.$createTemplateBtn,
				this.$manageBtn,
			].forEach((el) => {
				if (!el) return;
				el.classList.toggle("d-none", !show);
				// also flip inline style in case any CSS/inline rules were applied
				el.style.display = show ? "" : "none";
			});
		};

		// Fetch unread notifications at load and show via toast-notifications.js
		this.fetchAndToastNotifications?.();

		this.updateConnectionDisplay();
		this.refreshSessions();
		this.goToStep("upload");
		this.updateMappingEditState();
		// Recalculate Save button enable based on preselected mapping
		try {
			this.mappingTableContainer
				.querySelectorAll(".target-field-select, .import-mode-select")
				.forEach((sel) =>
					sel.addEventListener("change", () => this.updateSaveMappingEnabled())
				);
		} catch {}
        this.updateSaveMappingEnabled();
        this.updateSessionActionButtons();
        // Preload tables for relationships and render the panel
        this.fetchTablesForConnection().then(() => this.renderRelationshipsPanel());
        // Re-render relationships when mapping UI renders or template selection changes
        try {
            document.addEventListener('ii-mapping-rendered', () => this.renderRelationshipsPanel());
        } catch(_) {}
	}

	getImportMode() {
		return document.getElementById("import-mode")?.value || "auto";
	}

	getActiveConnectionId() {
		return this.activeConnectionId;
	}

	async fetchTablesForConnection() {
		const connId = this.getActiveConnectionId();
		this.availableTables = [];
		if (!connId) return;

		const url = `/intelligent-import/api/connections/${encodeURIComponent(
			connId
		)}/tables/`;
		try {
			const r = await fetch(url, { credentials: "same-origin" });
			const j = await r.json();
			if (Array.isArray(j)) {
				this.availableTables = j.map((t) => `.${t}`);
			} else if (j?.success && Array.isArray(j.tables)) {
				this.availableTables = j.tables.map((t) => `${t.schema}.${t.table}`);
			}
		} catch (e) {
			console.warn("Could not load tables for connection:", e);
		}
	}

	ensureNewTableName(defaultLabel = "new_table") {
		if (!this.builderState.newTableName) {
			const role = (
				window.prompt(
					"Table role? (fact/ref)",
					this.builderState.newTableRole
				) || "fact"
			).toLowerCase();
			const name = (
				window.prompt("New table name (snake_case)", defaultLabel) ||
				defaultLabel
			).trim();
			this.builderState.newTableRole = role === "ref" ? "ref" : "fact";
			this.builderState.newTableName = name;
		}
		return this.builderState.newTableName;
	}

    updateSaveMappingEnabled() {
		const btn = document.getElementById("save-mapping-btn");
		if (!btn) return;
		const hasAny = Object.values(this.templateMapping || {}).some(
			(m) => (m?.table && m?.column) || m?.create_table || m?.create_column
		);
		const editableStatuses = ["template_suggested", "mapping_defined"];
		const statusEditable = editableStatuses.includes(this.currentStatus);
		btn.disabled =
			!hasAny ||
			!this.currentSession ||
			!this.canEditMapping ||
			!statusEditable;
    }

    // Render the Relationships panel inputs
    renderRelationshipsPanel() {
        const wrap = qs('#relationships-panel');
        if (!wrap) return;
        // Fill parent/child table selects
        const parentSel = qs('#rel-parent-table');
        const childSel = qs('#rel-child-table');
        const tables = Array.isArray(this.availableTables) ? this.availableTables : [];
        const prettify = (t) => String(t||'').replace(/^\./, '');
        if (parentSel) {
            parentSel.innerHTML = tables.map(function(t){ var v = prettify(t); return '<option value="'+v+'">'+v+'</option>'; }).join('');
        }
        if (childSel) {
            const dest = (this.getDestinationTables?.() || []);
            const childOptions = dest.length ? dest : tables.map(prettify);
            childSel.innerHTML = childOptions.map(function(t){ return '<option value="'+t+'">'+t+'</option>'; }).join('');
        }
        // Fill natural key from file columns (use keys of suggested mapping or current mapping grid)
        const nkSel = qs('#rel-natural-key');
        if (nkSel) {
            let headers = [];
            const sug = this.suggestedMapping || this.analysisResults?.suggested_mapping || {};
            headers = Object.keys(sug || {});
            if (!headers.length) {
                try {
                    const rows = Array.from(document.querySelectorAll('#column-mapping-container tr[data-source-column]'));
                    headers = rows.map(function(r){ return r.getAttribute('data-source-column'); }).filter(Boolean);
                } catch(_) {}
            }
            nkSel.innerHTML = (headers || []).map(function(h){ return '<option value="'+h+'">'+h+'</option>'; }).join('');
        }
        // Suggest default child FK name from parent
        const fkInput = qs('#rel-child-fk');
        if (fkInput && parentSel) {
            const setFk = () => {
                const p = parentSel.value || '';
                const base = p.split('.').pop() || p;
                fkInput.value = base ? (base + '_id') : 'parent_id';
            };
            if (!fkInput.value) setFk();
            parentSel.addEventListener('change', setFk);
        }
    }

	collectMappingFromUI() {
		const container = document.querySelector(
			"#column-mapping-container .card-body"
		);
		const rows = [...(container?.querySelectorAll(".row[data-header]") || [])];

		const mapping = {};
		for (const row of rows) {
			const header = row.dataset.header;
			const tableSel = row.querySelector(".tm-table");
			const colSel = row.querySelector(".tm-column");
			if (!header || !tableSel || !colSel) continue;

			const tVal = tableSel.value || "";
			const cVal = colSel.value || "";

			// skip if nothing picked in this row
			if (!tVal && !cVal) continue;

			const entry = (mapping[header] = {});

			// TABLE
			if (tVal === "__new__" || tVal === "__reuse_new__") {
				// prompt once per session and reuse
				this.builderState = this.builderState || {
					newTableName: null,
					newTableRole: "fact",
				};
				if (!this.builderState.newTableName) {
					const role = (
						window.prompt(
							"Table role? (fact/ref)",
							this.builderState.newTableRole
						) || "fact"
					).toLowerCase();
					const guess = header?.trim() || "new_table";
					const name = (
						window.prompt("New table name (snake_case)", guess) || guess
					).trim();
					this.builderState.newTableRole = role === "ref" ? "ref" : "fact";
					this.builderState.newTableName = name;
				}
				entry.table = null;
				entry.create_table = {
					role: this.builderState.newTableRole,
					label: this.builderState.newTableName,
					client_id: `t_${Math.random().toString(36).slice(2, 8)}`,
				};
			} else if (tVal) {
				entry.table = tVal.includes(".") ? tVal.split(".").pop() : tVal; // keep bare table
			}

			// COLUMN
			if (cVal === "__new__") {
				const proposal = (
					window.prompt("New column name (snake_case)", header) || header
				).trim();
				entry.column = null;
				entry.create_column = {
					table: entry.table || null,
					table_client_id: entry.create_table?.client_id || null,
					label: proposal,
					type: "TEXT",
				};
			} else if (cVal) {
				entry.column = cVal;
			}
		}

        // Also collect relationship panel config into instance state
        try {
            const enabled = qs('#rel-enable-switch')?.checked !== false;
            if (enabled) {
                const parentTable = qs('#rel-parent-table')?.value || '';
                const nk = qs('#rel-natural-key')?.value || '';
                const childTable = qs('#rel-child-table')?.value || '';
                const fkCol = (qs('#rel-child-fk')?.value || '').trim();
                const nkNormalize = !!qs('#rel-nk-normalize')?.checked;
                const addIndex = !!qs('#rel-add-index')?.checked;
                const addFkConstraint = !!qs('#rel-add-fk-constraint')?.checked;
                this.relationships = [
                    {
                        parent_table: parentTable,
                        natural_key_column: nk,
                        child_table: childTable,
                        child_fk_column: fkCol,
                        nk_normalize: nkNormalize,
                        add_index: addIndex,
                        add_fk_constraint: addFkConstraint,
                    },
                ];
            } else {
                this.relationships = [];
            }
        } catch (_) {}
        return mapping;
	}

	updateTargetFieldBadge(rowEl, tableName, colName) {
		const badge = rowEl.querySelector(".target-field-badge");
		if (!badge) return;
		if (tableName && colName) badge.textContent = `${tableName}.${colName}`;
		else if (tableName) badge.textContent = `${tableName}.(choose…)`;
		else badge.textContent = "–";
	}

	getElementSafe(id) {
		return getElementSafe(id);
	}

	bindEvents() {
		// Template builder actions
		this.getElementSafe("add-to-template-btn")?.addEventListener("click", () =>
			this.openTemplateBuilderFromCurrentFile()
		);
		this.getElementSafe("create-template-btn")?.addEventListener("click", () =>
			this.openTemplateBuilderManual()
		);
		this.getElementSafe("tpl-add-header")?.addEventListener("click", () =>
			this.addTemplateHeaderRow()
		);
		this.getElementSafe("tpl-save-btn")?.addEventListener("click", () =>
			this.saveTemplateAndApply()
		);

		this.$manageBtn?.addEventListener("click", (e) => {
			e.preventDefault();
			this.openTemplateManager();
		});
		this.$mtCreate?.addEventListener("click", async (e) => {
			e.preventDefault();
			await this.mtCreateTemplate();
		});

		// File upload actions
		const browseBtn = this.getElementSafe("browse-import-btn");
		const fileInput = this.getElementSafe("import-file");
		const dropArea = this.getElementSafe("file-upload-area");
		if (browseBtn && fileInput) {
			browseBtn.addEventListener("click", () => fileInput.click());
		}
		if (fileInput) {
			fileInput.addEventListener("change", (e) => {
				const f = e.target?.files?.[0];
				if (f) this.uploadFile(f);
			});
		}
		if (dropArea) {
			["dragenter", "dragover"].forEach((evt) =>
				dropArea.addEventListener(evt, (e) => {
					e.preventDefault();
					e.stopPropagation();
					dropArea.classList?.add("drag-over");
				})
			);
			["dragleave", "drop"].forEach((evt) =>
				dropArea.addEventListener(evt, (e) => {
					e.preventDefault();
					e.stopPropagation();
					dropArea.classList?.remove("drag-over");
				})
			);
			dropArea.addEventListener("drop", (e) => {
				const f = e.dataTransfer?.files?.[0];
				if (f) this.uploadFile(f);
			});
		}

		// Workflow buttons
		this.getElementSafe("proceed-mapping-btn")?.addEventListener(
			"click",
			async () => {
				await this.loadTablesAndRender("public");
				this.goToStep("mapping"); // or however you reveal the Mapping step
			}
		);
		this.$saveMappingBtn = document.getElementById("save-mapping-btn");
		this.$saveMappingBtn.addEventListener("click", () => {
			const mapping = this.collectMappingFromUI();
			if (Object.keys(mapping).length === 0) {
				this.showError("Map at least one column before saving.");
				return;
			}
			const body = {
				mapping: mapping,
				import_mode: this.getImportMode(),
			};
			this.persistMapping(body);
		});
		// Final review navigation and actions
		this.getElementSafe("proceed-review-btn")?.addEventListener(
			"click",
			async () => {
				await this.loadFinalReview();
				this.updateReviewSummary?.();
				this.goToStep("review");
			}
		);
		this.getElementSafe("review-back-btn")?.addEventListener("click", () =>
			this.goToStep("validate")
		);
		this.getElementSafe("back-to-mapping-btn")?.addEventListener(
			"click",
			async () => {
				// If status not editable, request reopen on server
				const editableStatuses = ["template_suggested", "mapping_defined"];
				if (!editableStatuses.includes(this.currentStatus)) {
					try {
						await this.fetchJson(
							`/intelligent-import/api/session/${this.currentSession}/reopen-mapping/`,
							{
								method: "POST",
								body: JSON.stringify({}),
							}
						);
						this.currentStatus = "mapping_defined";
						this.showInfo?.("Session reopened for mapping.");
					} catch (e) {
						this.showError?.(e.message || "Failed to reopen mapping.");
						return;
					}
				}
				this.updateMappingEditState();
				this.goToStep("mapping");
			}
		);
		this.getElementSafe("review-execute-btn")?.addEventListener("click", () =>
			this.executeImport()
		);
		this.getElementSafe("review-request-approval-btn")?.addEventListener(
			"click",
			async () => {
				if (!this.currentSession) return;
			try {
				const resp = await this.fetchJson(
					`/intelligent-import/api/session/${this.currentSession}/request-approval/`,
					{ method: "POST", body: JSON.stringify({}) }
				);
				if (!resp?.success)
					throw new Error(resp?.error || "Failed to request approval");
				this.showSuccess?.(resp.message || "Approval request sent.");
				if (resp && (resp.status || resp.session_status)) {
					this.currentStatus = resp.status || resp.session_status;
					this.updateSessionActionButtons?.();
				}
				await this.loadFinalReview();
			} catch (e) {
				this.showError?.(e.message || String(e));
			}
			}
		);
		this.getElementSafe("dup-approve-all-btn")?.addEventListener("click", () =>
			this.setAllDuplicateDecisions("approve")
		);
		this.getElementSafe("dup-skip-all-btn")?.addEventListener("click", () =>
			this.setAllDuplicateDecisions("skip")
		);
		this.getElementSafe("review-save-decisions-btn")?.addEventListener(
			"click",
			() => this.saveDuplicateDecisionsFromUI()
		);
		this.getElementSafe("cancel-import-btn")?.addEventListener("click", (e) => {
			e.preventDefault();
			this.cancelCurrentSession();
		});
		this.getElementSafe("delete-import-btn")?.addEventListener("click", (e) => {
			e.preventDefault();
			this.deleteCurrentSession();
		});

		// Template selection change
		if (this.templateSelect) {
			this.templateSelect.addEventListener("change", (e) =>
				this.onTemplateSelectionChange(e)
			);
		}

		// Recent sessions click-to-enter (event delegation)
		if (this.recentSessionsContainer) {
			this.recentSessionsContainer.addEventListener("click", (e) => {
				const card = e.target?.closest?.(".session-card");
				const id = card?.dataset?.sessionId;
				if (id) this.enterSession(id);
			});
		}
	}

	async openTemplateBuilderFromCurrentFile() {
		const fileName =
			this.analysisResults?.file_analysis?.original_filename || "New Report";
		const headers = this.analysisResults?.file_analysis?.columns || [];

		await this.openTemplateBuilder({
			name: fileName.replace(/\.[^/.]+$/, "").trim(),
			headers: headers,
		});
	}

	async openTemplateBuilderManual() {
		await this.openTemplateBuilder({
			name: "",
			headers: [],
		});
	}

    async openTemplateBuilder({ name, headers }) {
        await this.fetchTablesForConnection();
        const modalEl = this.getElementSafe("templateBuilderModal");
		if (!modalEl) {
			console.error("Template builder modal not found");
			return;
		}

		// TODO: Implement this.fetchTablesOnly
		// await this.fetchTablesOnly("public"); // ensures dropdown has existing tables

		// Hide/disable Master Data Source & Output Field columns for now
		const tableEl = document.getElementById("tpl-headers-table");
		if (tableEl) {
			// hide headers
			const ths = tableEl.querySelectorAll("thead th");
			// indexes in your HTML: 0=Source, 1=Target Table, 2=Target Column, 3=Data Type, 4=Master Data, 5=Output Field, ...
			if (ths[4]) ths[4].classList.add("d-none");
			if (ths[5]) ths[5].classList.add("d-none");
		}

		// Set basic template info
		this.getElementSafe("tpl-name").value = name || "";
		this.getElementSafe("tpl-target-table").value =
			this.analysisResults?.suggested_target?.table_name || "";

		// 3) rows
		const tbody = modalEl.querySelector("#tpl-headers-table tbody");
		tbody.innerHTML = "";

		let suggestions = {};
		if (headers?.length) {
			try {
				const resp = await this.fetchJson(
					"/intelligent-import/api/suggest-mapping/",
					{
						method: "POST",
						body: JSON.stringify({
							headers,
							session_id: this.currentSession || null,
						}),
					}
				);
				suggestions = resp?.suggestions || {};
			} catch {
				/* proceed without suggestions */
			}
			headers.forEach((h) =>
				this.addTemplateHeaderRow(h, suggestions[h] || [])
			);
		} else {
			this.addTemplateHeaderRow();
		}

		// 4) show modal
		// Ensure modal is not inert before showing (for a11y and focus)
		try {
			modalEl.removeAttribute("inert");
		} catch {}
		const bsModal = bootstrap.Modal.getOrCreateInstance(modalEl, {
			backdrop: "static",
			focus: true,
		});
        bsModal.show();

        // Populate template-level relationships UI
        this.renderTemplateRelationshipsPanel(headers);
    }

	async fetchTablesForConnection() {
		const connId = this.getActiveConnectionId();
		this.availableTables = [];
		if (!connId) return;

		const url = `/intelligent-import/api/connections/${encodeURIComponent(
			connId
		)}/tables/`;
		try {
			const r = await fetch(url, { credentials: "same-origin" });
			const j = await r.json();
			// data_views.get_visible_tables_for_connection typically returns a flat list of table names
			if (Array.isArray(j)) {
				this.availableTables = j.slice(); // ["users","orders",...]
			} else if (j?.success && Array.isArray(j.tables)) {
				// fallback shape {tables:[{schema,table}]}
				this.availableTables = j.tables.map((t) => t.table);
			}
		} catch (e) {
			console.warn("Could not load tables for connection:", e);
			this.availableTables = [];
		}
	}

    addTemplateHeaderRow(sourceHeader = "", suggested = []) {
		const tbody = document.querySelector("#tpl-headers-table tbody");
		if (!tbody) return;

		const row = document.createElement("tr");
		row.className = "tpl-row";
		row.innerHTML = `
		  <td><input class="form-control form-control-sm src" value="${this.escapeHtml(
				sourceHeader
			)}" ${sourceHeader ? "readonly" : ""}></td>

		  <td>
			<select class="form-select form-select-sm mtbl">
			  ${this.buildTableOptionsHTML()}
			</select>
		  </td>

		  <td>
			<select class="form-select form-select-sm mcol" disabled>
			  ${this.buildColumnOptionsHTML([])}
			</select>
		  </td>

		  <td>
			<span class="badge bg-light text-dark target-field-badge">–</span>
		  </td>

		  <td>
			<select class="form-select form-select-sm typ">
			  <option value="">(auto)</option>
			  <option value="text">text</option>
			  <option value="integer">integer</option>
			  <option value="decimal">decimal</option>
			  <option value="date">date</option>
			  <option value="datetime">datetime</option>
			  <option value="boolean">boolean</option>
			</select>
		  </td>

		  <td class="d-none"><select class="form-select form-select-sm mst" disabled><option value="">(none)</option></select></td>
		  <td class="d-none"><input class="form-control form-control-sm out" disabled></td>

		  <td class="text-center"><input type="checkbox" class="form-check-input req"></td>
		  <td><input class="form-control form-control-sm def" placeholder="Default value"></td>
		`;
		tbody.appendChild(row);

		const tableSel = row.querySelector(".mtbl");
		const colSel = row.querySelector(".mcol");
		tableSel.addEventListener("change", (e) => this.onTemplateTableChange(e));
		colSel.addEventListener("change", (e) => this.onTemplateColumnChange(e));
    }

    // Populate template relationships dropdowns
    renderTemplateRelationshipsPanel(headers = []) {
        const list = qs('#tpl-rel-list');
        if (!list) return;
        const tables = Array.isArray(this.availableTables) ? this.availableTables : [];
        const dest = (this.getDestinationTables?.() || []);
        // Helper to build one relationship row
        const rowHtml = () => {
            const tableOpts = tables.map(t => `<option value="${t}">${t}</option>`).join('');
            const childOpts = (dest.length ? dest : tables).map(t => `<option value="${t}">${t}</option>`).join('');
            const hdrs = (Array.isArray(headers) && headers.length)
                ? headers
                : Array.from(document.querySelectorAll('#tpl-headers-table tbody tr.tpl-row .src')).map(i=>i.value).filter(Boolean);
            const nkOpts = hdrs.map(h => `<option value="${h}">${h}</option>`).join('');
            return `
                <div class="tpl-rel-item border rounded p-2 mb-2">
                  <div class="row g-2 align-items-end">
                    <div class="col-md-4">
                      <label class="form-label small">Parent Table</label>
                      <select class="form-select form-select-sm tpl-rel-parent-table">${tableOpts}</select>
                    </div>
                    <div class="col-md-4">
                      <label class="form-label small">Natural Key (file column)</label>
                      <select class="form-select form-select-sm tpl-rel-natural-key">${nkOpts}</select>
                      <div class="form-check mt-1">
                        <input class="form-check-input tpl-rel-nk-normalize" type="checkbox" checked>
                        <label class="form-check-label small">Case-insensitive, trim spaces</label>
                      </div>
                      <div class="form-check mt-1">
                        <input class="form-check-input tpl-rel-enforce-unique" type="checkbox">
                        <label class="form-check-label small">Enforce unique on parent natural key</label>
                      </div>
                    </div>
                    <div class="col-md-4">
                      <label class="form-label small">Child FK Column</label>
                      <input type="text" class="form-control form-control-sm tpl-rel-child-fk" placeholder="e.g., buyer_id">
                      <div class="form-check mt-1">
                        <input class="form-check-input tpl-rel-add-index" type="checkbox" checked>
                        <label class="form-check-label small">Add index</label>
                      </div>
                    </div>
                  </div>
                  <div class="row g-2 mt-2">
                    <div class="col-md-6">
                      <label class="form-label small">Child Table</label>
                      <select class="form-select form-select-sm tpl-rel-child-table">${childOpts}</select>
                    </div>
                    <div class="col-md-6">
                      <div class="form-check mt-4">
                        <input class="form-check-input tpl-rel-add-fk-constraint" type="checkbox">
                        <label class="form-check-label small">Add FK constraint (advanced)</label>
                      </div>
                    </div>
                  </div>
                  <div class="row g-2 mt-2">
                    <div class="col-md-12">
                      <label class="form-label small">Parent PK Strategy (when not auto-increment)</label>
                      <div class="d-flex gap-2 align-items-center flex-wrap">
                        <select class="form-select form-select-sm tpl-rel-pk-mode" style="max-width:200px">
                          <option value="auto" selected>Auto/DB default</option>
                          <option value="uuid">UUID</option>
                          <option value="max_plus_one">Numeric MAX+1</option>
                          <option value="pattern">Pattern (prefix + zero-padded)</option>
                        </select>
                        <input type="text" class="form-control form-control-sm tpl-rel-pk-prefix" placeholder="Prefix (e.g., BUY)" style="max-width:180px">
                        <input type="number" min="1" class="form-control form-control-sm tpl-rel-pk-width" placeholder="Width (e.g., 6)" style="max-width:140px">
                        <button type="button" class="btn btn-outline-secondary btn-sm tpl-rel-remove">Remove</button>
                      </div>
                    </div>
                  </div>
                </div>`;
        };

        // Hook Add button
        const addBtn = qs('#tpl-rel-add');
        if (addBtn) {
            addBtn.onclick = () => {
                list.insertAdjacentHTML('beforeend', rowHtml());
                // default child_fk to parent + _id
                const item = list.querySelector('.tpl-rel-item:last-child');
                const parentSel = item.querySelector('.tpl-rel-parent-table');
                const fkInput = item.querySelector('.tpl-rel-child-fk');
                const setFk = () => { const p = parentSel.value || ''; const base = p.split('.').pop() || p; fkInput.value = base ? (base + '_id') : 'parent_id'; };
                setFk();
                parentSel.addEventListener('change', setFk);
                item.querySelector('.tpl-rel-remove').onclick = () => item.remove();
            };
        }

        // Import from Data Model button
        const importBtn = qs('#tpl-rel-import-dm');
        if (importBtn) {
            importBtn.onclick = async () => {
                try {
                    const connId = this.getActiveConnectionId();
                    if (!connId) { this.showError?.('Select a connection first.'); return; }
                    const model = await this.fetchJson(`/api/model/get/${encodeURIComponent(connId)}/`);
                    if (!model?.success) { this.showError?.('Could not load data model.'); return; }
                    const joins = Array.isArray(model.joins) ? model.joins : [];
                    const tables = Array.isArray(model.tables) ? model.tables : [];
                    const tableCols = {}; tables.forEach(t => { tableCols[t.name] = t.columns || []; });
                    // Convert joins to relationships (prefer one-to-many: left is parent, right is child)
                    const rels = [];
                    joins.forEach(j => {
                        const lt = j.left_table, rt = j.right_table;
                        const lc = j.left_column, rc = j.right_column;
                        const card = String(j.cardinality || '').toLowerCase();
                        if (!lt || !rt || !lc || !rc) return;
                        let parent = lt, child = rt, childFk = rc;
                        if (card.includes('many-to-one')) { parent = rt; child = lt; childFk = lc; }
                        // Natural key guess: pick first unique or 'name' column
                        let nk = '';
                        const cols = tableCols[parent] || [];
                        const uniq = cols.find(c => c.is_unique) || cols.find(c => /name$/i.test(c.name));
                        if (uniq) nk = uniq.name;
                        rels.push({ parent, child, nk, fk: childFk });
                    });
                    if (!rels.length) { this.showError?.('No joins found in data model.'); return; }
                    // Render rows for each relationship
                    list.innerHTML = '';
                    rels.forEach(r => {
                        addBtn?.click();
                        const item = list.querySelector('.tpl-rel-item:last-child');
                        if (!item) return;
                        const parentSel = item.querySelector('.tpl-rel-parent-table');
                        const childSel = item.querySelector('.tpl-rel-child-table');
                        const nkSel = item.querySelector('.tpl-rel-natural-key');
                        const fkInput = item.querySelector('.tpl-rel-child-fk');
                        if (parentSel) parentSel.value = r.parent;
                        if (childSel) childSel.value = r.child;
                        if (nkSel) {
                            // include guessed NK if not present in options
                            if (r.nk && !Array.from(nkSel.options).some(o=>o.value===r.nk)) {
                                nkSel.insertAdjacentHTML('beforeend', `<option value="${r.nk}">${r.nk}</option>`);
                            }
                            nkSel.value = r.nk || nkSel.value;
                        }
                        if (fkInput) fkInput.value = r.fk || fkInput.value;
                    });
                } catch (e) {
                    console.warn('Import from Data Model failed:', e);
                    this.showError?.('Failed to import relationships from Data Model.');
                }
            };
        }

        // Initial render from existing relationships (if any), else one row
        list.innerHTML = '';
        try {
            const tplMap = (this.getTemplateById?.(this.selectedTemplateId)?.mapping) || {};
            const rels = tplMap._relationships || [];
            if (Array.isArray(rels) && rels.length) {
                rels.forEach(r => {
                    list.insertAdjacentHTML('beforeend', rowHtml());
                    const item = list.querySelector('.tpl-rel-item:last-child');
                    item.querySelector('.tpl-rel-parent-table').value = r.parent_table || '';
                    item.querySelector('.tpl-rel-child-table').value = r.child_table || '';
                    // NK options may need to include r.natural_key_column even if not in headers
                    const nkSel = item.querySelector('.tpl-rel-natural-key');
                    if (r.natural_key_column && !Array.from(nkSel.options).some(o=>o.value===r.natural_key_column)) {
                        nkSel.insertAdjacentHTML('beforeend', `<option value="${r.natural_key_column}">${r.natural_key_column}</option>`);
                    }
                    nkSel.value = r.natural_key_column || '';
                    const fkInput = item.querySelector('.tpl-rel-child-fk');
                    fkInput.value = r.child_fk_column || '';
                    item.querySelector('.tpl-rel-nk-normalize').checked = !!r.nk_normalize;
                    item.querySelector('.tpl-rel-add-index').checked = r.add_index !== false;
                    item.querySelector('.tpl-rel-add-fk-constraint').checked = !!r.add_fk_constraint;
                    item.querySelector('.tpl-rel-enforce-unique').checked = !!r.enforce_unique;
                    const modeSel = item.querySelector('.tpl-rel-pk-mode');
                    const pref = item.querySelector('.tpl-rel-pk-prefix');
                    const wid = item.querySelector('.tpl-rel-pk-width');
                    const ps = r.pk_strategy || {}; modeSel.value = ps.mode || 'auto'; pref.value = ps.prefix || ''; wid.value = ps.width || '';
                    // remove handler
                    item.querySelector('.tpl-rel-remove').onclick = () => item.remove();
                });
            } else {
                addBtn?.click();
            }
        } catch(_) {
            addBtn?.click();
        }
    }

	escapeHtml(unsafe) {
		return unsafe
			.replace(/&/g, "&amp;")
			.replace(/</g, "&lt;")
			.replace(/>/g, "&gt;")
			.replace(/"/g, "&quot;")
			.replace(/'/g, "&#039;");
	}

    async saveTemplateAndApply() {
		const modal = document.getElementById("templateBuilderModal");
		if (!modal) return;

        const name = document.getElementById("tpl-name")?.value?.trim();
        if (!name) return this.showError?.("Report Name is required");

		const rows = [
			...modal.querySelectorAll("#tpl-headers-table tbody tr.tpl-row"),
		];
		const headers = [];
		const templateMapping = {}; // for template's persistent mapping

		for (const row of rows) {
			const src = row.querySelector(".src")?.value?.trim();
			if (!src) continue;

			const tblVal = row.querySelector(".mtbl")?.value || "";
			const colVal = row.querySelector(".mcol")?.value || "";
			const dtype = row.querySelector(".typ")?.value || "";
			const req = !!row.querySelector(".req")?.checked;
			const defv = row.querySelector(".def")?.value || "";

			let finalTable = "";
			if (tblVal === "__new__" || tblVal === "__reuse_new__") {
				finalTable = this.builderState.newTableName || "new_table";
			} else if (tblVal) {
				finalTable = tblVal; // existing table name from dropdown
			}

			// resolve column
			let finalColumn = "";
			if (colVal === "__new__") {
				const proposed = (
					window.prompt("New column name (snake_case)", src) || src
				).trim();
				finalColumn = proposed;
			} else {
				finalColumn = colVal || "";
			}

			headers.push({
				source_header: src,
				target_table: finalTable,
				target_column: finalColumn,
				data_type: dtype,
				is_required: req,
				default_value: defv,
			});
			if (src && (finalTable || finalColumn)) {
				templateMapping[src] = {
					target_table: finalTable || "",
					target_column: finalColumn || "",
					data_type: dtype || "",
					is_required: !!req,
					default_value: defv || "",
				};
			}
		}

		if (!headers.length) {
			return this.showError?.("Add at least one header mapping");
		}

		// POST to your existing template create endpoint (adjust fields if needed)
		const connection_id = this.getActiveConnectionId();
		const targetTableInput = document.getElementById("tpl-target-table");
		const target_table = (targetTableInput?.value || "").trim();
		let resp;
		const headerNames = headers
			.map((h) =>
				h && h.source_header ? h.source_header : typeof h === "string" ? h : ""
			)
			.filter(Boolean);
		try {
			resp = await this.fetchJson("/intelligent-import/api/report-templates/", {
				method: "POST",
				body: JSON.stringify({ name, headers, connection_id, target_table }),
			});
			if (!resp?.success)
				return this.showError?.(resp?.error || "Failed to save template");
			// Save template fields for new template
			try {
				if (headerNames.length) {
					await this.fetchJson(
						`/intelligent-import/api/report-templates/${resp.id}/fields/`,
						{
							method: "PUT",
							body: JSON.stringify({ fields: headerNames }),
						}
					);
				}
			} catch (err) {
				/* non-fatal */
			}
		} catch (e) {
			// Handle name conflict: offer update or rename flow
			if (e && e.status === 409) {
				const update = window.confirm(
					`A template named "${name}" already exists.\n\nOK = Update existing (keeps name)\nCancel = Enter a new name`
				);
				if (!update) {
					const newName = window.prompt(
						"Enter a new template name:",
						`${name} (copy)`
					);
					if (!newName) return; // user cancelled
					// retry create with new name
					resp = await this.fetchJson(
						"/intelligent-import/api/report-templates/",
						{
							method: "POST",
							body: JSON.stringify({
								name: newName.trim(),
								headers,
								connection_id,
								target_table,
							}),
						}
					);
					if (!resp?.success)
						return this.showError?.(resp?.error || "Failed to save template");
					try {
						if (headerNames.length) {
							await this.fetchJson(
								`/intelligent-import/api/report-templates/${resp.id}/fields/`,
								{
									method: "PUT",
									body: JSON.stringify({ fields: headerNames }),
								}
							);
						}
					} catch (err) {
						/* non-fatal */
					}
				} else {
					// Update existing template's attributes and fields
					const list = await this.fetchJson(
						"/intelligent-import/api/report-templates/"
					);
					const found = (list.templates || []).find(
						(t) => (t.name || "") === name
					);
					if (!found)
						return this.showError(
							"Could not locate existing template to update."
						);
					try {
						await this.fetchJson(
							`/intelligent-import/api/report-templates/${found.id}/`,
							{
								method: "PUT",
								body: JSON.stringify({ target_table }),
							}
						);
					} catch (err) {
						// non-fatal
					}
					try {
						await this.fetchJson(
							`/intelligent-import/api/report-templates/${found.id}/fields/`,
							{
								method: "PUT",
								body: JSON.stringify({ fields: headerNames }),
							}
						);
					} catch (err) {
						// non-fatal
					}
					resp = { success: true, id: found.id };
				}
			} else {
				throw e; // rethrow other errors
			}
		}

        // Collect template-level relationships
        try {
            const enabled = qs('#tpl-rel-enable')?.checked !== false;
            if (enabled) {
                const items = Array.from(document.querySelectorAll('#tpl-rel-list .tpl-rel-item'));
                const rels = items.map(item => ({
                    parent_table: item.querySelector('.tpl-rel-parent-table')?.value || '',
                    natural_key_column: item.querySelector('.tpl-rel-natural-key')?.value || '',
                    child_table: item.querySelector('.tpl-rel-child-table')?.value || '',
                    child_fk_column: (item.querySelector('.tpl-rel-child-fk')?.value || '').trim(),
                    nk_normalize: !!item.querySelector('.tpl-rel-nk-normalize')?.checked,
                    add_index: !!item.querySelector('.tpl-rel-add-index')?.checked,
                    add_fk_constraint: !!item.querySelector('.tpl-rel-add-fk-constraint')?.checked,
                    enforce_unique: !!item.querySelector('.tpl-rel-enforce-unique')?.checked,
                    pk_strategy: {
                        mode: item.querySelector('.tpl-rel-pk-mode')?.value || 'auto',
                        prefix: item.querySelector('.tpl-rel-pk-prefix')?.value || '',
                        width: parseInt(item.querySelector('.tpl-rel-pk-width')?.value || '0', 10) || null,
                    },
                })).filter(r => r.parent_table && r.child_table && r.natural_key_column);
                if (rels.length) templateMapping._relationships = rels;
            }
        } catch (_) {}

        // clear builder state for next time
        this.builderState = { newTableName: null, newTableRole: "fact" };

		this.showSuccess?.("Template created");
		// Persist template mapping (target table/column) on the template
		try {
            if (resp?.id && Object.keys(templateMapping).length) {
                await this.fetchJson(
                    `/intelligent-import/api/report-templates/${resp.id}/mapping/`,
                    {
                        method: "PUT",
                        body: JSON.stringify({ mapping: templateMapping, relationships: (templateMapping._relationships||[]) }),
                    }
                );
            }
        } catch (e) {
            /* non-fatal */
        }

		// Optionally set this template for the current session
		try {
			if (this.currentSession && resp.id) {
				const stResp = await this.fetchJson(
					`/intelligent-import/api/session/${this.currentSession}/report-template/`,
					{
						method: "POST",
						body: JSON.stringify({ template_id: resp.id, target_table }),
					}
				);
				if (stResp && (stResp.status || stResp.session_status)) {
					this.currentStatus = stResp.status || stResp.session_status;
					this.updateSessionActionButtons?.();
				}
			}
		} catch (e) {
			/* non-fatal */
		}

		// Close modal safely: move focus out before hiding to avoid aria-hidden warning
		if (window.bootstrap && bootstrap.Modal) {
			try {
				if (modal.contains(document.activeElement)) {
					document.activeElement.blur();
				}
				const inst =
					bootstrap.Modal.getInstance(modal) ||
					bootstrap.Modal.getOrCreateInstance(modal);
				inst.hide();
			} catch (e) {
				console.warn("Modal hide fallback due to error:", e);
				modal.setAttribute("inert", "");
				modal.style.display = "none";
			}
		} else {
			if (modal.contains(document.activeElement)) {
				document.activeElement.blur();
			}
			// Use inert to prevent focus while hidden
			modal.setAttribute("inert", "");
			modal.style.display = "none";
		}
		await this.fetchAndRenderTemplates?.();
	}

	async fetchAndRenderTemplates() {
		try {
			const response = await this.fetchJson(
				"/intelligent-import/api/report-templates/"
			);
			if (response.success) {
				this.templateOptions = response.templates || [];
				if (typeof this.renderTemplateOptions === "function") {
					this.renderTemplateOptions();
				}
				this.updateTemplateButtonsVisibility(true);
			}
		} catch (error) {
			console.warn("Failed to load templates:", error);
			this.templateOptions = [];
			this.updateTemplateButtonsVisibility(!!this.currentSession);
		}
	}

	getCsrfToken() {
		const name = "csrftoken";
		const cookieValue = document.cookie
			.split(";")
			.map((cookie) => cookie.trim())
			.find((cookie) => cookie.startsWith(`${name}=`));
		return cookieValue ? decodeURIComponent(cookieValue.split("=")[1]) : "";
	}

	loadConnections() {
		try {
			const scriptTag = document.getElementById(
				"intelligent-import-connections"
			);
			if (!scriptTag) {
				return [];
			}
			return JSON.parse(scriptTag.textContent || "[]");
		} catch (error) {
			console.warn("Unable to parse connections JSON:", error);
			return [];
		}
	}

	updateConnectionDisplay() {
		const nameEl = document.getElementById("connection-display-name");
		const detailsEl = document.getElementById("connection-display-details");

		if (!nameEl || !detailsEl) {
			return;
		}

		const connection = this.connections.find(
			(conn) => String(conn.id) === String(this.activeConnectionId)
		);

		if (!connection) {
			nameEl.textContent = "No database selected";
			detailsEl.textContent =
				"Destination database information will appear here.";
			return;
		}

		nameEl.textContent = connection.nickname || "Selected Database";
		const parts = [];
		if (connection.db_type_display) {
			parts.push(connection.db_type_display);
		}
		if (connection.db_name) {
			parts.push(connection.db_name);
		}
		if (connection.schema) {
			parts.push(`Schema: ${connection.schema}`);
		}
		detailsEl.textContent = parts.length
			? parts.join(" | ")
			: "Destination database selected.";
	}

	setProgressMessage(message, level = "info", progress = null) {
		if (!this.progressBanner) {
			return;
		}
		const levelClass =
			{
				info: "alert-info",
				success: "alert-success",
				warning: "alert-warning",
				error: "alert-danger",
			}[level] || "alert-info";

		this.progressBanner.className = `alert ${levelClass}`;

		let progressHtml = "";
		if (progress !== null) {
			if (typeof progress === "number") {
				progressHtml = `
					<div class="progress mt-2" style="height: 20px;">
						<div class="progress-bar" role="progressbar" style="width: ${progress}%;" aria-valuenow="${progress}" aria-valuemin="0" aria-valuemax="100">
							${progress}%
						</div>
					</div>
				`;
			} else {
				progressHtml = `
					<div class="progress mt-2" style="height: 20px;">
						<div class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 100%;" aria-valuenow="100" aria-valuemin="0" aria-valuemax="100"></div>
					</div>
				`;
			}
		}

		this.progressBanner.innerHTML = `<div>${message}</div>${progressHtml}`;
		this.progressBanner.style.display = "block";
	}

	clearProgressMessage() {
		if (!this.progressBanner) {
			return;
		}
		this.progressBanner.style.display = "none";
		this.progressBanner.innerHTML = "";
	}

	getTemplateById(templateId) {
		if (!templateId || !this.templateOptions) return null;
		return (
			this.templateOptions.find(
				(option) => String(option.id) === String(templateId)
			) || null
		);
	}

	// Load template-provided target fields for the selected template.
	// Strategy:
	// 1) Try mapping API and collect target_column/column/field values
	// 2) Fallback to fields API (list of headers) if mapping empty
	async loadTemplateFieldOptions(templateId) {
		this.templateTargetFields = [];
		const id = templateId || this.selectedTemplateId;
		if (!id) return;
		const collected = new Set();
		try {
			// Prefer explicit mapping which should carry intended target columns
			const mapResp = await this.fetchJson(
				`/intelligent-import/api/report-templates/${id}/mapping/`
			);
			if (mapResp?.success && mapResp.mapping) {
				this.templateMapping = mapResp.mapping; // cache mapping for destination computation
				Object.values(mapResp.mapping || {}).forEach((m) => {
					if (!m || typeof m !== "object") return;
					const name = m.target_column || m.column || m.field || "";
					if (name) collected.add(String(name));
				});
			}
		} catch (e) {
			// non-fatal; continue to fields fallback
		}
		if (collected.size === 0) {
			try {
				const fldResp = await this.fetchJson(
					`/intelligent-import/api/report-templates/${id}/fields/`
				);
				if (fldResp?.success && Array.isArray(fldResp.fields)) {
					fldResp.fields.forEach((f) => {
						if (f) collected.add(String(f));
					});
				}
			} catch (e) {
				// ignore
			}
		}
		this.templateTargetFields = Array.from(collected);
	}

	renderTemplateOptions() {
		if (!this.templateSelect) {
			return;
		}

		const options = this.templateOptions || [];

		if (!options.length) {
			this.templateSelect.innerHTML =
				'<option value="">No report templates available</option>';
			this.templateSelect.disabled = true;
			return;
		}

		const optionHtml = [
			'<option value="">-- Select report template --</option>',
		];
		options.forEach((option) => {
			const label = option.target_table
				? `${option.name} (${option.target_table})`
				: option.name;
			optionHtml.push(`<option value="${option.id}">${label}</option>`);
		});

		this.templateSelect.innerHTML = optionHtml.join("");
		this.templateSelect.disabled = false;

		// Set the selected value
		const preferredId =
			this.selectedTemplateId || this.detectedTemplateId || "";
		if (preferredId) {
			this.templateSelect.value = preferredId;
		}

		// Update detection message
		if (this.templateDetectionMessage) {
			let message = "";
			let cssClass = "text-muted";

			const selectedTemplate = this.getTemplateById(this.selectedTemplateId);
			const detectedTemplate = this.getTemplateById(this.detectedTemplateId);

			if (selectedTemplate) {
				message = `Selected template: ${selectedTemplate.name}`;
				if (this.detectedTemplateReason === "manual_selection") {
					message += " (set manually)";
				}
				cssClass = "text-success";
			} else if (detectedTemplate) {
				const reason =
					this.detectedTemplateReason === "filename_pattern"
						? "filename pattern"
						: this.detectedTemplateReason === "column_similarity"
						? "column similarity"
						: "system analysis";
				message = `Auto-detected template: ${detectedTemplate.name} (${reason})`;
				cssClass = "text-success";
			} else if (options.length) {
				message = "No template detected. Please select the matching report.";
			} else {
				message = "Create a report template to enable intelligent import.";
			}

			this.templateDetectionMessage.textContent = message;
			this.templateDetectionMessage.className = `small mt-2 ${cssClass}`;
		}
	}

	updateMappingEditState() {
		const editableStatuses = ["template_suggested", "mapping_defined"];
		const statusEditable = editableStatuses.includes(this.currentStatus);
		const canEditNow = this.canEditMapping && statusEditable;
		if (this.mappingPermissionsInfo) {
			if (canEditNow) {
				this.mappingPermissionsInfo.style.display = "none";
			} else {
				this.mappingPermissionsInfo.style.display = "block";
				this.mappingPermissionsInfo.textContent = statusEditable
					? "Only moderators and admins can adjust mappings. You can review the current mapping below."
					: "Mapping is locked after validation/approval. Use 'Back to Mapping' to reopen (moderator/admin).";
			}
		}

		if (this.mappingReadonlyBanner) {
			if (canEditNow) {
				this.mappingReadonlyBanner.style.display = "none";
			} else {
				const hasMapping = Object.keys(this.columnMapping || {}).length > 0;
				this.mappingReadonlyBanner.style.display = hasMapping
					? "block"
					: "none";
				this.mappingReadonlyBanner.textContent = hasMapping
					? statusEditable
						? "Mapping is read-only for your role. Contact a moderator or admin for changes."
						: "Mapping is locked after validation/approval."
					: "Awaiting mapping definition from a moderator or admin.";
			}
		}

		if (this.saveMappingBtn) {
			this.saveMappingBtn.style.display = canEditNow ? "inline-flex" : "none";
		}

		if (this.mappingTableContainer) {
			const inputs =
				this.mappingTableContainer.querySelectorAll("select, input");
			inputs.forEach((input) => {
				input.disabled = !canEditNow;
			});
		}

		if (this.createTemplateContainer) {
			this.createTemplateContainer.style.display = canEditNow ? "flex" : "none";
		}
	}

	updateSessionActionButtons() {
		if (this.cancelButton) {
			const cancellable = [
				"file_uploaded",
				"analyzing",
				"template_suggested",
				"mapping_defined",
				"mapping_approved",
				"data_validated",
				"pending_approval",
			];
			const canCancel =
				!!this.currentSession &&
				(!this.currentStatus || cancellable.includes(this.currentStatus));
			this.cancelButton.disabled = !canCancel;
		}
		if (this.deleteButton) {
			this.deleteButton.disabled = !this.currentSession;
		}
	}

	onTemplateSelectionChange(event) {
		if (!event || !this.currentSession) {
			const previous = this.selectedTemplateId || this.detectedTemplateId || "";
			if (this.templateSelect) {
				this.templateSelect.value = previous;
			}
			this.showError("No active session available. Upload a file first.");
			return;
		}
		const templateId = event.target.value || null;
		const currentSelection =
			this.selectedTemplateId || this.detectedTemplateId || null;
		if ((templateId || "") === (currentSelection || "")) {
			return;
		}
		this.updateReportTemplate(templateId);
	}

	async updateReportTemplate(templateId) {
		if (!this.currentSession) {
			this.showError("Load a session before updating the report template.");
			this.renderTemplateOptions();
			return;
		}

		try {
			this.setProgressMessage("Updating report template...", "info", true);
			const response = await this.fetchJson(
				`/intelligent-import/api/session/${this.currentSession}/report-template/`,
				{
					method: "POST",
					body: JSON.stringify({ template_id: templateId }),
				}
			);

			this.selectedTemplateId = response.selected_template_id || null;
			if (templateId && !this.selectedTemplateId) {
				this.selectedTemplateId = templateId;
			}
			if (templateId) {
				this.detectedTemplateId = this.selectedTemplateId;
				this.detectedTemplateReason = "manual_selection";
			} else {
				this.detectedTemplateId = null;
				this.detectedTemplateReason = "manual_selection";
				// Clearing template: clear template-defined fields
				this.templateTargetFields = [];
			}

			const templateInfo = this.getTemplateById(this.selectedTemplateId);
			if (!this.analysisResults) {
				this.analysisResults = {};
			}
			if (templateInfo) {
				const previousScore = this.analysisResults.template_match
					? this.analysisResults.template_match.score
					: null;
				this.analysisResults.template_match = {
					template_id: templateInfo.id,
					template_name: templateInfo.name,
					score: previousScore,
					reasons: ["manual_selection"],
				};
			} else {
				this.analysisResults.template_match = null;
			}

			// If backend returned a chosen target table, load its columns
			let chosenTarget =
				response.target_table ||
				this.analysisResults?.selected_target_table ||
				null;
			// Fallbacks: selected template's target_table, then builder field
			if (!chosenTarget) {
				const selInfo = this.getTemplateById?.(this.selectedTemplateId);
				if (selInfo?.target_table) chosenTarget = selInfo.target_table;
			}
			if (!chosenTarget) {
				const builderTT = document
					.getElementById("tpl-target-table")
					?.value?.trim();
				if (builderTT) chosenTarget = builderTT;
			}
			if (chosenTarget) {
				try {
					const connId = this.getActiveConnectionId();
					if (connId) {
						const url = `/intelligent-import/api/connections/${encodeURIComponent(
							connId
						)}/tables/${encodeURIComponent(chosenTarget)}/columns/`;
						const colsResp = await fetch(url, {
							credentials: "same-origin",
						}).then((r) => r.json());
						if (colsResp?.success && Array.isArray(colsResp.columns)) {
							// Normalize to targetColumns shape used by renderColumnMapping
							this.targetColumns = {};
							colsResp.columns.forEach((c) => {
								this.targetColumns[c.name] = {
									data_type: c.type || "",
									nullable: true,
									is_primary_key: false,
								};
							});
						}
					}
				} catch (e) {
					/* non-fatal */
				}
			}
			// Persist for later
			if (!this.analysisResults) this.analysisResults = {};
			if (chosenTarget)
				this.analysisResults.selected_target_table = chosenTarget;

			// If a template is selected, try to prefill mapping from template mapping API
			if (this.selectedTemplateId) {
				try {
					const mapResp = await this.fetchJson(
						`/intelligent-import/api/report-templates/${this.selectedTemplateId}/mapping/`
					);
					if (mapResp?.success && mapResp.mapping) {
						// Convert template mapping to columnMapping form { source_header: { field: target_column } }
						const cm = {};
						Object.entries(mapResp.mapping || {}).forEach(([src, m]) => {
							const tgt = (m && (m.target_column || m.column || m.field)) || "";
							if (tgt) cm[src] = { field: tgt };
						});
						this.columnMapping = cm;
					}
				} catch (e) {
					/* non-fatal */
				}
			}

			// Load template target fields (prefer template-defined fields over DB columns)
			try {
				await this.loadTemplateFieldOptions(this.selectedTemplateId);
			} catch {}

			// Re-render UI and enable Save button if any mapping present
			this.renderTemplateOptions();
			this.renderAnalysisSummary();
			if (typeof this.renderColumnMapping === "function") {
				this.renderColumnMapping();
			}
			this.updateSaveMappingEnabled();
			this.clearProgressMessage();
			this.showSuccess("Report template updated.");
		} catch (error) {
			this.showError(error.message || "Failed to update report template.");
			this.renderTemplateOptions();
		}
	}

	goToStep(stepName) {
		Object.entries(this.sectionMap).forEach(([name, element]) => {
			if (!element) {
				return;
			}
			element.style.display = name === stepName ? "block" : "none";
		});
		this.updateStepIndicators(stepName);
	}

	updateStepIndicators(activeStep) {
		let activeReached = false;
		this.stepOrder.forEach((stepName) => {
			const indicator = this.stepIndicatorMap[stepName];
			if (!indicator) {
				return;
			}
			indicator.classList.remove("active", "completed");
			if (stepName === activeStep) {
				indicator.classList.add("active");
				activeReached = true;
			}
			const fetchAndToastNotifications = async (limit = 5) => {
				try {
					const resp = await this.fetchJson(
						`/intelligent-import/api/notifications/unread/?limit=${limit}`
					);
					if (!resp?.success) return;
					const items = Array.isArray(resp.notifications)
						? resp.notifications
						: [];
					const toMark = [];
					items.forEach((n) => {
						const title = n.title || "";
						const message = n.message || "";
						const full = title ? `${title}: ${message}` : message;
						switch ((n.type || "").toLowerCase()) {
							case "success":
								this.showSuccess?.(full);
								break;
							case "warning":
								this.showWarning?.(full);
								break;
							case "error":
								this.showError?.(full);
								break;
							default:
								this.showInfo?.(full);
						}
						if (n.id) toMark.push(n.id);
					});
					if (toMark.length) {
						await this.fetchJson(
							"/intelligent-import/api/notifications/mark-read/",
							{
								method: "POST",
								body: JSON.stringify({ ids: toMark }),
							}
						);
					}
				} catch (e) {
					// Silent fail: toast UX enhancement
				}
			};
			if (!activeReached) {
				indicator.classList.add("completed");
			}
		});
	}

	async loadFinalReview() {
		if (!this.currentSession) return;
		try {
			const payload = await this.fetchJson(
				`/intelligent-import/api/session/${this.currentSession}/final-review/`
			);
			if (!payload?.success)
				throw new Error(payload?.error || "Failed to load final review");
			this.renderFinalReview(payload);
		} catch (e) {
			this.showError?.(e.message || String(e));
		}
	}

	renderFinalReview(payload) {
		const summaryEl = document.getElementById("final-review-summary");
		this.updateReviewSummary?.();
		const dupPanel = document.getElementById("duplicates-panel");
		const dupTbody = document.querySelector("#duplicates-table tbody");
		const conflictsPanel = document.getElementById("conflicts-panel");
		const conflictsTbody = document.querySelector("#conflicts-table tbody");
		const conflictsHint = document.getElementById("conflicts-hint");
		const awaitingBanner = document.getElementById("awaiting-approval-banner");
		if (summaryEl) {
			const s = payload.summary || {};
			summaryEl.innerHTML = `
				<div class="row g-2">
					<div class="col-md-3"><div class="card border-0 bg-light p-2"><div class="small text-muted">Total Rows</div><div class="fw-semibold">${
						s.total_rows || 0
					}</div></div></div>
					<div class="col-md-3"><div class="card border-0 bg-light p-2"><div class="small text-muted">To Insert</div><div class="fw-semibold">${
						s.to_insert_count || 0
					}</div></div></div>
					<div class="col-md-3"><div class="card border-0 bg-light p-2"><div class="small text-muted">Duplicates</div><div class="fw-semibold">${
						s.duplicates_count || 0
					}</div></div></div>
					<div class="col-md-3"><div class="card border-0 bg-light p-2"><div class="small text-muted">Conflicts</div><div class="fw-semibold">${
						s.conflicts_count || 0
					}</div></div></div>
				</div>`;
		}
		const dups = (payload.duplicates || {}).sample || [];
		if (dupPanel && dupTbody) {
			dupTbody.innerHTML = "";
			if (dups.length > 0) {
				dupPanel.style.display = "block";
				dups.forEach((item) => {
					const rowIdx = item.row;
					const decision = (item.decision || "").toLowerCase();
					const tr = document.createElement("tr");
					const tdRow = document.createElement("td");
					tdRow.textContent = rowIdx;
					const tdData = document.createElement("td");
					tdData.textContent = JSON.stringify(item.data || {});
					const tdAction = document.createElement("td");
					tdAction.innerHTML = `
						<div class="btn-group btn-group-sm" role="group">
							<button type="button" class="btn ${
								decision === "approve" ? "btn-success" : "btn-outline-success"
							} dup-approve" data-row="${rowIdx}">Approve</button>
							<button type="button" class="btn ${
								decision === "skip" ? "btn-secondary" : "btn-outline-secondary"
							} dup-skip" data-row="${rowIdx}">Skip</button>
						</div>`;
					tr.appendChild(tdRow);
					tr.appendChild(tdData);
					tr.appendChild(tdAction);
					dupTbody.appendChild(tr);
				});
				dupTbody.addEventListener("click", (e) => {
					const btn = e.target?.closest?.("button");
					if (!btn) return;
					const row = parseInt(btn.getAttribute("data-row"));
					if (btn.classList.contains("dup-approve"))
						this.setDuplicateDecision(row, "approve");
					if (btn.classList.contains("dup-skip"))
						this.setDuplicateDecision(row, "skip");
				});
			} else {
				dupPanel.style.display = "none";
			}
		}

		// Render conflicts list and hint
		const confSample = (payload.conflicts || {}).sample || [];
		if (conflictsPanel && conflictsTbody) {
			conflictsTbody.innerHTML = "";
			if (confSample.length > 0) {
				conflictsPanel.style.display = "block";
				if (conflictsHint) {
					const hintText = (payload.conflicts && payload.conflicts.hint) || "";
					conflictsHint.textContent = hintText;
					conflictsHint.style.display = hintText ? "block" : "none";
				}
				confSample.forEach((item) => {
					const rowIdx = item.row;
					const tr = document.createElement("tr");
					const tdRow = document.createElement("td");
					tdRow.textContent = rowIdx;
					const tdData = document.createElement("td");
					tdData.textContent = JSON.stringify(item.data || {});
					tr.appendChild(tdRow);
					tr.appendChild(tdData);
					conflictsTbody.appendChild(tr);
				});
			} else {
				conflictsPanel.style.display = "none";
			}
		}

		// Per-table plan rendering
		try {
			const ptList = document.getElementById('per-table-list');
			const ptDetailHead = document.querySelector('#per-table-detail-table thead');
			const ptDetailBody = document.querySelector('#per-table-detail-table tbody');
			if (ptList && ptDetailHead && ptDetailBody) {
				ptList.innerHTML = '';
				ptDetailHead.innerHTML = '';
				ptDetailBody.innerHTML = '';
				const plan = Array.isArray(payload.per_table_plan) ? payload.per_table_plan : [];
				const previews = payload.preview_by_table || {};
				plan.forEach(item => {
					const a = document.createElement('a');
					a.href = '#';
					a.className = 'list-group-item list-group-item-action d-flex justify-content-between align-items-center';
					a.dataset.table = item.table;
					a.innerHTML = `<span>${item.table}</span><span class="badge bg-secondary">${item.planned_inserts} insert(s), ${item.planned_updates} update(s)</span>`;
					ptList.appendChild(a);
				});

				const renderDetail = (tableName) => {
					ptDetailHead.innerHTML = '';
					ptDetailBody.innerHTML = '';
					const rows = previews[tableName] || [];
					if (!rows.length) return;
					const cols = Object.keys(rows[0]);
					const trh = document.createElement('tr');
					cols.forEach(c => { const th = document.createElement('th'); th.textContent = c; trh.appendChild(th); });
					ptDetailHead.appendChild(trh);
					rows.forEach(r => {
						const tr = document.createElement('tr');
						cols.forEach(c => { const td = document.createElement('td'); td.textContent = (r[c] ?? ''); tr.appendChild(td); });
						ptDetailBody.appendChild(tr);
					});
				};

				ptList.addEventListener('click', (e) => {
					e.preventDefault();
					const item = e.target.closest('.list-group-item');
					if (!item) return;
					renderDetail(item.dataset.table);
				});

				if (plan.length) renderDetail(plan[0].table);
			}
		} catch (e) {
			// non-fatal
		}

		// Buttons visibility/guard based on backend permission
		const approveBtn = this.getElementSafe("review-execute-btn");
		const reqBtn = this.getElementSafe("review-request-approval-btn");
		if (payload.can_approve) {
			if (approveBtn) approveBtn.style.display = "inline-block";
			if (reqBtn) reqBtn.style.display = "none";
		} else {
			if (approveBtn) approveBtn.style.display = "none";
			if (reqBtn) reqBtn.style.display = "inline-block";
		}
		if (awaitingBanner) {
			if (payload.awaiting_approval && !payload.can_approve) {
				awaitingBanner.textContent = "Awaiting approval by Moderator/Admin.";
				awaitingBanner.style.display = "block";
			} else {
				awaitingBanner.style.display = "none";
			}
		}
	}

	updateReviewSummary() {
		try {
			const line = document.getElementById('review-destination-line');
			if (!line) return;
				const connName = document.getElementById('connection-display-name')?.innerText?.trim() || '';
				const tables = (this.getDestinationTables && typeof this.getDestinationTables === 'function') ? this.getDestinationTables() : [];
			const modeSel = document.getElementById('import-mode');
			const mode = (modeSel?.value || 'append').toLowerCase();
			const modeLabel = mode === 'upsert' ? 'Upsert (update existing)' : (mode === 'replace' ? 'Replace table' : 'Append (skip duplicates)');
				line.textContent = `${connName || 'Not selected'} • ${ (tables.length?tables.join(', '):'(not set)') } • ${modeLabel}`;
		} catch (_) {}
	}

	setDuplicateDecision(row, action) {
		const tbody = document.querySelector("#duplicates-table tbody");
		const rows = Array.from(tbody?.querySelectorAll("tr") || []);
		const tr = rows.find(
			(r) => parseInt(r.firstChild?.textContent || "-1") === row
		);
		if (!tr) return;
		const approveBtn = tr.querySelector(".dup-approve");
		const skipBtn = tr.querySelector(".dup-skip");
		if (!approveBtn || !skipBtn) return;
		if (action === "approve") {
			approveBtn.classList.remove("btn-outline-success");
			approveBtn.classList.add("btn-success");
			skipBtn.classList.remove("btn-secondary");
			skipBtn.classList.add("btn-outline-secondary");
		} else {
			skipBtn.classList.remove("btn-outline-secondary");
			skipBtn.classList.add("btn-secondary");
			approveBtn.classList.remove("btn-success");
			approveBtn.classList.add("btn-outline-success");
		}
	}

	setAllDuplicateDecisions(action) {
		const rows = Array.from(
			document.querySelectorAll("#duplicates-table tbody tr")
		);
		rows.forEach((r) => {
			const rowNum = parseInt(r.firstChild?.textContent || "-1");
			if (rowNum >= 0) this.setDuplicateDecision(rowNum, action);
		});
	}

	async saveDuplicateDecisionsFromUI() {
		if (!this.currentSession) return;
		const body = { decisions: [] };
		const rows = Array.from(
			document.querySelectorAll("#duplicates-table tbody tr")
		);
		rows.forEach((r) => {
			const rowNum = parseInt(r.firstChild?.textContent || "-1");
			if (rowNum < 0) return;
			const approveActive = r
				.querySelector(".dup-approve")
				?.classList.contains("btn-success");
			const action = approveActive ? "approve" : "skip";
			body.decisions.push({ row: rowNum, action });
		});
		try {
			const resp = await this.fetchJson(
				`/intelligent-import/api/session/${this.currentSession}/duplicates/decisions/`,
				{ method: "POST", body: JSON.stringify(body) }
			);
			if (!resp?.success)
				throw new Error(resp?.error || "Failed to save decisions");
			this.showSuccess?.(
				`Saved ${resp.saved || body.decisions.length} decision(s).`
			);
		} catch (e) {
			this.showError?.(e.message || String(e));
		}
	}

	showError(message) {
		const displayMessage =
			typeof message === "string"
				? message
				: message?.message || String(message);
		console.error(displayMessage);
		if (window.showError) {
			window.showError(displayMessage);
		} else {
			window.alert(displayMessage);
		}
	}

	showSuccess(message) {
		if (window.showSuccess) {
			window.showSuccess(message);
		} else {
			window.alert(message);
		}
	}

	showWarning(message) {
		if (window.showWarning) {
			window.showWarning(message);
		} else {
			window.alert(message);
		}
	}

	showInfo(message) {
		if (window.showInfo) {
			window.showInfo(message);
		} else {
			window.alert(message);
		}
	}

	disableControls(disabled) {
		const controls = [
			"browse-import-btn",
			"proceed-mapping-btn",
			"save-mapping-btn",
			"execute-import-btn",
			"cancel-import-btn",
			"delete-import-btn",
		];
		controls.forEach((id) => {
			const btn = document.getElementById(id);
			if (btn) {
				btn.disabled = disabled;
			}
		});
	}

	makeUncacheableUrl(url) {
		try {
			const u = new URL(url, window.location.origin);
			u.searchParams.set("_ts", Date.now().toString());
			return u.pathname + u.search;
		} catch {
			// fallback if url is relative without origin
			const sep = url.includes("?") ? "&" : "?";
			return `${url}${sep}_ts=${Date.now()}`;
		}
	}

	async fetchJson(url, options = {}) {
		const defaultOptions = {
			credentials: "same-origin",
			headers: {},
		};
		const merged = { ...defaultOptions, ...options };

		if (!merged.headers["X-CSRFToken"] && this.csrfToken) {
			merged.headers["X-CSRFToken"] = this.csrfToken;
		}

		const method = (merged.method || "GET").toUpperCase();
		if (method === "GET") {
			url = this.makeUncacheableUrl(url);
			merged.cache = "no-store";
		}

		if (merged.body && !(merged.body instanceof FormData)) {
			merged.headers["Content-Type"] = "application/json";
		}

		const response = await fetch(url, merged);
		const text = await response.text();
		let payload;
		try {
			payload = text ? JSON.parse(text) : {};
		} catch (error) {
			throw new Error(`Invalid JSON response from ${url}: ${text}`);
		}
		if (!response.ok) {
			const message =
				payload?.error ||
				payload?.message ||
				`Request failed with ${response.status}`;
			const err = new Error(message);
			err.status = response.status;
			err.payload = payload;
			throw err;
		}
		return payload;
	}

	async uploadFile(file) {
		if (!file) {
			return;
		}

		if (!this.activeConnectionId) {
			this.showError("Please select a destination database before uploading.");
			return;
		}

		const formData = new FormData();
		formData.append("file", file);
		formData.append("connection_id", this.activeConnectionId);

		this.disableControls(true);
		this.setProgressMessage("Uploading and analysing file...", "info", 0);

		const xhr = new XMLHttpRequest();
		xhr.open("POST", "/intelligent-import/api/upload-analyze/", true);
		xhr.setRequestHeader("X-CSRFToken", this.csrfToken);
		xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");

		xhr.upload.onprogress = (event) => {
			if (event.lengthComputable) {
				const percentage = Math.round((event.loaded / event.total) * 100);
				this.setProgressMessage(
					"Uploading and analysing file...",
					"info",
					percentage
				);
			}
		};

		xhr.onload = () => {
			this.disableControls(false);
			if (xhr.status >= 200 && xhr.status < 300) {
				try {
					const response = JSON.parse(xhr.responseText);
					this.handleAnalysisResponse(response);
					this.showSuccess("Analysis complete. Review the suggested mapping.");
					this.refreshSessions();
				} catch (error) {
					this.showError(error.message || "Failed to process server response.");
				}
			} else {
				try {
					const errorResponse = JSON.parse(xhr.responseText);
					if (errorResponse?.duplicate_session_id) {
						this.showError(errorResponse.message || "Duplicate file detected.");
						this.enterSession(errorResponse.duplicate_session_id);
					} else {
						this.showError(
							errorResponse.error || `Upload failed with status: ${xhr.status}`
						);
					}
				} catch (e) {
					this.showError(`Upload failed with status: ${xhr.status}`);
				}
			}
		};

		xhr.onerror = () => {
			this.disableControls(false);
			this.showError("An error occurred during the upload. Please try again.");
		};

		xhr.send(formData);
	}

	updateSaveMappingEnabled() {
		const btn = document.getElementById("save-mapping-btn");
		if (!btn) return;
		let hasAnyDOM = false;
		try {
			const uiMap = this.collectMappingFromUI?.();
			hasAnyDOM = uiMap && Object.keys(uiMap).length > 0;
		} catch {}
		const hasAnyTemplate = Object.values(this.templateMapping || {}).some(
			(m) =>
				m &&
				((m.table && m.column) ||
					(m.create_column && m.create_column.label) ||
					m.create_table ||
					m.column)
		);
		const enabled =
			(hasAnyDOM || hasAnyTemplate) &&
			!!this.currentSession &&
			!!this.canEditMapping;
		btn.disabled = !enabled;
	}

	handleAnalysisResponse(response) {
		if (!response || response.success !== true) {
			this.showError(response?.error || "Unexpected analysis response.");
			return;
		}

		this.currentSession = response.session_id;
		this.updateSessionActionButtons();
		this.analysisResults = response.analysis_results || {};
		this.currentStatus = "template_suggested";
		this.updateSessionActionButtons();
		this.suggestedMapping = this.analysisResults.suggested_mapping || {};
		this.targetColumns = response.analysis_results?.target_columns || {};

		// Handle column mapping
		this.columnMapping = response.analysis_results?.suggested_mapping
			? this.cloneMapping(response.analysis_results.suggested_mapping)
			: {};

		if (
			response.column_mapping &&
			Object.keys(response.column_mapping).length > 0
		) {
			this.columnMapping = this.cloneMapping(response.column_mapping);
		}

		this.templateOptions = response.template_options || this.templateOptions;
		this.detectedTemplateId = response.detected_template_id || null;
		this.detectedTemplateReason = response.detected_template_reason || null;
		this.selectedTemplateId =
			response.selected_template_id ||
			this.selectedTemplateId ||
			this.detectedTemplateId ||
			null;

		// Safely call rendering methods
		if (typeof this.renderTemplateOptions === "function") {
			this.renderTemplateOptions();
		}

		this.updateMappingEditState();

		if (typeof this.renderAnalysisSummary === "function") {
			this.renderAnalysisSummary();
		}

		// First render using whatever is available, then refresh after loading template fields
		if (typeof this.renderColumnMapping === "function") {
			this.renderColumnMapping();
		}
		const tplIdForFields =
			this.selectedTemplateId || this.detectedTemplateId || null;
		if (tplIdForFields) {
			this.loadTemplateFieldOptions(tplIdForFields)
				.then(() => {
					if (typeof this.renderColumnMapping === "function") {
						this.renderColumnMapping();
					}
				})
				.catch(() => {});
		}

		this.goToStep("mapping");
		this.updateTemplateButtonsVisibility(true);
	}

	renderAnalysisSummary() {
		const container = document.getElementById("analysis-results");
		const details = document.getElementById("upload-details");
		if (!container || !details) {
			return;
		}

		const fileInfo = this.analysisResults.file_analysis || {};
		const suggestedTarget = this.analysisResults.suggested_target || {};

		const columnsList = (fileInfo.columns || [])
			.map((column) => `<li>${column}</li>`)
			.join("");
		const summaryLines = [];
		if (fileInfo.total_rows !== undefined) {
			summaryLines.push(`<li>Total rows: ${fileInfo.total_rows}</li>`);
		}
		if (fileInfo.total_columns !== undefined) {
			summaryLines.push(`<li>Total columns: ${fileInfo.total_columns}</li>`);
		}
		if (suggestedTarget.table_name) {
			const confidence = suggestedTarget.score
				? Math.round(suggestedTarget.score * 100)
				: null;
			const confidenceLabel =
				confidence !== null ? ` (confidence ${confidence}%)` : "";
			summaryLines.push(
				`<li>Suggested target table: <strong>${suggestedTarget.table_name}</strong>${confidenceLabel}</li>`
			);
		}

		const selectedTemplate = this.getTemplateById(this.selectedTemplateId);
		const detectedTemplate = this.getTemplateById(this.detectedTemplateId);
		if (selectedTemplate) {
			summaryLines.push(
				`<li>Report template: <strong>${selectedTemplate.name}</strong> (selected)</li>`
			);
		} else if (detectedTemplate) {
			const reason =
				this.detectedTemplateReason === "filename_pattern"
					? "filename pattern"
					: this.detectedTemplateReason === "column_similarity"
					? "column similarity"
					: "system analysis";
			summaryLines.push(
				`<li>Report template: <strong>${detectedTemplate.name}</strong> (auto-detected via ${reason})</li>`
			);
		} else if ((this.templateOptions || []).length > 0) {
			summaryLines.push("<li>No report template selected yet.</li>");
		}

		details.innerHTML = `
			<ul class="mb-2">
				${summaryLines.join("")}
			</ul>
			<div>
				<strong>Columns:</strong>
				<ul class="mb-0">${columnsList}</ul>
			</div>
		`;

		const targetTableNameEl = document.getElementById("target-table-name");
		if (targetTableNameEl) {
			targetTableNameEl.textContent =
				suggestedTarget.table_name || "Target table not identified";
		}

		container.style.display = "block";
	}

	cloneMapping(mapping) {
		const clone = {};
		Object.entries(mapping || {}).forEach(([source, details]) => {
			clone[source] = { ...details };
		});
		return clone;
	}

	// Helper: get columns from uploaded file
	getFileColumns() {
		return this.analysisResults?.file_analysis?.columns || [];
	}

	// Helper: normalize target columns into a list
	getTargetColumnsList() {
		const entries = Object.entries(this.targetColumns || {});
		if (entries.length > 0) {
			return entries.map(([name, metadata]) => ({
				name,
				data_type: metadata.data_type,
				nullable: metadata.nullable,
				is_primary_key: metadata.is_primary_key,
			}));
		}
		return [];
	}

	// Helper: read mapping selections from the UI table
	collectMappingFromUI() {
		if (!this.mappingTableContainer) {
			return {};
		}

		const updatedMapping = {};
		this.mappingTableContainer
			.querySelectorAll("tr[data-source-column]")
			.forEach((row) => {
				const sourceColumn = row.getAttribute("data-source-column");
				const select = row.querySelector(".target-field-select");
				const importModeSel = row.querySelector(".import-mode-select");

				const targetField = select?.value?.trim() || "";
				if (!targetField) {
					return;
				}

				const entry = { field: targetField };

				const original =
					this.columnMapping[sourceColumn] ||
					this.suggestedMapping[sourceColumn] ||
					{};
				if (original.confidence !== undefined)
					entry.confidence = original.confidence;
				if (importModeSel && importModeSel.value)
					entry.import_mode = importModeSel.value;

				updatedMapping[sourceColumn] = entry;
			});

		return updatedMapping;
	}

	renderColumnMapping() {
		if (!this.mappingTableContainer) {
			return;
		}

		const fileColumns = this.getFileColumns();
		// Prefer template-defined fields if available; fallback to DB columns
		let targetColumns = [];
		if (
			Array.isArray(this.templateTargetFields) &&
			this.templateTargetFields.length > 0
		) {
			targetColumns = this.templateTargetFields.map((name) => ({
				name,
				data_type: "",
				nullable: true,
				is_primary_key: false,
			}));
		} else {
			targetColumns = this.getTargetColumnsList();
		}

		if (fileColumns.length === 0) {
			this.mappingTableContainer.innerHTML =
				'<div class="p-3 text-muted">No columns detected in the uploaded file.</div>';
			this.updateMappingEditState();
			return;
		}

		if (targetColumns.length === 0) {
			this.mappingTableContainer.innerHTML =
				'<div class="p-3 text-muted">Target table columns could not be loaded. Please check the database connection.</div>';
			this.updateMappingEditState();
			return;
		}

		const rowsHtml = fileColumns
			.map((sourceColumn) => {
				const mapping =
					this.columnMapping[sourceColumn] ||
					this.suggestedMapping[sourceColumn] ||
					{};
				const selectedField = mapping.field || "";
				const importModeVal = mapping.import_mode || "auto";
				const confidence =
					mapping.confidence !== undefined
						? Math.round(mapping.confidence * 100)
						: null;
				const confidenceBadge =
					confidence !== null
						? `<span class="badge bg-info ms-2">Confidence ${confidence}%</span>`
						: "";

				const options = [
					`<option value="">-- Select field --</option>`,
					...targetColumns.map((target) => {
						const pkBadge = target.is_primary_key ? " (PK)" : "";
						const dataType = target.data_type ? ` [${target.data_type}]` : "";
						const nullable = target.nullable ? "" : " *";
						const label = `${target.name}${dataType}${pkBadge}${nullable}`;
						const selected = target.name === selectedField ? "selected" : "";
						return `<option value="${target.name}" ${selected}>${label}</option>`;
					}),
				].join("");

				const sampleValues = (
					this.analysisResults?.suggested_mapping?.[sourceColumn]
						?.sample_values || []
				)
					.slice(0, 5)
					.map(
						(value) =>
							`<span class="badge bg-light text-dark me-1">${value}</span>`
					)
					.join("");

				return `
                    <tr data-source-column="${sourceColumn}">
                        <td><strong>${sourceColumn}</strong>${confidenceBadge}<div class="text-muted small">${sampleValues}</div></td>
                        <td>
                            <select class="form-select form-select-sm target-field-select">${options}</select>
                        </td>
                        <td style="width: 1%; min-width: 160px;">
                            <select class="form-select form-select-sm import-mode-select">
                                <option value="auto" ${
																	importModeVal === "auto" ? "selected" : ""
																}>Auto</option>
                                <option value="append" ${
																	importModeVal === "append" ? "selected" : ""
																}>Append</option>
                                <option value="replace" ${
																	importModeVal === "replace" ? "selected" : ""
																}>Replace</option>
                            </select>
                        </td>
                    </tr>
                `;
			})
			.join("");

        this.mappingTableContainer.innerHTML = `
                <div class="table-responsive">
                    <table class="table table-sm table-striped mb-0">
                        <thead>
                            <tr>
                                <th>File Column</th>
                                <th>Target Field</th>
                                <th>Import Mode</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${rowsHtml}
                        </tbody>
                    </table>
                </div>
            `;

        this.updateMappingEditState();
        // Notify others that mapping UI (and source headers) is ready
        try { document.dispatchEvent(new CustomEvent('ii-mapping-rendered')); } catch(_) {}
    }

	async persistMapping() {
		if (!this.currentSession) {
			this.showError("No active session. Upload a file first.");
			return;
		}

		if (!this.canEditMapping) {
			this.showError("You do not have permission to edit column mappings.");
			return;
		}

		// Guard: mapping can only be saved in specific statuses
		const editableStatuses = ["template_suggested", "mapping_defined"];
		if (!editableStatuses.includes(this.currentStatus)) {
			this.showError(
				"Mapping is locked after validation/approval. Use 'Back to Mapping' to reopen."
			);
			return;
		}

		const mapping = this.collectMappingFromUI();
		if (Object.keys(mapping).length === 0) {
			this.showError("Map at least one column before saving.");
			return;
		}

		this.disableControls(true);
		this.setProgressMessage("Saving column mapping...", "info", true);

		try {
			// Try to compute a target table hint
			let target_table =
				document.getElementById("tpl-target-table")?.value?.trim() || "";
			if (!target_table) {
				// Prefer the selected template's target_table
				const selTpl = this.getTemplateById?.(this.selectedTemplateId);
				if (selTpl?.target_table) {
					target_table = selTpl.target_table;
				}
			}
			if (!target_table) {
				const tables = Object.values(mapping)
					.map((m) => m?.table || m?.create_table?.label || "")
					.filter(Boolean);
				if (tables.length) {
					// pick the most common
					const counts = tables.reduce(
						(acc, t) => ((acc[t] = (acc[t] || 0) + 1), acc),
						{}
					);
					target_table = Object.keys(counts).sort(
						(a, b) => counts[b] - counts[a]
					)[0];
				} else if (this.analysisResults?.suggested_target?.table_name) {
					target_table = this.analysisResults.suggested_target.table_name;
				} else if (this.analysisResults?.selected_target_table) {
					target_table = this.analysisResults.selected_target_table;
				}
			}

			const saveResp = await this.fetchJson("/intelligent-import/api/define-mapping/", {
				method: "POST",
				body: JSON.stringify({
					session_id: this.currentSession,
					column_mapping: mapping,
					target_table,
				}),
			});

			if (saveResp && (saveResp.status || saveResp.session_status)) {
				this.currentStatus = saveResp.status || saveResp.session_status;
				this.updateMappingEditState?.();
				this.updateSessionActionButtons?.();
			}

			this.columnMapping = mapping;
			this.showSuccess("Mapping saved. Validating the data...");
			await this.validateData();
		} catch (error) {
			this.showError(error.message || "Failed to save mapping.");
		} finally {
			this.disableControls(false);
		}
	}

	async validateData() {
		if (!this.currentSession) {
			this.showError("No active session. Upload a file first.");
			return;
		}

		this.setProgressMessage("Validating data...", "info", true);

		try {
			// Ensure session has a target_table before validation
			try {
				let chosenTarget =
					document.getElementById("tpl-target-table")?.value?.trim() || "";
				if (!chosenTarget) {
					const selTpl = this.getTemplateById?.(this.selectedTemplateId);
					if (selTpl?.target_table) chosenTarget = selTpl.target_table;
				}
				if (
					!chosenTarget &&
					this.analysisResults?.suggested_target?.table_name
				) {
					chosenTarget = this.analysisResults.suggested_target.table_name;
				}
				if (!chosenTarget && this.analysisResults?.selected_target_table) {
					chosenTarget = this.analysisResults.selected_target_table;
				}
				if (chosenTarget || this.selectedTemplateId) {
					const tplResp = await this.fetchJson(
						`/intelligent-import/api/session/${this.currentSession}/report-template/`,
						{
							method: "POST",
							body: JSON.stringify({
								template_id: this.selectedTemplateId || null,
								target_table: chosenTarget || "",
							}),
						}
					);
					if (tplResp && (tplResp.status || tplResp.session_status)) {
						this.currentStatus = tplResp.status || tplResp.session_status;
						this.updateSessionActionButtons?.();
					}
				}
			} catch (e) {
				/* not fatal */
			}

			const response = await this.fetchJson(
				"/intelligent-import/api/validate-preview-data/",
				{
					method: "POST",
					body: JSON.stringify({
						session_id: this.currentSession,
					}),
				}
			);
			// Keep client status in sync with server state transition
			if (response && (response.status || response.session_status)) {
				this.currentStatus = response.status || response.session_status;
				this.updateMappingEditState?.();
				this.updateSessionActionButtons?.();
			} else {
				// Server moves to 'pending_approval' after successful validation
				this.currentStatus = this.currentStatus || "pending_approval";
			}

            this.displayValidationResults(response);
            this.goToStep("validate");
            this.showSuccess(
                "Validation complete. Review the results before importing."
            );
			this.refreshSessions();
		} catch (error) {
			this.showError(error.message || "Validation failed.");
		}
	}

		displayValidationResults(result) {
		if (!result) {
			return;
		}

		const validation = result.validation_results || {};
		const errors = validation.errors || [];
		const warnings = validation.warnings || [];

		if (this.validationContainer) {
			const errorsHtml = errors
				.map((err) => `<li><strong>${err.column}:</strong> ${err.issue}</li>`)
				.join("");
			const warningsHtml = warnings
				.map(
					(warn) => `<li><strong>${warn.column}:</strong> ${warn.issue}</li>`
				)
				.join("");

			this.validationContainer.innerHTML = `
					<div class="alert alert-${errors.length ? "danger" : "success"}">
						${
							errors.length
								? `${errors.length} issue(s) detected`
								: "No blocking errors detected"
						}
					</div>
					${
						warnings.length
							? `<div class="alert alert-warning">Warnings:<ul>${warningsHtml}</ul></div>`
							: ""
					}
					${
						errors.length
							? `<div class="alert alert-danger">Errors:<ul>${errorsHtml}</ul></div>`
							: ""
					}
				`;
		}

		// Render quick fixes for common mapping/data errors
		this.renderValidationFixes(validation);

		// Render quick fixes for common mapping/data errors
		this.renderValidationFixes(validation);

		if (this.masterDataLink) {
			const masterWarning = warnings.find(
				(warn) =>
					typeof warn.issue === "string" &&
					warn.issue.toLowerCase().includes("master data")
			);
			if (masterWarning) {
				this.masterDataLink.innerHTML = `
					<div class="alert alert-info">
						Some master data values require approval.
						<a href="/intelligent-import/session/${this.currentSession}/approve-master-data/" target="_blank" rel="noopener">
							Review candidates
						</a>
					</div>
				`;
			} else {
				this.masterDataLink.innerHTML = "";
			}
		}

			this.renderPreviewTable(result.preview_data?.sample_data || []);
			this.updateDestinationOptions?.();
			this.renderNewFieldsHint?.();
		}

		updateDestinationOptions() {
			const destConnEl = document.getElementById('dest-connection');
			const destTableEl = document.getElementById('dest-table');
			if (!destConnEl && !destTableEl) return;
			try {
				const connName = document.getElementById('connection-display-name')?.innerText?.trim() || '';
				const tables = this.getDestinationTables();
				if (destConnEl) destConnEl.textContent = connName || 'Not selected';
				if (destTableEl) destTableEl.textContent = tables.length ? tables.join(', ') : '(not set)';
				// Default duplicate policy
				const sel = document.getElementById('import-mode');
				if (sel && !sel.value) sel.value = 'append';
			} catch (_) {}
		}

		renderNewFieldsHint() {
			const box = document.getElementById('new-fields-hint');
			if (!box) return;
			try {
				const mapping = this.columnMapping || {};
				const isNewPrefix = (s) => /^(?:__?reuse_new__?:|__?new(?:col|table)?__?:|new(?:col|table)?\:)/i.test(String(s||''));
				const normalize = (s) => {
					let v = String(s||'').trim();
					v = v.replace(/^(?:__?reuse_new__?:|__?new(?:col|table)?__?:|new(?:col|table)?\:)/i, '');
					v = v.replace(/[^A-Za-z0-9_]+/g, '_').replace(/^_+|_+$/g, '').toLowerCase();
					return v || 'x';
				};
				const items = [];
				Object.values(mapping).forEach(m => {
					const field = (typeof m === 'string') ? m : (m?.field || m?.column || m?.target_column || '');
					if (field && isNewPrefix(field)) {
						items.push(`${field} → ${normalize(field)}`);
					}
				});
				box.textContent = items.length ? items.join(', ') : 'None';
			} catch (_) {
				box.textContent = 'None';
			}
		}

		getDestinationTables() {
			const out = new Set();
			const stripPrefixes = (s) => {
				if (!s) return '';
				if (s.startsWith('__new__:')) return s.split(':', 2)[1] || '';
				if (s.startsWith('__reuse_new__:')) return s.split(':', 2)[1] || '';
				return s;
			};
			const norm = (s) => {
				const x = stripPrefixes(String(s||'').trim());
				return x.replace(/[^A-Za-z0-9_]+/g, '_').replace(/^_+|_+$/g, '').toLowerCase();
			};
			const mapping = this.columnMapping && Object.keys(this.columnMapping).length ? this.columnMapping : (this.suggestedMapping || {});
			Object.keys(mapping || {}).forEach(src => {
				const tm = (this.templateMapping || {})[src] || {};
				const t = norm(tm.target_table || '');
				if (t) out.add(t);
			});
			if (!out.size) {
				let t = document.getElementById('tpl-target-table')?.value?.trim() || '';
				if (!t) {
					const selTpl = this.getTemplateById?.(this.selectedTemplateId);
					if (selTpl?.target_table) t = selTpl.target_table;
				}
				if (!t && this.analysisResults?.suggested_target?.table_name) {
					t = this.analysisResults.suggested_target.table_name;
				}
				t = norm(t);
				if (t) out.add(t);
			}
			return Array.from(out);
		}

	renderPreviewTable(rows) {
		if (!this.previewTableHead || !this.previewTableBody) {
			return;
		}

		this.previewTableHead.innerHTML = "";
		this.previewTableBody.innerHTML = "";

		if (!rows || rows.length === 0) {
			this.previewTableHead.innerHTML =
				"<tr><th>No preview data available</th></tr>";
			return;
		}

		const columns = Object.keys(rows[0]);
		const headRow = document.createElement("tr");
		columns.forEach((column) => {
			const th = document.createElement("th");
			th.textContent = column;
			headRow.appendChild(th);
		});
		this.previewTableHead.appendChild(headRow);

		rows.forEach((row) => {
			const tr = document.createElement("tr");
			columns.forEach((column) => {
				const td = document.createElement("td");
				const value = row[column];
				td.textContent = value === null || value === undefined ? "" : value;
				tr.appendChild(td);
			});
			this.previewTableBody.appendChild(tr);
		});
	}

    async executeImport() {
		if (!this.currentSession) {
			this.showError("No active session to import.");
			return;
		}

		this.disableControls(true);
        this.setProgressMessage("Executing import...", "info", 0);
        const stopPolling = this.startImportProgressPolling();

		try {
			const response = await this.fetchJson(
				"/intelligent-import/api/approve-import/",
				{
					method: "POST",
					body: JSON.stringify({
						session_id: this.currentSession,
						comments: "",
						import_mode: this.getImportMode(),
					}),
				}
			);

				if (response && (response.status || response.session_status)) {
					this.currentStatus = response.status || response.session_status;
					this.updateSessionActionButtons?.();
				}

				if (response.success) {
					const count = response.import_results?.imported_count || 0;
					this.showSuccess(
						`Import completed successfully. ${count} row(s) imported.`
					);
					this.goToStep("done");
				} else {
					this.showError(response.import_results?.error || "Import failed.");
				}

			this.refreshSessions();
        } catch (error) {
            this.showError(error.message || "Import failed.");
        } finally {
            try { stopPolling?.(); } catch(_) {}
            this.disableControls(false);
        }
    }

    startImportProgressPolling(intervalMs = 1000) {
        let stopped = false;
        const tick = async () => {
            if (stopped) return;
            try {
                const id = this.currentSession;
                if (!id) return;
                const resp = await this.fetchJson(`/intelligent-import/api/session/${id}/status/`);
                const sess = resp?.session || {};
                const prog = Number(sess.import_progress || 0);
                this.setProgressMessage(`Executing import...`, "info", Math.max(0, Math.min(100, prog)));
                const status = String(sess.status || '').toLowerCase();
                if (["completed","failed","rolled_back"].includes(status) || prog >= 100) {
                    stopped = true;
                }
            } catch(_) { /* ignore transient errors */ }
        };
        const h = setInterval(tick, intervalMs);
        tick();
        return () => { stopped = true; clearInterval(h); };
    }

	async cancelCurrentSession() {
		if (!this.currentSession) {
			this.showError("No active session to cancel.");
			return;
		}

		try {
			const response = await this.fetchJson(
				`/intelligent-import/api/session/${this.currentSession}/cancel/`,
				{
					method: "POST",
					body: JSON.stringify({}),
				}
			);
			const message = response?.message || "Session cancelled.";
			this.showSuccess(message);
			this.resetState();
			this.refreshSessions();
		} catch (error) {
			const msg = String(error?.message || error || "");
			if (
				/already cancelled/i.test(msg) ||
				/cannot be cancelled.*Cancelled/i.test(msg)
			) {
				this.showSuccess("Session already cancelled.");
				this.resetState();
				this.refreshSessions();
				return;
			}
			this.showError(msg || "Failed to cancel session.");
			this.refreshSessions();
		}
	}

	async deleteCurrentSession() {
		if (!this.currentSession) {
			this.showError("No active session to delete.");
			return;
		}

		if (typeof window.confirm === "function") {
			const confirmed = window.confirm(
				"This will permanently delete the session and its progress. Continue?"
			);
			if (!confirmed) {
				return;
			}
		}

		try {
			const response = await this.fetchJson(
				`/intelligent-import/api/session/${this.currentSession}/delete/`,
				{
					method: "POST",
					body: JSON.stringify({}),
				}
			);
			const message = response?.message || "Session deleted.";
			this.showSuccess(message);
			this.resetState();
			this.refreshSessions();
		} catch (error) {
			const msg = String(error?.message || error || "");
			const isStateError = /Cannot delete a session/i.test(msg);
			if (isStateError) {
				const proceed =
					typeof window.confirm === "function"
						? window.confirm(
								"This session cannot be deleted in its current state.\n\nCancel the session and delete it?"
						  )
						: true;
				if (proceed) {
					// Try to cancel first; ignore if it's already cancelled
					try {
						await this.fetchJson(
							`/intelligent-import/api/session/${this.currentSession}/cancel/`,
							{ method: "POST", body: JSON.stringify({}) }
						);
					} catch (err2) {
						const em = String(err2?.message || err2 || "");
						if (
							!(
								/already cancelled/i.test(em) ||
								/cannot be cancelled.*Cancelled/i.test(em)
							)
						) {
							this.showError(em || "Failed to cancel session before delete.");
							this.refreshSessions();
							return;
						}
					}
					const delResp = await this.fetchJson(
						`/intelligent-import/api/session/${this.currentSession}/delete/`,
						{ method: "POST", body: JSON.stringify({}) }
					);
					const message = delResp?.message || "Session cancelled and deleted.";
					this.showSuccess(message);
					this.resetState();
					this.refreshSessions();
					return;
				}
			}
			this.showError(msg || "Failed to delete session.");
			this.refreshSessions();
		}
	}

	nextAvailableTemplateName(base, existingList = []) {
		const taken = new Set(
			(existingList || []).map((t) => (t.name || "").toLowerCase())
		);
		if (!taken.has((base || "").toLowerCase())) return base;
		let n = 2;
		while (true) {
			const cand = `${base} (${n})`;
			if (!taken.has(cand.toLowerCase())) return cand;
			n += 1;
		}
	}

	resetState() {
		this.currentSession = null;
		this.analysisResults = {};
		this.targetColumns = {};
		this.columnMapping = {};
		this.suggestedMapping = {};
		this.templateOptions = [];
		this.selectedTemplateId = null;
		this.detectedTemplateId = null;
		this.detectedTemplateReason = null;
		if (this.validationContainer) {
			this.validationContainer.innerHTML = "";
		}
		if (this.masterDataLink) {
			this.masterDataLink.innerHTML = "";
		}
		if (this.previewTableHead) {
			this.previewTableHead.innerHTML = "";
		}
		if (this.previewTableBody) {
			this.previewTableBody.innerHTML = "";
		}
		if (this.mappingTableContainer) {
			this.mappingTableContainer.innerHTML =
				'<div class="p-3 text-muted">Mapping will appear after analysis.</div>';
		}
		this.renderTemplateOptions();
		this.updateMappingEditState();
		this.updateSessionActionButtons();
		document
			.getElementById("analysis-results")
			?.setAttribute("style", "display:none;");
		this.goToStep("upload");
		this.clearProgressMessage();
		this.updateTemplateButtonsVisibility(false);
	}

	async refreshSessions() {
		try {
			const response = await this.fetchJson(
				"/intelligent-import/api/sessions/"
			);
			this.renderSessions(response.sessions || []);
		} catch (error) {
			console.warn("Failed to refresh sessions:", error);
		}
	}

	renderSessions(sessions) {
		if (!this.recentSessionsContainer) {
			return;
		}

		if (!sessions || !sessions.length) {
			this.recentSessionsContainer.innerHTML =
				'<p class="text-muted">No recent sessions.</p>';
			return;
		}

		const items = sessions
			.map((session) => {
				const status = session.status || "unknown";
				const created = session.created_at
					? new Date(session.created_at).toLocaleString()
					: "Unknown date";

				return `
                <div class="border rounded p-2 mb-2 session-card" data-session-id="${session.id}">
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            <div class="fw-semibold text-truncate" style="max-width: 200px;">${session.original_filename}</div>
                            <div class="text-muted small">${created}</div>
                        </div>
                        <span class="badge bg-secondary text-uppercase">${status}</span>
                    </div>
                </div>
            `;
			})
			.join("");

		this.recentSessionsContainer.innerHTML = items;
	}

	async enterSession(sessionId) {
		try {
			const response = await this.fetchJson(
				`/intelligent-import/api/session/${sessionId}/enter/`
			);

			if (response.success) {
				this.loadSessionState(response);
				this.showSuccess("Session loaded successfully.");
			} else {
				this.showError(response.error || "Failed to load session");
			}
		} catch (error) {
			this.showError(error.message || "Failed to load session.");
			this.refreshSessions();
		}
	}

	loadSessionState(payload) {
		if (!payload || payload.success !== true) {
			return;
		}

		this.currentSession = payload.session_id;
		this.currentStatus = payload.status || this.currentStatus || null;
		this.analysisResults = payload.analysis_results || {};
		if (
			(!this.analysisResults ||
				Object.keys(this.analysisResults).length === 0) &&
			payload &&
			(payload.columns || payload.headers)
		) {
			this.analysisResults = {
				file_analysis: { columns: payload.headers || payload.columns || [] },
				suggested_target: payload.suggested_target || null,
			};
		}
		this.suggestedMapping = payload.suggested_mapping || {};
		this.columnMapping = payload.column_mapping || {};
		this.targetColumns =
			payload.target_columns || this.analysisResults.target_columns || {};
		this.templateOptions = payload.template_options || this.templateOptions;
		this.selectedTemplateId =
			payload.selected_template_id || this.selectedTemplateId || null;
		this.detectedTemplateId =
			payload.detected_template_id || this.detectedTemplateId || null;
		this.detectedTemplateReason =
			payload.detected_template_reason || this.detectedTemplateReason || null;

		this.updateSessionActionButtons();
		this.updateMappingEditState();
		this.renderTemplateOptions();
		this.updateMappingEditState();

		this.renderAnalysisSummary();
		this.renderColumnMapping();

		if (
			payload.validation_results &&
			Object.keys(payload.validation_results).length > 0
		) {
			this.displayValidationResults({
				validation_results: payload.validation_results,
				preview_data: payload.preview_data,
			});
		}

		this.goToStep(payload.step || "upload");
		this.refreshSessions();
		this.updateTemplateButtonsVisibility(true);
	}

	async openTemplateManager() {
		// Permission check (only Admin/Moderator may modify)
		const canManage = ["Admin", "Moderator"].includes(
			window.currentUser?.user_type
		);
		if (!canManage) {
			alert("You don't have permission to manage templates.");
			return;
		}
		// Refresh list then open modal
		await this.fetchAndRenderTemplates();
		this.mtRenderTable(this.templateOptions || []);
		const modal = new bootstrap.Modal(this.$mtModal, { backdrop: "static" });
		modal.show();
	}

    mtRenderTable(templates) {
		if (!this.$mtTbody) return;
		const rows = (templates || [])
			.map((t) => {
				const id = t.id;
				const safeName = (t.name || "").replace(/"/g, "&quot;");
				const safeDesc = (t.description || "").replace(/"/g, "&quot;");
				const checked = t.is_active ? "checked" : "";
				return `
      <tr data-id="${id}">
        <td>
          <input class="form-control form-control-sm mt-name" value="${safeName}">
        </td>
        <td>
          <input class="form-control form-control-sm mt-desc" value="${safeDesc}">
        </td>
        <td class="text-center">
          <input class="form-check-input mt-active" type="checkbox" ${checked}>
        </td>
        <td class="text-end">
          <button class="btn btn-outline-primary btn-sm mt-edit">Edit</button>
          <button class="btn btn-outline-success btn-sm mt-save ms-2">Save</button>
          <button class="btn btn-outline-danger  btn-sm mt-del ms-2">Delete</button>
        </td>
      </tr>`;
			})
			.join("");
		this.$mtTbody.innerHTML = rows;

		// Bind per-row actions
        this.$mtTbody.querySelectorAll(".mt-edit").forEach((btn) => {
            btn.addEventListener("click", async (e) => {
                e.preventDefault();
                const tr = e.target.closest("tr");
                await this.mtOpenEditor(tr);
            });
        });
        this.$mtTbody.querySelectorAll(".mt-save").forEach((btn) => {
			btn.addEventListener("click", async (e) => {
				e.preventDefault();
				const tr = e.target.closest("tr");
				await this.mtSaveRow(tr);
			});
		});
		this.$mtTbody.querySelectorAll(".mt-del").forEach((btn) => {
			btn.addEventListener("click", async (e) => {
				e.preventDefault();
				const tr = e.target.closest("tr");
				await this.mtDeleteRow(tr);
			});
		});
	}

	async mtCreateTemplate() {
		const base = (this.$mtNewName?.value || "").trim();
		const description = (this.$mtNewDesc?.value || "").trim();
		if (!base) return alert("Template name is required.");

		// refresh list first
		if (!Array.isArray(this.templateOptions)) {
			await this.fetchAndRenderTemplates();
		}
		const uniqueName = this.nextAvailableTemplateName(
			base,
			this.templateOptions
		);

		let resp = await this.fetchJson(
			"/intelligent-import/api/report-templates/",
			{
				method: "POST",
				body: JSON.stringify({ name: uniqueName, description }),
			}
		);

		if (
			!resp?.success &&
			(resp?.error === "name_conflict" || resp?._status === 409)
		) {
			const suggested =
				resp?.suggested_name ||
				this.nextAvailableTemplateName(base, this.templateOptions);
			resp = await this.fetchJson("/intelligent-import/api/report-templates/", {
				method: "POST",
				body: JSON.stringify({
					name: base,
					description,
					final_name: suggested,
				}),
			});
		}

		if (!resp?.success) {
			alert(
				resp?.error === "name_conflict"
					? `Template name already exists. Try: ${resp?.suggested_name}`
					: resp?.error || "Failed to create template."
			);
			return;
		}

		this.$mtNewName.value = "";
		this.$mtNewDesc.value = "";
		await this.fetchAndRenderTemplates();
		this.mtRenderTable(this.templateOptions || []);
	}

    async mtSaveRow(tr) {
		const id = tr?.dataset?.id;
		if (!id) return;

		const name = tr.querySelector(".mt-name")?.value?.trim();
		const description = tr.querySelector(".mt-desc")?.value?.trim();
		const is_active = !!tr.querySelector(".mt-active")?.checked;

		if (!name) return alert("Name is required.");
		const resp = await this.fetchJson(
			`/intelligent-import/api/report-templates/${id}/`,
			{
				method: "PUT",
				body: JSON.stringify({ name, description, is_active }),
			}
		);
		if (!resp?.success) return alert(resp?.error || "Failed to save template.");
		await this.fetchAndRenderTemplates();
		this.mtRenderTable(this.templateOptions || []);
    }

    async mtOpenEditor(tr) {
        const id = tr?.dataset?.id;
        if (!id) return;
        try {
            await this.fetchTablesForConnection();
            const detail = await this.fetchJson(`/intelligent-import/api/report-templates/${id}/`);
            const mapping = await this.fetchJson(`/intelligent-import/api/report-templates/${id}/mapping/`);
            const modalEl = this.getElementSafe('templateBuilderModal');
            if (!modalEl) return;
            // Prefill name and target
            this.getElementSafe('tpl-name').value = detail?.name || '';
            this.getElementSafe('tpl-target-table').value = detail?.target_table || '';
            // Render one empty mapping row for now (full mapping edit not required here)
            const tbody = modalEl.querySelector('#tpl-headers-table tbody');
            if (tbody) tbody.innerHTML = '';
            this.addTemplateHeaderRow();
            // Prefill template-level relationships if present
            const rels = (mapping?.mapping && mapping.mapping._relationships) || [];
            const rel = Array.isArray(rels) && rels[0] ? rels[0] : {};
            // Show modal first then populate dropdowns
            const bsModal = bootstrap.Modal.getOrCreateInstance(modalEl, { backdrop: 'static', focus: true });
            bsModal.show();
            // Populate dropdowns
            this.renderTemplateRelationshipsPanel();
            // Apply selected values
            try {
                if (rel.parent_table) qs('#tpl-rel-parent-table').value = rel.parent_table;
                if (rel.child_table) qs('#tpl-rel-child-table').value = rel.child_table;
                if (rel.natural_key_column) qs('#tpl-rel-natural-key').value = rel.natural_key_column;
                if (rel.child_fk_column) qs('#tpl-rel-child-fk').value = rel.child_fk_column;
                qs('#tpl-rel-nk-normalize').checked = !!rel.nk_normalize;
                qs('#tpl-rel-add-index').checked = rel.add_index !== false;
                qs('#tpl-rel-add-fk-constraint').checked = !!rel.add_fk_constraint;
                qs('#tpl-rel-enable').checked = true;
            } catch(_) {}
        } catch (e) {
            console.warn('Failed to open template editor:', e);
        }
    }

	async mtDeleteRow(tr) {
		const id = tr?.dataset?.id;
		if (!id) return;
		if (!confirm("Delete this template? This cannot be undone.")) return;

		const resp = await this.fetchJson(
			`/intelligent-import/api/report-templates/${id}/`,
			{
				method: "DELETE",
			}
		);
		if (!resp?.success)
			return alert(resp?.error || "Failed to delete template.");
		tr.remove();
		await this.fetchAndRenderTemplates();
	}

	async loadTablesAndRender(schema = "public") {
		try {
			const res = await fetch(
				`/intelligent-import/api/metadata/tables/?schema=${encodeURIComponent(
					schema
				)}`
			);
			const json = await res.json();
			if (!json?.success) throw new Error("Failed to load tables");
			// as "schema.table" values
			this.availableTables = (json.tables || []).map(
				(t) => `${t.schema}.${t.table}`
			);
		} catch (e) {
			console.warn("Tables metadata failed:", e);
			this.availableTables = [];
		}

		const headers =
			this.analysisResults?.file_analysis?.columns ||
			this.analysisResults?.detected_headers ||
			[];

		this.renderTemplateMappingGrid(headers);
		this.updateSaveMappingEnabled();
	}

	renderTemplateMappingGrid(headers = []) {
		if (!this.$mappingCardBody) return;

		const tableOpts = (this.availableTables || [])
			.map((fq) => {
				const [schema, tbl] = fq.split(".");
				return `<option value="${fq}">${tbl}</option>`;
			})
			.join("");

		const rows = headers
			.map(
				(h) => `
		<div class="row align-items-center g-2 m-0 p-2 border-bottom" data-header="${h}">
		  <div class="col-4"><span class="badge bg-light text-dark">${h}</span></div>
	
		  <div class="col-4">
			<select class="form-select form-select-sm tm-table">
			  <option value="" selected disabled>Select table</option>
			  <option value="__new__">+ New Table…</option>
			  ${tableOpts}
			</select>
		  </div>
	
		  <div class="col-3">
			<select class="form-select form-select-sm tm-column" disabled>
			  <option value="" selected disabled>Select column</option>
			  <option value="__new__">+ New Column…</option>
			</select>
		  </div>
	
		  <div class="col-1 text-end">
			<span class="badge bg-secondary d-none tm-proposed">proposed</span>
		  </div>
		</div>
	  `
			)
			.join("");

		this.$mappingCardBody.innerHTML = rows;

		this.$mappingCardBody.querySelectorAll(".tm-table").forEach((sel) => {
			sel.addEventListener("change", async (e) => {
				const row = e.target.closest(".row[data-header]");
				const header = row?.dataset?.header || "new_table";
				const colSel = row?.querySelector(".tm-column");

				// If "New Table…" -> prompt once and show a reusable option
				if (e.target.value === "__new__") {
					this.builderState = this.builderState || {
						newTableName: null,
						newTableRole: "fact",
					};
					if (!this.builderState.newTableName) {
						const role = (
							window.prompt(
								"Table role? (fact/ref)",
								this.builderState.newTableRole
							) || "fact"
						).toLowerCase();
						const name = (
							window.prompt("New table name (snake_case)", header) || header
						).trim();
						this.builderState.newTableRole = role === "ref" ? "ref" : "fact";
						this.builderState.newTableName = name;
					}
					// add “[New] name” option once and select it
					const existed = [...e.target.options].some(
						(o) => o.value === "__reuse_new__"
					);
					if (!existed) {
						const opt = document.createElement("option");
						opt.value = "__reuse_new__";
						opt.textContent = `[New] ${this.builderState.newTableName}`;
						e.target.insertBefore(
							opt,
							e.target.querySelector('option[value="__new__"]')?.nextSibling ||
								e.target.firstChild
						);
					}
					e.target.value = "__reuse_new__";

					// for a new table, do not fetch columns; keep only “+ New Column…”
					if (colSel) {
						colSel.disabled = false;
						colSel.innerHTML = `<option value="" selected disabled>Select column</option><option value="__new__">+ New Column…</option>`;
					}
					this.updateSaveMappingEnabled();
					return;
				}

				// If an existing table selected, fetch columns for the selected connection
				if (colSel && e.target.value) {
					const tableName = e.target.value.includes(".")
						? e.target.value.split(".").pop()
						: e.target.value;
					try {
						const connId = document.getElementById(
							"active-connection-id"
						)?.value;
						if (connId) {
							const url = `/intelligent-import/api/connections/${encodeURIComponent(
								connId
							)}/tables/${encodeURIComponent(tableName)}/columns/`;
							const j = await fetch(url, { credentials: "same-origin" }).then(
								(r) => r.json()
							);
							if (j?.success && Array.isArray(j.columns)) {
								const opts = j.columns
									.map((c) => `<option value="${c.name}">${c.name}</option>`)
									.join("");
								colSel.disabled = false;
								colSel.innerHTML = `<option value="" selected disabled>Select column</option><option value="__new__">+ New Column…</option>${opts}`;
							} else {
								colSel.disabled = false;
								colSel.innerHTML = `<option value="" selected disabled>Select column</option><option value="__new__">+ New Column…</option>`;
							}
						}
					} catch {}
				}

				this.updateSaveMappingEnabled();
			});
		});

		// column change
		this.$mappingCardBody.querySelectorAll(".tm-column").forEach((sel) => {
			sel.addEventListener("change", () => this.updateSaveMappingEnabled());
		});
	}

	onTemplateTableChange = async (e) => {
		const sel = e.target;

		// find the row in either layout
		const row =
			sel.closest("[data-header]") ||
			sel.closest("tr.tpl-row") ||
			sel.closest(".tpl-row") ||
			sel.closest(".row");
		if (!row) {
			console.warn("onTemplateTableChange: no row container found");
			this.updateSaveMappingEnabled?.();
			return;
		}

		const header =
			row.dataset?.header || row.querySelector(".src")?.value || "";
		const tableVal = sel.value || "";
		// NOTE: builder uses .mcol, mapping grid uses .tm-column – handle both:
		const colSel = row.querySelector(".tm-column, .mcol") || null;

		// safe badge updater
		const updateBadge = (tbl, col) => {
			const badge = row.querySelector(".target-field-badge");
			if (!badge) return;
			if (tbl && col) badge.textContent = `${tbl}.${col}`;
			else if (tbl) badge.textContent = `${tbl}.(choose…)`;
			else badge.textContent = "–";
		};

		// mapping bucket for this header
		const map = (this.templateMapping[header] =
			this.templateMapping[header] || {});

		// Always reset column UI if it exists
		if (colSel) {
			colSel.disabled = false;
			colSel.innerHTML =
				`<option value="" selected disabled>Select column</option>` +
				`<option value="__new__">+ New Column…</option>`;
		}

		// 1) “+ New Table…” -> prompt now, register proposal, reuse everywhere
		if (tableVal === "__new__") {
			const role = (
				window.prompt("Table role? (fact/ref)", "fact") || "fact"
			).toLowerCase();
			const guessed = header || "new_table";
			const name = this.normalizeSnake
				? this.normalizeSnake(
						window.prompt("New table name (snake_case)", guessed) || guessed
				  )
				: guessed.toLowerCase().replace(/\W+/g, "_");

			// add to proposed list and inject option into all table selects
			this.addProposedTable?.(name, role);

			// rebuild THIS select so it includes “[New] name” and select it
			if (this.buildTableOptionsHTML) {
				sel.innerHTML = this.buildTableOptionsHTML();
				sel.value = `__new__:${name}`;
			}

			// mapping state for a proposed table
			map.table = null;
			map.create_table = {
				role: role === "ref" ? "ref" : "fact",
				label: name,
				client_id:
					(this.proposedTables || []).find((t) => t.name === name)?.clientId ||
					null,
			};

			updateBadge(name, null);
			this.updateSaveMappingEnabled?.();
			return;
		}

		// 2) previously proposed “[New] xxx”
		if (typeof tableVal === "string" && tableVal.startsWith("__new__:")) {
			const name = tableVal.split(":")[1];
			const pt = (this.proposedTables || []).find((t) => t.name === name) || {};
			map.table = null;
			map.create_table = {
				role: pt.role || "fact",
				label: name,
				client_id: pt.clientId || null,
			};

			updateBadge(name, null);
			this.updateSaveMappingEnabled?.();
			return;
		}

		// 3) existing table from selected connection → fetch columns (if we have a column select)
		map.table = tableVal;
		delete map.create_table;

		if (colSel && tableVal) {
			try {
				const connId = this.getActiveConnectionId?.();
				if (connId) {
					const url = `/intelligent-import/api/connections/${encodeURIComponent(
						connId
					)}/tables/${encodeURIComponent(tableVal)}/columns/`;
					const j = await fetch(url, { credentials: "same-origin" }).then((r) =>
						r.json()
					);
					const cols =
						j?.success && Array.isArray(j.columns)
							? j.columns.map((c) => c.name)
							: [];
					if (this.buildColumnOptionsHTML) {
						colSel.innerHTML = this.buildColumnOptionsHTML(cols);
					} else {
						const opts = cols
							.map((c) => `<option value="${c}">${c}</option>`)
							.join("");
						colSel.innerHTML = `<option value="" selected disabled>Select column</option><option value="__new__">+ New Column…</option>${opts}`;
					}
				}
			} catch (err) {
				console.warn("Columns fetch failed:", err);
				// leave + New Column… available
			}
		}

		updateBadge(tableVal || "", null);
		this.updateSaveMappingEnabled?.();
	};

	onTemplateColumnChange = (e) => {
		const sel = e.target;
		const row =
			sel.closest("[data-header]") ||
			sel.closest("tr.tpl-row") ||
			sel.closest(".tpl-row") ||
			sel.closest(".row");
		if (!row) {
			console.warn("onTemplateColumnChange: no row found for", sel);
			this.updateSaveMappingEnabled?.();
			return;
		}

		const header =
			row.dataset?.header || row.querySelector(".src")?.value || "";
		const colVal = sel.value || "";

		const tableVal = row.querySelector(".tm-table, .mtbl")?.value || "";
		const map = (this.templateMapping[header] =
			this.templateMapping[header] || {});

		// resolve visible table name for badge
		let tableNameForBadge = "";
		if (tableVal.startsWith("__new__:"))
			tableNameForBadge = tableVal.split(":")[1];
		else tableNameForBadge = tableVal || map.create_table?.label || "";

		const updateBadge = (tbl, col) => {
			const b = row.querySelector(".target-field-badge");
			if (!b) return;
			if (tbl && col) b.textContent = `${tbl}.${col}`;
			else if (tbl) b.textContent = `${tbl}.(choose…)`;
			else b.textContent = "–";
		};

		if (colVal === "__new__") {
			const guess = header || "new_column";
			const proposed = this.normalizeSnake
				? this.normalizeSnake(guess)
				: guess.toLowerCase().replace(/\W+/g, "_");
			const named =
				window.prompt("New column name (snake_case)", proposed) || proposed;

			const marker = `__newcol__:${named}`;
			if (![...sel.options].some((o) => o.value === marker)) {
				const opt = document.createElement("option");
				opt.value = marker;
				opt.textContent = `[New] ${named}`;
				sel.insertBefore(
					opt,
					sel.querySelector('option[value="__new__"]')?.nextSibling ||
						sel.firstChild
				);
			}
			sel.value = marker;

			map.column = null;
			map.create_column = {
				table: tableVal && !tableVal.startsWith("__new__:") ? tableVal : null,
				table_client_id: map.create_table?.client_id || null,
				label: named,
				type: "TEXT",
			};

			updateBadge(tableNameForBadge, named);
			this.updateSaveMappingEnabled?.();
			return;
		}

		if (typeof colVal === "string" && colVal.startsWith("__newcol__:")) {
			const name = colVal.split(":")[1];
			map.column = null;
			map.create_column = {
				table: tableVal && !tableVal.startsWith("__new__:") ? tableVal : null,
				table_client_id: map.create_table?.client_id || null,
				label: name,
				type: "TEXT",
			};
			updateBadge(tableNameForBadge, name);
			this.updateSaveMappingEnabled?.();
			return;
		}

		// existing column
		map.column = colVal;
		delete map.create_column;
		updateBadge(tableNameForBadge, colVal);
		this.updateSaveMappingEnabled?.();
	};

	updateTargetFieldBadge(rowEl, tableName, colName) {
		const badge = rowEl.querySelector(".target-field-badge");
		if (!badge) return;
		if (tableName && colName) badge.textContent = `${tableName}.${colName}`;
		else if (tableName) badge.textContent = `${tableName}.(choose…)`;
		else badge.textContent = "–";
	}
	async fetchTablesOnly(schema = "public") {
		try {
			const r = await fetch(
				`/intelligent-import/api/metadata/tables/?schema=${encodeURIComponent(
					schema
				)}`
			);
			const j = await r.json();
			if (!j?.success) throw new Error(j?.error || "tables");
			this.availableTables = (j.tables || []).map(
				(t) => `${t.schema}.${t.table}`
			);
		} catch (e) {
			console.warn("tables metadata failed", e);
			this.availableTables = []; // still allow "New Table…"
		}
	}

	updateSaveMappingEnabled() {
		if (!this.$saveMappingBtn) return;
		let hasAnyDOM = false;
		try {
			const uiMap = this.collectMappingFromUI?.();
			hasAnyDOM = uiMap && Object.keys(uiMap).length > 0;
		} catch {}
		const hasAnyTemplate = Object.values(this.templateMapping || {}).some(
			(m) =>
				m &&
				((m.table && m.column) ||
					(m.create_column && m.create_column.label) ||
					m.create_table ||
					m.column)
		);
		const enabled =
			(hasAnyDOM || hasAnyTemplate) &&
			!!this.currentSession &&
			!!this.canEditMapping;
		this.$saveMappingBtn.disabled = !enabled;
	}

	renderValidationFixes(validation) {
		const container = document.getElementById("validation-fixes");
		const body = document.getElementById("validation-fix-body");
		const applyBtn = document.getElementById("apply-fixes-btn"); // ,
		if (!container || !body || !applyBtn) return;

		// Ensure available tables are loaded for the Target Table dropdowns
		if (
			!Array.isArray(this.availableTables) ||
			this.availableTables.length === 0
		) {
			body.innerHTML = "Loading available tables...";
			container.style.display = "block";
			// Try to load tables for the active connection; fall back silently
			this.fetchTablesForConnection?.()
				.then(() => this.renderValidationFixes(validation))
				.catch(() => this.renderValidationFixes(validation));
			return;
		}

		const errors =
			validation && Array.isArray(validation.errors) ? validation.errors : [];
		const missing = errors.filter(
			(e) =>
				(e?.issue || "").toLowerCase().indexOf("required column is missing") !==
				-1
		);
		if (missing.length === 0) {
			container.style.display = "none";
			return;
		}

		const fileCols = Array.isArray(this.analysisResults?.file_analysis?.columns)
			? [...this.analysisResults.file_analysis.columns]
			: [];
		if (fileCols.length === 0) {
			try {
				const thead = document.querySelector("#data-preview-table thead tr");
				if (thead)
					Array.from(thead.children).forEach((th) =>
						fileCols.push(th.textContent || "")
					);
			} catch {}
		}

		const editableStatuses = ["template_suggested", "mapping_defined"];
		const statusEditable = editableStatuses.includes(this.currentStatus);
		const canEditNow = this.canEditMapping && statusEditable;

		let html = "";
		if (!canEditNow) {
			const reason = !this.canEditMapping
				? "Only moderators and admins can edit mapping."
				: "Mapping is locked after validation/approval. Use 'Back to Mapping' to reopen.";
			html += `<div class="alert alert-warning">${reason}</div>`;
		} // ,
		html +=
			'<div class="table-responsive"><table class="table table-sm align-middle"><thead><tr><th style="width:180px">Missing Field</th><th style="width:160px">Strategy</th><th style="width:220px">Target Table</th><th style="width:220px">Target Column</th><th>Value / Source Column</th></tr></thead><tbody>';
		missing.forEach((err, idx) => {
			const target = err?.column || "";
			const stratId = `fix-strategy-${idx}`;
			const tableId = `fix-table-${idx}`;
			const inputId = `fix-input-${idx}`;
			const colId = `fix-col-${idx}`;
			const tgtColId = `fix-targetcol-${idx}`;
			const tableOpts = ['<option value="">(current)</option>']
				.concat(this.availableTables || [])
				.map((t) => `<option value="${t}">${t}</option>`)
				.join("");
			const colOpts = ['<option value="">-- Select --</option>']
				.concat(fileCols)
				.map((c) => `<option value="${c}">${c}</option>`)
				.join("");
			html += `
                <tr data-row="${idx}" data-target="${target}">
                    <td><code>${target}</code></td>
                    <td>
                        <select id="${stratId}" class="form-select form-select-sm" ${
				canEditNow ? "" : "disabled"
			}>
                            <option value="map" selected>Map from file</option>
                            <option value="constant">Fill constant</option>
                            <option value="sequence">Auto sequence</option>
                        </select>
                    </td>
                    <td>
                        <select id="${tableId}" class="form-select form-select-sm" ${
				canEditNow ? "" : "disabled"
			}>
                            ${tableOpts}
                        </select>
                    </td>
                    <td>
                        <select id="${tgtColId}" class="form-select form-select-sm" ${
				canEditNow ? "" : "disabled"
			}>
                            <option value="">(select target column)</option>
                        </select>
                    </td>
                    <td>
                        <div class="d-flex gap-2 align-items-center">
                            <select id="${colId}" class="form-select form-select-sm" ${
				canEditNow ? "" : "disabled"
			}>
                                ${colOpts}
                            </select>
                            <input id="${inputId}" type="text" class="form-control form-control-sm" placeholder="Enter value" style="display:none" ${
				canEditNow ? "" : "disabled"
			} />
                        </div>
                    </td>
                </tr>`;
		});
		html += "</tbody></table></div>";
		body.innerHTML = html;
		container.style.display = "block";
		applyBtn.disabled = !canEditNow;
		// Toggle row controls based on strategy selection and load target columns
		missing.forEach((err, idx) => {
			const stratEl = document.getElementById(`fix-strategy-${idx}`);
			const inputEl = document.getElementById(`fix-input-${idx}`);
			const colEl = document.getElementById(`fix-col-${idx}`);
			const tableEl = document.getElementById(`fix-table-${idx}`);
			const tgtColEl = document.getElementById(`fix-targetcol-${idx}`);
			const updateVis = () => {
				const v = stratEl?.value || "map";
				if (v === "map") {
					colEl.style.display = "";
					inputEl.style.display = "none";
				} else if (v === "constant") {
					colEl.style.display = "none";
					inputEl.style.display = "";
				} else {
					colEl.style.display = "none";
					inputEl.style.display = "none";
				}
			};
			stratEl?.addEventListener("change", updateVis);
			updateVis();

			const populateTargetColumns = async (schemaTable) => {
				if (!tgtColEl) return;
				const opts = ['<option value="">(select target column)</option>'];
				let cols = [];
				try {
					if (schemaTable && schemaTable.includes(".")) {
						if (Array.isArray(this.tableColumns?.[schemaTable])) {
							cols = this.tableColumns[schemaTable];
						} else {
							const [schema, table] = schemaTable.split(".", 2);
							const resp = await fetch(
								`/intelligent-import/api/metadata/tables/${encodeURIComponent(
									schema
								)}/${encodeURIComponent(table)}/columns/`,
								{ credentials: "same-origin" }
							);
							const j = await resp.json();
							if (j?.success && Array.isArray(j.columns)) {
								cols = j.columns.map((c) => c.name);
								this.tableColumns = this.tableColumns || {};
								this.tableColumns[schemaTable] = cols;
							}
						}
					} else {
						const tc = this.analysisResults?.target_columns || {};
						cols = Object.keys(tc);
					}
				} catch (e) {
					cols = [];
				}
				cols.forEach((c) => opts.push(`<option value="${c}">${c}</option>`));
				tgtColEl.innerHTML = opts.join("");
				const missingTarget = err?.column || "";
				if (missingTarget && cols.includes(missingTarget))
					tgtColEl.value = missingTarget;
			};
			populateTargetColumns(tableEl?.value || "");
			tableEl?.addEventListener("change", () =>
				populateTargetColumns(tableEl.value || "")
			);
		});

		// Guess handler (event delegation)
		body.addEventListener("click", (e) => {
			const btn = e.target?.closest?.(".fix-guess");
			if (!btn) return;
			const idx = parseInt(btn.getAttribute("data-row"));
			const tableEl = document.getElementById(ix - table - 108658);
			const tgtColEl = document.getElementById(ix - targetcol - 108658);
			const srcColEl = document.getElementById(ix - col - 108658);
			const stratEl = document.getElementById(ix - strategy - 108658);

			const norm = (s) =>
				String(s || "")
					.toLowerCase()
					.replace(/[^a-z0-9]+/g, "_")
					.replace(/^_+|_+$/g, "");
			const score = (target, candidate) => {
				const a = norm(target),
					b = norm(candidate);
				if (!a || !b) return 0;
				if (a === b) return 100;
				if (a.includes(b) || b.includes(a)) return 60;
				// token overlap
				const at = a.split("_").filter(Boolean);
				const bt = b.split("_").filter(Boolean);
				const overlap = at.filter((t) => bt.includes(t)).length;
				return overlap * 10;
			};
			const guessFrom = (target, list) => {
				let best = "";
				let bestScore = -1;
				for (const name of list || []) {
					const s = score(target, name);
					if (s > bestScore) {
						bestScore = s;
						best = name;
					}
				}
				return best;
			};

			// Guess target column from table columns
			const schemaTable = tableEl?.value || "";
			let tableCols = [];
			if (
				schemaTable &&
				this.tableColumns &&
				Array.isArray(this.tableColumns[schemaTable])
			) {
				tableCols = this.tableColumns[schemaTable];
			} else {
				const tc = this.analysisResults?.target_columns || {};
				tableCols = Object.keys(tc);
			}
			const missingTarget =
				document
					.querySelector(`tr[data-row="${idx}"]`)
					?.getAttribute("data-target") || "";
			const bestTargetCol = guessFrom(missingTarget, tableCols);
			if (bestTargetCol && tgtColEl) tgtColEl.value = bestTargetCol;

			// Guess source column from file headers
			const fileCols = Array.isArray(
				this.analysisResults?.file_analysis?.columns
			)
				? [...this.analysisResults.file_analysis.columns]
				: [];
			const bestSrcCol = guessFrom(bestTargetCol || missingTarget, fileCols);
			if (bestSrcCol && srcColEl) srcColEl.value = bestSrcCol;

			if (stratEl) stratEl.value = "map";
			this.showInfo?.("Guessed mapping applied. Review and Apply Fixes.");
		});
		applyBtn.onclick = async () => {
			try {
				if (!canEditNow) return;
				const rows = Array.from(body.querySelectorAll("tr[data-row]"));
				const fixes = [];
				for (const tr of rows) {
					const idx = parseInt(tr.getAttribute("data-row"));
					const defaultTarget = tr.getAttribute("data-target");
					const strategy =
						document.getElementById(`fix-strategy-${idx}`)?.value || "map";
					const table =
						document.getElementById(`fix-table-${idx}`)?.value || "";
					const targetColumn =
						document.getElementById(`fix-targetcol-${idx}`)?.value ||
						defaultTarget;
					const source = (
						document.getElementById(`fix-col-${idx}`)?.value || ""
					).trim();
					const value = (
						document.getElementById(`fix-input-${idx}`)?.value || ""
					).trim();
					fixes.push({
						target: targetColumn || defaultTarget,
						strategy,
						table,
						source,
						value,
					});
				}
				if (!fixes.length) {
					this.showWarning?.("Pick at least one mapping to apply.");
					return;
				}
				await this.applyValidationFixes(fixes);
			} catch (e) {
				this.showError?.(e.message || String(e));
			}
		};
	}

	async applyValidationFixes(fixes) {
		const editableStatuses = ["template_suggested", "mapping_defined"];
		if (!editableStatuses.includes(this.currentStatus) && this.canEditMapping) {
			await this.fetchJson(
				`/intelligent-import/api/session/${this.currentSession}/reopen-mapping/`,
				{ method: "POST", body: JSON.stringify({}) }
			);
			this.currentStatus = "mapping_defined";
		}
		const mapping = this.cloneMapping?.(this.columnMapping || {}) || {
			...this.columnMapping,
		};
		for (const fix of fixes) {
			const targetField = fix.target;
			const table = fix.table || "";
			if (fix.strategy === "map") {
				const srcHeader = fix.source;
				if (srcHeader && targetField) {
					mapping[srcHeader] = { field: targetField };
					if (table) mapping[srcHeader].table = table;
				}
			} else if (fix.strategy === "constant") {
				const key = `__const__${targetField}`;
				mapping[key] = {
					field: targetField,
					fill_mode: "constant",
					fill_value: fix.value,
				};
				if (table) mapping[key].table = table;
			} else if (fix.strategy === "sequence") {
				const key = `__seq__${targetField}`;
				mapping[key] = { field: targetField, fill_mode: "auto_sequence" };
				if (table) mapping[key].table = table;
			}
		}
        let target_table =
            document.getElementById("tpl-target-table")?.value?.trim() || "";
        if (!target_table)
            target_table =
                this.analysisResults?.selected_target_table ||
                this.analysisResults?.suggested_target?.table_name ||
                "";
        try {
            const dmResp1 = await this.fetchJson("/intelligent-import/api/define-mapping/", {
                method: "POST",
                body: JSON.stringify({
                    session_id: this.currentSession,
                    column_mapping: mapping,
                    target_table,
                    relationships: this.relationships || [],
                }),
            });
            if (dmResp1 && (dmResp1.status || dmResp1.session_status)) {
                this.currentStatus = dmResp1.status || dmResp1.session_status;
                this.updateMappingEditState?.();
                this.updateSessionActionButtons?.();
            }
        } catch (err) {
            const msg = String(err?.message || '');
            if (this.canEditMapping && /Invalid session status/i.test(msg)) {
                // Try to reopen then retry
                await this.fetchJson(
                    `/intelligent-import/api/session/${this.currentSession}/reopen-mapping/`,
                    { method: 'POST', body: JSON.stringify({}) }
                );
                this.currentStatus = 'mapping_defined';
                const dmResp2 = await this.fetchJson("/intelligent-import/api/define-mapping/", {
                    method: "POST",
                    body: JSON.stringify({
                        session_id: this.currentSession,
                        column_mapping: mapping,
                        target_table,
                    }),
                });
                if (dmResp2 && (dmResp2.status || dmResp2.session_status)) {
                    this.currentStatus = dmResp2.status || dmResp2.session_status;
                    this.updateMappingEditState?.();
                    this.updateSessionActionButtons?.();
                }
            } else {
                throw err;
            }
        }
		this.columnMapping = mapping;
		this.showSuccess?.("Mapping fixes applied. Revalidating...");
		await this.validateData();
	}
}

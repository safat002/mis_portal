// mis_app/static/js/data_prep.js

(() => {
	"use strict";

	const DataPrepState = {
		recipe: [],
		originalPreviewData: [],
		currentPreviewData: [],
		columnMetadata: {},
		reportConfig: null,
		isLoading: true,
	};

	let fillMissingModal = null; // To hold the modal instance

	// Main entry point called from report_builder_django.js
	window.initializeDataPrepWorkflow = async function (reportConfig) {
		console.log("Initializing Data Prep Workflow with config:", reportConfig);
		DataPrepState.reportConfig = reportConfig;
		DataPrepState.recipe = []; // Reset recipe every time modal is opened

		const contentEl = document.getElementById("dataPrepContent");
		if (!contentEl) {
			console.error("Data Prep modal content area not found!");
			return;
		}
		contentEl.innerHTML = `<div class="d-flex justify-content-center align-items-center h-100"><div class="spinner-border text-primary" style="width: 3rem; height: 3rem;" role="status"><span class="visually-hidden">Loading...</span></div></div>`;

		try {
			const response = await fetch("/data-prep-modal-content/");
			if (!response.ok) throw new Error("Could not load modal UI.");
			contentEl.innerHTML = await response.text();

			// Initialize the sub-modal and bind all event listeners
			fillMissingModal = new bootstrap.Modal(
				document.getElementById("fillMissingModal")
			);
			bindEventListeners();

			await loadAndProfileData();
		} catch (error) {
			console.error("Data Prep Initialization Error:", error);
			contentEl.innerHTML = `<div class="alert alert-danger m-3">Failed to initialize data preparation: ${error.message}</div>`;
		}
	};

	// Fetches initial data from the backend
	async function loadAndProfileData() {
		console.log("--- [DEBUG] Fetching preview data...");

		try {
			const response = await fetch("/api/reports/profile_data/", {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					"X-CSRFToken": csrfToken,
				},
				body: JSON.stringify({
					...DataPrepState.reportConfig,
					data_prep_recipe: DataPrepState.recipe,
				}),
			});

			const result = await response.json();

			if (!result.success) {
				console.error(`--- [ERROR] API error: ${result.error}`);
				throw new Error(result.error);
			}

			console.log("--- [DEBUG] Profiled data received:", result.preview_data);

			DataPrepState.originalPreviewData = JSON.parse(
				JSON.stringify(result.preview_data)
			);
			DataPrepState.currentPreviewData = JSON.parse(
				JSON.stringify(result.preview_data)
			);
			DataPrepState.columnMetadata = result.column_metadata;

			renderAll();
		} catch (error) {
			console.error("--- [ERROR] Error loading data and profiling:", error);
			const container = document.getElementById("dataPreviewContainer");
			if (container) {
				container.innerHTML = `<div class="alert alert-danger m-3">${error.message}</div>`;
			}
		}
	}

	// =================================================================
	// 2. EVENT BINDING
	// =================================================================

	function bindEventListeners() {
		const contentEl = document.getElementById("dataPrepContent");
		if (!contentEl) return;

		// Centralized event listener for all clicks within the data prep modal
		contentEl.addEventListener("click", async (e) => {
			const t = e.target.closest("button, a");
			if (!t) return; // Exit if the click wasn't on a button or anchor

			// New: top-right "Add Cleaning Step" button above the pipeline
			if (t.id === "addCleaningStepBtn") {
				// If you have a step chooser, open it here.
				// For now, open your existing Fill Missing modal as the first step:
				e.preventDefault();
				openFillMissingModal(); // <-- existing function you already use
				return;
			}

			// (Optional) quick-step dropdown items (if you kept the dropdown)
			if (t.dataset && t.dataset.dpAction) {
				e.preventDefault();
				switch (t.dataset.dpAction) {
					case "fill-missing":
						openFillMissingModal();
						break;
					case "cast":
						openCastTypeModal();
						break;
					case "trim":
						openTrimTextModal();
						break;
					case "replace":
						openReplaceValuesModal();
						break;
					default:
						break;
				}
				return;
			}

			// --- Existing handlers from original file ---
			if (t.id === "addTransformationBtn") {
				openFillMissingModal();
			}
			if (t.id === "addFillStepBtn") {
				addFillMissingStep();
			}
			if (t.id === "applyTransformationsBtn") {
				if (typeof window.onDataPrepApplied === "function") {
					window.onDataPrepApplied(DataPrepState.recipe);
				}
				// close modal
				const rbModal = document.getElementById("dataPrepModal");
				if (rbModal) bootstrap.Modal.getInstance(rbModal)?.hide();
			}
			if (t.id === "saveCleanSourceBtn") {
				// Optional: you can implement save-as-source later; for now show a toast
				alert("Save-as-clean-source will be available in the next iteration.");
			}
		});

		const fillMethodSelect = document.getElementById("fillMethodSelect");
		if (fillMethodSelect) {
			fillMethodSelect.addEventListener("change", () => {
				const customValueContainer = document.getElementById(
					"customValueContainer"
				);
				customValueContainer.style.display =
					fillMethodSelect.value === "custom" ? "block" : "none";
			});
		}

		// Basic re-render after adding steps
		function renderAll() {
			renderPreviewTable();
			renderColumnList();
			renderPipelineList();
		}

		function renderPreviewTable() {
			const container = document.getElementById("dataPreviewContainer");
			const rows = DataPrepState.currentPreviewData || [];
			if (!container) return;

			if (!rows.length) {
				container.innerHTML = `<div class="p-3 text-muted small">No rows to display.</div>`;
				return;
			}
			const headers = Object.keys(rows[0]);
			const thead = `<thead><tr>${headers
				.map((h) => `<th>${h}</th>`)
				.join("")}</tr></thead>`;
			const tbody = `<tbody>${rows
				.map(
					(r) =>
						`<tr>${headers.map((h) => `<td>${r[h] ?? ""}</td>`).join("")}</tr>`
				)
				.join("")}</tbody>`;
			container.innerHTML = `<table class="table table-sm table-striped mb-0">${thead}${tbody}</table>`;
		}

		function renderColumnList() {
			const list = document.getElementById("columnList");
			if (!list) return;
			const meta = DataPrepState.columnMetadata || {};
			const names = Object.keys(meta);
			list.innerHTML = names
				.map((n) => {
					const p = meta[n] || {};
					const nullPct = p.null_pct ?? 0;
					const uniq = p.unique_pct ?? 0;
					const inferred = p.inferred_type ?? "";
					return `<button type="button" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
      <span>${n}</span>
      <span class="badge bg-light text-dark">${inferred}</span>
        </button>`;
				})
				.join("");
		}

		function renderPipelineList() {
			const wrap = document.getElementById("pipelineList");
			if (!wrap) return;
			const steps = DataPrepState.recipe || [];
			if (!steps.length) {
				wrap.innerHTML = `<p class="text-muted small text-center p-3">No cleaning steps have been applied yet.</p>`;
				return;
			}
			wrap.innerHTML = steps
				.map((s, i) => {
					const label = s.strategy || s.action;
					const col = s.column;
					return `<div class="card card-body py-2 px-3 mb-2">
      <div class="d-flex justify-content-between">
        <div><strong>${label}</strong> <span class="text-muted">on</span> <code>${col}</code></div>
        <div class="text-muted">#${i + 1}</div>
      </div>
        </div>`;
				})
				.join("");
		}
	}

	// =================================================================
	// 3. CORE LOGIC
	// =================================================================

	function openFillMissingModal() {
		// Populate the column dropdown before showing
		const selectEl = document.getElementById("fillColumnSelect");
		const columns = Object.keys(DataPrepState.columnMetadata);
		selectEl.innerHTML = columns
			.map((c) => `<option value="${c}">${c}</option>`)
			.join("");
		fillMissingModal.show();
	}

	async function addFillMissingStep() {
		const column = document.getElementById("fillColumnSelect").value;
		const method = document.getElementById("fillMethodSelect").value;
		const customValue = document.getElementById("fillCustomValue").value;

		if (method === "custom" && !customValue) {
			showError("Please enter a custom value.");
			return;
		}

		// Create the step object
		const step = {
			strategy: "handle_missing",
			column: column,
			params: { method: method },
		};

		if (method === "custom") {
			step.params.value = customValue;
		}

		// Add to recipe and update UI
		DataPrepState.recipe.push(step);
		renderPipeline();
		applyFullRecipeToPreview();
		fillMissingModal.hide();
		renderPipelineList();
		await loadAndProfileData();
	}

	/**
	 * Re-calculates the preview data by applying all steps from the recipe
	 * to a fresh copy of the original data. This happens entirely in the browser.
	 */
	function applyFullRecipeToPreview() {
		// Start fresh from the original data
		let tempData = JSON.parse(
			JSON.stringify(DataPrepState.originalPreviewData)
		);

		DataPrepState.recipe.forEach((step) => {
			if (step.strategy === "handle_missing") {
				const col = step.column;
				let fillValue;

				// Calculate fill value based on the *current state* of tempData
				const nonNullValues = tempData
					.map((row) => row[col])
					.filter((v) => v !== null && v !== undefined);

				if (step.params.method === "mean") {
					const numbers = nonNullValues.map(Number).filter((n) => !isNaN(n));
					fillValue =
						numbers.length > 0
							? numbers.reduce((a, b) => a + b, 0) / numbers.length
							: 0;
				} else if (step.params.method === "median") {
					const numbers = nonNullValues
						.map(Number)
						.filter((n) => !isNaN(n))
						.sort((a, b) => a - b);
					if (numbers.length > 0) {
						const mid = Math.floor(numbers.length / 2);
						fillValue =
							numbers.length % 2 !== 0
								? numbers[mid]
								: (numbers[mid - 1] + numbers[mid]) / 2;
					} else {
						fillValue = 0;
					}
				} else if (step.params.method === "mode") {
					if (nonNullValues.length > 0) {
						const counts = nonNullValues.reduce(
							(acc, val) => acc.set(val, (acc.get(val) || 0) + 1),
							new Map()
						);
						fillValue = [...counts.entries()].reduce((a, b) =>
							b[1] > a[1] ? b : a
						)[0];
					}
				} else if (step.params.method === "custom") {
					fillValue = step.params.value;
				}

				// Apply the fill value to the tempData
				if (fillValue !== undefined) {
					tempData.forEach((row) => {
						if (row[col] === null || row[col] === undefined) {
							row[col] = fillValue;
						}
					});
				}
			}
			// We will add more "if (step.strategy === ...)" blocks here later
		});

		DataPrepState.currentPreviewData = tempData;
		renderPreviewTable(); // Re-render the table with transformed data
	}

	// =================================================================
	// 4. RENDERING FUNCTIONS
	// =================================================================

	function renderAll() {
		renderColumnList();
		renderPreviewTable();
		renderPipeline();
	}

	function renderColumnList() {
		const columnListEl = document.getElementById("columnList");
		if (!columnListEl) return;
		const columns = Object.keys(DataPrepState.columnMetadata);
		if (columns.length === 0) {
			columnListEl.innerHTML =
				'<div class="list-group-item text-muted">No columns found.</div>';
			return;
		}
		columnListEl.innerHTML = columns
			.map((col) => {
				const meta = DataPrepState.columnMetadata[col];
				const hasIssues = meta.null_count > 0;
				const null_percent =
					meta.total_count > 0
						? ((meta.null_count / meta.total_count) * 100).toFixed(0)
						: 0;
				return `
                <a href="#" class="list-group-item list-group-item-action ${
									hasIssues ? "list-group-item-warning" : ""
								}">
                    <div class="d-flex w-100 justify-content-between">
                        <h6 class="mb-1 fw-bold small">${col}</h6><small class="text-info">${
					meta.inferred_type
				}</small>
                    </div>
                    <div class="d-flex w-100 justify-content-between small">
                         <span class="text-muted">Missing: ${null_percent}%</span><span class="text-muted">Unique: ${
					meta.unique_count
				}</span>
                    </div>
                </a>`;
			})
			.join("");
	}

	function renderPreviewTable() {
		const previewContainer = document.getElementById("dataPreviewContainer");
		if (!previewContainer) return;
		const data = DataPrepState.currentPreviewData;
		if (!data || data.length === 0) {
			previewContainer.innerHTML =
				'<p class="text-muted p-5 text-center">No data to display.</p>';
			return;
		}
		const headers = Object.keys(data[0]);
		let tableHtml =
			'<table class="table table-sm table-bordered table-hover"><thead><tr>';
		headers.forEach((h) => (tableHtml += `<th>${h}</th>`));
		tableHtml += "</tr></thead><tbody>";

		data.forEach((row, rowIndex) => {
			tableHtml += "<tr>";
			headers.forEach((h) => {
				const originalValue = DataPrepState.originalPreviewData[rowIndex]
					? DataPrepState.originalPreviewData[rowIndex][h]
					: undefined;
				const currentValue = row[h];
				const isChanged = String(originalValue) !== String(currentValue);
				const cellClass = isChanged ? "table-success" : "";
				tableHtml += `<td class="${cellClass}">${
					currentValue === null
						? '<em class="text-muted">null</em>'
						: currentValue
				}</td>`;
			});
			tableHtml += "</tr>";
		});

		tableHtml += "</tbody></table>";
		previewContainer.innerHTML = tableHtml;
	}

	function renderPipeline() {
		const pipelineListEl = document.getElementById("pipelineList");
		if (!pipelineListEl) return;

		if (DataPrepState.recipe.length === 0) {
			pipelineListEl.innerHTML =
				'<p class="text-muted small text-center p-3">No cleaning steps have been applied yet.</p>';
			return;
		}

		pipelineListEl.innerHTML = DataPrepState.recipe
			.map((step, index) => {
				let description = `Fill missing <b>${step.column}</b> with <b>${step.params.method}</b>`;
				if (step.params.method === "custom") {
					description += ` (<i>${step.params.value}</i>)`;
				}
				return `
                <div class="alert alert-secondary p-2 d-flex justify-content-between align-items-center">
                    <span class="small">${index + 1}. ${description}</span>
                    <button type="button" class="btn-close btn-sm remove-step-btn" data-index="${index}" title="Remove this step"></button>
                </div>
            `;
			})
			.join("");

		// Add event listeners for the remove buttons
		pipelineListEl.querySelectorAll(".remove-step-btn").forEach((btn) => {
			btn.addEventListener("click", (e) => {
				const indexToRemove = parseInt(e.target.dataset.index, 10);
				DataPrepState.recipe.splice(indexToRemove, 1);
				renderPipeline();
				applyFullRecipeToPreview();
			});
		});
	}
})();

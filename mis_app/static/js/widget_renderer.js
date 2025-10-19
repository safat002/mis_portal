/* widget_renderer.js
 * Non-module UMD-style: attaches WidgetRenderer to window.
 * Depends on global AppState (providing dashboardId) and a CSRF input in DOM.
 */
(function (global) {
	"use strict";

	// ---- helpers -------------------------------------------------------------

	function getCsrfToken() {
		const el = document.querySelector('input[name="csrfmiddlewaretoken"]');
		return el ? el.value : "";
	}

	function sanitize(str) {
		if (str === null || str === undefined) return "";
		return String(str)
			.replace(/&/g, "&amp;")
			.replace(/</g, "&lt;")
			.replace(/>/g, "&gt;");
	}

	// inside your widget render/template (per widget)
	function renderWidgetChrome(widgetId, title = "Untitled") {
		return `
    <div class="dd-widget-chrome">
      <div class="dd-widget-title">${title}</div>
      <div class="dd-widget-actions">
        <button class="btn btn-sm btn-light gs-no-drag" 
                title="Edit"
                data-action="edit-widget"
                data-widget-id="${widgetId}">
          <i class="bi bi-sliders"></i>
        </button>
        <button class="btn btn-sm btn-light gs-no-drag" 
                title="Refresh"
                data-action="refresh-widget"
                data-widget-id="${widgetId}">
          <i class="bi bi-arrow-repeat"></i>
        </button>
        <button class="btn btn-sm btn-light text-danger gs-no-drag" 
                title="Delete"
                data-action="delete-widget"
                data-widget-id="${widgetId}">
          <i class="bi bi-trash"></i>
        </button>
      </div>
    </div>
  `;
	}

	// Minimal renderer; replace with your real chart/table renderers if available
	function renderWidgetContent(container, type, rows) {
		console.debug("[renderWidgetContent] type:", type, "rows:", rows);
		const widgetEl = container.closest(".dd-widget");
		if (widgetEl) {
			if (type === "table") widgetEl.classList.add("is-table");
			else widgetEl.classList.remove("is-table"); // <- MUST remove for pie
		}

		// If you have a dedicated renderer elsewhere, call it here instead.
		// e.g., global.Renderers.render(container, type, rows);

		// Graceful fallback rendering
		if (!Array.isArray(rows) || rows.length === 0) {
			container.innerHTML = `
        <div class="p-2 small text-muted">No data returned.</div>
      `;
			return;
		}

		if (type === "table") {
			// Build a simple table from result rows
			const cols = Object.keys(rows[0]);
			let thead = cols.map((c) => `<th>${sanitize(c)}</th>`).join("");
			let tbody = rows
				.map(
					(r) =>
						`<tr>${cols.map((c) => `<td>${sanitize(r[c])}</td>`).join("")}</tr>`
				)
				.join("");
			container.innerHTML = `
        <div class="table-responsive">
          <table class="table table-sm table-striped mb-0">
            <thead><tr>${thead}</tr></thead>
            <tbody>${tbody}</tbody>
          </table>
        </div>
      `;
			return;
		}

		if (["bar", "line", "area", "pie", "doughnut"].includes(type)) {
			if (typeof Chart === "function" && rows.length) {
				const keys = Object.keys(rows[0] || {});
				if (keys.length >= 2) {
					const labelKey = keys[0];
					const valueKeys = keys.slice(1);
					const labels = rows.map((row) => (row[labelKey] ?? "").toString());
					const palette = [
						"#4e79a7",
						"#f28e2b",
						"#e15759",
						"#76b7b2",
						"#59a14f",
						"#edc949",
						"#af7aa1",
						"#ff9da7",
						"#9c755f",
						"#bab0ab",
					];
					const chartType =
						type === "area" ? "line" : type === "doughnut" ? "doughnut" : type;
					const ctx = mountChartCanvas(container);
					if (ctx) {
						if (
							container._chart &&
							typeof container._chart.destroy === "function"
						) {
							container._chart.destroy();
						}

						if (["pie", "doughnut"].includes(type)) {
							const valueKeys = keys.slice(1);
							const numericKeys = valueKeys.filter((k) =>
								rows.some((r) => Number.isFinite(Number(r[k])))
							);
							const valueKey = numericKeys[0] || valueKeys[0];
							const dataPoints = rows.map(
								(row) => Number(row?.[valueKey]) || 0
							);

							container._chart = new Chart(ctx, {
								type,
								data: {
									labels,
									datasets: [
										{
											label: valueKey || "value",
											data: dataPoints,
											backgroundColor: labels.map(
												(_, idx) => palette[idx % palette.length]
											),
											borderWidth: 1,
										},
									],
								},
								options: { responsive: true, maintainAspectRatio: false },
							});
							attachAutoResize(container._chart, container);
						} else {
							const datasets = valueKeys.map((key, idx) => {
								const color = palette[idx % palette.length];
								return {
									label: key,
									data: rows.map((row) => Number(row[key]) || 0),
									backgroundColor: type === "bar" ? color : `${color}B3`,
									borderColor: color,
									borderWidth: 2,
									fill: type === "area",
									tension: type === "line" || type === "area" ? 0.35 : 0,
								};
							});
							container._chart = new Chart(ctx, {
								type: chartType,
								data: { labels, datasets },
								options: {
									responsive: true,
									maintainAspectRatio: false,
									scales: { y: { beginAtZero: true } },
								},
							});
							attachAutoResize(container._chart, container);
						}
						return;
					}
				}
			}

			// Chart.js unavailable or data insufficient: fallback preview
			if (container._chart && typeof container._chart.destroy === "function") {
				container._chart.destroy();
				container._chart = null;
			}

			const first = rows[0] || {};
			const keys = Object.keys(first);
			const xKey = keys[0] || "x";
			const yKey = keys[1] || keys[0] || "y";
			const preview = rows
				.slice(0, 10)
				.map((r) => `${sanitize(r[xKey])} -> ${sanitize(r[yKey])}`)
				.join("<br>");
			container.innerHTML = `
        <div class="p-2">
          <div class="small text-muted mb-2">Preview (${sanitize(type)}):</div>
          <div class="small" style="line-height:1.4">${preview}</div>
          <div class="text-muted mt-2 small">Tip: plug in your charting lib here.</div>
        </div>
      `;
			return;
		}

		container.innerHTML = `
      <pre class="p-2 small overflow-auto">${sanitize(
				JSON.stringify(rows, null, 2)
			)}</pre>
    `;
	}

	// ---- class ---------------------------------------------------------------

	// small escape helper
	function escapeHtml(v) {
		return v === null || v === undefined
			? ""
			: String(v).replace(
					/[&<>]/g,
					(s) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[s])
			  );
	}

	// ---- CHART LAYOUT HELPERS (add once at top) ----
	function mountChartCanvas(targetEl) {
		// Wrap the canvas so CSS can give it 100% height/width
		targetEl.innerHTML = `<div class="chart-container"><canvas></canvas></div>`;
		const canvas = targetEl.querySelector("canvas");
		return canvas.getContext("2d");
	}

	function attachAutoResize(chart, targetEl) {
		// Resize when the DOM element changes size
		try {
			const el = targetEl.closest(".dd-widget") || targetEl;
			const ro = new ResizeObserver(() => chart.resize());
			ro.observe(el);
		} catch (_) {
			/* older browsers */
		}
	}

	// One-time GridStack resize hook -> resize all live charts
	if (!window.__chartResizeHookInstalled) {
		document.addEventListener("gridstack:resizestop", () => {
			document.querySelectorAll(".dd-widget canvas").forEach((c) => {
				const ch =
					window.Chart && window.Chart.getChart
						? window.Chart.getChart(c)
						: null;
				if (ch) ch.resize();
			});
		});
		window.__chartResizeHookInstalled = true;
	}
	// ---- END helpers ----

	class WidgetRenderer {
		constructor(containerEl) {
			this.containerEl = containerEl;
		}

		async renderWidget(widget) {
			console.log("Rendering widget:", widget); // Debug log

			const type = widget?.type;
			const dataConfig = widget?.dataConfig || {};

			try {
				this.containerEl.innerHTML = `<div class="widget-loader p-3 text-center">
            <div class="spinner-border spinner-border-sm me-2"></div>
            Loading data...
        </div>`;

				console.log("Sending request with config:", { type, dataConfig });

				const res = await fetch(
					`/api/dashboard/${global.AppState.dashboardId}/widget/${widget.id}/data/`,
					{
						method: "POST",
						headers: {
							"Content-Type": "application/json",
							"X-CSRFToken": getCsrfToken(),
						},
						body: JSON.stringify({
							type: type,
							dataConfig: dataConfig,
						}),
					}
				);

				console.log("Response status:", res.status);

				let json = {};
				try {
					json = await res.json();
					console.log("Full API response:", json);
				} catch (parseError) {
					console.error("JSON parse error:", parseError);
					throw new Error("Server returned invalid JSON");
				}

				if (!res.ok || !json.success) {
					const msg = json.error || `HTTP ${res.status}`;
					throw new Error(msg);
				}

				// Check if we have rows data
				if (json.rows && Array.isArray(json.rows) && json.rows.length > 0) {
					console.log(`Rendering ${json.rows.length} rows for ${type} widget`);
					this.renderWidgetContent(type, json.rows);
				} else {
					console.warn("No rows data in response:", json);
					this.containerEl.innerHTML = `
                <div class="widget-empty p-3 text-center text-muted">
                    <i class="bi bi-inbox"></i><br>
                    No data available<br>
                    <small>Configure fields and check data context</small>
                </div>`;
				}
			} catch (err) {
				console.error(`Widget render error:`, err);
				this.containerEl.innerHTML = `
            <div class="widget-error p-3 text-center text-danger">
                <i class="bi bi-exclamation-triangle"></i><br>
                Failed to load data<br>
                <small>${sanitize(err.message)}</small>
            </div>`;
			}
		}

		// Add this new method to handle content rendering
		renderWidgetContent(type, rows) {
			console.log(`Rendering ${type} with data:`, rows);

			// Clear any existing content
			this.containerEl.innerHTML = "";

			if (type === "kpi") {
				this.renderKPI(rows);
			} else if (type === "table") {
				this.renderTable(rows);
			} else if (["bar", "line", "area", "pie", "doughnut"].includes(type)) {
				this.renderChart(type, rows);
			} else {
				this.renderGeneric(rows);
			}
		}

		renderKPI(rows) {
			if (!rows || rows.length === 0) {
				this.containerEl.innerHTML =
					'<div class="p-3 text-muted text-center">No KPI data</div>';
				return;
			}

			// More robustly find the value and label keys.
			const firstRow = rows[0];
			const valueKey =
				Object.keys(firstRow).find((k) => k.toLowerCase() === "value") ||
				Object.keys(firstRow)[1] ||
				Object.keys(firstRow)[0];
			const labelKey =
				Object.keys(firstRow).find((k) => k.toLowerCase() === "label") ||
				Object.keys(firstRow)[0];

			const value = firstRow[valueKey] ?? 0;
			const label = firstRow[labelKey] ?? "Value";

			this.containerEl.innerHTML = `
        <div class="kpi-widget h-100 d-flex flex-column justify-content-center align-items-center p-3">
            <div class="kpi-value display-4 fw-bold text-primary">${this.formatValue(
							value
						)}</div>
            <div class="kpi-label text-uppercase text-muted small mt-2">${this.escapeHtml(
							label
						)}</div>
        </div>
    `;
		}

		renderTable(rows) {
			if (!rows || rows.length === 0) {
				this.containerEl.innerHTML =
					'<div class="p-3 text-muted text-center">No table data</div>';
				return;
			}

			const headers = Object.keys(rows[0]);
			const headerRow = headers
				.map((h) => `<th>${this.escapeHtml(h)}</th>`)
				.join("");

			const bodyRows = rows
				.map(
					(row) => `
        <tr>${headers
					.map((h) => `<td>${this.escapeHtml(row[h])}</td>`)
					.join("")}</tr>
    `
				)
				.join("");

			this.containerEl.innerHTML = `
        <div class="table-responsive h-100">
            <table class="table table-sm table-striped table-hover mb-0">
                <thead class="table-light">
                    <tr>${headerRow}</tr>
                </thead>
                <tbody>
                    ${bodyRows}
                </tbody>
            </table>
        </div>
    `;
		}

		renderChart(type, rows) {
			// 1) Error/empty handling
			if (
				!rows ||
				!Array.isArray(rows) ||
				rows.length === 0 ||
				rows[0]?.error
			) {
				const errorMsg =
					rows && rows[0] && rows[0].error
						? rows[0].error
						: "No data to display.";
				this.containerEl.innerHTML = `
      <div class="widget-error p-3 text-center text-danger">
        <i class="bi bi-exclamation-triangle"></i><br>
        <small>${this.escapeHtml(errorMsg)}</small>
      </div>`;
				return;
			}

			// 2) Non-table widgets should not scroll
			const widgetEl = this.containerEl.closest(".dd-widget");
			if (widgetEl) widgetEl.classList.remove("is-table");

			// 3) Mount canvas (no fixed height) + destroy old chart
			const ctx = mountChartCanvas(this.containerEl);
			if (this.containerEl._chartInstance) {
				try {
					this.containerEl._chartInstance.destroy();
				} catch (_) {}
				this.containerEl._chartInstance = null;
			}

			// 4) Keys & colors
			const firstRow = rows[0] || {};
			const keys = Object.keys(firstRow);
			if (keys.length < 2) {
				this.containerEl.innerHTML = `
      <div class="p-2 small text-muted">Not enough columns to chart.</div>`;
				return;
			}
			const labelKey = keys[0];
			const valueKeys = keys.slice(1);

			const palette =
				window.ColorService && window.ColorService.getColors
					? window.ColorService.getColors(window.AppState?.theme)
					: [
							"#4e79a7",
							"#f28e2b",
							"#e15759",
							"#76b7b2",
							"#59a14f",
							"#edc949",
							"#af7aa1",
							"#ff9da7",
							"#9c755f",
							"#bab0ab",
					  ];

			// 5) PIE / DOUGHNUT: robust DB mapping (handles TALL and WIDE shapes)
			if (type === "pie" || type === "doughnut") {
				let labels = [];
				let dataPoints = [];

				if (rows.length > 1) {
					// TALL: [{labelKey, valueKey}, ...]
					const numericKeys = valueKeys.filter((k) =>
						rows.some((r) => Number.isFinite(Number(r?.[k])))
					);
					const valueKey = numericKeys[0] || valueKeys[0];
					labels = rows.map((r) => (r?.[labelKey] ?? "").toString());
					dataPoints = rows.map((r) => Number(r?.[valueKey]) || 0);
				} else {
					// WIDE: [{labelKey, a: 10, b: 20, ...}] -> columns become slices
					const row = firstRow;
					labels = valueKeys;
					dataPoints = valueKeys.map((k) => Number(row?.[k]) || 0);
				}

				this.containerEl._chartInstance = new Chart(ctx, {
					type: type, // "pie" or "doughnut"
					data: {
						labels: labels,
						datasets: [
							{
								label: "value",
								data: dataPoints,
								backgroundColor: labels.map(
									(_, i) => palette[i % palette.length]
								),
								borderWidth: 1,
							},
						],
					},
					options: {
						responsive: true,
						maintainAspectRatio: false,
						layout: { padding: 0 },
						plugins: { legend: { position: "bottom" } },
					},
				});

				attachAutoResize(this.containerEl._chartInstance, this.containerEl);
				return; // <- important so the rest of the method doesn't run
			}

			// 6) BAR / LINE / AREA
			const labels = rows.map((r) => (r?.[labelKey] ?? "").toString());
			const datasets = valueKeys.map((key, idx) => {
				const color = palette[idx % palette.length];
				return {
					label: key,
					data: rows.map((r) => Number(r?.[key]) || 0),
					backgroundColor: type === "bar" ? color : `${color}33`,
					borderColor: color,
					borderWidth: 2,
					fill: type === "area",
					tension: type === "line" || type === "area" ? 0.3 : 0,
				};
			});

			this.containerEl._chartInstance = new Chart(ctx, {
				type: type === "area" ? "line" : type,
				data: { labels: labels, datasets: datasets },
				options: {
					responsive: true,
					maintainAspectRatio: false,
					layout: { padding: 0 },
					scales: { y: { beginAtZero: true } },
					plugins: {
						legend: { display: valueKeys.length > 1, position: "bottom" },
					},
				},
			});

			attachAutoResize(this.containerEl._chartInstance, this.containerEl);
		}

		renderGeneric(rows) {
			this.containerEl.innerHTML = `
        <pre class="p-3 small">${this.escapeHtml(
					JSON.stringify(rows, null, 2)
				)}</pre>
    `;
		}

		formatValue(value) {
			if (typeof value === "number") {
				return value.toLocaleString();
			}
			return this.escapeHtml(String(value));
		}

		escapeHtml(str) {
			if (str === null || str === undefined) return "";
			return String(str)
				.replace(/&/g, "&amp;")
				.replace(/</g, "&lt;")
				.replace(/>/g, "&gt;")
				.replace(/"/g, "&quot;")
				.replace(/'/g, "&#039;");
		}
	}

	// ---- expose --------------------------------------------------------------

	global.WidgetRenderer = WidgetRenderer;
	// Optional: expose helpers if you want to call from elsewhere
	global._widgetRendererHelpers = {
		renderWidgetContent,
		getCsrfToken,
	};
})(window);

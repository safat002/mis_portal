document.addEventListener("DOMContentLoaded", () => {
	const canvas = document.getElementById("canvas");
	if (!canvas) return;

	getCanvasContent();

	// --- STATE AND CONFIG ---
	let currentConnectionId = null;
	let schema = {};
	let isEditable =
		document.querySelector(".data-model-container")?.dataset.editable ===
		"true";
	let history = [];
	let historyIndex = -1;
	let zoom = 1;
	let selectedConnection = null;
	let sortOrder = "asc";
	let activeJoinWizard = { popover: null, sourceEl: null, previews: [] };

	// --- JSPLUMB INSTANCE ---
	const instance = jsPlumb.getInstance({
		Connector: ["Bezier", { curviness: 50 }],
		Anchors: ["Right", "Left"],
		PaintStyle: {
			strokeWidth: 2,
			stroke: "#4361ee",
			cursor: "pointer", // Add this
		},
		HoverPaintStyle: {
			stroke: "#3a0ca3",
			strokeWidth: 3,
			cursor: "pointer", // Add this
		},
		Endpoint: ["Dot", { radius: 5 }],
		EndpointStyle: {
			fill: "#4361ee",
			cursor: "pointer", // Add this
		},
		Container: document.getElementById("canvas-content"),
		DragOptions: { cursor: "pointer", zIndex: 2000 },
		ConnectionOverlays: [
			[
				"Arrow",
				{
					location: 1,
					id: "arrow",
					length: 10,
					foldback: 0.8,
				},
			],
		],
		ConnectionsDetachable: isEditable,
	});

	function sanitizeId(s) {
		return String(s).replace(/[^a-zA-Z0-9_-]/g, "_");
	}

	// --- UTILITY FUNCTIONS ---
	function getCookie(name) {
		let cookieValue = null;
		if (document.cookie && document.cookie !== "") {
			const cookies = document.cookie.split(";");
			for (let i = 0; i < cookies.length; i++) {
				const cookie = cookies[i].trim();
				if (cookie.substring(0, name.length + 1) === name + "=") {
					cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
					break;
				}
			}
		}
		return cookieValue;
	}

	function inferCardinality(sourceEl, targetEl) {
		if (!sourceEl || !targetEl) return "many-to-many";

		// Convert string '1'/'0' to boolean
		const spk = sourceEl.dataset.pk === "1";
		const sfk = sourceEl.dataset.fk === "1";
		const su = sourceEl.dataset.unique === "1";
		const sNumeric = sourceEl.dataset.isNumeric === "1";

		const tpk = targetEl.dataset.pk === "1";
		const tfk = targetEl.dataset.fk === "1";
		const tu = targetEl.dataset.unique === "1";
		const tNumeric = targetEl.dataset.isNumeric === "1";

		// Get column and table names for pattern matching
		const sourceColumn = sourceEl.dataset.column?.toLowerCase() || "";
		const targetColumn = targetEl.dataset.column?.toLowerCase() || "";
		const sourceTable =
			sourceEl.closest(".table-card")?.dataset.tableName?.toLowerCase() || "";
		const targetTable =
			targetEl.closest(".table-card")?.dataset.tableName?.toLowerCase() || "";

		console.log("Enhanced Cardinality Detection:", {
			source: {
				table: sourceTable,
				column: sourceColumn,
				pk: spk,
				fk: sfk,
				unique: su,
				numeric: sNumeric,
			},
			target: {
				table: targetTable,
				column: targetColumn,
				pk: tpk,
				fk: tfk,
				unique: tu,
				numeric: tNumeric,
			},
		});

		// Scoring system for different cardinality types
		const scores = {
			"one-to-one": 0,
			"one-to-many": 0,
			"many-to-one": 0,
			"many-to-many": 0,
		};

		// Rule 1: Primary Key based rules (highest weight)
		if (spk && tpk) {
			scores["one-to-one"] += 10; // Both are PKs - strong indicator of 1:1
		} else if (spk && !tpk) {
			scores["one-to-many"] += 8; // Source PK, target not PK - strong indicator of 1:N
		} else if (!spk && tpk) {
			scores["many-to-one"] += 8; // Target PK, source not PK - strong indicator of N:1
		}

		// Rule 2: Unique constraint rules (high weight)
		if (su && tu) {
			scores["one-to-one"] += 6; // Both unique - strong indicator of 1:1
		} else if (su && !tu) {
			scores["one-to-many"] += 5; // Source unique, target not unique
		} else if (!su && tu) {
			scores["many-to-one"] += 5; // Target unique, source not unique
		}

		// Rule 3: Foreign key rules (medium weight)
		if (sfk && !tfk) {
			scores["many-to-one"] += 4; // Source is FK, target is not
		} else if (!sfk && tfk) {
			scores["one-to-many"] += 4; // Target is FK, source is not
		} else if (sfk && tfk) {
			scores["many-to-many"] += 3; // Both are FKs - could be junction table
		}

		// Rule 4: Column naming pattern analysis (medium weight)
		const namingScore = analyzeColumnNaming(
			sourceColumn,
			targetColumn,
			sourceTable,
			targetTable
		);
		Object.keys(namingScore).forEach((key) => {
			scores[key] += namingScore[key];
		});

		// Rule 5: Data type compatibility (low weight)
		if (sNumeric && tNumeric) {
			scores["one-to-one"] += 1;
			scores["many-to-one"] += 1;
			scores["one-to-many"] += 1;
		}

		// Rule 6: Table relationship patterns (medium weight)
		const relationshipScore = analyzeTableRelationship(
			sourceTable,
			targetTable,
			sourceColumn,
			targetColumn
		);
		Object.keys(relationshipScore).forEach((key) => {
			scores[key] += relationshipScore[key];
		});

		console.log("Cardinality Scores:", scores);

		// Find the cardinality with the highest score
		let bestCardinality = "many-to-many";
		let highestScore = -1;

		for (const [cardinality, score] of Object.entries(scores)) {
			if (score > highestScore) {
				highestScore = score;
				bestCardinality = cardinality;
			}
		}

		// If all scores are 0, fall back to basic detection
		if (highestScore === 0) {
			return basicCardinalityDetection(spk, tpk, su, tu, sfk, tfk);
		}

		console.log(
			"Selected Cardinality:",
			bestCardinality,
			"with score:",
			highestScore
		);
		return bestCardinality;
	}

	// Helper function for basic cardinality detection (fallback)
	function basicCardinalityDetection(spk, tpk, su, tu, sfk, tfk) {
		// Primary Key based detection (most reliable)
		if (spk && tpk) return "one-to-one";
		if (spk && !tpk) return "one-to-many";
		if (!spk && tpk) return "many-to-one";

		// Unique constraint based detection
		if (su && !tu) return "one-to-many";
		if (!su && tu) return "many-to-one";
		if (su && tu) return "one-to-one";

		// Foreign key based detection
		if (sfk && !tfk) return "many-to-one";
		if (!sfk && tfk) return "one-to-many";

		return "many-to-many";
	}

	// Analyze column naming patterns for relationship clues
	function analyzeColumnNaming(
		sourceColumn,
		targetColumn,
		sourceTable,
		targetTable
	) {
		const scores = {
			"one-to-one": 0,
			"one-to-many": 0,
			"many-to-one": 0,
			"many-to-many": 0,
		};

		// Common patterns for primary keys
		const pkPatterns = ["id", "key", "pk", "code", "num", "no", "nr"];
		// Common patterns for foreign keys
		const fkPatterns = ["id", "fk", "ref", "code", "key"];

		// Check if source column looks like a primary key
		const sourceIsPkLike = pkPatterns.some(
			(pattern) =>
				sourceColumn.includes(pattern) ||
				sourceColumn === `${sourceTable}_id` ||
				sourceColumn === "id"
		);

		// Check if target column looks like a primary key
		const targetIsPkLike = pkPatterns.some(
			(pattern) =>
				targetColumn.includes(pattern) ||
				targetColumn === `${targetTable}_id` ||
				targetColumn === "id"
		);

		// Check if source column looks like a foreign key to target table
		const sourceIsFkToTarget =
			sourceColumn === `${targetTable}_id` ||
			sourceColumn === `${targetTable.slice(0, -1)}_id` || // Handle plural (users -> user_id)
			sourceColumn.includes(`${targetTable}_`) ||
			(sourceColumn.endsWith("_id") && !sourceIsPkLike);

		// Check if target column looks like a foreign key to source table
		const targetIsFkToSource =
			targetColumn === `${sourceTable}_id` ||
			targetColumn === `${sourceTable.slice(0, -1)}_id` || // Handle plural
			targetColumn.includes(`${sourceTable}_`) ||
			(targetColumn.endsWith("_id") && !targetIsPkLike);

		// Score based on naming patterns
		if (sourceIsPkLike && targetIsPkLike) {
			scores["one-to-one"] += 3;
		} else if (sourceIsPkLike && targetIsFkToSource) {
			scores["one-to-many"] += 4;
		} else if (sourceIsFkToTarget && targetIsPkLike) {
			scores["many-to-one"] += 4;
		} else if (sourceIsFkToTarget && targetIsFkToSource) {
			scores["many-to-many"] += 3;
		}

		return scores;
	}

	// Analyze table relationships based on table names and common patterns
	function analyzeTableRelationship(
		sourceTable,
		targetTable,
		sourceColumn,
		targetColumn
	) {
		const scores = {
			"one-to-one": 0,
			"one-to-many": 0,
			"many-to-one": 0,
			"many-to-many": 0,
		};

		// Common one-to-one relationships
		const oneToOnePatterns = [
			["user", "profile"],
			["profile", "user"],
			["customer", "details"],
			["details", "customer"],
			["product", "inventory"],
			["inventory", "product"],
		];

		// Common one-to-many relationship patterns
		const oneToManyPatterns = [
			["user", "post"],
			["customer", "order"],
			["category", "product"],
			["department", "employee"],
			["parent", "child"],
		];

		// Check for known one-to-one patterns
		for (const [table1, table2] of oneToOnePatterns) {
			if (
				(sourceTable.includes(table1) && targetTable.includes(table2)) ||
				(sourceTable.includes(table2) && targetTable.includes(table1))
			) {
				scores["one-to-one"] += 3;
				break;
			}
		}

		// Check for known one-to-many patterns
		for (const [oneTable, manyTable] of oneToManyPatterns) {
			if (sourceTable.includes(oneTable) && targetTable.includes(manyTable)) {
				scores["one-to-many"] += 3;
				break;
			} else if (
				sourceTable.includes(manyTable) &&
				targetTable.includes(oneTable)
			) {
				scores["many-to-one"] += 3;
				break;
			}
		}

		// Look for junction table patterns (many-to-many)
		const junctionTableIndicators = [
			"map",
			"mapping",
			"relation",
			"relationship",
			"link",
			"junction",
			"assoc",
			"association",
			"xref",
			"crossref",
		];

		const isJunctionTable = junctionTableIndicators.some(
			(indicator) =>
				sourceTable.includes(indicator) || targetTable.includes(indicator)
		);

		if (isJunctionTable) {
			scores["many-to-many"] += 4;
		}

		// Check for self-referencing relationships
		if (sourceTable === targetTable) {
			// Common self-referencing patterns (manager-employee, category-subcategory)
			if (
				sourceColumn.includes("parent") ||
				targetColumn.includes("parent") ||
				sourceColumn.includes("manager") ||
				targetColumn.includes("manager") ||
				sourceColumn.includes("report") ||
				targetColumn.includes("report")
			) {
				scores["many-to-one"] += 2; // Typically hierarchical
			}
		}

		return scores;
	}

	// Enhanced setupNewConnection function to use the smarter cardinality detection
	function setupNewConnection(connection, sourceEl, targetEl) {
		try {
			const newCardinality = inferCardinality(sourceEl, targetEl);

			connection.setData({
				joinType: "INNER",
				cardinality: newCardinality,
			});
			connection.addClass(joinCssClass("INNER"));
			setCardinalityOverlays(connection, newCardinality);
		} catch (error) {
			console.error("Failed to setup new connection:", error);
			instance.deleteConnection(connection);
		}
	}

	function upsertJoin(/* j */) {
		/* no-op to avoid ReferenceError */
	}

	function debugConnections() {
		const connections = instance.getAllConnections();
		console.log(`Total connections: ${connections.length}`);

		connections.forEach((conn, index) => {
			console.log(`Connection ${index}:`, {
				source: conn.sourceId,
				target: conn.targetId,
				data: conn.getData(),
				hasClickHandler:
					conn._jsPlumb &&
					conn._jsPlumb.listeners &&
					conn._jsPlumb.listeners.click,
			});
		});
	}

	function safeAddEventListener(elementId, event, handler) {
		const element = document.getElementById(elementId);
		if (element) element.addEventListener(event, handler);
	}

	function getTypeIcon(type) {
		const t = type.toLowerCase();
		if (
			t.includes("int") ||
			t.includes("num") ||
			t.includes("dec") ||
			t.includes("real")
		)
			return "123";
		else if (t.includes("date") || t.includes("time")) return "📅";
		return "Abc";
	}

	function findFieldElement(cardId, columnName) {
		const sanitizedColumnName = String(columnName).replace(
			/[^a-zA-Z0-9_-]/g,
			"_"
		);
		const fieldId = `${cardId}_col_${sanitizedColumnName}`;
		const element = document.getElementById(fieldId);

		if (!element) {
			console.warn("Field element not found:", fieldId);
			// Try alternative lookup by data attributes
			const card = document.getElementById(cardId);
			if (card) {
				const alternative = card.querySelector(
					`[data-column="${CSS.escape(columnName)}"]`
				);
				if (alternative) {
					console.log("Found alternative element for:", columnName);
					return alternative;
				}
			}
		}

		return element;
	}

	function joinCssClass(type) {
		if (!type) return "inner-join";
		const t = type.toUpperCase();
		if (t === "LEFT") return "left-join";
		if (t === "RIGHT") return "right-join";
		if (t === "FULL") return "full-join";
		return "inner-join";
	}

	function determineCardinality(sourceIsPk, targetIsPk) {
		if (sourceIsPk && targetIsPk) {
			return "one-to-one";
		} else if (sourceIsPk && !targetIsPk) {
			return "one-to-many";
		} else if (!sourceIsPk && targetIsPk) {
			return "many-to-one";
		} else {
			return "many-to-many";
		}
	}

	function setupNewConnection(connection, sourceEl, targetEl) {
		try {
			const sourceIsPk = sourceEl.dataset.pk === "1";
			const targetIsPk = targetEl.dataset.pk === "1";

			const newCardinality = determineCardinality(sourceIsPk, targetIsPk);

			connection.setData({
				joinType: "INNER",
				cardinality: newCardinality,
			});
			connection.addClass(joinCssClass("INNER"));
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

		if (document.getElementById("undoBtn")) {
			document.getElementById("undoBtn").disabled = historyIndex <= 0;
		}
		if (document.getElementById("redoBtn")) {
			document.getElementById("redoBtn").disabled =
				historyIndex >= history.length - 1;
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

		snapshot.layout.forEach((l) => {
			const tableSchema = schema[l.tableName];
			if (tableSchema) {
				addTableCard(tableSchema, l.x, l.y);
				if (l.collapsed) {
					const card = document.querySelector(
						`.table-card[data-table-name="${CSS.escape(l.tableName)}"]`
					);
					if (card) {
						const body = card.querySelector(".table-card-body");
						if (body) {
							body.classList.add("collapsed");
							const collapseBtn = card.querySelector(".btn-collapse i");
							if (collapseBtn) collapseBtn.className = "bi bi-chevron-right";
							rerouteConnectionsToHeader(card);
						}
					}
				}
			}
		});

		setTimeout(() => {
			snapshot.joins.forEach((j) => {
				const sourceCardId = `card_${j.leftTable}`;
				const targetCardId = `card_${j.rightTable}`;

				const sourceEl = findFieldElement(sourceCardId, j.leftColumn);
				const targetEl = findFieldElement(targetCardId, j.rightColumn);

				if (sourceEl && targetEl) {
					const conn = instance.connect({
						source: sourceEl,
						target: targetEl,
						data: {
							joinType: j.joinType || "INNER",
							cardinality: j.cardinality || "one-to-many",
							originalSourceId: sourceEl.id,
							originalTargetId: targetEl.id,
						},
					});
					conn.addClass(joinCssClass(j.joinType || "INNER"));
					setCardinalityOverlays(conn, j.cardinality || "one-to-many");
				}
			});
			instance.repaintEverything();
		}, 300);

		zoom = snapshot.viewport?.zoom || 1;
		applyZoom(zoom, { x: 0, y: 0 });
		canvas.scrollLeft = snapshot.viewport?.scrollLeft || 0;
		canvas.scrollTop = snapshot.viewport?.scrollTop || 0;

		if (document.getElementById("undoBtn")) {
			document.getElementById("undoBtn").disabled = historyIndex <= 0;
		}
		if (document.getElementById("redoBtn")) {
			document.getElementById("redoBtn").disabled =
				historyIndex >= history.length - 1;
		}
	}

	function getCanvasContent() {
		let content = document.getElementById("canvas-content");
		if (!content) {
			// Create the content container if it doesn't exist
			content = document.createElement("div");
			content.id = "canvas-content";
			content.className = "canvas-content";

			// Move existing placeholder into content
			const placeholder = document.getElementById("canvas-placeholder");
			if (placeholder) {
				placeholder.remove();
				content.appendChild(placeholder);
			}

			// Append content to canvas
			const canvas = document.getElementById("canvas");
			if (canvas) {
				canvas.appendChild(content);
			}
		}
		return content;
	}

	async function exportAsPNG() {
		try {
			showLoading("Generating PNG...");

			const content = getCanvasContent();
			const canvasElement = document.getElementById("canvas");

			if (!content || !canvasElement) {
				throw new Error("Canvas content not found");
			}

			// Store current zoom and temporarily reset for capture
			const originalZoom = zoom;
			const originalTransform = content.style.transform;

			// Reset zoom for clean capture
			content.style.transform = "scale(1)";
			content.style.left = "0px";
			content.style.top = "0px";

			// Calculate the bounds of all content
			const cards = content.querySelectorAll(".table-card");
			let minX = Infinity,
				minY = Infinity,
				maxX = -Infinity,
				maxY = -Infinity;

			if (cards.length === 0) {
				throw new Error("No tables to export");
			}

			cards.forEach((card) => {
				const rect = card.getBoundingClientRect();
				const contentRect = content.getBoundingClientRect();

				const x = parseInt(card.style.left) || 0;
				const y = parseInt(card.style.top) || 0;
				const width = card.offsetWidth;
				const height = card.offsetHeight;

				minX = Math.min(minX, x);
				minY = Math.min(minY, y);
				maxX = Math.max(maxX, x + width);
				maxY = Math.max(maxY, y + height);
			});

			// Add padding around the content
			const padding = 50;
			const contentWidth = Math.max(
				maxX - minX + padding * 2,
				canvasElement.clientWidth
			);
			const contentHeight = Math.max(
				maxY - minY + padding * 2,
				canvasElement.clientHeight
			);

			// Position content for capture
			content.style.width = `${contentWidth}px`;
			content.style.height = `${contentHeight}px`;
			content.style.left = `${padding - minX}px`;
			content.style.top = `${padding - minY}px`;

			// Wait for DOM to update
			await new Promise((resolve) => setTimeout(resolve, 100));

			// Use html2canvas to capture the content
			const canvas = await html2canvas(content, {
				backgroundColor: "#ffffff",
				scale: 2, // Higher resolution
				useCORS: true,
				allowTaint: true,
				scrollX: 0,
				scrollY: 0,
				width: contentWidth,
				height: contentHeight,
				onclone: function (clonedDoc) {
					// Ensure connections are visible in the clone
					const clonedContent = clonedDoc.getElementById("canvas-content");
					if (clonedContent) {
						clonedContent.style.transform = "scale(1)";
						clonedContent.style.left = `${padding - minX}px`;
						clonedContent.style.top = `${padding - minY}px`;
						clonedContent.style.width = `${contentWidth}px`;
						clonedContent.style.height = `${contentHeight}px`;
					}
				},
			});

			// Convert to PNG and download
			const pngData = canvas.toDataURL("image/png");
			downloadFile(
				pngData,
				`data-model-${currentConnectionId || "export"}.png`,
				"image/png"
			);

			// Restore original state
			content.style.transform = originalTransform;
			content.style.left = "0px";
			content.style.top = "0px";
			content.style.width = "";
			content.style.height = "";

			zoom = originalZoom;

			hideLoading();
			showSuccess("PNG exported successfully!");
		} catch (error) {
			console.error("PNG export error:", error);
			hideLoading();
			showError(`PNG export failed: ${error.message}`);
		}
	}

	async function exportAsPDF() {
		try {
			showLoading("Generating PDF...");

			const content = getCanvasContent();
			const canvasElement = document.getElementById("canvas");

			if (!content || !canvasElement) {
				throw new Error("Canvas content not found");
			}

			// Store current state
			const originalZoom = zoom;
			const originalTransform = content.style.transform;

			// Reset zoom for clean capture
			content.style.transform = "scale(1)";
			content.style.left = "0px";
			content.style.top = "0px";

			// Calculate content bounds
			const cards = content.querySelectorAll(".table-card");
			if (cards.length === 0) {
				throw new Error("No tables to export");
			}

			let minX = Infinity,
				minY = Infinity,
				maxX = -Infinity,
				maxY = -Infinity;

			cards.forEach((card) => {
				const x = parseInt(card.style.left) || 0;
				const y = parseInt(card.style.top) || 0;
				const width = card.offsetWidth;
				const height = card.offsetHeight;

				minX = Math.min(minX, x);
				minY = Math.min(minY, y);
				maxX = Math.max(maxX, x + width);
				maxY = Math.max(maxY, y + height);
			});

			// Add padding
			const padding = 50;
			const contentWidth = Math.max(
				maxX - minX + padding * 2,
				canvasElement.clientWidth
			);
			const contentHeight = Math.max(
				maxY - minY + padding * 2,
				canvasElement.clientHeight
			);

			// Position content
			content.style.width = `${contentWidth}px`;
			content.style.height = `${contentHeight}px`;
			content.style.left = `${padding - minX}px`;
			content.style.top = `${padding - minY}px`;

			// Wait for DOM update
			await new Promise((resolve) => setTimeout(resolve, 100));

			// Capture as image first
			const canvas = await html2canvas(content, {
				backgroundColor: "#ffffff",
				scale: 2,
				useCORS: true,
				allowTaint: true,
				width: contentWidth,
				height: contentHeight,
				onclone: function (clonedDoc) {
					const clonedContent = clonedDoc.getElementById("canvas-content");
					if (clonedContent) {
						clonedContent.style.transform = "none";
						clonedContent.style.left = `${padding - minX}px`;
						clonedContent.style.top = `${padding - minY}px`;
					}
				},
			});

			// Create PDF
			const { jsPDF } = window.jspdf;
			const pdf = new jsPDF({
				orientation: contentWidth > contentHeight ? "landscape" : "portrait",
				unit: "px",
				format: [contentWidth, contentHeight],
			});

			// Convert canvas to image data
			const imgData = canvas.toDataURL("image/png");

			// Calculate PDF dimensions (A4 size with margins)
			const pdfWidth = pdf.internal.pageSize.getWidth();
			const pdfHeight = pdf.internal.pageSize.getHeight();

			// Scale image to fit PDF page with margins
			const margin = 20;
			const maxWidth = pdfWidth - margin * 2;
			const maxHeight = pdfHeight - margin * 2;

			let imgWidth = contentWidth;
			let imgHeight = contentHeight;

			// Maintain aspect ratio
			if (imgWidth > maxWidth) {
				const ratio = maxWidth / imgWidth;
				imgWidth = maxWidth;
				imgHeight = imgHeight * ratio;
			}

			if (imgHeight > maxHeight) {
				const ratio = maxHeight / imgHeight;
				imgHeight = maxHeight;
				imgWidth = imgWidth * ratio;
			}

			// Center the image on the page
			const x = (pdfWidth - imgWidth) / 2;
			const y = (pdfHeight - imgHeight) / 2;

			pdf.addImage(imgData, "PNG", x, y, imgWidth, imgHeight);

			// Add title and timestamp
			const connectionName =
				document.getElementById("connectionSelect")?.selectedOptions[0]?.text ||
				"Data Model";
			pdf.setFontSize(12);
			pdf.text(`Data Model: ${connectionName}`, margin, 15);
			pdf.text(`Exported: ${new Date().toLocaleString()}`, margin, 30);

			// Save PDF
			pdf.save(`data-model-${currentConnectionId || "export"}.pdf`);

			// Restore original state
			content.style.transform = originalTransform;
			content.style.left = "0px";
			content.style.top = "0px";
			content.style.width = "";
			content.style.height = "";

			zoom = originalZoom;

			hideLoading();
			showSuccess("PDF exported successfully!");
		} catch (error) {
			console.error("PDF export error:", error);
			hideLoading();
			showError(`PDF export failed: ${error.message}`);
		}
	}

	// --- CONNECTION CLICK HANDLING ---
	function makeConnectionClickable(connection) {
		if (!connection) return;

		// Remove any existing click listeners
		connection.unbind();

		// Add click listener
		connection.bind("click", (component, originalEvent) => {
			if (originalEvent) {
				originalEvent.preventDefault();
				originalEvent.stopPropagation();
				console.log("Connection clicked:", connection);
				openJoinPopover(connection, originalEvent);
			}
		});

		// Add context menu listener
		connection.bind("contextmenu", (component, originalEvent) => {
			if (originalEvent) {
				originalEvent.preventDefault();
				originalEvent.stopPropagation();
				openJoinPopover(connection, originalEvent);
			}
		});
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
		safeAddEventListener("zoomSlider", "input", (e) => {
			const zoomValue = parseInt(e.target.value, 10) / 100;
			applyZoom(zoomValue);
		});
		safeAddEventListener("validateBtn", "click", validateModel);
		safeAddEventListener("testConnectionBtn", "click", testConnection);
		safeAddEventListener("tableSearch", "input", updateTableSidebar);
		safeAddEventListener("sortTableBtn", "click", toggleTableSort);
		safeAddEventListener("toggleSidebar", "click", toggleSidebar);
		safeAddEventListener("suggestJoinsBtn", "click", fetchSuggestedJoins);
		safeAddEventListener("applyAllSuggested", "click", applyAllSuggestedJoins);

		safeAddEventListener("zoomInBtn", "click", () => adjustZoom(0.2));
		safeAddEventListener("zoomOutBtn", "click", () => adjustZoom(-0.2));
		safeAddEventListener("resetZoomBtn", "click", resetZoom);

		safeAddEventListener("exportPngBtn", "click", exportAsPNG);
		safeAddEventListener("exportPdfBtn", "click", exportAsPDF);

		// Modal buttons
		safeAddEventListener("saveJoinBtn", "click", () => {
			if (selectedConnection) {
				const joinType = document.getElementById("joinType").value;
				const cardinality = document.getElementById("cardinality").value;
				selectedConnection.setData({ joinType, cardinality });
				setCardinalityOverlays(selectedConnection, cardinality);
				bootstrap.Modal.getInstance(
					document.getElementById("joinModal")
				).hide();
				saveHistory();
			}
		});

		safeAddEventListener("deleteJoinBtn", "click", () => {
			if (selectedConnection) {
				instance.deleteConnection(selectedConnection);
				bootstrap.Modal.getInstance(
					document.getElementById("joinModal")
				).hide();
				saveHistory();
			}
		});

		canvas.addEventListener("dragover", (e) => e.preventDefault());
		canvas.addEventListener("drop", handleCanvasDrop);
		canvas.addEventListener("click", handleCanvasClick);
		document.addEventListener("keydown", handleKeyboardShortcuts);

		// jsPlumb event bindings
		instance.bind("beforeDrop", (info) => {
			if (info.sourceId === info.targetId) return false;

			const sEl = document.getElementById(info.sourceId);
			const tEl = document.getElementById(info.targetId);
			if (!sEl || !tEl) return false;

			const sTable = sEl.closest(".table-card")?.dataset.tableName;
			const tTable = tEl.closest(".table-card")?.dataset.tableName;

			if (sTable && tTable && sTable === tTable) {
				showError?.("Self-join is not allowed");
				return false;
			}
			return true;
		});

		instance.bind("connection", (info, originalEvent) => {
			console.log("Connection event fired", {
				userInitiated: !!originalEvent,
				source: info.source?.dataset?.column,
				target: info.target?.dataset?.column,
			});

			if (originalEvent) {
				// User-dragged connection
				const cardinality = inferCardinality(info.source, info.target);
				console.log("Detected cardinality:", cardinality);

				info.connection.setData({
					joinType: "INNER",
					cardinality: cardinality,
					originalSourceId: info.source.id,
					originalTargetId: info.target.id,
				});

				info.connection.addClass(joinCssClass("INNER"));
				setCardinalityOverlays(info.connection, cardinality);

				// Make the connection clickable
				makeConnectionClickable(info.connection);

				saveHistory();
			}
		});

		// Make existing connections clickable when model loads
		instance.bind("connectionEstablished", (info) => {
			makeConnectionClickable(info.connection);
		});

		// Enhanced tooltip with timeout
		let tooltipTimeout;
		const tooltip = document.createElement("div");
		tooltip.className = "join-info-tooltip";
		document.body.appendChild(tooltip);

		const onOver = (connection, originalEvent) => {
			if (typeof connection.getData !== "function") return;
			clearTimeout(tooltipTimeout);
			const data = connection.getData() || {};
			const sourceEl = document.getElementById(connection.sourceId);
			const targetEl = document.getElementById(connection.targetId);

			if (!sourceEl || !targetEl) return;

			tooltip.innerHTML = `
            <strong>From:</strong> ${
							sourceEl.closest(".table-card").dataset.tableName
						}.${sourceEl.dataset.column}<br>
            <strong>To:</strong> ${
							targetEl.closest(".table-card").dataset.tableName
						}.${targetEl.dataset.column}<br>
            <strong>Type:</strong> ${data.joinType || "INNER"} (${
				data.cardinality || "1:N"
			})
        `;
			tooltip.style.left = `${originalEvent.pageX + 15}px`;
			tooltip.style.top = `${originalEvent.pageY}px`;
			tooltip.style.display = "block";
		};

		const onOut = (connection, originalEvent) => {
			tooltipTimeout = setTimeout(() => {
				tooltip.style.display = "none";
			}, 50);
		};

		instance.bind("mouseover", onOver);
		instance.bind("connectionMouseover", onOver);
		instance.bind("mouseout", onOut);
		instance.bind("connectionMouseout", onOut);

		initializeZoom();
	}

	// --- CORE LOGIC ---
	async function handleConnectionChange(e) {
		currentConnectionId = e.target.value;
		if (!currentConnectionId) {
			resetCanvas();
			updateConnectionStatus("disconnected");
			return;
		}
		updateConnectionStatus("connecting");
		await loadModelForConnection(currentConnectionId);
	}

	async function loadModelForConnection(connId) {
		try {
			const controller = new AbortController();
			const timeoutId = setTimeout(() => controller.abort(), 30000);

			const res = await fetch(`/api/model/get/${connId}/`, {
				signal: controller.signal,
			});
			clearTimeout(timeoutId);

			if (!res.ok) {
				let msg = `HTTP ${res.status}: ${res.statusText}`;
				try {
					const body = await res.json();
					if (body?.error) msg = body.error;
				} catch {}
				throw new Error(msg);
			}

			const data = await res.json();
			if (!data.success)
				throw new Error(data.error || "Unknown error from server");

			resetCanvas();
			schema = Object.fromEntries((data.tables || []).map((t) => [t.name, t]));
			updateTableSidebar();

			(data.layout || []).forEach((l) => {
				if (schema[l.table_name]) {
					addTableCard(schema[l.table_name], l.x_pos, l.y_pos, l.collapsed);
				}
			});

			// In loadModelForConnection function, update connection creation:
			setTimeout(() => {
				(data.joins || []).forEach((j) => {
					const sourceCardId = `card_${j.left_table}`;
					const targetCardId = `card_${j.right_table}`;

					const sourceEl = findFieldElement(sourceCardId, j.left_column);
					const targetEl = findFieldElement(targetCardId, j.right_column);

					if (sourceEl && targetEl) {
						// Ensure elements are visible and properly positioned
						instance.setSuspendDrawing(true);

						const conn = instance.connect({
							source: sourceEl,
							target: targetEl,
							overlays: [["Arrow", { width: 10, length: 10, location: 1 }]],
						});

						conn.setData({
							joinType: j.join_type || "INNER",
							cardinality: j.cardinality || "one-to-many",
							originalSourceId: sourceEl.id,
							originalTargetId: targetEl.id,
						});

						conn.addClass(joinCssClass(j.join_type || "INNER"));
						setCardinalityOverlays(conn, j.cardinality || "one-to-many");

						// Make the connection clickable
						makeConnectionClickable(conn);

						instance.setSuspendDrawing(false);
					}
				});

				// Repaint after all connections are created
				setTimeout(() => {
					instance.repaintEverything();
				}, 100);
			}, 300);

			saveHistory();
			updateConnectionStatus("connected");
		} catch (error) {
			const isTimeout = error.name === "AbortError";
			console.error("Connection Error:", isTimeout ? "Timeout" : error.message);
			showError(
				isTimeout
					? "Connection timeout. Please try again."
					: `Error loading model: ${error.message}`
			);
			resetCanvas();
			updateConnectionStatus("disconnected");
		}
	}

	async function saveModel() {
		if (!currentConnectionId) {
			showError("Please select a connection first.");
			return;
		}

		try {
			const csrftoken = getCookie("csrftoken");
			if (!csrftoken) {
				showError("CSRF token missing. Please refresh the page and try again.");
				return;
			}

			// --- Capture layout ---
			const layout = Array.from(canvas.querySelectorAll(".table-card")).map(
				(card) => ({
					tableName: card.dataset.tableName,
					x: parseInt(card.style.left, 10),
					y: parseInt(card.style.top, 10),
					collapsed:
						card
							.querySelector(".table-card-body")
							?.classList.contains("collapsed") || false,
				})
			);

			// --- Capture joins ---
			const joins = instance
				.getAllConnections()
				.filter((conn) => conn.source && conn.target)
				.map((conn) => {
					let sourceEl = conn.source;
					let targetEl = conn.target;

					// If connected to header, restore original field elements
					const data = conn.getData?.() || {};
					if (sourceEl.closest(".table-card-header") && data.originalSourceId) {
						sourceEl = document.getElementById(data.originalSourceId);
					}
					if (targetEl.closest(".table-card-header") && data.originalTargetId) {
						targetEl = document.getElementById(data.originalTargetId);
					}

					if (!sourceEl || !targetEl) return null;

					return {
						leftTable: sourceEl.closest(".table-card")?.dataset.tableName,
						leftColumn: sourceEl.dataset.column,
						rightTable: targetEl.closest(".table-card")?.dataset.tableName,
						rightColumn: targetEl.dataset.column,
						joinType: data.joinType || "INNER",
						cardinality: data.cardinality || "one-to-many",
					};
				})
				.filter((j) => j !== null);

			const payload = { layout, joins };
			console.log(
				"Data being sent to server:",
				JSON.stringify(payload, null, 2)
			);

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
			console.error("Save error:", error);
			showError(`Error saving model: ${error.message}`);
		}
	}

	function addTableCard(tableSchema, x, y, collapsed = false) {
		const content = getCanvasContent();
		const placeholder = document.getElementById("canvas-placeholder");

		if (placeholder) placeholder.style.display = "none";

		const cardId = `card_${tableSchema.name}`;
		if (document.getElementById(cardId)) return;

		const card = document.createElement("div");
		card.className = "table-card";
		card.id = cardId;
		card.dataset.tableName = tableSchema.name;
		card.style.left = `${x}px`;
		card.style.top = `${y}px`;

		const bodyClass = collapsed
			? "table-card-body collapsed"
			: "table-card-body";

		const sanitizeId = (s) => String(s).replace(/[^a-zA-Z0-9_-]/g, "_");

		const fieldsHTML = (tableSchema.columns || [])
			.map((col) => {
				const safeColId = sanitizeId(col.name);
				const t = String(col.type || "").toLowerCase();
				const looksNumeric = /\b(int|numeric|decimal|float|double|real)\b/.test(
					t
				);

				const iconClass =
					col.is_pk ?? col.is_primary
						? "bi-key-fill text-warning"
						: col.is_numeric ?? looksNumeric
						? "bi-hash"
						: "bi-type";

				return `
                <div class="table-card-field"
                    id="${cardId}_col_${safeColId}"
                    data-table="${tableSchema.name}"
                    data-column="${col.name}"
                    data-type="${col.type || ""}"
                    data-is-numeric="${col.is_numeric ?? looksNumeric ? 1 : 0}"
                    data-pk="${col.is_pk ?? col.is_primary ? 1 : 0}"
                    data-fk="${col.is_foreign ? 1 : 0}"
                    data-unique="${col.is_unique ? 1 : 0}">
                <i class="bi ${iconClass} me-2"></i>
                <span class="field-name">${col.name}</span>
                <span class="text-muted small ms-2">${col.type || ""}</span>
                <span class="join-wizard-btn ms-auto" title="Field settings">
                    <i class="bi bi-gear-fill"></i>
                </span>
                </div>`;
			})
			.join("");

		card.innerHTML = `
            <div class="table-card-header">
                <span><i class="bi bi-table me-2"></i>${tableSchema.name}</span>
                <div class="card-controls d-flex align-items-center">
                    <button type="button" class="btn btn-sm btn-link text-secondary p-0 btn-collapse" title="Collapse/Expand fields">
                        <i class="bi ${
													collapsed ? "bi-chevron-right" : "bi-chevron-down"
												}"></i>
                    </button>
                    
                    ${
											isEditable
												? '<button type="button" class="btn-close btn-sm remove-card-btn" title="Remove table" aria-label="Remove table"></button>'
												: ""
										}
                    
                </div>
            </div>
            <div class="${bodyClass}">${fieldsHTML}</div>
        `;

		// Append to content container
		content.appendChild(card);

		// --- 1. Draggable Logic ---
		if (isEditable) {
			instance.draggable(card, {
				handle: ".table-card-header",
				filter: ".card-controls, .card-controls *",
				filterExclude: true,
				grid: [10, 10],
				start: ({ el }) => {
					el.style.zIndex = 20;
					instance.setSuspendDrawing(true);
				},
				stop: ({ el }) => {
					el.style.zIndex = "";
					instance.setSuspendDrawing(false, true);
					saveHistory();
				},
				// Important: Set drag containment to the content container
				containment: true,
			});
		}
		// --- 2. Collapse Button ---
		const collapseBtn = card.querySelector(".btn-collapse");
		const body = card.querySelector(".table-card-body");
		collapseBtn?.addEventListener("click", () => {
			const isCollapsed = body.classList.toggle("collapsed");
			const icon = collapseBtn.querySelector("i");
			if (icon)
				icon.className = isCollapsed
					? "bi bi-chevron-right"
					: "bi bi-chevron-down";

			isCollapsed
				? rerouteConnectionsToHeader(card)
				: rerouteConnectionsToFields(card);
			instance.repaintEverything();
			saveHistory();
		});

		// --- 3. Remove Button ---
		card.querySelector(".remove-card-btn")?.addEventListener("click", () => {
			instance.remove(card);
			card.remove();
			if (!content.querySelector(".table-card") && placeholder) {
				placeholder.style.display = "block";
			}
			updateTableSidebar();
			saveHistory();
		});

		// --- 4. Join Wizard Button Click ---
		card.querySelectorAll(".join-wizard-btn").forEach((btn) => {
			btn.addEventListener("mousedown", (e) => {
				// prevent jsPlumb from interpreting this as a drag start
				e.stopPropagation();
				e.preventDefault();
			});
			btn.addEventListener("click", (e) => {
				e.stopPropagation();
				const fieldEl = btn.closest(".table-card-field");
				if (fieldEl) {
					openJoinWizard(fieldEl, { readonly: !isEditable });
				}
			});
		});

		// --- 5. Initialize jsPlumb Endpoints ---
		card.querySelectorAll(".table-card-field").forEach((fieldEl) => {
			instance.makeSource(fieldEl, {
				anchor: "Right",
				maxConnections: -1,
				// ignore the gear button AND anything inside it
				filter: ".join-wizard-btn, .join-wizard-btn *",
				filterExclude: true,
			});

			instance.makeTarget(fieldEl, {
				anchor: "Left",
				maxConnections: -1,
			});
		});

		updateTableSidebar();
		instance.recalculateOffsets();
		instance.repaintEverything();
		// --- End: Merged from OLD function ---
	}

	function updateTableSidebar() {
		const tableList = document.getElementById("tableList");
		if (!tableList || !schema) return;

		const searchTerm =
			document.getElementById("tableSearch")?.value.toLowerCase() || "";
		const tablesOnCanvas = new Set(
			Array.from(canvas.querySelectorAll(".table-card")).map(
				(card) => card.dataset.tableName
			)
		);

		const allTableNames = Object.keys(schema);
		let visibleTables = allTableNames.filter(
			(name) =>
				!tablesOnCanvas.has(name) && name.toLowerCase().includes(searchTerm)
		);
		let disabledTables = allTableNames.filter(
			(name) =>
				tablesOnCanvas.has(name) && name.toLowerCase().includes(searchTerm)
		);

		visibleTables.sort((a, b) => {
			if (sortOrder === "asc") return a.localeCompare(b);
			return b.localeCompare(a);
		});

		disabledTables.sort((a, b) => {
			if (sortOrder === "asc") return a.localeCompare(b);
			return b.localeCompare(a);
		});

		const renderItem = (name, disabled = false) => `
            <a href="#" class="list-group-item list-group-item-action ${
							disabled ? "disabled" : ""
						}" 
               draggable="${!disabled}" data-table-name="${name}">${name}</a>`;

		if (allTableNames.length === 0) {
			tableList.innerHTML = `
                <div class="p-3 text-muted small text-center">
                    Select a database connection to begin modeling.
                </div>`;
			return;
		}

		tableList.innerHTML = [
			...visibleTables.map((name) => renderItem(name, false)),
			...disabledTables.map((name) => renderItem(name, true)),
		].join("");

		// Re-attach drag listeners to non-disabled items
		tableList
			.querySelectorAll(".list-group-item:not(.disabled)")
			.forEach((item) => {
				item.addEventListener("dragstart", (e) => {
					e.dataTransfer.setData("text/plain", e.target.dataset.tableName);
				});
			});
	}

	function toggleTableSort() {
		const btn = document.getElementById("sortTableBtn");
		const icon = btn?.querySelector("i");
		sortOrder = sortOrder === "asc" ? "desc" : "asc";

		if (sortOrder === "asc") {
			if (icon) icon.className = "bi bi-sort-alpha-down";
			if (btn) btn.title = "Sort Ascending";
		} else {
			if (icon) icon.className = "bi bi-sort-alpha-up-alt";
			if (btn) btn.title = "Sort Descending";
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
		const wizardBtn = e.target.closest(".join-wizard-btn");
		if (wizardBtn && isEditable) {
			const fieldEl = wizardBtn.closest(".table-card-field");
			if (fieldEl) openJoinWizard(fieldEl);
		}
	}

	function handleKeyboardShortcuts(e) {
		const ctrl = e.ctrlKey || e.metaKey;
		if (ctrl && e.key.toLowerCase() === "z") {
			e.preventDefault();
			undo();
		} else if (ctrl && e.key.toLowerCase() === "y") {
			e.preventDefault();
			redo();
		} else if (e.key === "Delete") {
			if (selectedConnection) {
				instance.deleteConnection(selectedConnection);
				selectedConnection = null;
				saveHistory();
			}
		}
	}

	// --- UI & UX FUNCTIONS ---
	function updateZoomIndicator() {
		const zoomLevelElem = document.querySelector(".zoom-level");
		if (zoomLevelElem) {
			zoomLevelElem.textContent = Math.round(zoom * 100) + "%";
		}
	}

	function applyZoom(scale, zoomPoint = null) {
		const newZoom = Math.min(Math.max(scale, 0.2), 3);
		const oldZoom = zoom;

		const content = getCanvasContent();
		if (!content) return;

		// Store current scroll position before zoom
		const oldScrollLeft = canvas.scrollLeft;
		const oldScrollTop = canvas.scrollTop;

		if (zoomPoint) {
			// Calculate the point relative to the content
			const rect = canvas.getBoundingClientRect();
			const contentRect = content.getBoundingClientRect();

			const relativeX =
				(zoomPoint.x - rect.left + oldScrollLeft - content.offsetLeft) /
				oldZoom;
			const relativeY =
				(zoomPoint.y - rect.top + oldScrollTop - content.offsetTop) / oldZoom;

			// Apply zoom transform
			content.style.transform = `scale(${newZoom})`;
			zoom = newZoom;

			// Calculate new scroll position to keep the point fixed
			const newScrollX = relativeX * newZoom - (zoomPoint.x - rect.left);
			const newScrollY = relativeY * newZoom - (zoomPoint.y - rect.top);

			canvas.scrollLeft = newScrollX;
			canvas.scrollTop = newScrollY;
		} else {
			// Simple zoom without point fixation
			content.style.transform = `scale(${newZoom})`;
			zoom = newZoom;
		}

		// Update jsPlumb zoom and repaint
		instance.setZoom(newZoom);

		// Force repaint with a small delay to ensure DOM updates
		setTimeout(() => {
			instance.repaintEverything();
		}, 10);

		updateZoomIndicator();
	}

	function adjustZoom(delta) {
		const rect = canvas.getBoundingClientRect();
		const centerPoint = {
			x: rect.left + rect.width / 2,
			y: rect.top + rect.height / 2,
		};
		applyZoom(zoom + delta, centerPoint);
	}

	function resetZoom() {
		const rect = canvas.getBoundingClientRect();
		const centerPoint = {
			x: rect.left + rect.width / 2,
			y: rect.top + rect.height / 2,
		};
		applyZoom(1, centerPoint);
	}

	function initializeZoom() {
		const canvasContainer = canvas;

		// Mouse wheel zoom (Ctrl + Scroll)
		canvasContainer.addEventListener(
			"wheel",
			(e) => {
				if (e.ctrlKey) {
					e.preventDefault();
					const delta = e.deltaY > 0 ? -0.1 : 0.1;
					applyZoom(zoom + delta, { x: e.clientX, y: e.clientY });
				}
			},
			{ passive: false }
		);

		// Repaint on window resize
		window.addEventListener("resize", () => {
			instance.repaintEverything();
		});

		// Repaint when scroll changes (for connections that might be offscreen)
		canvasContainer.addEventListener("scroll", () => {
			instance.repaintEverything();
		});
	}

	function getTouchDistance(touches) {
		const dx = touches[0].clientX - touches[1].clientX;
		const dy = touches[0].clientY - touches[1].clientY;
		return Math.sqrt(dx * dx + dy * dy);
	}

	function getTouchMidpoint(touches) {
		return {
			x: (touches[0].clientX + touches[1].clientX) / 2,
			y: (touches[0].clientY + touches[1].clientY) / 2,
		};
	}

	function openJoinModal(connection) {
		selectedConnection = connection;
		const sourceEl = document.getElementById(connection.sourceId);
		const targetEl = document.getElementById(connection.targetId);

		document.getElementById("sourceField").textContent = `${
			sourceEl.closest(".table-card").dataset.tableName
		}.${sourceEl.dataset.column}`;
		document.getElementById("targetField").textContent = `${
			targetEl.closest(".table-card").dataset.tableName
		}.${targetEl.dataset.column}`;
		document.getElementById("joinType").value =
			connection.getData()?.joinType || "INNER";
		document.getElementById("cardinality").value =
			connection.getData()?.cardinality || "one-to-many";

		const modal = new bootstrap.Modal(document.getElementById("joinModal"));
		modal.show();
	}

	function openJoinPopover(connection, originalEvent) {
		// Close any existing popover first
		closeExistingPopovers();

		if (!connection || !originalEvent) return;

		const data = connection.getData?.() || {};
		const joinType = (data.joinType || "INNER").toUpperCase();
		const cardinality = data.cardinality || "one-to-many";

		const sourceEl = document.getElementById(connection.sourceId);
		const targetEl = document.getElementById(connection.targetId);

		if (!sourceEl || !targetEl) return;

		const srcLabel = `${sourceEl.closest(".table-card").dataset.tableName}.${
			sourceEl.dataset.column
		}`;
		const tgtLabel = `${targetEl.closest(".table-card").dataset.tableName}.${
			targetEl.dataset.column
		}`;

		// Create popover content
		const popoverContent = `
        <div class="p-2" style="min-width: 280px;">
            <div class="mb-2 small text-muted">
                <strong>From:</strong> ${srcLabel}<br>
                <strong>To:</strong> ${tgtLabel}
            </div>
            <div class="mb-2">
                <label class="form-label form-label-sm mb-1">Join Type</label>
                <select class="form-select form-select-sm" id="jp_joinType">
                    <option value="INNER" ${
											joinType === "INNER" ? "selected" : ""
										}>Inner Join</option>
                    <option value="LEFT" ${
											joinType === "LEFT" ? "selected" : ""
										}>Left Join</option>
                    <option value="RIGHT" ${
											joinType === "RIGHT" ? "selected" : ""
										}>Right Join</option>
                    <option value="FULL" ${
											joinType === "FULL" ? "selected" : ""
										}>Full Outer Join</option>
                </select>
            </div>
            <div class="mb-3">
                <label class="form-label form-label-sm mb-1">Cardinality</label>
                <select class="form-select form-select-sm" id="jp_card">
                    <option value="one-to-one" ${
											cardinality === "one-to-one" ? "selected" : ""
										}>One-to-One (1:1)</option>
                    <option value="one-to-many" ${
											cardinality === "one-to-many" ? "selected" : ""
										}>One-to-Many (1:N)</option>
                    <option value="many-to-one" ${
											cardinality === "many-to-one" ? "selected" : ""
										}>Many-to-One (N:1)</option>
                    <option value="many-to-many" ${
											cardinality === "many-to-many" ? "selected" : ""
										}>Many-to-Many (N:M)</option>
                </select>
            </div>
            <div class="d-flex justify-content-between pt-2 border-top">
                <button class="btn btn-sm btn-outline-danger" id="jp_delete">
                    <i class="bi bi-trash3 me-1"></i>Delete
                </button>
                <div>
                    <button class="btn btn-sm btn-outline-secondary me-2" id="jp_cancel">Cancel</button>
                    <button class="btn btn-sm btn-primary" id="jp_save">Save</button>
                </div>
            </div>
        </div>
    `;

		// Create and position the popover container
		const popoverContainer = document.createElement("div");
		popoverContainer.className = "connection-popover-container";
		popoverContainer.style.position = "absolute";
		popoverContainer.style.zIndex = "10000";
		popoverContainer.style.left = `${originalEvent.clientX + 10}px`;
		popoverContainer.style.top = `${originalEvent.clientY - 10}px`;

		popoverContainer.innerHTML = `
        <div class="popover bs-popover-end show join-wizard-popover" role="tooltip">
            <div class="popover-arrow"></div>
            <div class="popover-body">${popoverContent}</div>
        </div>
    `;

		document.body.appendChild(popoverContainer);

		// Store reference for cleanup
		window.activeConnectionPopover = {
			container: popoverContainer,
			connection: connection,
		};

		// Add event listeners
		setTimeout(() => {
			const cancelBtn = document.getElementById("jp_cancel");
			const deleteBtn = document.getElementById("jp_delete");
			const saveBtn = document.getElementById("jp_save");

			if (cancelBtn) {
				cancelBtn.addEventListener("click", closeActivePopover);
			}

			if (deleteBtn) {
				deleteBtn.addEventListener("click", () => {
					if (connection) {
						instance.deleteConnection(connection);
						saveHistory();
					}
					closeActivePopover();
				});
			}

			if (saveBtn) {
				saveBtn.addEventListener("click", () => {
					const jt = document.getElementById("jp_joinType").value;
					const card = document.getElementById("jp_card").value;

					const prev = connection.getData?.() || {};
					connection.setData({ ...prev, joinType: jt, cardinality: card });

					connection.removeClass("inner-join left-join right-join full-join");
					connection.addClass(joinCssClass(jt));
					setCardinalityOverlays(connection, card);

					saveHistory();
					closeActivePopover();
				});
			}
		}, 10);

		// Close popover when clicking outside
		setTimeout(() => {
			document.addEventListener("click", closePopoverOnClickOutside, true);
		}, 100);
	}

	// Helper functions for popover management
	function closeExistingPopovers() {
		if (window.activeConnectionPopover) {
			window.activeConnectionPopover.container.remove();
			window.activeConnectionPopover = null;
		}
		document.removeEventListener("click", closePopoverOnClickOutside, true);
	}

	function closeActivePopover() {
		closeExistingPopovers();
	}

	function closePopoverOnClickOutside(event) {
		if (
			window.activeConnectionPopover &&
			!window.activeConnectionPopover.container.contains(event.target) &&
			!event.target.closest(".jtk-connector")
		) {
			closeActivePopover();
		}
	}

	function setCardinalityOverlays(conn, cardinality) {
		try {
			// Remove existing overlays
			conn.getOverlays()?.forEach((overlay) => {
				if (
					overlay.id === "cardLeft" ||
					overlay.id === "cardRight" ||
					overlay.id === "cardinalityHint"
				) {
					conn.removeOverlay(overlay.id);
				}
			});
		} catch (e) {
			// Ignore errors if overlays don't exist
		}

		const map = {
			"one-to-one": ["1", "1"],
			"one-to-many": ["1", "∞"],
			"many-to-one": ["∞", "1"],
			"many-to-many": ["∞", "∞"],
		};

		const [left, right] = map[cardinality] || ["?", "?"];

		console.log("Setting overlays for:", cardinality, "->", left, right);

		try {
			if (left) {
				conn.addOverlay([
					"Label",
					{
						id: "cardLeft",
						label: left,
						location: 0.1,
						cssClass: "connection-label",
						visible: true,
					},
				]);
			}
			if (right) {
				conn.addOverlay([
					"Label",
					{
						id: "cardRight",
						label: right,
						location: 0.9,
						cssClass: "connection-label",
						visible: true,
					},
				]);
			}

			// Add cardinality class for styling
			conn.removeClass(
				"one-to-one-connection one-to-many-connection many-to-one-connection many-to-many-connection"
			);
			conn.addClass(`${cardinality.replace(/-/g, "-")}-connection`);
		} catch (e) {
			console.error("Error setting overlays:", e);
		}

		// Force repaint
		setTimeout(() => instance.repaintEverything(), 10);
	}

	function rerouteConnectionsToHeader(card) {
		const header = card.querySelector(".table-card-header");
		const fields = card.querySelectorAll(".table-card-field");
		let connectionsToModify = [];

		fields.forEach((field) => {
			connectionsToModify.push(...instance.getConnections({ source: field }));
			connectionsToModify.push(...instance.getConnections({ target: field }));
		});

		const uniqueConnections = [...new Set(connectionsToModify)];

		uniqueConnections.forEach((conn) => {
			if (!conn || !conn.source || !conn.target) return;

			const data = conn.getData();
			if (!data.originalSourceId && conn.source.closest(".table-card-field")) {
				data.originalSourceId = conn.source.id;
			}
			if (!data.originalTargetId && conn.target.closest(".table-card-field")) {
				data.originalTargetId = conn.target.id;
			}

			let newSource = conn.source;
			let newTarget = conn.target;

			if (conn.source.closest(".table-card") === card) newSource = header;
			if (conn.target.closest(".table-card") === card) newTarget = header;

			const tempConn = instance.connect({
				source: newSource,
				target: newTarget,
				anchors: ["Continuous", "Continuous"],
				cssClass: conn.getClass(),
				overlays: [["Arrow", { width: 10, length: 10, location: 1 }]],
			});

			tempConn.setData(data);
			if (data.cardinality) setCardinalityOverlays(tempConn, data.cardinality);
			instance.deleteConnection(conn);
		});

		// Hide field endpoints
		fields.forEach((field) => {
			instance.getEndpoints(field).forEach((ep) => ep.setVisible(false));
		});

		setTimeout(() => instance.repaintEverything(), 50);
	}

	function rerouteConnectionsToFields(card) {
		const header = card.querySelector(".table-card-header");
		if (!header) return;

		const connectionsOnHeader = [
			...instance.getConnections({ source: header }),
			...instance.getConnections({ target: header }),
		];
		const uniqueConnections = [...new Set(connectionsOnHeader)];

		uniqueConnections.forEach((conn) => {
			const data = conn.getData() || {};
			const cssClass = conn.getClass() || "";
			const sourceEl = document.getElementById(data.originalSourceId);
			const targetEl = document.getElementById(data.originalTargetId);

			if (sourceEl && targetEl) {
				instance.deleteConnection(conn);
				const newConn = instance.connect({
					source: sourceEl,
					target: targetEl,
					overlays: [["Arrow", { width: 10, length: 10, location: 1 }]],
				});
				newConn.setData(data);
				if (data.cardinality) setCardinalityOverlays(newConn, data.cardinality);
				if (cssClass) newConn.addClass(cssClass);
			} else {
				console.warn("Missing original field elements for rerouting:", data);
			}
		});

		// Restore visibility and reassociate endpoints
		card.querySelectorAll(".table-card-field").forEach((field) => {
			instance.getEndpoints(field).forEach((ep) => {
				ep.setVisible(true);
				ep.setElement(field);
			});
		});

		instance.repaintEverything();
	}

	function exportModel() {
		const content = getCanvasContent();
		const cards = Array.from(content.querySelectorAll(".table-card"));

		const layout = cards.map((card) => {
			const body = card.querySelector(".table-card-body");
			return {
				tableName: card.dataset.tableName,
				x: parseInt(card.style.left, 10) || 0,
				y: parseInt(card.style.top, 10) || 0,
				collapsed: body ? body.classList.contains("collapsed") : false,
			};
		});

		const joins = instance
			.getAllConnections()
			.filter((conn) => conn.source && conn.target)
			.map((conn) => {
				// ... your existing join mapping logic ...
			})
			.filter((j) => j !== null);

		return {
			layout,
			joins,
			viewport: {
				zoom,
				scrollLeft: canvas.scrollLeft,
				scrollTop: canvas.scrollTop,
			},
		};
	}

	function resetCanvas() {
		const content = getCanvasContent();
		content.innerHTML =
			'<div id="canvas-placeholder" class="placeholder-text"><i class="bi bi-diagram-3-fill fs-1"></i><p>Select a database connection to begin modeling.</p></div>';

		// Properly reset jsPlumb
		instance.reset();
		instance.deleteEveryConnection();
		instance.deleteEveryEndpoint();

		history = [];
		historyIndex = -1;
		selectedConnection = null;
		zoom = 1;
		applyZoom(1);
	}

	function autoArrangeLayout() {
		const content = getCanvasContent();
		const cards = Array.from(content.querySelectorAll(".table-card"));

		if (
			!cards.length ||
			typeof dagre === "undefined" ||
			!dagre.graphlib?.Graph ||
			!dagre.layout
		) {
			console.warn(
				"Auto-arrange skipped: Dagre not available or no cards present."
			);
			return;
		}

		const graph = new dagre.graphlib.Graph();
		graph.setGraph({
			rankdir: "LR",
			nodesep: 60,
			ranksep: 80,
			marginx: 50,
			marginy: 50,
		});
		graph.setDefaultEdgeLabel(() => ({}));

		// Add nodes
		cards.forEach((card) => {
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
		instance.getAllConnections().forEach((conn) => {
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

			graph.nodes().forEach((tableName) => {
				const node = graph.node(tableName);
				const card = canvas.querySelector(
					`.table-card[data-table-name="${CSS.escape(tableName)}"]`
				);
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
		const content = getCanvasContent();
		const cards = Array.from(content.querySelectorAll(".table-card"));

		if (!cards.length) return;

		let minX = Infinity,
			minY = Infinity,
			maxX = -Infinity,
			maxY = -Infinity;

		cards.forEach((card) => {
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

		// Adjust for current zoom level
		const offsetX = (canvasWidth - (maxX - minX) * zoom) / 2 / zoom - minX;
		const offsetY = (canvasHeight - (maxY - minY) * zoom) / 2 / zoom - minY;

		cards.forEach((card) => {
			const x = parseInt(card.style.left, 10) || 0;
			const y = parseInt(card.style.top, 10) || 0;
			card.style.left = Math.round(x + offsetX) + "px";
			card.style.top = Math.round(y + offsetY) + "px";
		});

		instance.repaintEverything();
		saveHistory();
	}

	function toggleSidebar() {
		const sidebar = document.getElementById("sourceTablesSidebar");
		const toggleBtn = document.getElementById("toggleSidebar");

		if (sidebar.style.width === "0px") {
			sidebar.style.width = "var(--sidebar-width)";
			toggleBtn.innerHTML = '<i class="bi bi-chevron-double-left"></i>';
		} else {
			sidebar.style.width = "0";
			toggleBtn.innerHTML = '<i class="bi bi-chevron-double-right"></i>';
		}

		setTimeout(() => instance.repaintEverything(), 300);
	}

	function filterTableList() {
		updateTableSidebar();
	}

	function filterCanvasElements() {
		const searchTerm = this.value.toLowerCase();
		const tableCards = document.querySelectorAll(".table-card");

		if (!searchTerm) {
			tableCards.forEach((card) => {
				card.style.opacity = 1;
				card.style.border = "1px solid #e1e5eb";
				card.querySelectorAll(".table-card-field").forEach((field) => {
					field.style.backgroundColor = "";
				});
			});
			return;
		}

		tableCards.forEach((card) => {
			const tableName = card.dataset.tableName.toLowerCase();
			const fields = card.querySelectorAll(".table-card-field");
			let hasMatch = tableName.includes(searchTerm);

			if (!hasMatch) {
				fields.forEach((field) => {
					const fieldName = field.dataset.column.toLowerCase();
					if (fieldName.includes(searchTerm)) {
						hasMatch = true;
						field.style.backgroundColor = "#fffce5";
					} else {
						field.style.backgroundColor = "";
					}
				});
			}

			if (hasMatch) {
				card.style.opacity = 1;
				card.style.border = "2px solid #4361ee";
			} else {
				card.style.opacity = 0.3;
				card.style.border = "1px solid #e1e5eb";
			}
		});
	}

	function updateConnectionStatus(status) {
		const statusElem = document.getElementById("connectionStatus");
		if (!statusElem) return;

		const indicator = statusElem.querySelector(".status-indicator");
		const text = statusElem.querySelector(".status-text");

		indicator.classList.remove(
			"status-connected",
			"status-disconnected",
			"status-connecting"
		);
		indicator.classList.add(`status-${status}`);

		switch (status) {
			case "connected":
				text.textContent = "Connected";
				break;
			case "connecting":
				text.textContent = "Connecting...";
				break;
			case "disconnected":
				text.textContent = "Not connected";
				break;
			default:
				text.textContent = "";
		}
	}

	async function fetchSuggestedJoins() {
		if (!currentConnectionId) {
			showError("Please select a connection first.");
			return;
		}

		try {
			updateConnectionStatus("connecting");
			const response = await fetch(
				`/api/model/suggest-joins/${currentConnectionId}/`
			);
			const data = await response.json();

			if (data.success) {
				showSuggestedJoins(data.joins);
			} else {
				showError(`Failed to fetch suggested joins: ${data.error}`);
			}
		} catch (error) {
			showError(`Error fetching suggested joins: ${error.message}`);
		} finally {
			updateConnectionStatus("connected");
		}
	}

	function showSuggestedJoins(joins) {
		const listContainer = document.getElementById("suggestedJoinsList");
		if (!listContainer) return;

		if (!joins.length) {
			listContainer.innerHTML =
				'<div class="text-center text-muted py-4">No suggested joins found.</div>';
		} else {
			joins.forEach((join) => {
				const joinElem = document.createElement("div");
				joinElem.className = "card mb-2";
				joinElem.innerHTML = `
                    <div class="card-body py-2">
                        <div class="form-check">
                            <input class="form-check-input" type="checkbox" value="" 
                                   id="suggest-${join.leftTable}-${
					join.leftColumn
				}-${join.rightTable}-${join.rightColumn}" checked>
                            <label class="form-check-label w-100 d-flex justify-content-between" 
                                   for="suggest-${join.leftTable}-${
					join.leftColumn
				}-${join.rightTable}-${join.rightColumn}">
                                <span>${join.leftTable}.${join.leftColumn} → ${
					join.rightTable
				}.${join.rightColumn}</span>
                                <span class="badge bg-${
																	join.confidence === "high"
																		? "success"
																		: "warning"
																}">${join.confidence}</span>
                            </label>
                        </div>
                    </div>`;
				listContainer.appendChild(joinElem);
			});
		}

		const modal = new bootstrap.Modal(
			document.getElementById("suggestJoinsModal")
		);
		modal.show();
	}

	function applyAllSuggestedJoins() {
		const checkboxes = document.querySelectorAll(
			"#suggestedJoinsList .form-check-input:checked"
		);
		checkboxes.forEach((checkbox) => {
			const idParts = checkbox.id.replace("suggest-", "").split("-");
			console.log("Would apply join with parts:", idParts);
		});

		bootstrap.Modal.getInstance(
			document.getElementById("suggestJoinsModal")
		).hide();
		showSuccess(`${checkboxes.length} joins applied successfully!`);
	}

	async function testConnection() {
		const connectionId = document.getElementById("connectionSelect").value;
		if (!connectionId) {
			showError("Please select a connection first.");
			return;
		}

		try {
			updateConnectionStatus("connecting");
			const response = await fetch(
				`/api/model/test-connection/${connectionId}/`
			);
			const data = await response.json();

			if (data.success) {
				updateConnectionStatus("connected");
				showSuccess("Connection test successful!");
			} else {
				updateConnectionStatus("disconnected");
				showError(`Connection test failed: ${data.error}`);
			}
		} catch (error) {
			updateConnectionStatus("disconnected");
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
			const response = await fetch(
				`/api/model/validate/${currentConnectionId}/`,
				{
					method: "POST",
					headers: {
						"Content-Type": "application/json",
						"X-CSRFToken": document.querySelector("[name=csrfmiddlewaretoken]")
							.value,
					},
					body: JSON.stringify(payload),
				}
			);

			const data = await response.json();
			if (data.success) {
				showSuccess("Validation passed!");
			} else {
				showError(`Validation failed: ${data.error}`);
			}
		} catch (error) {
			showError(`Validation failed: ${error.message}`);
		}
	}

	// --- JOIN WIZARD FUNCTIONS ---
	function openJoinWizard(sourceFieldEl, options = { readonly: false }) {
		closeJoinWizard();

		const isReadOnly = options.readonly;
		const popoverTemplate = document.getElementById("joinWizardTemplate");
		if (!popoverTemplate) return;

		const sourceTableName =
			sourceFieldEl.closest(".table-card").dataset.tableName;
		const sourceColumnName = sourceFieldEl.dataset.column;

		const popoverContentNode = popoverTemplate.content.cloneNode(true);

		// --- Dynamic Content Setup (Existing Joins) ---
		const existingJoinsContainer = popoverContentNode.querySelector(
			".existing-joins-container"
		);
		const allConnections = instance.getConnections({ element: sourceFieldEl });

		if (existingJoinsContainer) {
			if (allConnections.length > 0) {
				let joinsHtml =
					'<h6 class="join-wizard-sub-title">Existing Joins:</h6><ul class="list-group list-group-flush small">';

				allConnections.forEach((conn) => {
					const otherEl =
						conn.source === sourceFieldEl ? conn.target : conn.source;
					const otherTableName =
						otherEl.closest(".table-card")?.dataset.tableName;
					const otherColumnName = otherEl.dataset.column;
					joinsHtml += `<li class="list-group-item py-1 px-0">${otherTableName}.${otherColumnName}</li>`;
				});

				joinsHtml += "</ul>";
				existingJoinsContainer.innerHTML = joinsHtml;
			} else {
				existingJoinsContainer.remove();
			}
		}
		// --- End Dynamic Content Setup ---

		// --- READ-ONLY FIX: Hide editing components ---
		const body = popoverContentNode.querySelector(".join-wizard-body");
		const footer = popoverContentNode.querySelector(".join-wizard-footer");
		if (isReadOnly) {
			if (body) body.remove();
			if (footer) {
				// Only keep the 'Cancel' button or remove all
				footer.innerHTML = `<span class="text-muted small">Model is read-only.</span>`;
			}
		} else {
			// If editable, ensure the first row is added after the popover shows
			// (addJoinRow logic is correct later)
		}

		// 2. Create the Popover instance.
		const popover = new bootstrap.Popover(sourceFieldEl, {
			html: true,
			placement: "right",
			trigger: "manual",
			customClass: "join-wizard-popover",
			container: "body", // ✅ add this line
			title: `Join from ${sourceTableName}.${sourceColumnName} ${
				isReadOnly ? "(VIEW ONLY)" : ""
			}`,
			content: () => popoverContentNode.firstElementChild,
		});

		activeJoinWizard.popover = popover;
		activeJoinWizard.sourceEl = sourceFieldEl;
		activeJoinWizard.previews = [];

		// 4. Attach Listeners only if editable (and populate row)
		sourceFieldEl.addEventListener(
			"shown.bs.popover",
			function attachWizardListeners(e) {
				const popoverTip = document.querySelector(".popover");
				if (!popoverTip) return;

				if (!isReadOnly) {
					// Attach wizard event listeners to buttons within the popover
					popoverTip
						.querySelector(".add-join-row-btn")
						?.addEventListener("click", () => addJoinRow(popoverTip));
					popoverTip
						.querySelector(".cancel-join-btn")
						?.addEventListener("click", closeJoinWizard);
					popoverTip
						.querySelector(".apply-joins-btn")
						?.addEventListener("click", applyWizardJoins);

					// Populate the initial row immediately upon display
					addJoinRow(popoverTip);
				} else {
					// If read-only, ensure no ghost row appears if the existing joins container was removed
				}

				e.currentTarget.removeEventListener(
					"shown.bs.popover",
					attachWizardListeners
				);
			}
		);

		// 5. Show the Popover.
		popover.show();
	}

	function closeJoinWizard() {
		if (activeJoinWizard.popover) {
			activeJoinWizard.popover.dispose();
		}
		activeJoinWizard.previews.forEach((conn) =>
			instance.deleteConnection(conn)
		);
		activeJoinWizard.popover = null;
		activeJoinWizard.sourceEl = null;
		activeJoinWizard.previews = [];
	}

	function addJoinRow(popoverTip) {
		const body = popoverTip.querySelector(".join-wizard-body");
		const newRow = document.createElement("div");
		newRow.className = "join-wizard-row";

		const sourceTableName =
			activeJoinWizard.sourceEl.closest(".table-card").dataset.tableName;
		const allAvailableTables = Object.keys(schema).filter(
			(name) => name !== sourceTableName
		);

		newRow.innerHTML = `
            <select class="form-select form-select-sm target-table-select" aria-label="Select target table">
                <option value="">Select Target Table...</option>
                ${allAvailableTables
									.map((name) => `<option value="${name}">${name}</option>`)
									.join("")}
            </select>
            <select class="form-select form-select-sm target-column-select mt-1" aria-label="Select target column" disabled>
                <option value="">Select Column...</option>
            </select>
            <button type="button" class="btn btn-sm btn-outline-danger remove-join-row-btn mt-1" aria-label="Remove this join">×</button>
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
					const idx = activeJoinWizard.previews.findIndex(
						(c) => c.id === previewConnId
					);
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
		const row = tableSelectEl.closest(".join-wizard-row");
		const columnSelectEl = row.querySelector(".target-column-select");
		const selectedTable = tableSelectEl.value;

		columnSelectEl.innerHTML = '<option value="">Select Column...</option>';
		columnSelectEl.disabled = !selectedTable;

		if (!selectedTable || !schema[selectedTable]) return;

		schema[selectedTable].columns.forEach((col) => {
			const option = new Option(col.name, col.name);
			columnSelectEl.add(option);
		});
	}

	function handleColumnSelection(columnSelectEl) {
		const row = columnSelectEl.closest(".join-wizard-row");
		const targetTable = row.querySelector(".target-table-select")?.value;
		const targetColumn = columnSelectEl.value;

		// Remove old preview connection if it exists
		const oldPreviewId = row.dataset.previewConnId;
		if (oldPreviewId) {
			const idx = activeJoinWizard.previews.findIndex(
				(c) => c.id === oldPreviewId
			);
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
				overlays: [
					["Arrow", { location: 1, width: 8, length: 8, foldback: 0.8 }],
				],
			});

			previewConn.addClass("preview-join");
			row.dataset.previewConnId = previewConn.id;
			activeJoinWizard.previews.push(previewConn);
		}
	}

	function applyWizardJoins() {
		const popoverTip = document.querySelector(".popover");
		if (!popoverTip) return;

		const rows = popoverTip.querySelectorAll(".join-wizard-row");
		const sourceCard = activeJoinWizard.sourceEl.closest(".table-card");

		rows.forEach((row, idx) => {
			const targetTableName = row.querySelector(".target-table-select").value;
			const targetColumnName = row.querySelector(".target-column-select").value;

			if (!targetTableName || !targetColumnName) return;

			let targetCard = canvas.querySelector(
				`.table-card[data-table-name="${CSS.escape(targetTableName)}"]`
			);

			if (!targetCard && schema[targetTableName]) {
				const newX = sourceCard.offsetLeft + sourceCard.offsetWidth + 100;
				const newY = sourceCard.offsetTop + idx * 50;
				addTableCard(schema[targetTableName], newX, newY);
				targetCard = canvas.querySelector(
					`.table-card[data-table-name="${CSS.escape(targetTableName)}"]`
				);
			}

			if (targetCard) {
				// FIX 1: Construct the full Card ID for the target table
				const targetCardId = `card_${targetTableName}`; // Correct way to construct the Card ID

				// FIX 2: Pass the correctly constructed Card ID to findFieldElement
				const targetFieldEl = findFieldElement(targetCardId, targetColumnName);

				// CRITICAL CHECK: Ensure both elements exist before connecting
				if (activeJoinWizard.sourceEl && targetFieldEl) {
					const existing = instance
						.getConnections({
							source: activeJoinWizard.sourceEl,
							target: targetFieldEl,
						})
						.filter((c) => !c.hasClass("preview-join"));

					if (existing.length === 0) {
						const newConn = instance.connect({
							source: activeJoinWizard.sourceEl,
							target: targetFieldEl,
						});

						if (newConn) {
							setupNewConnection(
								newConn,
								activeJoinWizard.sourceEl,
								targetFieldEl
							);
						} else {
							console.error(
								"jsPlumb failed to create connection during wizard application."
							);
						}
					}
				}
			}
		});

		closeJoinWizard();
		saveHistory();
	}

	function downloadFile(data, filename, mimeType) {
		const blob = new Blob([data], { type: mimeType });
		const url = window.URL.createObjectURL(blob);
		const link = document.createElement("a");
		link.href = url;
		link.download = filename;
		document.body.appendChild(link);
		link.click();
		document.body.removeChild(link);
		window.URL.revokeObjectURL(url);
	}

	// Loading indicator functions
	function showLoading(message = "Processing...") {
		// Remove existing loader if any
		hideLoading();

		const loader = document.createElement("div");
		loader.id = "export-loader";
		loader.innerHTML = `
        <div class="position-fixed top-0 start-0 w-100 h-100 d-flex align-items-center justify-content-center" style="background: rgba(0,0,0,0.5); z-index: 9999;">
            <div class="bg-white rounded p-4 d-flex align-items-center">
                <div class="spinner-border text-primary me-3" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <span class="text-dark">${message}</span>
            </div>
        </div>
    `;
		document.body.appendChild(loader);
	}

	function hideLoading() {
		const existingLoader = document.getElementById("export-loader");
		if (existingLoader) {
			existingLoader.remove();
		}
	}

	// --- INITIALIZE THE APP ---
	function initUI() {
		updateConnectionStatus("disconnected");

		const tableSearch = document.getElementById("tableSearch");
		if (tableSearch) {
			tableSearch.addEventListener("input", filterTableList);
		}

		const canvasSearch = document.getElementById("canvasSearch");
		if (canvasSearch) {
			canvasSearch.addEventListener("input", filterCanvasElements);
		}
	}

	initUI();
	initializeEventListeners();

	if (document.getElementById("undoBtn")) {
		document.getElementById("undoBtn").disabled = true;
	}
	if (document.getElementById("redoBtn")) {
		document.getElementById("redoBtn").disabled = true;
	}
});

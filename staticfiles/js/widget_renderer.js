// In static/js/widget_renderer.js
class WidgetRenderer {
    constructor() {
        this.chartInstances = new Map();
    }

    async renderWidget(containerEl, widgetConfig, dashboardId, filters = []) {
        const widgetId = widgetConfig.id;
        this.destroyWidget(widgetId);
        containerEl.innerHTML = `<div class.widget-loader"><div class="spinner-border spinner-border-sm"></div></div>`;
        
        try {
            const response = await fetch(`/api/dashboard/${dashboardId}/widget/${widgetId}/data/`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-CSRFToken': getCsrfToken() },
                body: JSON.stringify({ widget_config: widgetConfig, filters: filters })
            });
            const result = await response.json();
            if (!response.ok || !result.success) {
                throw new Error(result.error || result.details || 'Failed to fetch data');
            }

            switch (widgetConfig.type) {
                case 'bar':
                case 'line':
                case 'pie':
                case 'doughnut':
                    this.renderChart(containerEl, widgetConfig, result.data);
                    break;
                case 'kpi':
                    this._renderKPI(containerEl, widgetConfig, result.data);
                    break;
                case 'table':
                    this._renderTable(containerEl, widgetConfig, result.data);
                    break;
                default:
                    containerEl.innerHTML = `<div class="p-2 text-warning small">Unsupported: ${widgetConfig.type}</div>`;
            }
        } catch (error) {
            console.error(`Error rendering widget ${widgetId}:`, error);
            containerEl.innerHTML = `<div class="widget-error">${error.message}</div>`;
        }
    }

    _renderKPI(containerEl, widgetConfig, data) {
        const measure = widgetConfig.dataConfig?.measures?.[0];
        const measureName = measure ? measure.field.split('.').pop() : 'Metric';
        const value = data.value || 0;
        const formattedValue = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 }).format(value);
        containerEl.innerHTML = `
            <div class="kpi-widget">
                <div class="kpi-value">${formattedValue}</div>
                <div class="kpi-label">${measureName}</div>
            </div>`;
    }

    _renderTable(containerEl, widgetConfig, data) {
        const { headers, rows } = data;
        if (!headers || !rows || rows.length === 0) {
            containerEl.innerHTML = `<div class="widget-loader">No data to display.</div>`;
            return;
        }
        let tableHTML = '<div class="table-responsive h-100"><table class="table table-sm table-striped">';
        tableHTML += `<thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead>`;
        tableHTML += '<tbody>';
        rows.forEach(row => {
            tableHTML += `<tr>${headers.map(h => `<td>${row[h] === null ? '' : row[h]}</td>`).join('')}</tr>`;
        });
        tableHTML += '</tbody></table></div>';
        containerEl.innerHTML = tableHTML;
    }

    renderChart(containerEl, widgetConfig, data) {
        containerEl.innerHTML = '';
        const canvas = document.createElement('canvas');
        containerEl.appendChild(canvas);
        const globalConfig = window.AppState?.config || {};
        const paletteName = globalConfig.themePalette || 'Tableau.Classic10';
        const colors = ColorService.getColors(paletteName);
        const isLineChart = widgetConfig.type === 'line';
        const dimensionField = widgetConfig.dataConfig?.dimensions?.[0]?.field || 'dimension';

        const chartConfig = {
            type: widgetConfig.type,
            data: {
                labels: data.labels,
                datasets: data.datasets.map((dataset, index) => {
                    const pointColors = data.labels.map((_, i) => colors[i % colors.length]);
                    return {
                        ...dataset,
                        backgroundColor: isLineChart ? colors[index % colors.length] + '33' : pointColors.map(c => c + '80'),
                        borderColor: isLineChart ? colors[index % colors.length] : pointColors,
                        borderWidth: 1.5
                    };
                })
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { 
                        display: widgetConfig.displayOptions.showLegend !== false 
                    }
                },
                scales: { y: { beginAtZero: true } },
                onClick: (event, elements) => {
                    if (elements.length === 0) return;
                    const index = elements[0].index;
                    const clickedLabel = data.labels[index];
                    eventBus.emit('filter:apply', {
                        sourceWidgetId: widgetConfig.id,
                        field: dimensionField,
                        value: clickedLabel,
                        operator: 'equals'
                    });
                }
            }
        };
        const chart = new Chart(canvas, chartConfig);
        this.chartInstances.set(widgetConfig.id, chart);
    }
   
    destroyWidget(widgetId) {
        if (this.chartInstances.has(widgetId)) {
            this.chartInstances.get(widgetId).destroy();
            this.chartInstances.delete(widgetId);
        }
    }
}
const widgetRenderer = new WidgetRenderer();
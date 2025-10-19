// In static/js/export_service.js

class ExportService {
    /**
     * Captures the dashboard grid as a PNG image and triggers a download.
     * @param {string} filename - The desired name of the downloaded file.
     */
    async exportGridToPNG(filename = 'dashboard.png') {
        const gridElement = document.getElementById('dashboard-grid');
        if (!gridElement) {
            showError('Error: Dashboard grid not found.');
            return;
        }
        try {
            const canvas = await html2canvas(gridElement, {
                useCORS: true,
                allowTaint: true,
                backgroundColor: '#f8f9fa' // Match our dashboard background
            });
            const link = document.createElement('a');
            link.download = filename.endsWith('.png') ? filename : `${filename}.png`;
            link.href = canvas.toDataURL('image/png');
            link.click();
        } catch (error) {
            console.error('Failed to export PNG:', error);
            showError('An error occurred while exporting the image.');
        }
    }

    /**
     * Fetches a widget's full dataset and exports it as a CSV file.
     * @param {string} widgetId - The ID of the widget to export.
     * @param {string} filename - The desired name of the downloaded file.
     */
    async exportWidgetToCSV(widgetId, filename = 'data.csv') {
        const widgetConfig = window.AppState.config.pages[0].widgets.find(w => w.id === widgetId);
        if (!widgetConfig) {
            showError('Error: Widget configuration not found.');
            return;
        }

        // Fetch the widget's data again, but this time without filters to get the full dataset.
        try {
            const response = await fetch(`/api/dashboard/${window.AppState.dashboardId}/widget/${widgetId}/data/`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-CSRFToken': getCsrfToken() },
                body: JSON.stringify({ widget_config: widgetConfig, filters: [] }) // No filters
            });
            const result = await response.json();
            if (!result.success) throw new Error(result.error);

            const { labels, datasets } = result.data;
            if (!labels || !datasets || datasets.length === 0) {
                showError('No data available to export for this widget.');
                return;
            }

            // Convert the data to CSV format
            const header = `"${widgetConfig.dataConfig.dimensions[0].field}","${datasets[0].label}"\n`;
            const rows = labels.map((label, index) => `"${label}",${datasets[0].data[index]}`);
            const csvContent = header + rows.join('\n');
            
            // Trigger download
            const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
            const link = document.createElement('a');
            const url = URL.createObjectURL(blob);
            link.setAttribute('href', url);
            link.setAttribute('download', filename.endsWith('.csv') ? filename : `${filename}.csv`);
            link.style.visibility = 'hidden';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        } catch (error) {
            console.error('Failed to export CSV:', error);
            showError('An error occurred while exporting the data.');
        }
    }
}

const exportService = new ExportService();

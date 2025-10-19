// mis_app/static/js/toast-notifications.js

(function (global) {
    if (!global) {
        return;
    }

    const namespaceKey = '__misAppToastRelay__';
    if (global[namespaceKey]) {
        const api = global[namespaceKey];
        global.showSuccess = api.success;
        global.showError = api.error;
        global.showWarning = api.warning;
        global.showInfo = api.info;
        return;
    }

    const noop = () => {};
    const warnMissingSwal = (message) => {
        if (console && typeof console.warn === 'function') {
            console.warn('SweetAlert2 (Swal) is required for toast notifications.', message);
        }
    };

    if (!global.Swal || typeof global.Swal.mixin !== 'function') {
        const fallback = {
            fire: noop,
            success: (msg) => warnMissingSwal(msg),
            error: (msg) => warnMissingSwal(msg),
            warning: (msg) => warnMissingSwal(msg),
            info: (msg) => warnMissingSwal(msg)
        };
        global[namespaceKey] = fallback;
    } else {
        const toast = global.Swal.mixin({
            toast: true,
            position: 'top-end',
            showConfirmButton: false,
            timer: 3000,
            timerProgressBar: true,
            didOpen: (toastEl) => {
                toastEl.addEventListener('mouseenter', global.Swal.stopTimer);
                toastEl.addEventListener('mouseleave', global.Swal.resumeTimer);
            }
        });

        const fire = (icon, message) => toast.fire({ icon, title: message });
        global[namespaceKey] = {
            fire: toast.fire.bind(toast),
            success: (message) => fire('success', message),
            error: (message) => fire('error', message),
            warning: (message) => fire('warning', message),
            info: (message) => fire('info', message)
        };
    }

    const api = global[namespaceKey];
    global.showSuccess = api.success;
    global.showError = api.error;
    global.showWarning = api.warning;
    global.showInfo = api.info;
})(typeof window !== 'undefined' ? window : globalThis);

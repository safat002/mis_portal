// Toast notification system using SweetAlert2
class ToastNotification {
    static success(message, title = 'Success') {
        Swal.fire({
            icon: 'success',
            title: title,
            text: message,
            toast: true,
            position: 'top-end',
            showConfirmButton: false,
            timer: 3000,
            timerProgressBar: true
        });
    }
    
    static error(message, title = 'Error') {
        Swal.fire({
            icon: 'error',
            title: title,
            text: message,
            toast: true,
            position: 'top-end',
            showConfirmButton: false,
            timer: 5000,
            timerProgressBar: true
        });
    }
    
    // More methods...
}

// Replace all alert() calls with ToastNotification methods
window.showSuccess = ToastNotification.success;
window.showError = ToastNotification.error;
window.showWarning = ToastNotification.warning;
window.showInfo = ToastNotification.info;
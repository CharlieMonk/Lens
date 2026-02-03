/**
 * UI Enhancements for CFR Viewer
 * - Toast notifications
 * - Copy to clipboard functionality
 * - Smooth scroll behavior
 * - Loading states for HTMX
 */

// Toast notification system
const Toast = {
    container: null,

    init() {
        if (this.container) return;
        this.container = document.createElement('div');
        this.container.className = 'toast-container';
        this.container.setAttribute('aria-live', 'polite');
        this.container.setAttribute('aria-atomic', 'true');
        document.body.appendChild(this.container);
    },

    show(message, type = 'success', duration = 3000) {
        this.init();
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.textContent = message;
        toast.setAttribute('role', 'status');

        this.container.appendChild(toast);

        // Trigger animation
        requestAnimationFrame(() => {
            toast.classList.add('toast-visible');
        });

        // Auto-remove
        setTimeout(() => {
            toast.classList.remove('toast-visible');
            toast.addEventListener('transitionend', () => toast.remove());
        }, duration);
    }
};

// Copy to clipboard functionality
function copyToClipboard(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        return navigator.clipboard.writeText(text);
    }
    // Fallback for older browsers
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.select();
    try {
        document.execCommand('copy');
        return Promise.resolve();
    } catch (err) {
        return Promise.reject(err);
    } finally {
        document.body.removeChild(textarea);
    }
}

// Initialize copy citation buttons
function initCopyCitation() {
    document.addEventListener('click', (e) => {
        const btn = e.target.closest('.copy-citation');
        if (!btn) return;

        const citation = btn.dataset.citation;
        if (!citation) return;

        copyToClipboard(citation)
            .then(() => {
                Toast.show(`Copied: ${citation}`, 'success');
                // Visual feedback on button
                const originalText = btn.innerHTML;
                btn.innerHTML = '<span class="copy-icon">✓</span> Copied!';
                btn.classList.add('copy-success');
                setTimeout(() => {
                    btn.innerHTML = originalText;
                    btn.classList.remove('copy-success');
                }, 2000);
            })
            .catch(() => {
                Toast.show('Failed to copy citation', 'error');
            });
    });
}

// HTMX loading states
function initHTMXLoadingStates() {
    document.body.addEventListener('htmx:beforeRequest', (e) => {
        const target = e.detail.target;
        if (target) {
            target.classList.add('htmx-loading');
        }
    });

    document.body.addEventListener('htmx:afterRequest', (e) => {
        const target = e.detail.target;
        if (target) {
            target.classList.remove('htmx-loading');
        }
    });
}

// Smooth scroll for anchor links
function initSmoothScroll() {
    document.addEventListener('click', (e) => {
        const link = e.target.closest('a[href^="#"]');
        if (!link) return;

        const targetId = link.getAttribute('href').slice(1);
        const target = document.getElementById(targetId);
        if (target) {
            e.preventDefault();
            target.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
    });
}

// Table filter - binds input[data-filter-table] to filter table rows
function initTableFilters() {
    document.querySelectorAll('input[data-filter-table]').forEach(input => {
        const tableId = input.dataset.filterTable;
        const table = document.getElementById(tableId);
        if (!table) return;

        const cols = (input.dataset.filterCols || '0,1').split(',').map(Number);

        input.addEventListener('input', () => {
            const filter = input.value.toLowerCase();
            table.querySelectorAll('tbody tr').forEach(row => {
                const text = cols.map(i => row.cells[i]?.textContent?.toLowerCase() || '').join(' ');
                row.style.display = text.includes(filter) ? '' : 'none';
            });
        });
    });
}

// Initialize all enhancements
document.addEventListener('DOMContentLoaded', () => {
    initCopyCitation();
    initHTMXLoadingStates();
    initSmoothScroll();
    initTableFilters();
});

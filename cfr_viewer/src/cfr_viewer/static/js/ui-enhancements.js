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

// Count-up animation for statistics
const CountUp = {
    duration: 1000,
    frameDuration: 1000 / 60,

    easeOutQuart(t) {
        return 1 - Math.pow(1 - t, 4);
    },

    format(value, options = {}) {
        const { decimals, comma, prefix = '', suffix = '' } = options;
        let formatted = decimals !== undefined ? value.toFixed(decimals) : Math.round(value).toString();
        if (comma) {
            formatted = Math.round(value).toLocaleString();
        }
        return prefix + formatted + suffix;
    },

    // Animate a single element to a target value
    // el: DOM element, target: number, options: { decimals, comma, prefix, suffix }
    animate(el, target, options = {}) {
        const totalFrames = Math.round(this.duration / this.frameDuration);
        let frame = 0;

        // Cancel any existing animation
        if (el._countUpInterval) clearInterval(el._countUpInterval);

        el._countUpInterval = setInterval(() => {
            frame++;
            const progress = this.easeOutQuart(frame / totalFrames);
            const current = target * progress;
            el.textContent = this.format(current, options);

            if (frame === totalFrames) {
                clearInterval(el._countUpInterval);
                el._countUpInterval = null;
                el.textContent = this.format(target, options);
            }
        }, this.frameDuration);
    },

    // Get change type: 'positive', 'negative', or 'zero' based on rounded value
    changeType(value) {
        const rounded = Math.round(value * 10) / 10;
        if (rounded === 0) return 'zero';
        return value > 0 ? 'positive' : 'negative';
    },

    // Get sign prefix: '+', '−', or '' based on rounded value
    changeSign(value) {
        const rounded = Math.round(value * 10) / 10;
        if (rounded === 0) return '';
        return value >= 0 ? '+' : '−';
    },

    // Animate a change percentage with proper sign and class
    // el: DOM element, value: change percentage, classPrefix: 'change', '' for no prefix, or false to skip class updates
    animateChange(el, value, classPrefix = 'change') {
        const type = this.changeType(value);
        const sign = this.changeSign(value);

        // Update CSS class (skip if classPrefix is false)
        if (classPrefix !== false) {
            const cls = classPrefix ? `${classPrefix}-` : '';
            el.classList.remove(`${cls}positive`, `${cls}negative`, `${cls}zero`);
            if (type !== 'zero') el.classList.add(`${cls}${type}`);
        }

        // Animate the absolute value with appropriate prefix
        this.animate(el, Math.abs(value), { decimals: 1, prefix: sign, suffix: '%' });
    },

    // Auto-initialize elements with data-count attribute
    initFromDataAttributes() {
        const elements = document.querySelectorAll('[data-count]');
        if (!elements.length) return;

        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const el = entry.target;
                    const target = parseFloat(el.dataset.count);
                    const options = {
                        decimals: el.dataset.decimals !== undefined ? parseInt(el.dataset.decimals) : undefined,
                        comma: el.dataset.format === 'comma',
                        prefix: el.dataset.prefix || '',
                        suffix: el.dataset.suffix || ''
                    };
                    this.animate(el, target, options);
                    observer.unobserve(el);
                }
            });
        }, { threshold: 0.5 });

        elements.forEach(el => observer.observe(el));
    }
};

// Tooltip positioning for elements inside overflow containers
function initTooltips() {
    document.querySelectorAll('.info-icon').forEach(icon => {
        if (icon._tooltipInit) return;
        icon._tooltipInit = true;

        const tooltip = icon.querySelector('.tooltip');
        if (!tooltip) return;

        icon.addEventListener('mouseenter', () => {
            const rect = icon.getBoundingClientRect();
            // Position tooltip below the icon, centered but within viewport
            tooltip.style.top = `${rect.bottom + 8}px`;
            tooltip.style.left = `${Math.max(10, Math.min(rect.left - 100, window.innerWidth - 230))}px`;
        });
    });
}

// Similar sections panel functionality
const SimilarSections = {
    STORAGE_KEY: 'similarSectionsCollapsed',
    MOBILE_BREAKPOINT: 768,

    init() {
        // Wait for HTMX to load similar sections content
        document.body.addEventListener('htmx:afterSettle', (e) => {
            if (e.detail.target.classList.contains('similar-content')) {
                this.initPreviewButtons();
                initTooltips();
            }
        });

        // Initialize collapse toggle
        this.initCollapseToggle();

        // Initialize mobile accordion behavior
        this.initMobileAccordion();

        // Re-init on window resize
        window.addEventListener('resize', () => this.handleResize());
    },

    initCollapseToggle() {
        document.addEventListener('click', (e) => {
            const toggle = e.target.closest('.collapse-toggle');
            if (!toggle) return;

            const panel = toggle.closest('.similar-sections');
            if (!panel) return;

            panel.classList.toggle('collapsed');
            const isCollapsed = panel.classList.contains('collapsed');
            localStorage.setItem(this.STORAGE_KEY, isCollapsed ? '1' : '0');
        });

        // Restore collapsed state from localStorage (desktop only)
        const panel = document.getElementById('similar-sections-panel');
        if (panel && window.innerWidth > this.MOBILE_BREAKPOINT) {
            if (localStorage.getItem(this.STORAGE_KEY) === '1') {
                panel.classList.add('collapsed');
            }
        }
    },

    initMobileAccordion() {
        const panel = document.getElementById('similar-sections-panel');
        if (!panel) return;

        if (window.innerWidth <= this.MOBILE_BREAKPOINT) {
            panel.classList.add('mobile-accordion', 'collapsed');
            // Make header clickable on mobile
            const header = panel.querySelector('header');
            if (header) {
                header.addEventListener('click', (e) => {
                    if (window.innerWidth <= this.MOBILE_BREAKPOINT) {
                        panel.classList.toggle('collapsed');
                    }
                });
            }
        }
    },

    handleResize() {
        const panel = document.getElementById('similar-sections-panel');
        if (!panel) return;

        if (window.innerWidth <= this.MOBILE_BREAKPOINT) {
            panel.classList.add('mobile-accordion');
        } else {
            panel.classList.remove('mobile-accordion');
            // Restore desktop collapsed state
            if (localStorage.getItem(this.STORAGE_KEY) === '1') {
                panel.classList.add('collapsed');
            } else {
                panel.classList.remove('collapsed');
            }
        }
    },

    initPreviewButtons() {
        document.querySelectorAll('.preview-toggle').forEach(btn => {
            if (btn._previewInitialized) return;
            btn._previewInitialized = true;

            btn.addEventListener('click', async () => {
                const title = btn.dataset.title;
                const section = btn.dataset.section;
                const year = btn.dataset.year || 0;
                const previewId = `preview-${section.replace(/\./g, '-')}`;
                const previewEl = document.getElementById(previewId);

                if (!previewEl) return;

                // Toggle expanded state
                if (previewEl.classList.contains('expanded')) {
                    previewEl.classList.remove('expanded');
                    btn.innerHTML = '&#x1F441; Preview';
                    return;
                }

                // Load preview if not already loaded
                if (!previewEl.dataset.loaded) {
                    previewEl.innerHTML = '<em>Loading...</em>';
                    try {
                        const response = await fetch(`/api/preview/${title}/${section}?year=${year}`);
                        previewEl.innerHTML = await response.text();
                        previewEl.dataset.loaded = 'true';
                    } catch (err) {
                        previewEl.innerHTML = '<em>Failed to load preview</em>';
                    }
                }

                previewEl.classList.add('expanded');
                btn.innerHTML = '&#x1F441; Hide';
            });
        });
    }
};

// Initialize all enhancements
document.addEventListener('DOMContentLoaded', () => {
    initCopyCitation();
    initHTMXLoadingStates();
    initSmoothScroll();
    initTableFilters();
    CountUp.initFromDataAttributes();
    SimilarSections.init();
});

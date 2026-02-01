/**
 * Sortable tables - automatically makes tables with class "sortable" sortable by clicking headers.
 *
 * Usage:
 * - Add class="sortable" to the table
 * - Add data-sort="string|number" to th elements to specify sort type
 * - Optionally add data-value="..." to td elements for custom sort values
 */
(function() {
    function initSortableTables() {
        document.querySelectorAll('table.sortable').forEach(table => {
            const headers = table.querySelectorAll('th');

            headers.forEach((header, index) => {
                // Skip if already initialized
                if (header.dataset.sortInit) return;
                header.dataset.sortInit = 'true';

                // Add sortable-header class for styling
                header.classList.add('sortable-header');

                header.addEventListener('click', () => {
                    const tbody = table.querySelector('tbody');
                    if (!tbody) return;

                    const rows = Array.from(tbody.querySelectorAll('tr'));
                    const sortType = header.dataset.sort || 'string';
                    const isAsc = header.classList.contains('sort-asc');

                    // Clear sort classes from all headers in this table
                    headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));

                    // Set new sort direction
                    header.classList.add(isAsc ? 'sort-desc' : 'sort-asc');

                    rows.sort((a, b) => {
                        const aCell = a.cells[index];
                        const bCell = b.cells[index];
                        if (!aCell || !bCell) return 0;

                        const aVal = aCell.dataset.value || aCell.textContent.trim();
                        const bVal = bCell.dataset.value || bCell.textContent.trim();

                        let cmp;
                        if (sortType === 'number') {
                            // Parse numbers, removing commas and % signs
                            const aNum = parseFloat(aVal.replace(/[,%]/g, '')) || 0;
                            const bNum = parseFloat(bVal.replace(/[,%]/g, '')) || 0;
                            cmp = aNum - bNum;
                        } else {
                            cmp = aVal.localeCompare(bVal);
                        }
                        return isAsc ? -cmp : cmp;
                    });

                    rows.forEach(row => tbody.appendChild(row));
                });
            });
        });
    }

    // Initialize on DOMContentLoaded
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initSortableTables);
    } else {
        initSortableTables();
    }

    // Re-initialize after HTMX swaps
    document.body.addEventListener('htmx:afterSettle', initSortableTables);
})();

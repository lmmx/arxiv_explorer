// static/js/filters.js
// Filter panel for years and categories - delegates filtering to server

const Filters = (function() {
    const $ = id => document.getElementById(id);

    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    // Available data from the loaded papers (derived from year_month column)
    let availableYearMonths = new Map(); // "2024-01" -> count
    let availableCategories = new Map(); // category -> count
    
    // What's actually selectable (has data)
    let selectableYearMonths = new Set();
    
    // Current selections
    let selectedYearMonths = new Set();
    let selectedCategories = new Set();
    
    let onFilterChange = null;

    function init(papers, callback) {
        onFilterChange = callback;
        
        // Extract available year-months and categories from papers
        // Now uses the year_month column from the server
        availableYearMonths.clear();
        availableCategories.clear();
        
        papers.forEach(p => {
            // Use the pre-computed year_month from server
            if (p.year_month) {
                const count = availableYearMonths.get(p.year_month) || 0;
                availableYearMonths.set(p.year_month, count + 1);
            }
        
            // Count categories
            if (p.primary_subject) {
                const count = availableCategories.get(p.primary_subject) || 0;
                availableCategories.set(p.primary_subject, count + 1);
            }
        });

        // Build set of selectable year-months
        selectableYearMonths = new Set(availableYearMonths.keys());

        // Default: all available year-months and categories selected
        selectedYearMonths = new Set(selectableYearMonths);
        selectedCategories = new Set(availableCategories.keys());

        renderYearFilters();
        renderCategoryFilters();
    }

    function getYearsFromYearMonths() {
        const years = new Set();
        for (const ym of availableYearMonths.keys()) {
            years.add(ym.substring(0, 4));
        }
        return Array.from(years).sort().reverse();
    }

    function getMonthsForYear(year) {
        const months = [];
        for (const ym of availableYearMonths.keys()) {
            if (ym.startsWith(year + '-')) {
                months.push(ym.substring(5, 7));
            }
        }
        return months.sort();
    }

    function renderYearFilters() {
        const container = $('year-filters');
        if (!container) return;

        const years = getYearsFromYearMonths();
        
        container.innerHTML = years.map(year => {
            const monthsAvailable = getMonthsForYear(year);
            const selectedInYear = monthsAvailable.filter(m => 
                selectedYearMonths.has(`${year}-${m}`)
            ).length;
            
            return `
                <div class="year-calendar-mini" data-year="${year}">
                    <div class="year-header-mini" onclick="Filters.toggleYear('${year}')">
                        <span class="year-label">${year}</span>
                        <span class="year-count">${selectedInYear}/${monthsAvailable.length}</span>
                    </div>
                    <div class="months-grid-mini">
                        ${monthNames.map((name, idx) => {
                            const month = String(idx + 1).padStart(2, '0');
                            const yearMonth = `${year}-${month}`;
                            const isAvailable = selectableYearMonths.has(yearMonth);
                            const isSelected = selectedYearMonths.has(yearMonth);
                            const count = availableYearMonths.get(yearMonth) || 0;
                            
                            let classes = ['month-chip-mini'];
                            if (!isAvailable) classes.push('unavailable');
                            if (isSelected && isAvailable) classes.push('selected');
                            
                            return `
                                <div class="${classes.join(' ')}" 
                                     data-year-month="${yearMonth}"
                                     ${isAvailable ? `onclick="Filters.toggleMonth('${yearMonth}')"` : ''}
                                     title="${isAvailable ? `${count.toLocaleString()} papers` : 'No data'}">
                                    ${name.substring(0, 1)}
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
            `;
        }).join('');
    }

    function renderCategoryFilters() {
        const container = $('category-filters');
        if (!container) return;

        // Sort categories by count (descending)
        const sortedCategories = Array.from(availableCategories.entries())
            .sort((a, b) => b[1] - a[1]);

        container.innerHTML = sortedCategories.map(([category, count]) => {
            const isSelected = selectedCategories.has(category);
            const color = CategoryColors.getColor(category);
            return `
                <div class="filter-chip category-chip ${isSelected ? 'selected' : ''}" 
                     data-category="${category}"
                     style="${isSelected ? `background: ${color}22; border-color: ${color};` : ''}"
                     onclick="Filters.toggleCategory('${category}')">
                    <span class="color-dot" style="background: ${color};"></span>
                    ${category}
                    <span class="count">(${count.toLocaleString()})</span>
                </div>
            `;
        }).join('');
    }

    function toggleMonth(yearMonth) {
        if (!selectableYearMonths.has(yearMonth)) return;
        
        if (selectedYearMonths.has(yearMonth)) {
            selectedYearMonths.delete(yearMonth);
        } else {
            selectedYearMonths.add(yearMonth);
        }
        
        renderYearFilters();
        notifyChange();
    }

    function toggleYear(year) {
        const monthsInYear = getMonthsForYear(year);
        const yearMonthsInYear = monthsInYear.map(m => `${year}-${m}`);
        
        const allSelected = yearMonthsInYear.every(ym => selectedYearMonths.has(ym));
        
        yearMonthsInYear.forEach(ym => {
            if (allSelected) {
                selectedYearMonths.delete(ym);
            } else {
                selectedYearMonths.add(ym);
            }
        });
        
        renderYearFilters();
        notifyChange();
    }

    function toggleCategory(category) {
        if (selectedCategories.has(category)) {
            selectedCategories.delete(category);
        } else {
            selectedCategories.add(category);
        }
        renderCategoryFilters();
        notifyChange();
    }

    function selectAllYears() {
        selectedYearMonths = new Set(selectableYearMonths);
        renderYearFilters();
        notifyChange();
    }

    function selectNoYears() {
        selectedYearMonths.clear();
        renderYearFilters();
        notifyChange();
    }

    function selectAllCategories() {
        selectedCategories = new Set(availableCategories.keys());
        renderCategoryFilters();
        notifyChange();
    }

    function selectNoCategories() {
        selectedCategories.clear();
        renderCategoryFilters();
        notifyChange();
    }

    function notifyChange() {
        if (onFilterChange) {
            onFilterChange();
        }
    }

    // Build query params for API calls
    function getFilterParams() {
        const params = new URLSearchParams();
        
        if (selectedYearMonths.size > 0 && selectedYearMonths.size < selectableYearMonths.size) {
            params.set('year_months', Array.from(selectedYearMonths).join(','));
        }
        
        if (selectedCategories.size > 0 && selectedCategories.size < availableCategories.size) {
            params.set('categories', Array.from(selectedCategories).join(','));
        }
        
        return params;
    }

    function getSelectedYearMonths() {
        return new Set(selectedYearMonths);
    }

    function getSelectedCategories() {
        return new Set(selectedCategories);
    }

    return {
        init,
        toggleMonth,
        toggleYear,
        toggleCategory,
        selectAllYears,
        selectNoYears,
        selectAllCategories,
        selectNoCategories,
        getFilterParams,
        getSelectedYearMonths,
        getSelectedCategories
    };
})();
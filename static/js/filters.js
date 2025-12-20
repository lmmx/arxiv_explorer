// static/js/filters.js
// Filter panel for years and categories

const Filters = (function() {
    const $ = id => document.getElementById(id);

    let availableYears = new Set();
    let availableCategories = new Map(); // category -> count
    let selectedYears = new Set();
    let selectedCategories = new Set();
    let onFilterChange = null;

    function init(papers, callback) {
        onFilterChange = callback;
        
        // Extract available years and categories from papers
        availableYears.clear();
        availableCategories.clear();
        
        papers.forEach(p => {
            // Extract year from submission_date (format: YYYY-MM-DD or similar)
            if (p.submission_date) {
                const year = p.submission_date.substring(0, 4);
                availableYears.add(year);
            }
            
            // Count categories
            if (p.primary_subject) {
                const count = availableCategories.get(p.primary_subject) || 0;
                availableCategories.set(p.primary_subject, count + 1);
            }
        });

        // Default: all years and categories selected
        selectedYears = new Set(availableYears);
        selectedCategories = new Set(availableCategories.keys());

        renderYearFilters();
        renderCategoryFilters();
    }

    function renderYearFilters() {
        const container = $('year-filters');
        if (!container) return;

        const sortedYears = Array.from(availableYears).sort().reverse();
        
        container.innerHTML = sortedYears.map(year => {
            const isSelected = selectedYears.has(year);
            return `
                <div class="filter-chip year-chip ${isSelected ? 'selected' : ''}" 
                     data-year="${year}" 
                     onclick="Filters.toggleYear('${year}')">
                    ${year}
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
                    <span class="count">(${count})</span>
                </div>
            `;
        }).join('');
    }

    function toggleYear(year) {
        if (selectedYears.has(year)) {
            selectedYears.delete(year);
        } else {
            selectedYears.add(year);
        }
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
        selectedYears = new Set(availableYears);
        renderYearFilters();
        notifyChange();
    }

    function selectNoYears() {
        selectedYears.clear();
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

    function filterPaper(paper) {
        // Check year
        if (paper.submission_date) {
            const year = paper.submission_date.substring(0, 4);
            if (!selectedYears.has(year)) {
                return false;
            }
        }

        // Check category
        if (paper.primary_subject) {
            if (!selectedCategories.has(paper.primary_subject)) {
                return false;
            }
        }

        return true;
    }

    function getSelectedYears() {
        return new Set(selectedYears);
    }

    function getSelectedCategories() {
        return new Set(selectedCategories);
    }

    return {
        init,
        toggleYear,
        toggleCategory,
        selectAllYears,
        selectNoYears,
        selectAllCategories,
        selectNoCategories,
        filterPaper,
        getSelectedYears,
        getSelectedCategories
    };
})();
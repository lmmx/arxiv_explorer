const $ = id => document.getElementById(id);

let categories = {};
let categoryStatus = {};
let selectedCategories = new Set();
let selectedYearMonths = new Set(); // Format: "2024-01", "2024-02", etc.
let availableData = {}; // { year: { months: [], cached: [] } }
let currentYear = new Date().getFullYear().toString();
let currentMonth = String(new Date().getMonth() + 1).padStart(2, '0');
let ws = null;

const groupDescriptions = {
    'cs': 'Computer Science',
    'physics': 'Physics',
    'math': 'Mathematics',
    'astro-ph': 'Astrophysics',
    'quant-ph': 'Quantum Physics',
    'cond-mat': 'Condensed Matter',
    'stat': 'Statistics',
    'econ': 'Economics',
    'eess': 'Electrical Engineering & Systems Science',
    'q-bio': 'Quantitative Biology',
    'q-fin': 'Quantitative Finance',
    'gr-qc': 'General Relativity & Quantum Cosmology',
    'hep-ex': 'High Energy Physics - Experiment',
    'hep-lat': 'High Energy Physics - Lattice',
    'hep-ph': 'High Energy Physics - Phenomenology',
    'hep-th': 'High Energy Physics - Theory',
    'math-ph': 'Mathematical Physics',
    'nlin': 'Nonlinear Sciences',
    'nucl-ex': 'Nuclear Experiment',
    'nucl-th': 'Nuclear Theory',
};

const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function getGroupPrefix(code) {
    const dot = code.indexOf('.');
    return dot > 0 ? code.substring(0, dot) : code;
}

// Load all available years and their months
async function loadAvailableData() {
    try {
        const yearsRes = await fetch('/api/available-years');
        const yearsData = await yearsRes.json();
        
        // Load months for each year in parallel
        const monthPromises = yearsData.years.map(async (year) => {
            const res = await fetch(`/api/available-months/${year}`);
            const data = await res.json();
            return { year, data };
        });
        
        const results = await Promise.all(monthPromises);
        
        for (const { year, data } of results) {
            availableData[year] = {
                months: data.available_on_hf,
                cached: data.cached_locally,
                isCurrentYear: data.is_current_year,
                currentMonth: data.current_month
            };
        }
        
        renderCalendar();
        updateTimeStatus();
        loadCategories();
    } catch (err) {
        console.error('Error loading available data:', err);
        $('calendar-container').innerHTML = `<span class="error">Error loading data: ${err.message}</span>`;
    }
}

function renderCalendar() {
    const container = $('calendar-container');
    const years = Object.keys(availableData).sort().reverse();
    
    container.innerHTML = years.map(year => {
        const data = availableData[year];
        const selectedCount = Array.from(selectedYearMonths).filter(ym => ym.startsWith(year + '-')).length;
        
        return `
            <div class="year-calendar" data-year="${year}">
                <div class="year-header" onclick="toggleYear('${year}')">
                    <h3>${year}</h3>
                    <span class="year-stats">${selectedCount} selected</span>
                    <span class="year-toggle">Select all</span>
                </div>
                <div class="months-grid">
                    ${monthNames.map((name, idx) => {
                        const month = String(idx + 1).padStart(2, '0');
                        const yearMonth = `${year}-${month}`;
                        const available = data.months.includes(month);
                        const cached = data.cached.includes(month);
                        const isFuture = data.isCurrentYear && month > data.currentMonth;
                        const isSelected = selectedYearMonths.has(yearMonth);
                        
                        let classes = ['month-btn'];
                        if (!available || isFuture) classes.push('unavailable');
                        if (cached) classes.push('cached');
                        if (isSelected) classes.push('selected');
                        
                        return `<button class="${classes.join(' ')}" 
                                        data-year-month="${yearMonth}"
                                        ${!available || isFuture ? 'disabled' : ''}
                                        onclick="toggleMonth('${yearMonth}')">${name}</button>`;
                    }).join('')}
                </div>
            </div>
        `;
    }).join('');
}

function toggleMonth(yearMonth) {
    if (selectedYearMonths.has(yearMonth)) {
        selectedYearMonths.delete(yearMonth);
    } else {
        selectedYearMonths.add(yearMonth);
    }
    
    updateMonthButton(yearMonth);
    updateYearStats(yearMonth.split('-')[0]);
    updateTimeStatus();
    updateEstimate();
}

function updateMonthButton(yearMonth) {
    const btn = document.querySelector(`[data-year-month="${yearMonth}"]`);
    if (btn) {
        btn.classList.toggle('selected', selectedYearMonths.has(yearMonth));
    }
}

function updateYearStats(year) {
    const yearCalendar = document.querySelector(`.year-calendar[data-year="${year}"]`);
    if (yearCalendar) {
        const selectedCount = Array.from(selectedYearMonths).filter(ym => ym.startsWith(year + '-')).length;
        yearCalendar.querySelector('.year-stats').textContent = `${selectedCount} selected`;
    }
}

function toggleYear(year) {
    const data = availableData[year];
    if (!data) return;
    
    const yearMonths = data.months
        .filter(m => !data.isCurrentYear || m <= data.currentMonth)
        .map(m => `${year}-${m}`);
    
    const allSelected = yearMonths.every(ym => selectedYearMonths.has(ym));
    
    yearMonths.forEach(ym => {
        if (allSelected) {
            selectedYearMonths.delete(ym);
        } else {
            selectedYearMonths.add(ym);
        }
        updateMonthButton(ym);
    });
    
    updateYearStats(year);
    updateTimeStatus();
    updateEstimate();
}

function selectAll() {
    Object.entries(availableData).forEach(([year, data]) => {
        data.months
            .filter(m => !data.isCurrentYear || m <= data.currentMonth)
            .forEach(m => {
                const ym = `${year}-${m}`;
                selectedYearMonths.add(ym);
                updateMonthButton(ym);
            });
        updateYearStats(year);
    });
    updateTimeStatus();
    updateEstimate();
}

function selectNone() {
    selectedYearMonths.clear();
    document.querySelectorAll('.month-btn').forEach(btn => {
        btn.classList.remove('selected');
    });
    Object.keys(availableData).forEach(year => updateYearStats(year));
    updateTimeStatus();
    updateEstimate();
}

function selectLast12Months() {
    selectNone();
    
    const now = new Date();
    for (let i = 0; i < 12; i++) {
        const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
        const year = d.getFullYear().toString();
        const month = String(d.getMonth() + 1).padStart(2, '0');
        const ym = `${year}-${month}`;
        
        if (availableData[year]?.months.includes(month)) {
            selectedYearMonths.add(ym);
            updateMonthButton(ym);
        }
    }
    
    Object.keys(availableData).forEach(year => updateYearStats(year));
    updateTimeStatus();
    updateEstimate();
}

function selectYear(year) {
    selectNone();
    
    const data = availableData[year];
    if (!data) return;
    
    data.months
        .filter(m => !data.isCurrentYear || m <= data.currentMonth)
        .forEach(m => {
            const ym = `${year}-${m}`;
            selectedYearMonths.add(ym);
            updateMonthButton(ym);
        });
    
    updateYearStats(year);
    updateTimeStatus();
    updateEstimate();
}

function updateTimeStatus() {
    const status = $('time-status');
    
    if (selectedYearMonths.size === 0) {
        status.innerHTML = 'Select months from the calendar above';
        return;
    }
    
    // Group by year for display
    const byYear = {};
    selectedYearMonths.forEach(ym => {
        const [year, month] = ym.split('-');
        if (!byYear[year]) byYear[year] = [];
        byYear[year].push(month);
    });
    
    const summary = Object.entries(byYear)
        .sort(([a], [b]) => b.localeCompare(a))
        .map(([year, months]) => {
            const monthList = months.sort().map(m => monthNames[parseInt(m) - 1]).join(', ');
            return `<strong>${year}</strong>: ${monthList}`;
        })
        .join('<br>');
    
    status.innerHTML = `${selectedYearMonths.size} month${selectedYearMonths.size > 1 ? 's' : ''} selected:<br>${summary}`;
}

// Categories
async function loadCategories() {
    const yearMonthsParam = Array.from(selectedYearMonths).join(',') || '';
    
    try {
        const res = await fetch(`/api/categories?months=${yearMonthsParam}`);
        const data = await res.json();
        
        categories = data.categories;
        categoryStatus = data.status;
        
        renderCategories();
    } catch (err) {
        $('categories-container').innerHTML = `<span class="error">Error loading categories: ${err.message}</span>`;
    }
}

function renderCategories() {
    const groups = {};
    for (const [code, fullName] of Object.entries(categories)) {
        const prefix = getGroupPrefix(code);
        if (!groups[prefix]) groups[prefix] = [];
        groups[prefix].push({ code, fullName });
    }

    const sortedPrefixes = Object.keys(groups).sort();
    
    $('categories-container').innerHTML = sortedPrefixes.map(prefix => {
        const items = groups[prefix].sort((a, b) => a.code.localeCompare(b.code));
        const groupDesc = groupDescriptions[prefix] || prefix;
        
        return `
            <div class="group" data-prefix="${prefix}">
                <div class="group-header" onclick="toggleGroup('${prefix}')">
                    <h3>${prefix} — ${groupDesc}</h3>
                    <span class="group-toggle">Select all</span>
                </div>
                <div class="categories">
                    ${items.map(({ code, fullName }) => {
                        const status = categoryStatus[code] || { embedded: false, downloaded: false, count: 0 };
                        let statusClass = 'not-embedded';
                        let statusText = 'New';
                        
                        if (status.embedded) {
                            if (status.months_embedded === selectedYearMonths.size || 
                                (selectedYearMonths.size === 0 && status.months_embedded > 0)) {
                                statusClass = '';
                                statusText = `${status.count} embedded`;
                            } else {
                                statusClass = 'partial';
                                statusText = `${status.months_embedded} mo`;
                            }
                        } else if (status.downloaded) {
                            statusClass = 'not-embedded';
                            statusText = `${status.months_downloaded} mo cached`;
                        }
                        
                        return `
                            <div class="category">
                                <input type="checkbox" id="cat-${code}" data-category="${code}">
                                <label for="cat-${code}">
                                    <strong>${code}</strong>
                                    ${fullName !== code ? fullName : ''}
                                </label>
                                <span class="status ${statusClass}">
                                    ${statusText}
                                </span>
                            </div>
                        `;
                    }).join('')}
                </div>
            </div>
        `;
    }).join('');

    for (const code of Object.keys(categories)) {
        const el = $(`cat-${code}`);
        if (el) el.addEventListener('change', updateEstimate);
    }
}

function toggleGroup(prefix) {
    const group = document.querySelector(`.group[data-prefix="${prefix}"]`);
    const checkboxes = group.querySelectorAll('input[type="checkbox"]');
    const allChecked = Array.from(checkboxes).every(cb => cb.checked);
    checkboxes.forEach(cb => cb.checked = !allChecked);
    updateEstimate();
}

// Estimation
async function updateEstimate() {
    selectedCategories.clear();
    for (const code of Object.keys(categories)) {
        const el = $(`cat-${code}`);
        if (el && el.checked) selectedCategories.add(code);
    }

    if (selectedCategories.size === 0 || selectedYearMonths.size === 0) {
        $('estimate').style.display = 'none';
        $('embed-btn').disabled = true;
        $('embed-btn').textContent = selectedYearMonths.size === 0 
            ? 'Select months first' 
            : 'Select categories to embed';
        return;
    }

    $('estimate').style.display = 'block';
    $('estimate').textContent = 'Estimating...';
    $('embed-btn').disabled = true;

    try {
        const res = await fetch('/api/estimate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                categories: Array.from(selectedCategories),
                year_months: Array.from(selectedYearMonths).sort()
            }),
        });
        const data = await res.json();

        if (data.error) {
            $('estimate').innerHTML = `<span class="error">${data.error}</span>`;
            return;
        }

        const total = data.total;
        const cached = data.total_cached;
        const estimated = data.total_estimated;
        
        let estimateHtml = `
            <div>
                <strong>${total.toLocaleString()}</strong> papers total
                ${cached > 0 ? `(<span class="success">${cached.toLocaleString()} cached</span>` : '('}
                ${estimated > 0 ? `<span class="warning">${estimated.toLocaleString()} estimated from HF</span>)` : ')'}
            </div>
        `;
        
        // Show breakdown if multiple categories
        if (selectedCategories.size > 1 && selectedCategories.size <= 10) {
            const breakdown = Object.entries(data.breakdown)
                .filter(([_, d]) => d.total > 0)
                .sort((a, b) => b[1].total - a[1].total)
                .slice(0, 5)
                .map(([cat, d]) => `${cat}: ${d.total.toLocaleString()}`)
                .join(', ');
            
            if (breakdown) {
                estimateHtml += `<div class="breakdown">Top categories: ${breakdown}</div>`;
            }
        }
        
        // Time estimate
        if (data.time_estimate) {
            estimateHtml += `
                <div class="time-estimate">
                    ⏱️ Estimated time: ${data.time_estimate.gpu_display} (GPU) / ${data.time_estimate.cpu_display} (CPU)
                </div>
            `;
        }

        $('estimate').innerHTML = estimateHtml;

        $('embed-btn').disabled = false;
        $('embed-btn').textContent = `Embed ${total.toLocaleString()} papers`;

    } catch (err) {
        $('estimate').innerHTML = `<span class="error">Error: ${err.message}</span>`;
    }
}

// Embedding
function startEmbedding() {
    if (selectedCategories.size === 0 || selectedYearMonths.size === 0) return;

    $('embed-btn').disabled = true;
    $('progress').classList.add('active');
    $('log').innerHTML = '';

    ws = new WebSocket(`ws://${window.location.host}/ws/embed`);

    ws.onopen = () => {
        log('Connected to server', 'success');
        ws.send(JSON.stringify({ 
            categories: Array.from(selectedCategories),
            year_months: Array.from(selectedYearMonths).sort()
        }));
    };

    ws.onmessage = (event) => handleProgress(JSON.parse(event.data));
    ws.onerror = () => log('WebSocket error', 'error');
    ws.onclose = () => log('Connection closed');
}

function handleProgress(data) {
    if (data.status === 'error') {
        log(`Error: ${data.error}`, 'error');
        $('embed-btn').disabled = false;
        return;
    }

    if (data.status === 'loading_cache') {
        log('Found cached UMAP result, loading...', 'success');
        $('progress-text').textContent = data.message;
    }

    if (data.status === 'progress') {
        const percent = (data.current / data.total) * 100;
        $('progress-fill').style.width = `${percent}%`;
        $('progress-text').textContent = data.message;
        
        if (data.skipped) {
            log(data.message, 'warning');
        } else {
            log(data.message);
        }
    }

    if (data.status === 'downloading') {
        log(data.message, 'warning');
        $('progress-text').textContent = data.message;
    }

    if (data.status === 'embedding') {
        log(data.message, 'success');
        $('progress-text').textContent = data.message;
    }

    if (data.status === 'visualizing') {
        log(data.message, 'success');
        $('progress-text').textContent = data.message;
    }

    if (data.status === 'complete') {
        const cacheNote = data.from_cache ? ' (from cache)' : '';
        log(`Complete! ${data.total_papers.toLocaleString()} papers embedded${cacheNote}`, 'success');
        $('progress-fill').style.width = '100%';
        $('progress-text').textContent = `Done! ${data.total_papers.toLocaleString()} papers ready.`;
        $('embed-btn').disabled = false;
        $('embed-btn').textContent = 'Embedding complete!';
        
        // Refresh data
        setTimeout(() => {
            loadCategories();
            loadCacheSummary();
        }, 1000);
    }
}

function log(message, type = '') {
    const entry = document.createElement('div');
    entry.className = `log-entry ${type}`;
    entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    $('log').appendChild(entry);
    $('log').scrollTop = $('log').scrollHeight;
}

// Cache summary
async function loadCacheSummary() {
    try {
        const res = await fetch('/api/cache-summary');
        const data = await res.json();
        
        if (Object.keys(data.years).length === 0) {
            $('cache-summary').innerHTML = '<p>No data cached yet. Select categories and embed to get started.</p>';
            return;
        }
        
        let html = `<p>Total: <strong>${data.total_papers.toLocaleString()}</strong> papers in <strong>${data.total_files}</strong> files</p>`;
        
        for (const [year, yearData] of Object.entries(data.years).sort().reverse()) {
            const months = Object.entries(yearData.months)
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([month, mData]) => 
                    `<span class="month-chip">${monthNames[parseInt(month) - 1]}: ${mData.papers.toLocaleString()}</span>`
                )
                .join('');
            
            html += `
                <div class="year-row">
                    <h4>${year} (${yearData.total.toLocaleString()} papers)</h4>
                    <div class="month-chips">${months}</div>
                </div>
            `;
        }
        
        $('cache-summary').innerHTML = html;
    } catch (err) {
        $('cache-summary').innerHTML = `<span class="error">Error: ${err.message}</span>`;
    }
}

// Init
$('embed-btn').addEventListener('click', startEmbedding);

loadAvailableData();
loadCacheSummary();

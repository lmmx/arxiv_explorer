// static/config.js
const $ = id => document.getElementById(id);

let categories = {};
let categoryStatus = {};
let selectedCategories = new Set();
let selectedYear = null;
let selectedMonths = new Set();
let availableMonths = [];
let cachedMonths = [];
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

// Year/Month selection
async function loadYears() {
    try {
        const res = await fetch('/api/available-years');
        const data = await res.json();
        
        const select = $('year-select');
        select.innerHTML = data.years.map(year => {
            const cached = data.cached_locally.includes(year);
            return `<option value="${year}">${year}${cached ? ' ✓' : ''}</option>`;
        }).join('');
        
        // Default to most recent year
        if (data.years.length > 0) {
            select.value = data.years[0];
            await loadMonthsForYear(data.years[0]);
        }
    } catch (err) {
        console.error('Error loading years:', err);
    }
}

async function loadMonthsForYear(year) {
    selectedYear = year;
    selectedMonths.clear();
    
    try {
        const res = await fetch(`/api/available-months/${year}`);
        const data = await res.json();
        
        availableMonths = data.available_on_hf;
        cachedMonths = data.cached_locally;
        
        renderMonthButtons(data);
        updateTimeStatus();
        loadCategories();
    } catch (err) {
        console.error('Error loading months:', err);
    }
}

function renderMonthButtons(data) {
    const container = $('month-buttons');
    
    container.innerHTML = monthNames.map((name, idx) => {
        const month = String(idx + 1).padStart(2, '0');
        const available = availableMonths.includes(month);
        const cached = cachedMonths.includes(month);
        const isFuture = data.is_current_year && month > data.current_month;
        
        let classes = ['month-btn'];
        if (!available || isFuture) classes.push('unavailable');
        if (cached) classes.push('cached');
        
        return `<button class="${classes.join(' ')}" 
                        data-month="${month}" 
                        ${!available || isFuture ? 'disabled' : ''}
                        onclick="toggleMonth('${month}')">${name}</button>`;
    }).join('');
}


function toggleMonth(month) {
    if (selectedMonths.has(month)) {
        selectedMonths.delete(month);
    } else {
        selectedMonths.add(month);
    }

    // Update button state
    const btn = document.querySelector(`[data-month="${month}"]`);
    if (btn) {
        btn.classList.toggle('selected', selectedMonths.has(month));
    }
    
    updateTimeStatus();
    updateEstimate();
}

function selectAllMonths() {
    availableMonths.forEach(month => {
        selectedMonths.add(month);
        const btn = document.querySelector(`[data-month="${month}"]`);
        if (btn && !btn.disabled) btn.classList.add('selected');
    });
    updateTimeStatus();
    updateEstimate();
}

function selectNoMonths() {
    selectedMonths.clear();
    document.querySelectorAll('.month-btn').forEach(btn => {
        btn.classList.remove('selected');
    });
    updateTimeStatus();
    updateEstimate();
}

function selectYTD() {
    selectNoMonths();
    const now = new Date();
    const currentMonth = now.getMonth() + 1;
    
    for (let m = 1; m <= currentMonth; m++) {
        const month = String(m).padStart(2, '0');
        if (availableMonths.includes(month)) {
            selectedMonths.add(month);
            const btn = document.querySelector(`[data-month="${month}"]`);
            if (btn && !btn.disabled) btn.classList.add('selected');
        }
    }
    updateTimeStatus();
    updateEstimate();
}

function updateTimeStatus() {
    const status = $('time-status');
    
    if (!selectedYear) {
        status.textContent = 'Select a year to see available data';
        return;
    }
    
    if (selectedMonths.size === 0) {
        status.innerHTML = `<strong>${selectedYear}</strong>: Select months to embed`;
        return;
    }
    
    const monthList = Array.from(selectedMonths).sort().map(m => monthNames[parseInt(m) - 1]);
    status.innerHTML = `<strong>${selectedYear}</strong>: ${monthList.join(', ')} selected (${selectedMonths.size} month${selectedMonths.size > 1 ? 's' : ''})`;
}

// Categories
async function loadCategories() {
    if (!selectedYear) return;
    
    const monthsParam = Array.from(selectedMonths).join(',') || availableMonths.join(',');
    
    try {
        const res = await fetch(`/api/categories?year=${selectedYear}&months=${monthsParam}`);
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
                            if (status.months_embedded === selectedMonths.size || 
                                (selectedMonths.size === 0 && status.months_embedded > 0)) {
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

    if (selectedCategories.size === 0 || selectedMonths.size === 0) {
        $('estimate').style.display = 'none';
        $('embed-btn').disabled = true;
        $('embed-btn').textContent = selectedMonths.size === 0 
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
                year: selectedYear,
                months: Array.from(selectedMonths).sort()
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
    if (selectedCategories.size === 0 || selectedMonths.size === 0) return;

    $('embed-btn').disabled = true;
    $('progress').classList.add('active');
    $('log').innerHTML = '';

    ws = new WebSocket(`ws://${window.location.host}/ws/embed`);

    ws.onopen = () => {
        log('Connected to server', 'success');
        ws.send(JSON.stringify({ 
            categories: Array.from(selectedCategories),
            year: selectedYear,
            months: Array.from(selectedMonths).sort()
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
$('year-select').addEventListener('change', (e) => loadMonthsForYear(e.target.value));
$('embed-btn').addEventListener('click', startEmbedding);

loadYears();
loadCacheSummary();

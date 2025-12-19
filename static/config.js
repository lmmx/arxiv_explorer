// static/config.js
const $ = id => document.getElementById(id);

let categories = {};
let categoryStatus = {};
let selectedCategories = new Set();
let monthsData = [];
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

// Months
async function loadMonths() {
    try {
        const res = await fetch('/api/months');
        const data = await res.json();
        monthsData = data.months;
        renderMonths();
    } catch (err) {
        $('months-container').innerHTML = `<span class="error">Error: ${err.message}</span>`;
    }
}

function renderMonths() {
    const container = $('months-container');
    
    container.innerHTML = monthsData.map(m => {
        const name = monthNames[parseInt(m.month) - 1];
        const statusClass = m.cached ? 'downloaded' : 'not-downloaded';
        const statusText = m.cached ? `${m.count.toLocaleString()} papers` : 'Not downloaded';
        
        return `
            <div class="month-item ${statusClass}" data-month="${m.month}">
                <span class="month-name">${name} ${m.year}</span>
                <span class="month-status">${statusText}</span>
                ${m.cached ? '' : `<button class="month-download-btn" onclick="downloadMonth('${m.year}', '${m.month}')">Download</button>`}
            </div>
        `;
    }).join('');
    
    updateDatasetStatus();
}

function updateDatasetStatus() {
    const cached = monthsData.filter(m => m.cached);
    const total = monthsData.reduce((sum, m) => sum + (m.count || 0), 0);
    
    const status = $('dataset-status');
    if (cached.length === 0) {
        status.className = 'download-status not-downloaded';
        status.innerHTML = `
            <strong>⚠️ No data downloaded</strong><br>
            <span style="color: #8b949e;">Download at least one month to get started.</span>
        `;
    } else {
        status.className = 'download-status downloaded';
        status.innerHTML = `
            <strong>✓ ${cached.length} month(s) downloaded</strong><br>
            <span style="color: #8b949e;">${total.toLocaleString()} papers available locally.</span>
        `;
    }
}

async function downloadMonth(year, month) {
    const btn = document.querySelector(`.month-item[data-month="${month}"] .month-download-btn`);
    if (btn) {
        btn.disabled = true;
        btn.textContent = 'Downloading...';
    }
    
    try {
        const res = await fetch('/api/download-month', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ year, month }),
        });
        const data = await res.json();
        
        // Refresh months display
        await loadMonths();
    } catch (err) {
        alert(`Download failed: ${err.message}`);
        if (btn) {
            btn.disabled = false;
            btn.textContent = 'Download';
        }
    }
}

// Categories
async function loadCategories() {
    const res = await fetch('/api/categories');
    const data = await res.json();
    
    categories = data.categories;
    categoryStatus = data.status;
    
    renderCategories();
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
                        const status = categoryStatus[code] || { embedded: false, count: 0 };
                        return `
                            <div class="category">
                                <input type="checkbox" id="cat-${code}" data-category="${code}">
                                <label for="cat-${code}">
                                    <strong>${code}</strong>
                                    ${fullName}
                                </label>
                                <span class="status ${status.embedded ? '' : 'not-embedded'}">
                                    ${status.embedded ? status.count : 'New'}
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

    if (selectedCategories.size === 0) {
        $('estimate').style.display = 'none';
        $('embed-btn').disabled = true;
        $('embed-btn').textContent = 'Embed 0 papers';
        return;
    }

    const cachedMonths = monthsData.filter(m => m.cached);
    if (cachedMonths.length === 0) {
        $('estimate').style.display = 'block';
        $('estimate').innerHTML = `<span class="warning">⚠️ Download at least one month first.</span>`;
        $('embed-btn').disabled = true;
        return;
    }

    $('estimate').style.display = 'block';
    $('estimate').textContent = 'Estimating...';
    $('embed-btn').disabled = true;

    try {
        const res = await fetch('/api/estimate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ categories: Array.from(selectedCategories) }),
        });
        const data = await res.json();

        if (data.error) {
            $('estimate').innerHTML = `<span class="error">${data.error}</span>`;
            return;
        }

        const total = data.total;
        const breakdown = Object.entries(data.counts)
            .filter(([_, count]) => count > 0)
            .map(([cat, count]) => `${cat}: <strong>${count}</strong>`)
            .join(', ');

        $('estimate').innerHTML = `
            Total papers: <strong>${total.toLocaleString()}</strong> 
            <span style="color: #8b949e;">(from ${data.months_checked} month(s))</span><br>
            <span style="font-size: 12px; color: #8b949e;">${breakdown || 'No papers found'}</span><br>
            <span style="color: #8b949e; font-size: 12px;">Estimated time: ~${Math.ceil(total / 500)} minutes</span>
        `;

        $('embed-btn').disabled = total === 0;
        $('embed-btn').textContent = `Embed ${total.toLocaleString()} papers`;

    } catch (err) {
        $('estimate').innerHTML = `<span class="error">Error: ${err.message}</span>`;
    }
}

// Embedding
function startEmbedding() {
    if (selectedCategories.size === 0) return;

    $('embed-btn').disabled = true;
    $('progress').classList.add('active');
    $('log').innerHTML = '';

    ws = new WebSocket(`ws://${window.location.host}/ws/embed`);

    ws.onopen = () => {
        log('Connected to server', 'success');
        ws.send(JSON.stringify({ categories: Array.from(selectedCategories) }));
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

    if (data.status === 'progress') {
        const percent = (data.current / data.total) * 100;
        $('progress-fill').style.width = `${percent}%`;
        $('progress-text').textContent = data.message;
        log(data.message);
    }

    if (data.status === 'embedding') {
        log(`Starting ${data.category}...`, 'success');
    }

    if (data.status === 'visualizing') {
        log('Creating visualization...', 'success');
    }

    if (data.status === 'complete') {
        log(`Complete! ${data.total_papers.toLocaleString()} papers embedded`, 'success');
        $('progress-fill').style.width = '100%';
        $('embed-btn').disabled = false;
        setTimeout(() => loadCategories(), 1000);
    }
}

function log(message, type = '') {
    const entry = document.createElement('div');
    entry.className = `log-entry ${type}`;
    entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    $('log').appendChild(entry);
    $('log').scrollTop = $('log').scrollHeight;
}

// Init
$('embed-btn').addEventListener('click', startEmbedding);

loadMonths();
loadCategories();

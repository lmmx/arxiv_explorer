// static/js/app.js
const $ = id => document.getElementById(id);

function debounce(fn, ms) {
    let timeout;
    return (...args) => {
        clearTimeout(timeout);
        timeout = setTimeout(() => fn(...args), ms);
    };
}

// Tooltip module
const Tooltip = (function() {
    const tooltip = $('tooltip');
    
    document.addEventListener('mousemove', (e) => {
        if (tooltip.classList.contains('visible')) {
            tooltip.style.left = (e.clientX + 12) + 'px';
            tooltip.style.top = (e.clientY + 12) + 'px';
        }
    });

    return {
        show(paper) {
            tooltip.querySelector('.meta').textContent = `${paper.primary_subject} · ${paper.arxiv_id}`;
            tooltip.querySelector('.title').textContent = paper.title;
            tooltip.classList.add('visible');
        },
        hide() {
            tooltip.classList.remove('visible');
        }
    };
})();

// Initialize
async function init() {
    $('status').textContent = 'Loading papers...';
    
    try {
        const count = await Plot.load();
        $('status').textContent = `${count} papers loaded`;
        Search.init();
        
        // Wire up the hide non-hits toggle
        const toggle = $('hide-non-hits');
        toggle.checked = Plot.getHideNonHits();
        toggle.addEventListener('change', (e) => {
            Plot.setHideNonHits(e.target.checked);
        });
    } catch (err) {
        $('status').textContent = `Error: ${err.message}`;
    }
}

window.addEventListener('resize', debounce(() => Plot.resize(), 200));

init();
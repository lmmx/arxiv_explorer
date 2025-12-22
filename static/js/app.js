// static/js/app.js - Updated
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
            const color = Topics.getColorMode() === 'topic' 
                ? (Topics.getAssignment(paper.arxiv_id) 
                    ? Topics.getTopicColor(Topics.getAssignment(paper.arxiv_id).dominant)
                    : CategoryColors.getColor(paper.primary_subject))
                : CategoryColors.getColor(paper.primary_subject);
            
            let metaHtml = `<span style="color: ${color};">●</span> ${paper.primary_subject} · ${paper.arxiv_id}`;
            
            // Add topic info if available
            const topicAssignment = Topics.getAssignment(paper.arxiv_id);
            if (topicAssignment) {
                metaHtml += ` · Topic ${topicAssignment.dominant + 1}`;
            }
            
            tooltip.querySelector('.meta').innerHTML = metaHtml;
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
        Topics.init();
        Search.updateStatus();
        Search.init();
        
        const toggle = $('hide-non-hits');
        toggle.checked = Plot.getHideNonHits();
        toggle.addEventListener('change', (e) => {
            Plot.setHideNonHits(e.target.checked);
            Search.updateStatus();
        });
    } catch (err) {
        $('status').textContent = `Error: ${err.message}`;
    }
}

window.addEventListener('resize', debounce(() => Plot.resize(), 200));

init();
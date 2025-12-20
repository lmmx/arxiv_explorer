// static/js/search.js
const Search = (function() {
    const $ = id => document.getElementById(id);
    
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    function renderResults(results, query) {
        const resultsContainer = $('results');
        
        resultsContainer.innerHTML = results.map(r => `
            <div class="paper-card" data-arxiv-id="${r.arxiv_id}">
                <div class="paper-meta">
                    <span class="badge ${r.primary_subject.split('.')[0]}">${r.primary_subject}</span>
                    <span class="badge">${r.submission_date}</span>
                    <span class="score">${(r.score * 100).toFixed(0)}%</span>
                </div>
                <div class="paper-title">${escapeHtml(r.title)}</div>
                <div class="paper-authors">${escapeHtml(r.authors.join(', '))}</div>
                <div class="paper-abstract">${escapeHtml(r.abstract)}</div>
                <a href="https://arxiv.org/abs/${r.arxiv_id}" target="_blank" class="arxiv-link">
                    View on arXiv →
                </a>
            </div>
        `).join('');

        $('results-header').textContent = `"${query}" → ${results.length} results`;

        // Attach hover listeners to cards
        resultsContainer.querySelectorAll('.paper-card').forEach(card => {
            const arxivId = card.dataset.arxivId;
            
            card.addEventListener('mouseenter', () => {
                Plot.highlightPaper(arxivId);
            });
            
            card.addEventListener('mouseleave', () => {
                Plot.unhighlightPaper();
            });
            
            card.addEventListener('click', (e) => {
                // Don't pan if clicking the arXiv link
                if (e.target.classList.contains('arxiv-link')) return;
                Plot.panToPaper(arxivId);
            });
        });
    }

    async function search() {
        const q = $('q').value.trim();
        
        if (!q) {
            Plot.clearSearchResults();
            $('results').innerHTML = '';
            $('results-header').textContent = 'Search results';
            return;
        }

        $('status').textContent = 'Searching...';
        $('btn').disabled = true;

        try {
            const res = await fetch(`/api/search?q=${encodeURIComponent(q)}&k=50`);
            const results = await res.json();

            renderResults(results, q);
            Plot.setSearchResults(results);
            $('status').textContent = 'Search complete';
        } catch (err) {
            $('status').textContent = `Error: ${err.message}`;
        }

        $('btn').disabled = false;
    }

    function clear() {
        Plot.clearSearchResults();
        $('results').innerHTML = '';
        $('results-header').textContent = 'Search results';
    }

    return {
        search,
        clear,
        init() {
            $('btn').onclick = search;
            $('q').onkeypress = e => { if (e.key === 'Enter') search(); };
            $('q').oninput = debounce(() => {
                if ($('q').value.trim() === '') clear();
            }, 200);
        }
    };
})();
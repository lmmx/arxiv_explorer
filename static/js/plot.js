// static/js/plot.js
const Plot = (function() {
    const svg = d3.select('#plot');
    const container = svg.append('g');
    const dotsGroup = container.append('g').attr('class', 'dots');
    
    let xScale, yScale;
    let currentTransform = d3.zoomIdentity;
    let allPapers = [];
    let displayPapers = []; // THE KEY: only these get rendered
    let searchResults = new Set();
    let highlightedPaperId = null;
    let hideNonHits = true;
    
    const subjectColors = {
        'cs': '#56d364',
        'physics': '#d29922',
        'math': '#58a6ff',
        'astro-ph': '#d29dff',
        'quant-ph': '#ff6b9d',
        'cond-mat': '#ffa657',
        'stat': '#79c0ff',
    };

    function getSubjectColor(subject) {
        const prefix = subject.split('.')[0];
        return subjectColors[prefix] || '#8b949e';
    }

    const zoom = d3.zoom()
        .scaleExtent([0.5, 20])
        .on('zoom', (event) => {
            currentTransform = event.transform;
            container.attr('transform', event.transform);
            updateDotSizes();
        });

    svg.call(zoom);
    svg.on('dblclick.zoom', null);
    svg.on('dblclick', () => {
        svg.transition().duration(500).call(zoom.transform, d3.zoomIdentity);
    });

    function updateDotSizes() {
        const baseSize = 2.5;
        const scale = currentTransform.k;
        const size = baseSize / Math.sqrt(scale);
        
        dotsGroup.selectAll('circle')
            .attr('r', d => {
                if (highlightedPaperId === d.arxiv_id) return size * 2.5;
                if (searchResults.size > 0) return size * 1.5;
                return size;
            });
    }

    function initializeScales() {
        const containerEl = document.querySelector('.plot-container');
        const width = containerEl.clientWidth;
        const height = containerEl.clientHeight;
        const padding = 40;

        svg.attr('width', width).attr('height', height);

        const xExtent = d3.extent(allPapers, d => d.x);
        const yExtent = d3.extent(allPapers, d => d.y);

        xScale = d3.scaleLinear()
            .domain(xExtent)
            .range([padding, width - padding]);

        yScale = d3.scaleLinear()
            .domain(yExtent)
            .range([padding, height - padding]);
    }

    function updateDisplayPapers() {
        // This is the key function - determines what gets rendered
        if (searchResults.size > 0 && hideNonHits) {
            displayPapers = allPapers.filter(p => searchResults.has(p.arxiv_id));
        } else {
            displayPapers = allPapers;
        }
    }

    function render() {
        const hasSearch = searchResults.size > 0;
    
        const circles = dotsGroup
            .selectAll('circle')
            .data(displayPapers, d => d.arxiv_id);
    
        // EXIT: remove non-hits
        circles.exit().remove();
    
        // ENTER
        const enter = circles.enter()
            .append('circle')
            .attr('cx', d => xScale(d.x))
            .attr('cy', d => yScale(d.y))
            .on('mouseenter', handleMouseEnter)
            .on('mouseleave', handleMouseLeave);
    
        // ENTER + UPDATE
        enter.merge(circles)
            .attr('fill', d => getSubjectColor(d.primary_subject))
            .attr('opacity', d => {
                if (highlightedPaperId === d.arxiv_id) return 1;
                return hasSearch ? 1 : 0.6;
            })
            .attr('stroke', d => {
                if (highlightedPaperId === d.arxiv_id) return '#fff';
                return hasSearch ? '#fff' : 'none';
            })
            .attr('stroke-width', d => highlightedPaperId === d.arxiv_id ? 3 : 1);
    
        updateDotSizes();
    }

    function handleMouseEnter(event, d) {
        Tooltip.show(d);
        highlightPaper(d.arxiv_id);
    }

    function handleMouseLeave(event, d) {
        Tooltip.hide();
        unhighlightPaper();
    }

    function highlightPaper(arxivId) {
        highlightedPaperId = arxivId;
        
        dotsGroup.selectAll('circle')
            .attr('opacity', d => d.arxiv_id === arxivId ? 1 : (searchResults.size > 0 ? 1 : 0.6))
            .attr('stroke', d => d.arxiv_id === arxivId ? '#fff' : (searchResults.size > 0 ? '#fff' : 'none'))
            .attr('stroke-width', d => d.arxiv_id === arxivId ? 3 : 1);
        
        updateDotSizes();
    }

    function unhighlightPaper() {
        highlightedPaperId = null;
        render();
    }

    function panToPaper(arxivId) {
        const paper = allPapers.find(p => p.arxiv_id === arxivId);
        if (!paper) return;

        const containerEl = document.querySelector('.plot-container');
        const width = containerEl.clientWidth;
        const height = containerEl.clientHeight;

        const x = xScale(paper.x);
        const y = yScale(paper.y);

        const scale = 3;
        const tx = width / 2 - x * scale;
        const ty = height / 2 - y * scale;

        svg.transition()
            .duration(500)
            .call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
    }

    return {
        async load() {
            const res = await fetch('/api/papers');
            allPapers = await res.json();
            displayPapers = allPapers; // Initially show all
            initializeScales();
            render();
            return allPapers.length;
        },
        
        setSearchResults(results) {
            searchResults = new Set(results.map(r => r.arxiv_id));
            updateDisplayPapers();
            render();
        },
        
        clearSearchResults() {
            searchResults.clear();
            updateDisplayPapers();
            render();
        },
        
        setHideNonHits(value) {
            hideNonHits = value;
            updateDisplayPapers();
            render();
        },
        
        getHideNonHits() {
            return hideNonHits;
        },
        
        highlightPaper,
        unhighlightPaper,
        panToPaper,
        
        resize() {
            initializeScales();
            render();
            svg.call(zoom.transform, currentTransform);
        },

        getPaperById(arxivId) {
            return allPapers.find(p => p.arxiv_id === arxivId);
        }
    };
})();

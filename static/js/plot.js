// static/js/plot.js
const Plot = (function() {
    const svg = d3.select('#plot');
    const container = svg.append('g');
    const dotsGroup = container.append('g').attr('class', 'dots');
    
    let xScale, yScale;
    let currentTransform = d3.zoomIdentity;
    let allPapers = [];
    let searchResults = new Set();
    let highlightedPaperId = null;
    
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

    // Zoom behavior
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
                if (highlightedPaperId === d.arxiv_id) {
                    return size * 2.5;
                }
                if (searchResults.size > 0 && searchResults.has(d.arxiv_id)) {
                    return size * 1.5;
                }
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

    function render() {
        const hasSearch = searchResults.size > 0;

        const dots = dotsGroup.selectAll('circle')
            .data(allPapers, d => d.arxiv_id);

        dots.exit().remove();

        const dotsEnter = dots.enter()
            .append('circle')
            .attr('cx', d => xScale(d.x))
            .attr('cy', d => yScale(d.y))
            .on('mouseenter', handleMouseEnter)
            .on('mouseleave', handleMouseLeave);

        dots.merge(dotsEnter)
            .attr('cx', d => xScale(d.x))
            .attr('cy', d => yScale(d.y))
            .attr('fill', d => getSubjectColor(d.primary_subject))
            .attr('opacity', d => {
                if (highlightedPaperId === d.arxiv_id) return 1;
                if (!hasSearch) return 0.6;
                return searchResults.has(d.arxiv_id) ? 1 : 0.08;
            })
            .attr('stroke', d => {
                if (highlightedPaperId === d.arxiv_id) return '#fff';
                if (!hasSearch) return 'none';
                return searchResults.has(d.arxiv_id) ? '#fff' : 'none';
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
        
        // Update the specific dot
        dotsGroup.selectAll('circle')
            .attr('opacity', d => {
                if (d.arxiv_id === arxivId) return 1;
                if (searchResults.size === 0) return 0.6;
                return searchResults.has(d.arxiv_id) ? 1 : 0.08;
            })
            .attr('stroke', d => {
                if (d.arxiv_id === arxivId) return '#fff';
                if (searchResults.size === 0) return 'none';
                return searchResults.has(d.arxiv_id) ? '#fff' : 'none';
            })
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

        // Calculate transform to center the paper
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
            initializeScales();
            render();
            return allPapers.length;
        },
        
        setSearchResults(results) {
            searchResults = new Set(results.map(r => r.arxiv_id));
            render();
        },
        
        clearSearchResults() {
            searchResults.clear();
            render();
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
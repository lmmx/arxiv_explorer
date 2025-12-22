// static/js/plot.js - Updated version with topic support
const Plot = (function() {
    const svg = d3.select('#plot');
    const container = svg.append('g');
    const dotsGroup = container.append('g').attr('class', 'dots');
    
    let xScale, yScale;
    let currentTransform = d3.zoomIdentity;
    let allPapers = [];
    let searchResults = new Set();
    let highlightedPaperId = null;
    let hideNonHits = true;
    
    // Topic-related state
    let topicAssignments = {};  // arxiv_id -> { weights, dominant }
    let topicColorFn = null;
    let colorMode = 'category';  // 'category' or 'topic'
    let selectedTopicId = null;

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

        if (allPapers.length === 0) return;

        const xExtent = d3.extent(allPapers, d => d.x);
        const yExtent = d3.extent(allPapers, d => d.y);

        xScale = d3.scaleLinear()
            .domain(xExtent)
            .range([padding, width - padding]);

        yScale = d3.scaleLinear()
            .domain(yExtent)
            .range([padding, height - padding]);
    }

    function getDisplayPapers() {
        let papers = allPapers;
        
        // Filter by search results if applicable
        if (searchResults.size > 0 && hideNonHits) {
            papers = papers.filter(p => searchResults.has(p.arxiv_id));
        }
        
        // Filter by selected topic if applicable
        if (selectedTopicId !== null && Object.keys(topicAssignments).length > 0) {
            papers = papers.filter(p => {
                const assignment = topicAssignments[p.arxiv_id];
                return assignment && assignment.dominant === selectedTopicId;
            });
        }
        
        return papers;
    }

    function getPointColor(paper) {
        if (colorMode === 'topic' && topicColorFn) {
            const assignment = topicAssignments[paper.arxiv_id];
            if (assignment) {
                return topicColorFn(assignment.dominant);
            }
        }
        return CategoryColors.getColor(paper.primary_subject);
    }

    function render() {
        const hasSearch = searchResults.size > 0;
        const displayPapers = getDisplayPapers();
    
        const circles = dotsGroup
            .selectAll('circle')
            .data(displayPapers, d => d.arxiv_id);
    
        circles.exit().remove();
    
        const enter = circles.enter()
            .append('circle')
            .attr('cx', d => xScale(d.x))
            .attr('cy', d => yScale(d.y))
            .on('mouseenter', handleMouseEnter)
            .on('mouseleave', handleMouseLeave);
    
        enter.merge(circles)
            .attr('cx', d => xScale(d.x))
            .attr('cy', d => yScale(d.y))
            .attr('fill', d => getPointColor(d))
            .attr('opacity', d => {
                if (highlightedPaperId === d.arxiv_id) return 0.9;
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

    async function loadPapers(filterParams) {
        let url = '/api/papers';
        if (filterParams && filterParams.toString()) {
            url += '?' + filterParams.toString();
        }
        
        const res = await fetch(url);
        allPapers = await res.json();
        initializeScales();
        render();
        return allPapers.length;
    }

    return {
        async load() {
            const count = await loadPapers();
            Filters.init(allPapers, () => this.applyFilters());
            return count;
        },
        
        async applyFilters() {
            const filterParams = Filters.getFilterParams();
            await loadPapers(filterParams);
            
            // Clear topic data when filters change
            topicAssignments = {};
            selectedTopicId = null;
            Topics.clear();
            
            if (Search.hasActiveQuery()) {
                await Search.rerunSearch();
            }
            
            Search.updateStatus();
        },
        
        setSearchResults(results) {
            searchResults = new Set(results.map(r => r.arxiv_id));
            render();
        },
        
        clearSearchResults() {
            searchResults.clear();
            render();
        },
        
        setHideNonHits(value) {
            hideNonHits = value;
            render();
        },
        
        getHideNonHits() {
            return hideNonHits;
        },
        
        // Topic-related methods
        setTopicData(assignments, colorFn) {
            topicAssignments = assignments;
            topicColorFn = colorFn;
            render();
        },
        
        clearTopicData() {
            topicAssignments = {};
            topicColorFn = null;
            selectedTopicId = null;
            colorMode = 'category';
            render();
        },
        
        setColorMode(mode) {
            colorMode = mode;
            render();
        },
        
        setSelectedTopic(topicId) {
            selectedTopicId = topicId;
            render();
            Search.updateStatus();
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
        },
        
        getDisplayedCount() {
            return getDisplayPapers().length;
        },
        
        getTotalCount() {
            return allPapers.length;
        },
        
        getAllPapers() {
            return allPapers;
        }
    };
})();
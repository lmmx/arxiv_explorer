// static/js/topics.js
const Topics = (function() {
    const $ = id => document.getElementById(id);

    // Topic color palette (distinct from category colors)
    const TOPIC_COLORS = [
        '#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#ffeaa7',
        '#dfe6e9', '#fd79a8', '#a29bfe', '#6c5ce7', '#00b894',
        '#e17055', '#74b9ff', '#55a3ff', '#ff7675', '#fdcb6e',
    ];

    let topics = [];
    let assignments = {};  // arxiv_id -> { weights, dominant }
    let selectedTopicId = null;
    let colorMode = 'category';  // 'category' or 'topic'
    let isExpanded = false;
    let isLoading = false;

    function getTopicColor(topicId) {
        return TOPIC_COLORS[topicId % TOPIC_COLORS.length];
    }

    function render() {
        const panel = $('topics-panel');
        if (!panel) return;

        const controlsVisible = isExpanded ? 'visible' : '';
        const listVisible = topics.length > 0 ? 'visible' : '';

        panel.innerHTML = `
            <div class="topics-header" onclick="Topics.toggle()">
                <h3>🎯 Topic Modeling</h3>
                <span class="topics-toggle">${isExpanded ? '▼' : '▶'}</span>
            </div>
            <div class="topics-controls ${controlsVisible}">
                <div class="topics-control-row">
                    <label>Topics:</label>
                    <input type="range" id="n-topics" min="2" max="50" value="10" 
                           oninput="Topics.updateTopicCount(this.value)">
                    <span class="value" id="n-topics-value">8</span>
                </div>
                <button class="topics-extract-btn" id="extract-btn" onclick="Topics.extract()"
                        ${isLoading ? 'disabled' : ''}>
                    ${isLoading ? 'Extracting...' : 'Extract Topics'}
                </button>
            </div>
            ${topics.length > 0 ? `
                <div class="color-mode-toggle">
                    <label>Color by:</label>
                    <select id="color-mode" onchange="Topics.setColorMode(this.value)">
                        <option value="category" ${colorMode === 'category' ? 'selected' : ''}>Category</option>
                        <option value="topic" ${colorMode === 'topic' ? 'selected' : ''}>Topic</option>
                    </select>
                </div>
            ` : ''}
            <div class="topics-list ${listVisible}">
                ${renderTopicList()}
            </div>
            <div class="topics-status ${isLoading ? 'loading' : ''}" id="topics-status">
                ${getStatusText()}
            </div>
        `;
    }

    function renderTopicList() {
        return topics.map(topic => {
            const color = getTopicColor(topic.id);
            const isSelected = selectedTopicId === topic.id;
            const topTerms = topic.terms.slice(0, 5);
            
            return `
                <div class="topic-item ${isSelected ? 'selected' : ''}" 
                     style="${isSelected ? `border-color: ${color};` : ''}"
                     onclick="Topics.selectTopic(${topic.id})">
                    <div class="topic-item-header">
                        <span class="topic-color-dot" style="background: ${color};"></span>
                        <span class="topic-label">Topic ${topic.id + 1}</span>
                        <span class="topic-count">${topic.doc_count} papers</span>
                    </div>
                    <div class="topic-terms">
                        ${topTerms.map(t => 
                            `<span class="topic-term">${t.term}</span>`
                        ).join('')}
                    </div>
                </div>
            `;
        }).join('');
    }

    function getStatusText() {
        if (isLoading) return 'Extracting topics...';
        if (topics.length > 0) {
            const total = topics.reduce((sum, t) => sum + t.doc_count, 0);
            return `${topics.length} topics from ${total} papers`;
        }
        return 'Extract topics to discover themes';
    }

    function toggle() {
        isExpanded = !isExpanded;
        render();
    }

    function updateTopicCount(value) {
        $('n-topics-value').textContent = value;
    }

    async function extract() {
        const nTopics = parseInt($('n-topics')?.value || '8');
        
        isLoading = true;
        render();

        try {
            // Get current filters
            const filterParams = Filters.getFilterParams();
            const yearMonths = filterParams.get('year_months')?.split(',').filter(Boolean) || null;
            const categories = filterParams.get('categories')?.split(',').filter(Boolean) || null;

            const res = await fetch('/api/topics/extract', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    n_components: nTopics,
                    year_months: yearMonths,
                    categories: categories,
                }),
            });

            const data = await res.json();

            if (data.error) {
                $('topics-status').textContent = data.error;
                $('topics-status').className = 'topics-status error';
                isLoading = false;
                render();
                return;
            }

            topics = data.topics;
            assignments = data.assignments;
            selectedTopicId = null;
            
            // Update plot with topic data
            Plot.setTopicData(assignments, getTopicColor);
            
        } catch (err) {
            console.error('Topic extraction failed:', err);
            $('topics-status').textContent = `Error: ${err.message}`;
            $('topics-status').className = 'topics-status error';
        }

        isLoading = false;
        render();
    }

    function selectTopic(topicId) {
        if (selectedTopicId === topicId) {
            selectedTopicId = null;
        } else {
            selectedTopicId = topicId;
        }
        
        Plot.setSelectedTopic(selectedTopicId);
        render();
    }

    function setColorMode(mode) {
        colorMode = mode;
        Plot.setColorMode(mode);
    }

    function getColorMode() {
        return colorMode;
    }

    function getAssignment(arxivId) {
        return assignments[arxivId] || null;
    }

    function clear() {
        topics = [];
        assignments = {};
        selectedTopicId = null;
        colorMode = 'category';
        Plot.clearTopicData();
        render();
    }

    return {
        init() {
            render();
        },
        toggle,
        updateTopicCount,
        extract,
        selectTopic,
        setColorMode,
        getColorMode,
        getTopicColor,
        getAssignment,
        clear,
        render,
    };
})();

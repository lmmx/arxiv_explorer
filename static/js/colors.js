// static/js/colors.js
// Stable color palette for arXiv categories
// Uses a deterministic hash to assign colors, ensuring consistency across sessions

const CategoryColors = (function() {
    // Large palette of visually distinct colors that work on dark backgrounds
    // Avoiding colors too similar to the dark background (#0d1117)
    const PALETTE = [
        '#56d364', // green
        '#58a6ff', // blue
        '#f78166', // coral/orange
        '#d29dff', // purple
        '#ffd33d', // yellow
        '#ff6b9d', // pink
        '#79c0ff', // light blue
        '#ffa657', // orange
        '#a5d6ff', // sky blue
        '#7ee787', // light green
        '#ff7b72', // salmon
        '#d2a8ff', // lavender
        '#ffc658', // gold
        '#f0883e', // tangerine
        '#bc8cff', // violet
        '#39d353', // bright green
        '#6cb6ff', // azure
        '#ffab70', // peach
        '#e6edf3', // off-white
        '#8b949e', // gray
        '#ff9bce', // light pink
        '#89e5a6', // mint
        '#ffdf5d', // lemon
        '#ff8578', // light coral
        '#b4befe', // periwinkle
        '#c4b5fd', // soft violet
        '#94e2d5', // teal
        '#f9e2af', // cream
        '#fab387', // apricot
        '#eba0ac', // rose
        '#89b4fa', // cornflower
        '#74c7ec', // sapphire
        '#94e2d5', // aqua
        '#a6e3a1', // pastel green
        '#f5c2e7', // mauve
        '#cba6f7', // magenta
        '#f38ba8', // maroon
        '#fab387', // peach
        '#f9e2af', // yellow
        '#a6e3a1', // green
    ];

    // Simple hash function for strings
    function hashCode(str) {
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            const char = str.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash; // Convert to 32bit integer
        }
        return Math.abs(hash);
    }

    // Cache for computed colors
    const colorCache = {};

    // Get a stable color for a category
    function getColor(category) {
        if (colorCache[category]) {
            return colorCache[category];
        }

        // Use hash to pick a color index
        const hash = hashCode(category);
        const color = PALETTE[hash % PALETTE.length];
        colorCache[category] = color;
        return color;
    }

    // Get color for primary subject (group level)
    function getGroupColor(subject) {
        const prefix = subject.split('.')[0];
        return getColor(prefix);
    }

    return {
        getColor,
        getGroupColor,
        PALETTE
    };
})();
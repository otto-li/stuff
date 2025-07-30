const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Serve static files from public directory
app.use(express.static(path.join(__dirname, 'public')));

// Main route - serve the sparkle cursor page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// API endpoint to get sparkle configuration
app.get('/api/sparkle-config', (req, res) => {
    res.json({
        sparkleCount: 15,
        sparkleLifetime: 1000,
        sparkleColors: ['#FFD700', '#FF69B4', '#00CED1', '#FF6347', '#98FB98', '#DDA0DD'],
        sparkleSize: { min: 4, max: 12 },
        sparkleSpeed: { min: 0.5, max: 2 }
    });
});

// Start the server
app.listen(PORT, () => {
    console.log(`✨ Sparkle Cursor Server running on http://localhost:${PORT}`);
    console.log(`🎆 Move your mouse around to see the magic!`);
}); 
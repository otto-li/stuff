// Sparkle Cursor Magic ✨
class SparkleTrail {
    constructor() {
        this.container = document.getElementById('sparkle-container');
        this.sparkles = [];
        this.config = {
            sparkleCount: 15,
            sparkleLifetime: 1000,
            sparkleColors: ['#FFD700', '#FF69B4', '#00CED1', '#FF6347', '#98FB98', '#DDA0DD'],
            sparkleSize: { min: 4, max: 12 },
            sparkleSpeed: { min: 0.5, max: 2 }
        };
        
        this.mouse = { x: 0, y: 0 };
        this.isShiftPressed = false;
        this.lastSparkleTime = 0;
        this.sparkleThrottle = 50; // Minimum ms between sparkles
        
        this.init();
        this.loadConfig();
    }

    async loadConfig() {
        try {
            const response = await fetch('/api/sparkle-config');
            const config = await response.json();
            this.config = { ...this.config, ...config };
        } catch (error) {
            console.log('Using default sparkle configuration');
        }
    }

    init() {
        this.createCustomCursor();
        this.bindEvents();
        this.startCleanupLoop();
        
        // Create initial sparkles
        this.createWelcomeSparkles();
    }

    createCustomCursor() {
        const cursor = document.createElement('div');
        cursor.className = 'custom-cursor';
        cursor.id = 'custom-cursor';
        document.body.appendChild(cursor);
        this.cursor = cursor;
    }

    bindEvents() {
        // Mouse movement
        document.addEventListener('mousemove', (e) => {
            this.mouse.x = e.clientX;
            this.mouse.y = e.clientY;
            
            // Update custom cursor position
            if (this.cursor) {
                this.cursor.style.left = `${e.clientX - 10}px`;
                this.cursor.style.top = `${e.clientY - 10}px`;
            }
            
            this.createSparkle(e.clientX, e.clientY);
        });

        // Click for extra sparkles
        document.addEventListener('click', (e) => {
            this.createSparkleburst(e.clientX, e.clientY);
            
            // Cursor click effect
            if (this.cursor) {
                this.cursor.classList.add('clicked');
                setTimeout(() => this.cursor.classList.remove('clicked'), 150);
            }
        });

        // Shift key for mega sparkles
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Shift') {
                this.isShiftPressed = true;
            }
        });

        document.addEventListener('keyup', (e) => {
            if (e.key === 'Shift') {
                this.isShiftPressed = false;
            }
        });

        // Prevent context menu
        document.addEventListener('contextmenu', (e) => {
            e.preventDefault();
            this.createSparkleburst(e.clientX, e.clientY, 'mega');
        });

        // Touch support for mobile
        document.addEventListener('touchmove', (e) => {
            e.preventDefault();
            const touch = e.touches[0];
            this.createSparkle(touch.clientX, touch.clientY);
        });
    }

    createSparkle(x, y, type = 'normal') {
        const now = Date.now();
        if (now - this.lastSparkleTime < this.sparkleThrottle && type === 'normal') {
            return;
        }
        this.lastSparkleTime = now;

        const sparkle = document.createElement('div');
        sparkle.className = 'sparkle';
        
        // Random properties
        const size = this.randomBetween(this.config.sparkleSize.min, this.config.sparkleSize.max);
        const color = this.getRandomColor();
        const angle = Math.random() * 360;
        const velocity = this.randomBetween(this.config.sparkleSpeed.min, this.config.sparkleSpeed.max);
        
        // Enhanced sparkles when shift is pressed
        if (this.isShiftPressed || type === 'mega') {
            sparkle.classList.add('mega-sparkle');
            const megaSize = size * 1.5;
            sparkle.style.width = `${megaSize}px`;
            sparkle.style.height = `${megaSize}px`;
        } else {
            sparkle.style.width = `${size}px`;
            sparkle.style.height = `${size}px`;
        }
        
        sparkle.style.left = `${x - size/2}px`;
        sparkle.style.top = `${y - size/2}px`;
        sparkle.style.color = color;
        sparkle.style.transform = `rotate(${angle}deg)`;
        
        // Add slight random offset
        const offsetX = (Math.random() - 0.5) * 20;
        const offsetY = (Math.random() - 0.5) * 20;
        sparkle.style.left = `${x - size/2 + offsetX}px`;
        sparkle.style.top = `${y - size/2 + offsetY}px`;
        
        this.container.appendChild(sparkle);
        this.sparkles.push({
            element: sparkle,
            createdAt: now,
            lifetime: this.config.sparkleLifetime * (this.isShiftPressed ? 1.5 : 1)
        });

        // Remove sparkle after animation
        setTimeout(() => {
            this.removeSparkle(sparkle);
        }, sparkle.classList.contains('mega-sparkle') ? 1500 : 1000);
    }

    createSparkleburst(x, y, type = 'normal') {
        const burstCount = type === 'mega' ? 25 : 15;
        const radius = type === 'mega' ? 60 : 40;
        
        for (let i = 0; i < burstCount; i++) {
            setTimeout(() => {
                const angle = (i / burstCount) * Math.PI * 2;
                const distance = Math.random() * radius;
                const sparkleX = x + Math.cos(angle) * distance;
                const sparkleY = y + Math.sin(angle) * distance;
                
                this.createSparkle(sparkleX, sparkleY, type);
            }, i * 20);
        }
    }

    createWelcomeSparkles() {
        const centerX = window.innerWidth / 2;
        const centerY = window.innerHeight / 2;
        
        for (let i = 0; i < 20; i++) {
            setTimeout(() => {
                const angle = (i / 20) * Math.PI * 2;
                const radius = 100 + Math.random() * 100;
                const x = centerX + Math.cos(angle) * radius;
                const y = centerY + Math.sin(angle) * radius;
                
                this.createSparkle(x, y);
            }, i * 100);
        }
    }

    removeSparkle(sparkleElement) {
        if (sparkleElement && sparkleElement.parentNode) {
            sparkleElement.parentNode.removeChild(sparkleElement);
        }
        
        // Remove from tracking array
        this.sparkles = this.sparkles.filter(sparkle => sparkle.element !== sparkleElement);
    }

    startCleanupLoop() {
        setInterval(() => {
            const now = Date.now();
            const expiredSparkles = this.sparkles.filter(sparkle => 
                now - sparkle.createdAt > sparkle.lifetime
            );
            
            expiredSparkles.forEach(sparkle => {
                this.removeSparkle(sparkle.element);
            });
        }, 1000);
    }

    getRandomColor() {
        return this.config.sparkleColors[Math.floor(Math.random() * this.config.sparkleColors.length)];
    }

    randomBetween(min, max) {
        return Math.random() * (max - min) + min;
    }

    // Public methods for external control
    setSparkleIntensity(intensity) {
        this.sparkleThrottle = Math.max(10, 100 - (intensity * 10));
    }

    toggleMegaMode() {
        this.isShiftPressed = !this.isShiftPressed;
    }
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    const sparkleTrail = new SparkleTrail();
    
    // Make it globally available for debugging/control
    window.sparkleTrail = sparkleTrail;
    
    // Console welcome message
    console.log(`
    ✨✨✨ SPARKLE CURSOR LOADED ✨✨✨
    
    🎮 Controls:
    • Move mouse = Create sparkles
    • Click = Extra sparkle burst
    • Hold Shift = Mega sparkles
    • Right click = Mega burst
    
    🛠️ Debug commands:
    • sparkleTrail.setSparkleIntensity(1-10)
    • sparkleTrail.toggleMegaMode()
    
    Enjoy the magic! ✨
    `);
});

// Easter egg: Konami code for rainbow mode
(function() {
    const konamiCode = [38, 38, 40, 40, 37, 39, 37, 39, 66, 65];
    let userInput = [];
    
    document.addEventListener('keydown', (e) => {
        userInput.push(e.keyCode);
        userInput = userInput.slice(-konamiCode.length);
        
        if (userInput.join('') === konamiCode.join('')) {
            // Rainbow mode activated!
            document.body.style.animation = 'rainbow 2s linear infinite';
            const style = document.createElement('style');
            style.textContent = `
                @keyframes rainbow {
                    0% { filter: hue-rotate(0deg); }
                    100% { filter: hue-rotate(360deg); }
                }
            `;
            document.head.appendChild(style);
            
            // Create celebration burst
            setTimeout(() => {
                window.sparkleTrail?.createSparkleburst(window.innerWidth/2, window.innerHeight/2, 'mega');
            }, 100);
            
            console.log('🌈 RAINBOW MODE ACTIVATED! 🌈');
        }
    });
})();

// Performance monitoring
let frameCount = 0;
let lastFpsTime = Date.now();

function updateFPS() {
    frameCount++;
    const now = Date.now();
    
    if (now - lastFpsTime >= 1000) {
        const fps = Math.round((frameCount * 1000) / (now - lastFpsTime));
        
        // Log performance occasionally
        if (frameCount % 60 === 0) {
            console.log(`✨ Sparkle FPS: ${fps} | Active sparkles: ${window.sparkleTrail?.sparkles.length || 0}`);
        }
        
        frameCount = 0;
        lastFpsTime = now;
    }
    
    requestAnimationFrame(updateFPS);
}

// Start FPS monitoring
requestAnimationFrame(updateFPS); 
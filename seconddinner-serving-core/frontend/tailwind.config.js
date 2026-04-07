/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        void: "#050510",
        panel: "#0a0a1a",
        "panel-light": "#0f0f2a",
        border: "#1a1a3a",
        cyan: "#00d4ff",
        gold: "#ffd700",
        "neon-green": "#00ff88",
        "neon-purple": "#a855f7",
        "neon-red": "#ff4444",
        "neon-orange": "#ff8800",
        "neon-yellow": "#ffcc00",
        muted: "#4a4a6a",
        "text-dim": "#8888aa",
      },
      fontFamily: {
        orbitron: ["Orbitron", "sans-serif"],
        mono: ["JetBrains Mono", "monospace"],
      },
      boxShadow: {
        "neon-cyan": "0 0 10px #00d4ff, 0 0 20px #00d4ff40",
        "neon-gold": "0 0 10px #ffd700, 0 0 20px #ffd70040",
        "neon-green": "0 0 10px #00ff88, 0 0 20px #00ff8840",
        "neon-red": "0 0 10px #ff4444, 0 0 20px #ff444440",
        "neon-purple": "0 0 10px #a855f7, 0 0 20px #a855f740",
        "neon-orange": "0 0 10px #ff8800, 0 0 20px #ff880040",
      },
      animation: {
        "pulse-slow": "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite",
        "slide-in": "slideIn 0.3s ease-out",
        "glow-cyan": "glowCyan 2s ease-in-out infinite",
      },
      keyframes: {
        slideIn: {
          "0%": { transform: "translateY(-12px)", opacity: "0" },
          "100%": { transform: "translateY(0)", opacity: "1" },
        },
        glowCyan: {
          "0%, 100%": { boxShadow: "0 0 6px #00d4ff, 0 0 12px #00d4ff40" },
          "50%": { boxShadow: "0 0 20px #00d4ff, 0 0 40px #00d4ff60" },
        },
      },
    },
  },
  plugins: [],
};

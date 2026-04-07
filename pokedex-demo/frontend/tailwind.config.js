/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        pixel: ['"Press Start 2P"', "monospace"],
        mono: ["Courier New", "monospace"],
      },
      colors: {
        pokedex: {
          red: "#CC0000",
          darkred: "#880000",
          screen: "#98c820",
          screenDark: "#1a2e00",
          gray: "#d0d0d0",
          dark: "#1a1a1a",
        },
      },
      animation: {
        "fill-bar": "fillBar 1s ease-out forwards",
        "scan": "scan 2s linear infinite",
        "pulse-glow": "pulseGlow 2s ease-in-out infinite",
      },
      keyframes: {
        fillBar: {
          "0%": { width: "0%" },
          "100%": { width: "var(--bar-width)" },
        },
        scan: {
          "0%": { transform: "translateY(-100%)" },
          "100%": { transform: "translateY(100%)" },
        },
        pulseGlow: {
          "0%, 100%": { boxShadow: "0 0 5px #98c820, 0 0 10px #98c820" },
          "50%": { boxShadow: "0 0 20px #98c820, 0 0 40px #98c820" },
        },
      },
    },
  },
  plugins: [],
};

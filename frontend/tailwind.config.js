/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        border: "rgba(39, 39, 42, 0.8)",
        background: "#09090b",
        foreground: "#fafafa",
        primary: {
          DEFAULT: "#38bdf8",
          foreground: "#000000",
        },
        secondary: {
          DEFAULT: "#18181b",
          foreground: "#f8fafc",
        },
        accent: {
          DEFAULT: "#38bdf8",
          foreground: "#000000",
        },
        card: {
          DEFAULT: "rgba(24, 24, 27, 0.4)",
          foreground: "#fafafa",
        },
      },
      borderRadius: {
        lg: "16px",
        md: "12px",
        sm: "8px",
      },
      backdropBlur: {
        xs: '2px',
      }
    },
  },
  plugins: [],
}

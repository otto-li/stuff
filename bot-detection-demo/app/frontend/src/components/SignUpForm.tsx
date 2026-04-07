import { useState, useRef, FormEvent } from "react";

interface Props {
  onSubmit: () => Promise<void>;
  onSimulateBot: () => void;
  loading: boolean;
}

const inputStyle: React.CSSProperties = {
  width: "100%",
  padding: "0.75rem 1rem",
  borderRadius: "8px",
  border: "1px solid #2d2d44",
  background: "#12121f",
  color: "#e2e8f0",
  fontSize: "0.95rem",
  outline: "none",
  transition: "border-color 0.2s",
};

const labelStyle: React.CSSProperties = {
  display: "block",
  marginBottom: "0.35rem",
  fontSize: "0.85rem",
  fontWeight: 500,
  color: "#94a3b8",
};

export default function SignUpForm({ onSubmit, onSimulateBot, loading }: Props) {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [company, setCompany] = useState("");
  const [password, setPassword] = useState("");

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    await onSubmit();
  };

  return (
    <div
      style={{
        width: "100%",
        maxWidth: "440px",
        position: "relative",
      }}
    >
      {/* Header */}
      <div style={{ textAlign: "center", marginBottom: "2rem" }}>
        <div
          style={{
            width: "48px",
            height: "48px",
            borderRadius: "12px",
            background: "linear-gradient(135deg, #6366f1, #8b5cf6)",
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            marginBottom: "1rem",
            fontSize: "1.4rem",
          }}
        >
          A
        </div>
        <h1
          style={{
            fontSize: "1.6rem",
            fontWeight: 700,
            color: "#f1f5f9",
            marginBottom: "0.25rem",
          }}
        >
          Create your account
        </h1>
        <p style={{ fontSize: "0.9rem", color: "#64748b" }}>
          Start your 14-day free trial. No credit card required.
        </p>
      </div>

      {/* Form card */}
      <form
        onSubmit={handleSubmit}
        style={{
          background: "#111122",
          border: "1px solid #1e1e36",
          borderRadius: "16px",
          padding: "2rem",
          boxShadow: "0 8px 32px rgba(0,0,0,0.4)",
        }}
      >
        <div style={{ display: "flex", flexDirection: "column", gap: "1.1rem" }}>
          {/* Full Name */}
          <div>
            <label style={labelStyle}>Full Name</label>
            <input
              type="text"
              placeholder="Jane Smith"
              value={name}
              onChange={(e) => setName(e.target.value)}
              style={inputStyle}
              onFocus={(e) => (e.currentTarget.style.borderColor = "#6366f1")}
              onBlur={(e) => (e.currentTarget.style.borderColor = "#2d2d44")}
            />
          </div>

          {/* Work Email */}
          <div>
            <label style={labelStyle}>Work Email</label>
            <input
              type="email"
              placeholder="jane@company.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              style={inputStyle}
              onFocus={(e) => (e.currentTarget.style.borderColor = "#6366f1")}
              onBlur={(e) => (e.currentTarget.style.borderColor = "#2d2d44")}
            />
          </div>

          {/* Company */}
          <div>
            <label style={labelStyle}>Company</label>
            <input
              type="text"
              placeholder="Acme Inc."
              value={company}
              onChange={(e) => setCompany(e.target.value)}
              style={inputStyle}
              onFocus={(e) => (e.currentTarget.style.borderColor = "#6366f1")}
              onBlur={(e) => (e.currentTarget.style.borderColor = "#2d2d44")}
            />
          </div>

          {/* Password */}
          <div>
            <label style={labelStyle}>Password</label>
            <input
              type="password"
              placeholder="8+ characters"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              style={inputStyle}
              onFocus={(e) => (e.currentTarget.style.borderColor = "#6366f1")}
              onBlur={(e) => (e.currentTarget.style.borderColor = "#2d2d44")}
            />
          </div>
        </div>

        {/* Submit */}
        <button
          type="submit"
          disabled={loading}
          style={{
            width: "100%",
            padding: "0.8rem",
            marginTop: "1.5rem",
            borderRadius: "8px",
            border: "none",
            background: loading
              ? "#4b4b6b"
              : "linear-gradient(135deg, #6366f1, #8b5cf6)",
            color: "#fff",
            fontSize: "1rem",
            fontWeight: 600,
            cursor: loading ? "wait" : "pointer",
            transition: "opacity 0.2s",
          }}
        >
          {loading ? "Analyzing..." : "Create Account"}
        </button>

        {/* Terms */}
        <p
          style={{
            fontSize: "0.75rem",
            color: "#475569",
            textAlign: "center",
            marginTop: "1rem",
            lineHeight: 1.5,
          }}
        >
          By signing up, you agree to our Terms of Service and Privacy Policy.
        </p>
      </form>

      {/* Bot simulation */}
      <button
        onClick={onSimulateBot}
        disabled={loading}
        style={{
          display: "block",
          width: "100%",
          marginTop: "1rem",
          padding: "0.65rem",
          borderRadius: "8px",
          border: "1px dashed #ef444480",
          background: "transparent",
          color: "#ef4444",
          fontSize: "0.85rem",
          fontWeight: 500,
          cursor: loading ? "wait" : "pointer",
          transition: "background 0.2s",
        }}
        onMouseEnter={(e) =>
          (e.currentTarget.style.background = "rgba(239,68,68,0.08)")
        }
        onMouseLeave={(e) =>
          (e.currentTarget.style.background = "transparent")
        }
      >
        Simulate Bot
      </button>
    </div>
  );
}

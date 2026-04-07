import type { DetectResult, SessionData } from "../App";

interface Props {
  result: DetectResult;
  session: SessionData;
  onReset: () => void;
}

function SignalBadge({
  label,
  positive,
}: {
  label: string;
  positive: boolean;
}) {
  return (
    <span
      style={{
        display: "inline-block",
        padding: "0.3rem 0.75rem",
        borderRadius: "999px",
        fontSize: "0.78rem",
        fontWeight: 500,
        background: positive
          ? "rgba(34,197,94,0.12)"
          : "rgba(239,68,68,0.12)",
        color: positive ? "#22c55e" : "#ef4444",
        border: `1px solid ${positive ? "rgba(34,197,94,0.25)" : "rgba(239,68,68,0.25)"}`,
      }}
    >
      {positive ? "+" : "!"} {label}
    </span>
  );
}

function MetricCard({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div
      style={{
        background: "#12121f",
        borderRadius: "10px",
        padding: "0.85rem 1rem",
        border: "1px solid #1e1e36",
        textAlign: "center",
      }}
    >
      <div style={{ fontSize: "1.15rem", fontWeight: 700, color: "#f1f5f9" }}>
        {value}
      </div>
      <div style={{ fontSize: "0.72rem", color: "#64748b", marginTop: "0.2rem" }}>
        {label}
      </div>
    </div>
  );
}

export default function BotResult({ result, session, onReset }: Props) {
  const isBot = result.is_bot;
  const accentColor = isBot ? "#ef4444" : "#22c55e";
  const features = result.features;

  /* Determine which signals to highlight */
  const signals: { label: string; positive: boolean }[] = [];

  if (features.has_mouse_activity === 0) {
    signals.push({ label: "No mouse activity", positive: false });
  } else {
    signals.push({ label: "Natural mouse movement", positive: true });
  }

  if (features.is_high_speed === 1) {
    signals.push({ label: "High request speed", positive: false });
  } else {
    signals.push({ label: "Normal interaction pace", positive: true });
  }

  if (features.ua_risk_score >= 0.8) {
    signals.push({ label: "Suspicious user agent", positive: false });
  } else if (features.ua_risk_score <= 0.1) {
    signals.push({ label: "Recognized browser", positive: true });
  }

  if (features.mouse_events_per_click === 0 && features.click_count === 0) {
    signals.push({ label: "No click interactions", positive: false });
  }

  if (features.js_execution_score >= 0.7) {
    signals.push({ label: "Clean JS execution", positive: true });
  } else {
    signals.push({ label: "JS errors detected", positive: false });
  }

  if (features.referrer_risk >= 0.5) {
    signals.push({ label: "No referrer detected", positive: false });
  } else {
    signals.push({ label: "Valid referrer", positive: true });
  }

  const durationSecs = features.session_duration_secs ?? 0;

  return (
    <div
      style={{
        width: "100%",
        maxWidth: "480px",
        textAlign: "center",
      }}
    >
      {/* Icon */}
      <div
        style={{
          width: "80px",
          height: "80px",
          borderRadius: "50%",
          background: isBot
            ? "rgba(239,68,68,0.12)"
            : "rgba(34,197,94,0.12)",
          border: `2px solid ${accentColor}`,
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: "2.4rem",
          marginBottom: "1.25rem",
        }}
      >
        {isBot ? "\uD83E\uDD16" : "\u2705"}
      </div>

      {/* Header */}
      <h1
        style={{
          fontSize: "2rem",
          fontWeight: 800,
          color: accentColor,
          marginBottom: "0.3rem",
        }}
      >
        {isBot ? "Bot Detected" : "Welcome!"}
      </h1>
      <p
        style={{
          fontSize: "1rem",
          color: "#94a3b8",
          marginBottom: "1.5rem",
        }}
      >
        {isBot
          ? "Access Denied - Automated behavior detected"
          : "Account created successfully"}
      </p>

      {/* Confidence bar */}
      <div
        style={{
          background: "#12121f",
          borderRadius: "12px",
          padding: "1rem 1.25rem",
          border: "1px solid #1e1e36",
          marginBottom: "1.25rem",
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            marginBottom: "0.5rem",
            fontSize: "0.82rem",
          }}
        >
          <span style={{ color: "#94a3b8" }}>Confidence</span>
          <span style={{ color: accentColor, fontWeight: 700 }}>
            {(result.confidence * 100).toFixed(0)}%
          </span>
        </div>
        <div
          style={{
            height: "6px",
            borderRadius: "3px",
            background: "#1e1e36",
            overflow: "hidden",
          }}
        >
          <div
            style={{
              height: "100%",
              width: `${result.confidence * 100}%`,
              borderRadius: "3px",
              background: accentColor,
              transition: "width 0.6s ease",
            }}
          />
        </div>
      </div>

      {/* Signal badges */}
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "0.5rem",
          justifyContent: "center",
          marginBottom: "1.5rem",
        }}
      >
        {signals.map((sig, i) => (
          <SignalBadge key={i} label={sig.label} positive={sig.positive} />
        ))}
      </div>

      {/* Metric cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr 1fr 1fr",
          gap: "0.6rem",
          marginBottom: "1.5rem",
        }}
      >
        <MetricCard
          label="Session"
          value={`${durationSecs.toFixed(1)}s`}
        />
        <MetricCard
          label="Mouse Events"
          value={String(features.mouse_events ?? 0)}
        />
        <MetricCard
          label="Clicks"
          value={String(features.click_count ?? 0)}
        />
        <MetricCard
          label="Heuristic"
          value={(features.heuristic_bot_score ?? 0).toFixed(2)}
        />
      </div>

      {/* Try Again */}
      <button
        onClick={onReset}
        style={{
          padding: "0.7rem 2rem",
          borderRadius: "8px",
          border: `1px solid ${accentColor}40`,
          background: "transparent",
          color: accentColor,
          fontSize: "0.9rem",
          fontWeight: 600,
          cursor: "pointer",
          transition: "background 0.2s",
        }}
        onMouseEnter={(e) =>
          (e.currentTarget.style.background = `${accentColor}10`)
        }
        onMouseLeave={(e) =>
          (e.currentTarget.style.background = "transparent")
        }
      >
        Try Again
      </button>
    </div>
  );
}

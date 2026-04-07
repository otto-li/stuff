import { useState, useRef, useEffect, useCallback } from "react";
import SignUpForm from "./components/SignUpForm";
import BotResult from "./components/BotResult";

export interface SessionData {
  startTime: number;
  mouseEvents: number;
  clickCount: number;
  keyEvents: number;
  interactionTimestamps: number[];
}

export interface DetectResult {
  is_bot: boolean;
  confidence: number;
  features: Record<string, number>;
}

export default function App() {
  const [result, setResult] = useState<DetectResult | null>(null);
  const [loading, setLoading] = useState(false);
  const sessionRef = useRef<SessionData>({
    startTime: Date.now(),
    mouseEvents: 0,
    clickCount: 0,
    keyEvents: 0,
    interactionTimestamps: [],
  });

  /* ---------- behavioral tracking ---------- */
  useEffect(() => {
    const onMouseMove = () => {
      sessionRef.current.mouseEvents++;
    };
    const onClick = () => {
      sessionRef.current.clickCount++;
      sessionRef.current.interactionTimestamps.push(Date.now());
    };
    const onKeyDown = () => {
      sessionRef.current.keyEvents++;
      sessionRef.current.interactionTimestamps.push(Date.now());
    };

    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("click", onClick);
    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("click", onClick);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, []);

  /* ---------- submit handler ---------- */
  const handleSubmit = useCallback(
    async (
      sessionOverrides?: Partial<SessionData>,
      payloadOverrides?: Record<string, unknown>,
    ) => {
      setLoading(true);
      const s = { ...sessionRef.current, ...(sessionOverrides ?? {}) };
      const durationSecs = (Date.now() - s.startTime) / 1000;

      // compute avg time between interactions
      let avgMs = 0;
      if (s.interactionTimestamps.length > 1) {
        const diffs: number[] = [];
        for (let i = 1; i < s.interactionTimestamps.length; i++) {
          diffs.push(s.interactionTimestamps[i] - s.interactionTimestamps[i - 1]);
        }
        avgMs = diffs.reduce((a, b) => a + b, 0) / diffs.length;
      }

      const payload = {
        session_duration_secs: durationSecs,
        num_requests: Math.max(s.interactionTimestamps.length, 1),
        avg_time_between_requests_ms: avgMs,
        page_views: 1,
        click_count: s.clickCount,
        mouse_events: s.mouseEvents,
        form_submissions: 1,
        js_errors: 0,
        user_agent: navigator.userAgent,
        referrer: document.referrer || "",
        ...(payloadOverrides ?? {}),
      };

      try {
        const resp = await fetch("/api/detect", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (!resp.ok) {
          const errText = await resp.text();
          throw new Error(`Server returned ${resp.status}: ${errText}`);
        }
        const data: DetectResult = await resp.json();
        setResult(data);
      } catch (err) {
        console.error("Detection failed:", err);
        alert("Detection request failed. Check the console for details.");
      } finally {
        setLoading(false);
      }
    },
    []
  );

  /* ---------- bot simulation ---------- */
  const simulateBot = useCallback(() => {
    const now = Date.now();
    const botSession: SessionData = {
      startTime: now - 800, // 0.8 seconds ago
      mouseEvents: 0,
      clickCount: 0,
      keyEvents: 2,
      interactionTimestamps: [
        now - 400, now - 350, now - 300, now - 250,
        now - 200, now - 150, now - 100, now - 50,
      ],
    };
    // Override the user agent to a bot-like string so the heuristic
    // picks up the suspicious UA signal alongside zero mouse activity
    // and high-speed interactions.
    handleSubmit(botSession, {
      user_agent: "python-urllib/3.11",
      referrer: "",
    });
  }, [handleSubmit]);

  /* ---------- reset ---------- */
  const reset = useCallback(() => {
    setResult(null);
    sessionRef.current = {
      startTime: Date.now(),
      mouseEvents: 0,
      clickCount: 0,
      keyEvents: 0,
      interactionTimestamps: [],
    };
  }, []);

  /* ---------- render ---------- */
  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: "2rem",
        position: "relative",
        background: "radial-gradient(ellipse at 50% 0%, #1a1a2e 0%, #0a0a0f 70%)",
      }}
    >
      {result ? (
        <BotResult result={result} session={sessionRef.current} onReset={reset} />
      ) : (
        <SignUpForm
          onSubmit={handleSubmit}
          onSimulateBot={simulateBot}
          loading={loading}
        />
      )}
    </div>
  );
}

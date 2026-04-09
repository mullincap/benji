"use client";

import { useRouter } from "next/navigation";
import { useCallback } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

const SUGGESTED_PROMPTS = [
  "Give me a portfolio briefing",
  "How has my strategy performed this month?",
  "What symbols are deployed today?",
];

export default function ChatEmptyState() {
  const router = useRouter();

  const handleChip = useCallback(
    async (prompt: string) => {
      try {
        // Create new conversation
        const resp = await fetch(`${API_BASE}/api/manager/conversations`, {
          method: "POST",
          credentials: "include",
        });
        const data = await resp.json();
        const convId = data.conversation_id;

        // Send the message
        await fetch(`${API_BASE}/api/manager/conversations/${convId}/messages`, {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ content: prompt }),
        });

        // Navigate to the conversation
        router.push(`/manager/chat/${convId}`);
      } catch {
        // ignore
      }
    },
    [router]
  );

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        height: "100%",
        padding: 28,
      }}
    >
      {/* 3M avatar */}
      <div
        style={{
          width: 56,
          height: 56,
          borderRadius: "50%",
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: 18,
          fontWeight: 700,
          color: "var(--t1)",
          marginBottom: 16,
        }}
      >
        3M
      </div>

      <div
        style={{
          fontSize: 18,
          fontWeight: 700,
          color: "var(--t0)",
          marginBottom: 24,
        }}
      >
        Good morning.
      </div>

      <div
        style={{
          display: "flex",
          gap: 10,
          flexWrap: "wrap",
          justifyContent: "center",
        }}
      >
        {SUGGESTED_PROMPTS.map((prompt) => (
          <button
            key={prompt}
            onClick={() => handleChip(prompt)}
            style={{
              background: "var(--bg2)",
              border: "1px solid var(--line)",
              borderRadius: 5,
              padding: "8px 14px",
              fontSize: 10,
              color: "var(--t1)",
              cursor: "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
              transition: "border-color 0.15s ease, color 0.15s ease",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.borderColor = "var(--module-accent)";
              e.currentTarget.style.color = "var(--t0)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.borderColor = "var(--line)";
              e.currentTarget.style.color = "var(--t1)";
            }}
          >
            {prompt}
          </button>
        ))}
      </div>
    </div>
  );
}

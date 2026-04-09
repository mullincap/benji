"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import { useParams, useRouter } from "next/navigation";
import ReactMarkdown from "react-markdown";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

interface Message {
  message_id: string;
  role: "user" | "assistant";
  content: string;
  created_at: string;
}

// ── Markdown components for assistant messages ──────────────────────────────

const mdComponents = {
  p: ({ children }: { children?: React.ReactNode }) => (
    <p style={{ fontSize: 10, color: "var(--t2)", lineHeight: 1.7, margin: "0 0 8px" }}>
      {children}
    </p>
  ),
  strong: ({ children }: { children?: React.ReactNode }) => (
    <strong style={{ color: "var(--t0)", fontWeight: 700 }}>{children}</strong>
  ),
  ul: ({ children }: { children?: React.ReactNode }) => (
    <ul style={{ margin: "6px 0 8px", paddingLeft: 16, display: "flex", flexDirection: "column" as const, gap: 3, listStyle: "disc" }}>
      {children}
    </ul>
  ),
  ol: ({ children }: { children?: React.ReactNode }) => (
    <ol style={{ margin: "6px 0 8px", paddingLeft: 16, display: "flex", flexDirection: "column" as const, gap: 3 }}>
      {children}
    </ol>
  ),
  li: ({ children }: { children?: React.ReactNode }) => (
    <li style={{ fontSize: 10, color: "var(--t2)", lineHeight: 1.6 }}>{children}</li>
  ),
  code: ({ children }: { children?: React.ReactNode }) => (
    <code style={{ background: "var(--bg3)", padding: "1px 4px", borderRadius: 3, fontSize: 10 }}>{children}</code>
  ),
  h1: ({ children }: { children?: React.ReactNode }) => (
    <h1 style={{ fontSize: 11, fontWeight: 700, color: "var(--t0)", margin: "12px 0 4px" }}>{children}</h1>
  ),
  h2: ({ children }: { children?: React.ReactNode }) => (
    <h2 style={{ fontSize: 11, fontWeight: 700, color: "var(--t0)", margin: "12px 0 4px" }}>{children}</h2>
  ),
  h3: ({ children }: { children?: React.ReactNode }) => (
    <h3 style={{ fontSize: 11, fontWeight: 700, color: "var(--t0)", margin: "12px 0 4px" }}>{children}</h3>
  ),
};

export default function ConversationPage() {
  const { conversationId } = useParams<{ conversationId: string }>();
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(true);
  const [sending, setSending] = useState(false);
  const [title, setTitle] = useState<string | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);

  // Signal the layout to refresh its conversation list
  const refreshSidebar = useCallback(() => {
    window.dispatchEvent(new CustomEvent("manager:refresh-conversations"));
  }, []);

  // Load conversation
  useEffect(() => {
    if (!conversationId) return;
    setLoading(true);
    fetch(`${API_BASE}/api/manager/conversations/${conversationId}`, {
      credentials: "include",
    })
      .then((r) => r.json())
      .then((data) => {
        setMessages(data.messages || []);
        setTitle(data.title);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [conversationId]);

  // Auto-scroll
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleSend = useCallback(async () => {
    const text = input.trim();
    if (!text || sending) return;
    setInput("");
    setSending(true);

    // Optimistic user message
    const tempMsg: Message = {
      message_id: `temp-${Date.now()}`,
      role: "user",
      content: text,
      created_at: new Date().toISOString(),
    };
    setMessages((prev) => [...prev, tempMsg]);

    try {
      const resp = await fetch(
        `${API_BASE}/api/manager/conversations/${conversationId}/messages`,
        {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ content: text }),
        }
      );
      const data = await resp.json();
      const asstMsg: Message = {
        message_id: data.message_id,
        role: "assistant",
        content: data.content,
        created_at: data.created_at,
      };
      setMessages((prev) => [...prev, asstMsg]);
      refreshSidebar();
    } catch {
      const errMsg: Message = {
        message_id: `err-${Date.now()}`,
        role: "assistant",
        content: "[Error: Failed to send message]",
        created_at: new Date().toISOString(),
      };
      setMessages((prev) => [...prev, errMsg]);
    } finally {
      setSending(false);
    }
  }, [input, sending, conversationId, refreshSidebar]);

  // ── Parse action blocks from assistant messages ───────────────────────────

  function renderMessage(msg: Message) {
    if (msg.role === "user") {
      return (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "flex-end",
          }}
        >
          <div
            style={{
              fontSize: 8,
              fontWeight: 700,
              letterSpacing: "0.08em",
              color: "var(--t3)",
              textTransform: "uppercase",
              marginBottom: 2,
              textAlign: "right",
            }}
          >
            You
          </div>
          <div
            style={{
              background: "var(--bg2)",
              border: "0.5px solid var(--line)",
              borderRadius: 6,
              padding: "8px 12px",
              maxWidth: "60%",
            }}
          >
            <div style={{ fontSize: 10, color: "var(--t0)", lineHeight: 1.6, whiteSpace: "pre-wrap" }}>
              {msg.content}
            </div>
          </div>
        </div>
      );
    }

    // Assistant message — check for trailing JSON action block
    const actionMatch = msg.content.match(
      /\n?\{"action":\s*true[^}]*\}\s*$/
    );
    let displayContent = msg.content;
    let actionBlock: { action: boolean; type: string; summary: string } | null = null;

    if (actionMatch) {
      try {
        actionBlock = JSON.parse(actionMatch[0].trim());
        displayContent = msg.content.slice(0, actionMatch.index).trimEnd();
      } catch {
        // Not valid JSON, show as-is
      }
    }

    const actionLabel = actionBlock?.type
      ? `Proposed Action \u00b7 ${actionBlock.type.replace(/_/g, " ").toUpperCase()}`
      : "Proposed Action";

    return (
      <div style={{ display: "flex", flexDirection: "column" }}>
        <div
          style={{
            fontSize: 8,
            fontWeight: 700,
            letterSpacing: "0.08em",
            color: "var(--green)",
            textTransform: "uppercase",
            marginBottom: 2,
          }}
        >
          3M
        </div>
        <div
          style={{
            borderLeft: "2px solid var(--line)",
            paddingLeft: 14,
          }}
        >
          <ReactMarkdown components={mdComponents}>{displayContent}</ReactMarkdown>
        </div>
        {actionBlock && (
          <div
            style={{
              marginTop: 10,
              marginLeft: 16,
              background: "var(--bg1)",
              border: "0.5px solid var(--line)",
              borderRadius: 6,
              padding: "12px 14px",
            }}
          >
            <div
              style={{
                fontSize: 8,
                fontWeight: 700,
                letterSpacing: "0.08em",
                color: "var(--t3)",
                textTransform: "uppercase",
                marginBottom: 6,
              }}
            >
              {actionLabel}
            </div>
            <div
              style={{
                fontSize: 10,
                color: "var(--t0)",
                lineHeight: 1.5,
                marginBottom: 10,
              }}
            >
              {actionBlock.summary}
            </div>
            <div style={{ display: "flex", gap: 8 }}>
              <button
                onClick={() => {
                  setInput("Confirmed \u2014 please proceed");
                  setTimeout(() => {
                    const btn = document.querySelector("[data-send-btn]") as HTMLButtonElement;
                    btn?.click();
                  }, 50);
                }}
                style={{
                  background: "var(--bg1)",
                  border: "1px solid rgba(0, 200, 150, 0.4)",
                  color: "var(--green)",
                  fontSize: 9,
                  fontWeight: 700,
                  padding: "5px 14px",
                  borderRadius: 4,
                  cursor: "pointer",
                  fontFamily: "var(--font-space-mono), Space Mono, monospace",
                }}
              >
                Confirm
              </button>
              <button
                onClick={() => {
                  setInput("Cancel \u2014 do not proceed");
                  setTimeout(() => {
                    const btn = document.querySelector("[data-send-btn]") as HTMLButtonElement;
                    btn?.click();
                  }, 50);
                }}
                style={{
                  background: "transparent",
                  border: "1px solid var(--line)",
                  color: "var(--t3)",
                  fontSize: 9,
                  fontWeight: 700,
                  padding: "5px 14px",
                  borderRadius: 4,
                  cursor: "pointer",
                  fontFamily: "var(--font-space-mono), Space Mono, monospace",
                }}
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>
    );
  }

  // ── Loading state ─────────────────────────────────────────────────────────

  if (loading) {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: "100%",
          fontSize: 9,
          color: "var(--t3)",
          textTransform: "uppercase",
          letterSpacing: "0.12em",
        }}
      >
        Loading conversation...
      </div>
    );
  }

  // ── Render ────────────────────────────────────────────────────────────────

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      {/* Header */}
      <div
        style={{
          padding: "12px 20px",
          borderBottom: "0.5px solid var(--line)",
          fontSize: 10,
          fontWeight: 700,
          color: "var(--t1)",
        }}
      >
        {title || "New conversation"}
      </div>

      {/* Messages */}
      <div style={{ flex: 1, overflowY: "auto", padding: "20px 24px" }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
          {messages.map((msg) => (
            <div key={msg.message_id}>
              {renderMessage(msg)}
            </div>
          ))}

          {sending && (
            <div style={{ display: "flex", flexDirection: "column" }}>
              <div
                style={{
                  fontSize: 8,
                  fontWeight: 700,
                  letterSpacing: "0.08em",
                  color: "var(--green)",
                  textTransform: "uppercase",
                  marginBottom: 2,
                }}
              >
                3M
              </div>
              <div
                style={{
                  borderLeft: "2px solid var(--line)",
                  paddingLeft: 14,
                }}
              >
                <span
                  style={{
                    fontSize: 10,
                    color: "var(--t3)",
                    animation: "pulse-dot 1.5s ease-in-out infinite",
                  }}
                >
                  thinking...
                </span>
              </div>
            </div>
          )}
        </div>

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div
        style={{
          padding: "12px 20px",
          borderTop: "0.5px solid var(--line)",
          display: "flex",
          gap: 8,
        }}
      >
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              handleSend();
            }
          }}
          placeholder="Message 3M..."
          style={{
            flex: 1,
            background: "var(--bg3)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "8px 12px",
            fontSize: 10,
            color: "var(--t0)",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            outline: "none",
          }}
          disabled={sending}
        />
        <button
          data-send-btn
          onClick={handleSend}
          disabled={sending || !input.trim()}
          style={{
            background:
              sending || !input.trim() ? "var(--bg3)" : "var(--module-accent)",
            border: "none",
            borderRadius: 5,
            padding: "8px 16px",
            fontSize: 10,
            fontWeight: 700,
            color:
              sending || !input.trim() ? "var(--t3)" : "var(--bg0)",
            cursor: sending || !input.trim() ? "not-allowed" : "pointer",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}
        >
          Send
        </button>
      </div>
    </div>
  );
}

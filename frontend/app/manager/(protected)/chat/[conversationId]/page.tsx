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
  }, [input, sending, conversationId]);

  // Parse action blocks from assistant messages
  function renderMessage(msg: Message) {
    if (msg.role !== "assistant") {
      return (
        <div style={{ fontSize: 10, color: "var(--t0)", whiteSpace: "pre-wrap", lineHeight: 1.6 }}>
          {msg.content}
        </div>
      );
    }

    // Check for trailing JSON action block
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

    return (
      <>
        <div className="manager-md" style={{ fontSize: 10, color: "var(--t0)", lineHeight: 1.6 }}>
          <ReactMarkdown>{displayContent}</ReactMarkdown>
        </div>
        {actionBlock && (
          <div
            style={{
              marginTop: 10,
              background: "var(--bg3)",
              border: "1px solid var(--line2)",
              borderRadius: 5,
              padding: 12,
            }}
          >
            <div
              style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.12em",
                color: "var(--amber)",
                textTransform: "uppercase",
                marginBottom: 6,
              }}
            >
              Proposed Action
            </div>
            <div style={{ fontSize: 10, color: "var(--t1)", marginBottom: 10 }}>
              {actionBlock.summary}
            </div>
            <div style={{ display: "flex", gap: 8 }}>
              <button
                onClick={() => {
                  setInput("Confirmed \u2014 please proceed");
                  // Auto-send confirmation
                  setTimeout(() => {
                    const btn = document.querySelector("[data-send-btn]") as HTMLButtonElement;
                    btn?.click();
                  }, 50);
                }}
                style={{
                  background: "var(--green-dim)",
                  border: "1px solid var(--green)",
                  color: "var(--green)",
                  fontSize: 9,
                  fontWeight: 700,
                  padding: "4px 12px",
                  borderRadius: 3,
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
                  background: "var(--red-dim)",
                  border: "1px solid var(--red)",
                  color: "var(--red)",
                  fontSize: 9,
                  fontWeight: 700,
                  padding: "4px 12px",
                  borderRadius: 3,
                  cursor: "pointer",
                  fontFamily: "var(--font-space-mono), Space Mono, monospace",
                }}
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </>
    );
  }

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
          borderBottom: "1px solid var(--line)",
          fontSize: 10,
          fontWeight: 700,
          color: "var(--t1)",
        }}
      >
        {title || "New conversation"}
      </div>

      {/* Messages */}
      <div style={{ flex: 1, overflowY: "auto", padding: "16px 20px" }}>
        {messages.map((msg) => (
          <div
            key={msg.message_id}
            style={{
              marginBottom: 16,
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div
              style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.1em",
                color: msg.role === "user" ? "var(--t2)" : "var(--module-accent)",
                textTransform: "uppercase",
                marginBottom: 4,
              }}
            >
              {msg.role === "user" ? "You" : "3M"}
            </div>
            {renderMessage(msg)}
          </div>
        ))}

        {sending && (
          <div style={{ marginBottom: 16 }}>
            <div
              style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.1em",
                color: "var(--module-accent)",
                textTransform: "uppercase",
                marginBottom: 4,
              }}
            >
              3M
            </div>
            <div
              style={{
                fontSize: 10,
                color: "var(--t2)",
                animation: "pulse-dot 1.5s ease-in-out infinite",
              }}
            >
              thinking...
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div
        style={{
          padding: "12px 20px",
          borderTop: "1px solid var(--line)",
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

"use client";

/**
 * frontend/app/manager/(protected)/layout.tsx
 * ============================================
 * Protected layout for /manager/*. Auth-checked, sidebar with conversations +
 * allocations, Topbar.
 *
 * Sidebar structure:
 *   - OVERVIEW nav item
 *   - Divider
 *   - CHAT nav item
 *   - "CONVERSATIONS" label with + NEW button
 *   - Conversation list (title + relative time, active has green left border)
 *   - Bottom: "ALLOCATIONS" summary
 */

import { useEffect, useState, useCallback } from "react";
import { usePathname, useRouter } from "next/navigation";
import Topbar from "../../components/Topbar";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

type AuthState = "loading" | "authed" | "unauthed";

interface Conversation {
  conversation_id: string;
  title: string | null;
  updated_at: string;
}

interface Allocation {
  allocation_id: string;
  exchange: string;
  strategy_display_name: string;
  capital_usd: number;
}

interface SnapshotData {
  total_live_equity_usd: number | null;
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function relativeTime(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max) + "..." : s;
}

// ─── Nav item ───────────────────────────────────────────────────────────────

function NavItem({
  label,
  href,
  active,
}: {
  label: string;
  href: string;
  active: boolean;
}) {
  const router = useRouter();
  return (
    <button
      onClick={() => router.push(href)}
      style={{
        display: "block",
        width: "100%",
        background: "transparent",
        border: "none",
        borderLeft: `2px solid ${active ? "var(--module-accent)" : "transparent"}`,
        color: active ? "var(--t0)" : "var(--t2)",
        fontSize: 10,
        fontWeight: active ? 700 : 400,
        letterSpacing: "0.08em",
        textTransform: "uppercase" as const,
        textAlign: "left" as const,
        padding: "8px 14px 8px 16px",
        cursor: "pointer",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        transition: "color 0.15s ease",
      }}
      onMouseEnter={(e) => {
        if (!active) e.currentTarget.style.color = "var(--t1)";
      }}
      onMouseLeave={(e) => {
        if (!active) e.currentTarget.style.color = "var(--t2)";
      }}
    >
      {label}
    </button>
  );
}

// ─── Manager sidebar ────────────────────────────────────────────────────────

function ManagerSidebar({
  conversations,
  allocations,
  totalAum,
  onNewConversation,
  onDeleteConversation,
}: {
  conversations: Conversation[];
  allocations: Allocation[];
  totalAum: number;
  onNewConversation: () => void;
  onDeleteConversation: (id: string) => void;
}) {
  const pathname = usePathname();
  const router = useRouter();

  // Active conversation ID from URL
  const chatMatch = pathname.match(/\/manager\/chat\/(.+)/);
  const activeConvId = chatMatch?.[1] ?? null;

  return (
    <div
      style={{
        width: 240,
        flexShrink: 0,
        background: "var(--bg0)",
        borderRight: "1px solid var(--line)",
        display: "flex",
        flexDirection: "column",
        paddingTop: 14,
        overflow: "hidden",
      }}
    >
      {/* Module label */}
      <div
        style={{
          padding: "0 16px 10px",
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.12em",
          color: "var(--t3)",
          textTransform: "uppercase",
        }}
      >
        Manager
      </div>

      <NavItem
        label="Overview"
        href="/manager/overview"
        active={pathname === "/manager/overview"}
      />

      {/* Divider */}
      <div
        style={{
          height: 1,
          background: "var(--line)",
          margin: "8px 16px",
        }}
      />

      <NavItem
        label="Chat"
        href="/manager/chat"
        active={pathname === "/manager/chat" && !activeConvId}
      />

      {/* Conversations header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "12px 16px 6px",
        }}
      >
        <span
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.12em",
            color: "var(--t3)",
            textTransform: "uppercase",
          }}
        >
          Conversations
        </span>
        <button
          onClick={onNewConversation}
          style={{
            background: "transparent",
            border: "1px solid var(--green)",
            color: "var(--green)",
            fontSize: 9,
            fontWeight: 700,
            padding: "2px 8px",
            borderRadius: 3,
            cursor: "pointer",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            letterSpacing: "0.06em",
            textTransform: "uppercase",
          }}
        >
          + New
        </button>
      </div>

      {/* Conversation list */}
      <div style={{ flex: 1, overflowY: "auto", padding: "4px 0" }}>
        {conversations.map((c) => {
          const isActive = activeConvId === c.conversation_id;
          return (
            <div
              key={c.conversation_id}
              style={{
                display: "flex",
                alignItems: "center",
                background: isActive ? "var(--bg2)" : "transparent",
                borderLeft: `2px solid ${isActive ? "var(--green)" : "transparent"}`,
                transition: "background 0.15s ease",
              }}
              onMouseEnter={(e) => {
                if (!isActive) e.currentTarget.style.background = "var(--bg1)";
                const del = e.currentTarget.querySelector("[data-del]") as HTMLElement;
                if (del) del.style.opacity = "1";
              }}
              onMouseLeave={(e) => {
                if (!isActive) e.currentTarget.style.background = "transparent";
                const del = e.currentTarget.querySelector("[data-del]") as HTMLElement;
                if (del) del.style.opacity = "0";
              }}
            >
              <button
                onClick={() => router.push(`/manager/chat/${c.conversation_id}`)}
                style={{
                  flex: 1,
                  background: "transparent",
                  border: "none",
                  padding: "6px 0 6px 16px",
                  cursor: "pointer",
                  textAlign: "left",
                  fontFamily: "var(--font-space-mono), Space Mono, monospace",
                  minWidth: 0,
                }}
              >
                <div
                  style={{
                    fontSize: 10,
                    color: isActive ? "var(--t0)" : "var(--t1)",
                    fontWeight: isActive ? 700 : 400,
                    whiteSpace: "nowrap",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    maxWidth: 160,
                  }}
                >
                  {c.title || "Untitled"}
                </div>
                <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 2 }}>
                  {relativeTime(c.updated_at)}
                </div>
              </button>
              <button
                data-del
                onClick={(e) => {
                  e.stopPropagation();
                  onDeleteConversation(c.conversation_id);
                }}
                style={{
                  background: "transparent",
                  border: "none",
                  color: "var(--t3)",
                  fontSize: 11,
                  cursor: "pointer",
                  padding: "4px 10px",
                  opacity: 0,
                  transition: "opacity 0.15s ease, color 0.15s ease",
                  fontFamily: "var(--font-space-mono), Space Mono, monospace",
                  flexShrink: 0,
                }}
                onMouseEnter={(e) => { e.currentTarget.style.color = "var(--red)"; }}
                onMouseLeave={(e) => { e.currentTarget.style.color = "var(--t3)"; }}
                title="Delete conversation"
              >
                x
              </button>
            </div>
          );
        })}
        {conversations.length === 0 && (
          <div
            style={{
              padding: "8px 16px",
              fontSize: 9,
              color: "var(--t3)",
            }}
          >
            No conversations yet
          </div>
        )}
      </div>

      {/* Allocations footer */}
      <div
        style={{
          borderTop: "1px solid var(--line)",
          padding: "10px 16px",
        }}
      >
        <div
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.12em",
            color: "var(--t3)",
            textTransform: "uppercase",
            marginBottom: 6,
          }}
        >
          Allocations
        </div>
        {allocations.map((a) => (
          <div
            key={a.allocation_id}
            style={{
              fontSize: 9,
              color: "var(--t2)",
              padding: "2px 0",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
            }}
          >
            {a.exchange} · ${(a.capital_usd || 0).toLocaleString()} ·{" "}
            {truncate(a.strategy_display_name, 18)}
          </div>
        ))}
        {allocations.length === 0 && (
          <div style={{ fontSize: 9, color: "var(--t3)" }}>
            No active allocations
          </div>
        )}
        <div
          style={{
            fontSize: 9,
            fontWeight: 700,
            color: "var(--t1)",
            marginTop: 6,
            borderTop: "1px solid var(--line)",
            paddingTop: 6,
          }}
        >
          Total AUM: ${totalAum.toLocaleString()}
        </div>
      </div>
    </div>
  );
}

// ─── Layout ─────────────────────────────────────────────────────────────────

export default function ManagerProtectedLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const [authState, setAuthState] = useState<AuthState>("loading");
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [allocations, setAllocations] = useState<Allocation[]>([]);
  const [totalAum, setTotalAum] = useState(0);

  // Auth check
  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/api/admin/whoami`, { credentials: "include" })
      .then((r) => r.json())
      .then((data) => {
        if (cancelled) return;
        if (data?.authenticated) {
          setAuthState("authed");
        } else {
          setAuthState("unauthed");
          router.replace("/compiler/login");
        }
      })
      .catch(() => {
        if (cancelled) return;
        setAuthState("unauthed");
        router.replace("/compiler/login");
      });
    return () => {
      cancelled = true;
    };
  }, [router]);

  // Load sidebar data on auth + pathname changes
  useEffect(() => {
    if (authState !== "authed") return;

    fetch(`${API_BASE}/api/manager/conversations`, { credentials: "include" })
      .then((r) => r.json())
      .then((data) => setConversations(data.conversations || []))
      .catch(() => {});

    fetch(`${API_BASE}/api/manager/overview`, { credentials: "include" })
      .then((r) => r.json())
      .then((data) => {
        setAllocations(data.allocations || []);
        setTotalAum(data.total_aum || 0);
      })
      .catch(() => {});
  }, [authState, pathname]);

  const refreshConversations = useCallback(() => {
    fetch(`${API_BASE}/api/manager/conversations`, { credentials: "include" })
      .then((r) => r.json())
      .then((data) => setConversations(data.conversations || []))
      .catch(() => {});
  }, []);

  // Listen for refresh events from chat pages
  useEffect(() => {
    const handler = () => refreshConversations();
    window.addEventListener("manager:refresh-conversations", handler);
    return () => window.removeEventListener("manager:refresh-conversations", handler);
  }, [refreshConversations]);

  const handleDeleteConversation = useCallback(async (id: string) => {
    try {
      await fetch(`${API_BASE}/api/manager/conversations/${id}`, {
        method: "DELETE",
        credentials: "include",
      });
      refreshConversations();
      // If we just deleted the active conversation, go to chat empty state
      if (window.location.pathname.includes(id)) {
        router.push("/manager/chat");
      }
    } catch {
      // ignore
    }
  }, [refreshConversations, router]);

  const handleNewConversation = useCallback(async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/manager/conversations`, {
        method: "POST",
        credentials: "include",
      });
      const data = await resp.json();
      refreshConversations();
      router.push(`/manager/chat/${data.conversation_id}`);
    } catch {
      // ignore
    }
  }, [refreshConversations, router]);

  if (authState === "loading") {
    return (
      <div
        style={{
          minHeight: "100vh",
          background: "var(--bg0)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: 9,
          color: "var(--t3)",
          textTransform: "uppercase",
          letterSpacing: "0.12em",
        }}
      >
        Verifying session...
      </div>
    );
  }

  if (authState === "unauthed") return null;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Topbar />
      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        <ManagerSidebar
          conversations={conversations}
          allocations={allocations}
          totalAum={totalAum}
          onNewConversation={handleNewConversation}
          onDeleteConversation={handleDeleteConversation}
        />
        <div style={{ flex: 1, overflow: "auto" }}>
          {typeof children === "object" && children !== null
            ? // Pass refreshConversations to children via context-like prop
              (() => {
                // We render children directly — chat page will fetch its own data
                return children;
              })()
            : children}
        </div>
      </div>
    </div>
  );
}

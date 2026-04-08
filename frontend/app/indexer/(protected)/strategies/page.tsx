"use client";

/**
 * frontend/app/indexer/(protected)/strategies/page.tsx
 * ====================================================
 * Indexer Strategies page — read-only view of audit.strategies and their
 * versions. No edit affordances this round.
 *
 * Data source:
 *   GET /api/indexer/strategies
 *
 * Response is a flat array of strategies, each with a `versions` array
 * (already sorted DESC by created_at server-side via the LATERAL/jsonb_agg
 * subquery). Each version carries a `config_excerpt` — the first 200 chars
 * of the JSONB config as a string. Full config is intentionally not
 * exposed in this view to keep the payload small.
 *
 * Single fetch on mount, no polling — strategies change manually and
 * infrequently.
 */

import { useEffect, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Response shapes ─────────────────────────────────────────────────────────

type StrategyVersion = {
  strategy_version_id: string;
  version_label: string | null;
  is_active: boolean;
  published_at: string | null;
  created_at: string | null;
  config_excerpt: string | null;
};

type Strategy = {
  strategy_id: number;
  name: string | null;
  display_name: string | null;
  description: string | null;
  filter_mode: string | null;
  is_published: boolean;
  capital_cap_usd: number | null;
  created_at: string | null;
  updated_at: string | null;
  versions: StrategyVersion[];
};

type StrategiesResponse = {
  strategies_returned: number;
  strategies: Strategy[];
};

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; strategies: Strategy[] };

// ─── Helpers ─────────────────────────────────────────────────────────────────

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  // Render as YYYY-MM-DD; the API returns full ISO timestamps
  return iso.slice(0, 10);
}

function formatUsd(n: number | null): string {
  if (n === null) return "—";
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}k`;
  return `$${n.toLocaleString("en-US")}`;
}

// ─── Subcomponents ───────────────────────────────────────────────────────────

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
      color: "var(--t3)", textTransform: "uppercase",
      marginBottom: 8,
    }}>
      {children}
    </div>
  );
}

function PublishedBadge({ published }: { published: boolean }) {
  const color = published ? "var(--green)" : "var(--t2)";
  const bg = published ? "var(--green-dim)" : "var(--bg3)";
  return (
    <span style={{
      display: "inline-block",
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color,
      background: bg,
      border: `1px solid ${color}`,
      borderRadius: 3,
      padding: "2px 6px",
    }}>
      {published ? "Published" : "Draft"}
    </span>
  );
}

function ActiveBadge({ active }: { active: boolean }) {
  if (!active) return null;
  return (
    <span style={{
      display: "inline-block",
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color: "var(--green)",
      background: "var(--green-dim)",
      border: "1px solid var(--green)",
      borderRadius: 3,
      padding: "2px 6px",
    }}>
      Active
    </span>
  );
}

function MetaField({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      <span style={{
        fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
        color: "var(--t3)", textTransform: "uppercase",
      }}>
        {label}
      </span>
      <span style={{ fontSize: 10, color: "var(--t1)" }}>{value}</span>
    </div>
  );
}

function VersionRow({ version }: { version: StrategyVersion }) {
  const label = version.version_label ?? version.strategy_version_id.slice(0, 8);
  return (
    <div style={{
      borderTop: "1px solid var(--line)",
      padding: "12px 16px",
      display: "flex",
      flexDirection: "column",
      gap: 8,
    }}>
      <div style={{
        display: "flex",
        alignItems: "center",
        gap: 10,
        flexWrap: "wrap",
      }}>
        <span style={{
          fontSize: 10,
          fontWeight: 700,
          color: "var(--t0)",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}>
          {label}
        </span>
        <ActiveBadge active={version.is_active} />
        <span style={{ fontSize: 9, color: "var(--t3)" }}>
          published {formatDate(version.published_at)} · created {formatDate(version.created_at)}
        </span>
      </div>
      {version.config_excerpt && (
        <pre
          title={version.config_excerpt}
          style={{
            margin: 0,
            padding: "8px 10px",
            background: "var(--bg3)",
            border: "1px solid var(--line)",
            borderRadius: 4,
            fontSize: 9,
            lineHeight: 1.5,
            color: "var(--t2)",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            overflow: "hidden",
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
          }}
        >
          {version.config_excerpt}
        </pre>
      )}
    </div>
  );
}

function StrategyCard({ strategy }: { strategy: Strategy }) {
  const title = strategy.display_name || strategy.name || `strategy #${strategy.strategy_id}`;
  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      overflow: "hidden",
      marginBottom: 16,
    }}>
      <div style={{ padding: "16px 18px" }}>
        <div style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          marginBottom: 6,
          flexWrap: "wrap",
        }}>
          <h2 style={{
            fontSize: 14,
            fontWeight: 700,
            color: "var(--t0)",
            margin: 0,
            letterSpacing: "-0.01em",
          }}>
            {title}
          </h2>
          <PublishedBadge published={strategy.is_published} />
          <span style={{
            fontSize: 9,
            color: "var(--t3)",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}>
            #{strategy.strategy_id}
          </span>
        </div>
        {strategy.description && (
          <div style={{
            fontSize: 10,
            color: "var(--t1)",
            lineHeight: 1.6,
            marginBottom: 14,
          }}>
            {strategy.description}
          </div>
        )}
        <div style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: 12,
          marginBottom: 4,
        }}>
          <MetaField label="Filter Mode" value={strategy.filter_mode ?? "—"} />
          <MetaField label="Capital Cap" value={formatUsd(strategy.capital_cap_usd)} />
          <MetaField label="Created" value={formatDate(strategy.created_at)} />
          <MetaField label="Updated" value={formatDate(strategy.updated_at)} />
        </div>
      </div>

      <div style={{
        background: "var(--bg1)",
        padding: "10px 18px 4px",
      }}>
        <SectionLabel>
          Versions · {strategy.versions.length}
        </SectionLabel>
      </div>
      {strategy.versions.length === 0 ? (
        <div style={{
          padding: "12px 18px 18px",
          fontSize: 10,
          color: "var(--t2)",
        }}>
          No versions published for this strategy.
        </div>
      ) : (
        strategy.versions.map((v) => (
          <VersionRow key={v.strategy_version_id} version={v} />
        ))
      )}
    </div>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

export default function IndexerStrategiesPage() {
  const [state, setState] = useState<LoadState>({ kind: "loading" });

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const res = await fetch(`${API_BASE}/api/indexer/strategies`, {
          credentials: "include",
        });
        if (cancelled) return;
        if (res.status === 401) {
          setState({ kind: "error", message: "Session expired. Please log in again." });
          return;
        }
        if (!res.ok) {
          setState({ kind: "error", message: `Strategies endpoint returned ${res.status}` });
          return;
        }
        const data = (await res.json()) as StrategiesResponse;
        if (cancelled) return;
        setState({ kind: "ready", strategies: data.strategies });
      } catch (err) {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : String(err);
        setState({ kind: "error", message: `Network error: ${message}` });
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <SectionLabel>Indexer · Strategies</SectionLabel>
        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 8,
          letterSpacing: "-0.01em",
        }}>
          Strategies
        </h1>
        <div style={{
          fontSize: 10, color: "var(--t2)",
          marginBottom: 24,
        }}>
          Read-only view of <code>audit.strategies</code> and their versions
        </div>

        {state.kind === "loading" && (
          <div style={{
            fontSize: 9, color: "var(--t3)",
            textTransform: "uppercase", letterSpacing: "0.12em",
            padding: "40px 0",
          }}>
            Loading strategies…
          </div>
        )}

        {state.kind === "error" && (
          <div style={{
            background: "var(--red-dim)",
            border: "1px solid var(--red)",
            borderRadius: 6,
            padding: "14px 18px",
            fontSize: 10,
            color: "var(--red)",
          }}>
            {state.message}
          </div>
        )}

        {state.kind === "ready" && state.strategies.length === 0 && (
          <div style={{
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 6,
            padding: "20px 24px",
            fontSize: 10,
            color: "var(--t2)",
          }}>
            No strategies found in <code>audit.strategies</code>.
          </div>
        )}

        {state.kind === "ready" && state.strategies.length > 0 && (
          <>
            {state.strategies.map((s) => (
              <StrategyCard key={s.strategy_id} strategy={s} />
            ))}
          </>
        )}
      </div>
    </div>
  );
}

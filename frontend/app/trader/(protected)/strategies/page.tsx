"use client";

import { useCallback, useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useTrader, STRATEGY_CATALOG, CAPACITY_DATA, StrategyType, StrategyCatalogEntry, fmt, RISK_COLOR, RISK_DIM } from "../../context";
import { allocatorApi } from "../../api";

// TODO: extract a shared useIsAdmin() hook once >2 pages need it (simulator,
// strategies, manager all duplicate this whoami probe).
const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

function fmtAbbrev(n: number): string {
  if (n >= 1000000) return `$${(n / 1000000).toFixed(1)}m`;
  if (n >= 1000) return `$${Math.round(n / 1000)}k`;
  return `$${n}`;
}

function capacityColor(remaining: number, capacity: number): string {
  const pct = remaining / capacity;
  if (pct > 0.5) return "var(--green)";
  if (pct >= 0.2) return "var(--amber)";
  return "var(--red)";
}

function CapacityRow({ type, dominant }: { type: string; dominant: boolean }) {
  const cap = CAPACITY_DATA[type] ?? { allocators: 0, deployed: 0, capacity: 1000000 };
  const remaining = cap.capacity - cap.deployed;
  const remainingPct = remaining / cap.capacity;
  const color = capacityColor(remaining, cap.capacity);
  const isLow = remainingPct < 0.2;

  return (
    <div style={{ marginTop: 10 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 0, fontSize: 9 }}>
        <span style={{ color: dominant ? "var(--t2)" : "var(--t3)", transition: "color 0.15s ease" }}>{cap.allocators} allocators</span>
        <span style={{ color: "var(--t3)", margin: "0 5px" }}>&middot;</span>
        <span style={{ color: dominant ? "var(--t2)" : "var(--t3)", transition: "color 0.15s ease" }}>{fmtAbbrev(cap.deployed)} deployed</span>
        <span style={{ color: "var(--t3)", margin: "0 5px" }}>&middot;</span>
        <span style={{ color, fontWeight: 700 }}>{isLow ? `Only ${fmtAbbrev(remaining)} left` : `${fmtAbbrev(remaining)} max limit`}</span>
      </div>
      <div style={{ height: 3, background: "var(--bg3)", borderRadius: 2, marginTop: 6, overflow: "hidden" }}>
        <div style={{ height: "100%", width: `${(cap.deployed / cap.capacity) * 100}%`, background: "#6a6a6a", borderRadius: 2 }} />
      </div>
    </div>
  );
}

// ─── View toggle icons ───────────────────────────────────────────────────────

function ListIcon({ color }: { color: string }) {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <rect x="1" y="2" width="12" height="1.5" rx="0.5" fill={color} />
      <rect x="1" y="6" width="12" height="1.5" rx="0.5" fill={color} />
      <rect x="1" y="10" width="12" height="1.5" rx="0.5" fill={color} />
    </svg>
  );
}

function GridIcon({ color }: { color: string }) {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <rect x="1" y="1" width="5" height="5" rx="1" fill={color} />
      <rect x="8" y="1" width="5" height="5" rx="1" fill={color} />
      <rect x="1" y="8" width="5" height="5" rx="1" fill={color} />
      <rect x="8" y="8" width="5" height="5" rx="1" fill={color} />
    </svg>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

type ToggleError = { strategyId: number; message: string } | null;

function AdminToggleButton({
  isPublished, busy, onClick,
}: {
  isPublished: boolean;
  busy: boolean;
  onClick: (e: React.MouseEvent) => void;
}) {
  const color = isPublished ? "var(--amber)" : "var(--green)";
  const bg = isPublished ? "var(--amber-dim)" : "var(--green-dim)";
  return (
    <button
      type="button"
      disabled={busy}
      onClick={e => { e.stopPropagation(); onClick(e); }}
      style={{
        position: "absolute", top: 8, right: 8,
        background: bg, border: `1px solid ${color}`, color,
        borderRadius: 3, padding: "3px 8px",
        fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
        textTransform: "uppercase",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        cursor: busy ? "not-allowed" : "pointer",
        opacity: busy ? 0.5 : 1,
        zIndex: 2,
      }}
    >
      {isPublished ? "Retire" : "Publish"}
    </button>
  );
}

function RetiredPill() {
  return (
    <span style={{
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
      textTransform: "uppercase",
      color: "var(--amber)", background: "var(--amber-dim)",
      border: "1px solid var(--amber)",
      borderRadius: 3, padding: "3px 8px",
    }}>
      Retired
    </span>
  );
}

function ConfirmRetireModal({
  displayName, submitting, onCancel, onConfirm,
}: {
  displayName: string;
  submitting: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <div
      role="dialog"
      aria-modal="true"
      onClick={() => { if (!submitting) onCancel(); }}
      style={{
        position: "fixed", inset: 0,
        background: "rgba(0,0,0,0.7)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          background: "var(--bg2)",
          border: "1px solid var(--line2)",
          borderRadius: 6, padding: "20px 24px",
          width: 480, maxWidth: "92vw",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase",
          marginBottom: 10,
        }}>
          Retire this strategy?
        </div>
        <div style={{ fontSize: 11, color: "var(--t1)", lineHeight: 1.6, marginBottom: 16 }}>
          Retire <span style={{ color: "var(--t0)" }}>&lsquo;{displayName}&rsquo;</span>?
          Allocator users with active positions will keep them, but no new allocations can be made.
        </div>
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button
            type="button"
            disabled={submitting}
            onClick={onCancel}
            style={{
              background: "transparent",
              border: "1px solid var(--line2)",
              borderRadius: 4, padding: "8px 16px",
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              textTransform: "uppercase",
              color: "var(--t2)",
              cursor: submitting ? "not-allowed" : "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={submitting}
            onClick={onConfirm}
            style={{
              background: "var(--amber-dim)",
              border: "1px solid var(--amber)",
              borderRadius: 4, padding: "8px 16px",
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              textTransform: "uppercase",
              color: "var(--amber)",
              cursor: submitting ? "not-allowed" : "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            Retire
          </button>
        </div>
      </div>
    </div>
  );
}

export default function StrategiesPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { instances, loading, includeRetired, setIncludeRetired, refresh } = useTrader();
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const [view, setView] = useState<"list" | "grid">("list");

  const [isAdmin, setIsAdmin] = useState(false);
  const [publishOverrides, setPublishOverrides] = useState<Record<number, boolean>>({});
  const [inflight, setInflight] = useState<Record<number, boolean>>({});
  const [toggleError, setToggleError] = useState<ToggleError>(null);
  const [confirmTarget, setConfirmTarget] = useState<
    { strategyId: number; displayName: string } | null
  >(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/admin/whoami`, { credentials: "include" });
        if (!cancelled) setIsAdmin(res.ok);
      } catch {
        if (!cancelled) setIsAdmin(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // URL ?show_retired=1 only honored for admins. Non-admins never see retired strategies
  // (the server also enforces this via the include_retired admin gate).
  const urlShowRetired = searchParams?.get("show_retired") === "1";
  const showRetired = isAdmin && urlShowRetired;

  useEffect(() => {
    if (showRetired !== includeRetired) {
      setIncludeRetired(showRetired);
    }
  }, [showRetired, includeRetired, setIncludeRetired]);

  const updateShowRetiredParam = useCallback((next: boolean) => {
    const params = new URLSearchParams(searchParams?.toString() ?? "");
    if (next) params.set("show_retired", "1");
    else params.delete("show_retired");
    const qs = params.toString();
    router.replace(qs ? `/trader/strategies?${qs}` : "/trader/strategies");
  }, [router, searchParams]);

  const handleRetire = useCallback(async (strategyId: number) => {
    setConfirmTarget(null);
    setInflight(prev => ({ ...prev, [strategyId]: true }));
    setToggleError(null);
    const prevValue = publishOverrides[strategyId];
    setPublishOverrides(prev => ({ ...prev, [strategyId]: false }));
    try {
      await allocatorApi.unpublishStrategy(strategyId);
    } catch (err) {
      setPublishOverrides(prev => {
        const next = { ...prev };
        if (prevValue === undefined) delete next[strategyId];
        else next[strategyId] = prevValue;
        return next;
      });
      const msg = err instanceof Error ? err.message : String(err);
      const status = /^API (\d+):/.exec(msg)?.[1];
      if (status === "401" || status === "403") {
        setToggleError({ strategyId, message: "Admin access required" });
      } else if (status === "404") {
        setToggleError({ strategyId, message: "Strategy not found — may have been deleted." });
        refresh();
      } else {
        setToggleError({ strategyId, message: "Something went wrong. Try again or check server logs." });
        console.error("unpublishStrategy failed:", err);
      }
    } finally {
      setInflight(prev => {
        const next = { ...prev };
        delete next[strategyId];
        return next;
      });
    }
  }, [publishOverrides, refresh]);

  const handlePublish = useCallback(async (strategyId: number) => {
    setInflight(prev => ({ ...prev, [strategyId]: true }));
    setToggleError(null);
    const prevValue = publishOverrides[strategyId];
    setPublishOverrides(prev => ({ ...prev, [strategyId]: true }));
    try {
      await allocatorApi.publishStrategy(strategyId);
    } catch (err) {
      setPublishOverrides(prev => {
        const next = { ...prev };
        if (prevValue === undefined) delete next[strategyId];
        else next[strategyId] = prevValue;
        return next;
      });
      const msg = err instanceof Error ? err.message : String(err);
      const status = /^API (\d+):/.exec(msg)?.[1];
      if (status === "401" || status === "403") {
        setToggleError({ strategyId, message: "Admin access required" });
      } else if (status === "404") {
        setToggleError({ strategyId, message: "Strategy not found — may have been deleted." });
        refresh();
      } else {
        setToggleError({ strategyId, message: "Something went wrong. Try again or check server logs." });
        console.error("publishStrategy failed:", err);
      }
    } finally {
      setInflight(prev => {
        const next = { ...prev };
        delete next[strategyId];
        return next;
      });
    }
  }, [publishOverrides, refresh]);

  const CATALOG_ENTRIES = Object.entries(STRATEGY_CATALOG) as [string, StrategyCatalogEntry][];

  function isDominant(index: number) {
    return hoveredIndex === null ? index === 0 : hoveredIndex === index;
  }

  const trans = "all 0.15s ease";

  return (
    <div style={{ background: "var(--bg0)", padding: "28px", minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>

        {/* Header row: label + view toggle */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
          <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase" }}>
            STRATEGIES
          </div>
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            {isAdmin && (
              <label
                style={{
                  display: "inline-flex", alignItems: "center", gap: 6,
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  color: "var(--t2)", textTransform: "uppercase",
                  cursor: "pointer", marginRight: 8,
                }}
              >
                <input
                  type="checkbox"
                  checked={urlShowRetired}
                  onChange={e => updateShowRetiredParam(e.target.checked)}
                  style={{ cursor: "pointer" }}
                />
                Show retired
              </label>
            )}
            <button
              onClick={() => setView("list")}
              style={{
                padding: 0, border: "none", background: "transparent",
                cursor: "pointer", display: "inline-flex", alignItems: "center",
                transition: "color 0.15s ease",
              }}
              onMouseEnter={e => { if (view !== "list") e.currentTarget.querySelector("svg")?.setAttribute("fill", "var(--t1)"); }}
              onMouseLeave={e => { if (view !== "list") e.currentTarget.querySelector("svg")?.setAttribute("fill", "var(--t3)"); }}
            ><ListIcon color={view === "list" ? "var(--t1)" : "var(--t2)"} /></button>
            <button
              onClick={() => setView("grid")}
              style={{
                padding: 0, border: "none", background: "transparent",
                cursor: "pointer", display: "inline-flex", alignItems: "center",
                transition: "color 0.15s ease",
              }}
              onMouseEnter={e => { if (view !== "grid") e.currentTarget.querySelector("svg")?.setAttribute("fill", "var(--t1)"); }}
              onMouseLeave={e => { if (view !== "grid") e.currentTarget.querySelector("svg")?.setAttribute("fill", "var(--t3)"); }}
            ><GridIcon color={view === "grid" ? "var(--t1)" : "var(--t2)"} /></button>
          </div>
        </div>

        {loading && (
          <div style={{ textAlign: "center", padding: "40px 0", color: "var(--t2)", fontSize: 10 }}>Loading strategies...</div>
        )}

        {/* Cards container */}
        <div
          onMouseLeave={() => setHoveredIndex(null)}
          style={view === "list"
            ? { display: "flex", flexDirection: "column", gap: 8 }
            : { display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 12 }
          }
        >
          {CATALOG_ENTRIES.map(([type, cat], index) => {
            const hasInstance = instances.some(i => i.strategyType === type);
            const dominant = isDominant(index);
            const strategyId = cat.strategyId;
            const hasAdminToggle = isAdmin && strategyId > 0;
            const effectivePublished = publishOverrides[strategyId] ?? cat.isPublished;
            const busy = Boolean(inflight[strategyId]);
            const cardError = toggleError && toggleError.strategyId === strategyId ? toggleError.message : null;
            const onAdminToggle = () => {
              if (effectivePublished) {
                setConfirmTarget({ strategyId, displayName: cat.name });
              } else {
                handlePublish(strategyId);
              }
            };

            if (view === "list") {
              return (
                <div
                  key={type}
                  onMouseEnter={() => setHoveredIndex(index)}
                  style={{
                    background: dominant ? "var(--bg2)" : "var(--bg1)",
                    border: `1px solid ${hasInstance && dominant ? "var(--green-mid)" : "var(--line)"}`,
                    borderRadius: 5, padding: "20px 22px",
                    transition: trans, cursor: "pointer",
                    position: "relative",
                    opacity: effectivePublished ? 1 : 0.5,
                  }}
                  onClick={() => router.push(`/trader/strategies/${type}`)}
                >
                  {hasAdminToggle && (
                    <AdminToggleButton
                      isPublished={effectivePublished}
                      busy={busy}
                      onClick={onAdminToggle}
                    />
                  )}
                  {/* Top row */}
                  <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 16 }}>
                    <div style={{ flex: 1 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                        <h3 style={{ fontSize: 13, fontWeight: 700, color: dominant ? "var(--t0)" : "var(--t2)", margin: 0, transition: trans }}>{cat.name}</h3>
                        <span style={{
                          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                          color: RISK_COLOR[cat.risk], background: RISK_DIM[cat.risk],
                          borderRadius: 3, padding: "3px 8px",
                        }}>{cat.risk}</span>
                        {!effectivePublished && <RetiredPill />}
                      </div>
                      {cardError && (
                        <div style={{ fontSize: 9, color: "var(--red)", marginTop: 4, letterSpacing: "0.06em" }}>
                          {cardError}
                        </div>
                      )}
                      <p style={{ fontSize: 10, color: dominant ? "var(--t1)" : "var(--t3)", margin: 0, lineHeight: 1.5, maxWidth: 560, transition: trans }}>
                        {cat.description.split(".")[0]}.
                      </p>
                    </div>

                    <div style={{ display: "flex", alignItems: "center", gap: 16, flexShrink: 0, marginLeft: 24 }}>
                      <span style={{ fontSize: 18, fontWeight: 700, color: dominant ? "var(--green)" : "var(--t2)", transition: trans }}>
                        +{fmt(cat.ytd, 1)}%
                        <span style={{ fontSize: 9, fontWeight: 400, color: "var(--t2)", marginLeft: 4 }}>YTD</span>
                      </span>

                      {hasInstance ? (
                        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                          <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--green)" }} />
                          <span style={{ fontSize: 10, color: "var(--green)", whiteSpace: "nowrap" }}>In your traders</span>
                        </div>
                      ) : (
                        <button
                          onClick={e => { e.stopPropagation(); router.push(`/trader/strategies/${type}`); }}
                          style={{
                            padding: "8px 16px",
                            background: dominant ? "var(--green)" : "transparent",
                            color: dominant ? "var(--bg0)" : "var(--t2)",
                            border: dominant ? "none" : "1px solid var(--line)",
                            borderRadius: 3,
                            fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                            cursor: "pointer", whiteSpace: "nowrap",
                            transition: trans,
                          }}
                        >
                          VIEW STRATEGY
                        </button>
                      )}
                    </div>
                  </div>

                  {/* Stats row */}
                  <div style={{ display: "flex", gap: 14, paddingTop: 14, borderTop: "1px solid var(--line)" }}>
                    {[
                      { label: "SHARPE", value: fmt(cat.sharpe, 2), first: true },
                      { label: "CAGR", value: `${fmt(cat.cagr, 1)}%` },
                      { label: "MAX DD", value: `-${fmt(cat.maxDd, 1)}%` },
                      { label: "WIN RATE", value: `${fmt(cat.winRate, 0)}%` },
                      { label: "AVG 1M", value: `+${fmt(cat.avg1m, 1)}%` },
                    ].map(s => (
                      <div key={s.label} style={{ borderLeft: s.first ? "none" : "1px solid var(--line)", paddingLeft: s.first ? 0 : 14 }}>
                        <div style={{ fontSize: 9, color: "var(--t2)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 3 }}>{s.label}</div>
                        <div style={{ fontSize: 12, fontWeight: 700, color: dominant ? "var(--t1)" : "var(--t3)", transition: trans }}>{s.value}</div>
                      </div>
                    ))}
                  </div>

                  {/* Capacity row */}
                  <CapacityRow type={type} dominant={dominant} />
                </div>
              );
            }

            // Grid view
            return (
              <div
                key={type}
                onMouseEnter={() => setHoveredIndex(index)}
                onClick={() => router.push(`/trader/strategies/${type}`)}
                style={{
                  background: dominant ? "var(--bg2)" : "var(--bg1)",
                  border: `1px solid ${hasInstance && dominant ? "var(--green-mid)" : "var(--line)"}`,
                  borderRadius: 5, padding: "20px 18px",
                  display: "flex", flexDirection: "column",
                  transition: trans, cursor: "pointer",
                  position: "relative",
                  opacity: effectivePublished ? 1 : 0.5,
                }}
              >
                {hasAdminToggle && (
                  <AdminToggleButton
                    isPublished={effectivePublished}
                    busy={busy}
                    onClick={onAdminToggle}
                  />
                )}
                {/* Name + badge */}
                <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8, flexWrap: "wrap" }}>
                  <h3 style={{ fontSize: 13, fontWeight: 700, color: dominant ? "var(--t0)" : "var(--t2)", margin: 0, transition: trans }}>{cat.name}</h3>
                  <span style={{
                    fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                    color: RISK_COLOR[cat.risk], background: RISK_DIM[cat.risk],
                    borderRadius: 3, padding: "3px 8px",
                  }}>{cat.risk}</span>
                  {!effectivePublished && <RetiredPill />}
                </div>
                {cardError && (
                  <div style={{ fontSize: 9, color: "var(--red)", marginBottom: 6, letterSpacing: "0.06em" }}>
                    {cardError}
                  </div>
                )}

                {/* Description */}
                <p style={{ fontSize: 10, color: dominant ? "var(--t1)" : "var(--t3)", margin: "0 0 14px", lineHeight: 1.5, flex: 1, transition: trans }}>
                  {cat.description.split(".")[0]}.
                </p>

                {/* Stats — vertical in grid mode, no dividers */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8, paddingTop: 12, borderTop: "1px solid var(--line)", marginBottom: 12 }}>
                  {[
                    { label: "SHARPE", value: fmt(cat.sharpe, 2) },
                    { label: "MAX DD", value: `-${fmt(cat.maxDd, 1)}%` },
                    { label: "WIN RATE", value: `${fmt(cat.winRate, 0)}%` },
                  ].map(s => (
                    <div key={s.label}>
                      <div style={{ fontSize: 9, color: "var(--t2)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 3 }}>{s.label}</div>
                      <div style={{ fontSize: 12, fontWeight: 700, color: dominant ? "var(--t1)" : "var(--t3)", transition: trans }}>{s.value}</div>
                    </div>
                  ))}
                </div>

                {/* Capacity row */}
                <CapacityRow type={type} dominant={dominant} />

                {/* YTD */}
                <div style={{ fontSize: 18, fontWeight: 700, color: dominant ? "var(--green)" : "var(--t2)", marginTop: 14, marginBottom: 14, transition: trans }}>
                  +{fmt(cat.ytd, 1)}%
                  <span style={{ fontSize: 9, fontWeight: 400, color: "var(--t2)", marginLeft: 4 }}>YTD</span>
                </div>

                {/* CTA */}
                {hasInstance ? (
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 6, padding: "8px 0" }}>
                    <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--green)" }} />
                    <span style={{ fontSize: 10, color: "var(--green)" }}>In your traders</span>
                  </div>
                ) : (
                  <button
                    onClick={e => { e.stopPropagation(); router.push(`/trader/strategies/${type}`); }}
                    style={{
                      width: "100%", padding: "9px 0",
                      background: dominant ? "var(--green)" : "transparent",
                      color: dominant ? "var(--bg0)" : "var(--t2)",
                      border: dominant ? "none" : "1px solid var(--line)",
                      borderRadius: 3,
                      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                      cursor: "pointer",
                      transition: trans,
                    }}
                  >
                    VIEW STRATEGY
                  </button>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {confirmTarget && (
        <ConfirmRetireModal
          displayName={confirmTarget.displayName}
          submitting={Boolean(inflight[confirmTarget.strategyId])}
          onCancel={() => setConfirmTarget(null)}
          onConfirm={() => handleRetire(confirmTarget.strategyId)}
        />
      )}
    </div>
  );
}

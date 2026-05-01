"use client";

/**
 * frontend/app/components/useLivePoll.ts
 * =========================================
 * Polling hook for the Manager Live tab. One per endpoint:
 *
 *     const account = useLivePoll<AccountSnapshot>(
 *       `${API_BASE}/api/manager/live/account`, 2000
 *     );
 *
 * Behavior:
 *   * Fires immediately on mount, then on `intervalMs` repeat.
 *   * When `document.hidden` fires, drops to a slow 60s cadence per
 *     Data Dictionary §1's T0-throttle rule. Restores fast cadence on
 *     visibilitychange → not hidden.
 *   * Tracks the most recent successful payload (`data`) and the last
 *     error (`error`). On a fetch failure, `data` retains its prior
 *     value so the section keeps rendering the most-recent good data
 *     rather than blanking out.
 *   * `isStale` is true when the last attempt failed AND it has been
 *     more than 2× the active interval since the last success — drives
 *     the per-section "STALE Ns" badge in the page header.
 */

import { useCallback, useEffect, useRef, useState } from "react";

const HIDDEN_MS = 60_000;

export interface LivePollState<T> {
  data: T | null;
  error: Error | null;
  isStale: boolean;
  lastUpdatedAt: Date | null;
  lastAttemptAt: Date | null;
  /** Force an out-of-band refetch — e.g. manual REFRESH button. */
  refresh: () => Promise<void>;
}

export function useLivePoll<T>(
  url: string,
  intervalMs: number,
): LivePollState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<Date | null>(null);
  const [lastAttemptAt, setLastAttemptAt] = useState<Date | null>(null);
  const [, setTick] = useState(0);

  // Refs so the polling loop closes over latest values without
  // re-installing the interval on every render.
  const urlRef = useRef(url);
  urlRef.current = url;
  const intervalMsRef = useRef(intervalMs);
  intervalMsRef.current = intervalMs;
  const hiddenRef = useRef(false);

  const fetchOnce = useCallback(async () => {
    setLastAttemptAt(new Date());
    try {
      const resp = await fetch(urlRef.current, { credentials: "include" });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      const json = (await resp.json()) as T;
      setData(json);
      setError(null);
      setLastUpdatedAt(new Date());
    } catch (e) {
      setError(e instanceof Error ? e : new Error(String(e)));
    }
  }, []);

  // Visibility-aware polling loop. The setTimeout-based recursion (vs
  // setInterval) lets us pick up an interval change on the next tick
  // when document.hidden flips.
  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const tick = async () => {
      if (cancelled) return;
      await fetchOnce();
      if (cancelled) return;
      const next = hiddenRef.current ? HIDDEN_MS : intervalMsRef.current;
      timer = setTimeout(tick, next);
    };
    void tick();

    const onVisibilityChange = () => {
      hiddenRef.current = document.hidden;
      // If we just became visible, fetch immediately to refresh any
      // staleness rather than wait for the next interval.
      if (!document.hidden && !cancelled) {
        if (timer) clearTimeout(timer);
        void tick();
      }
    };
    document.addEventListener("visibilitychange", onVisibilityChange);

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [fetchOnce]);

  // 1Hz tick so isStale flips automatically without a fetch round-trip.
  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 1000);
    return () => clearInterval(id);
  }, []);

  // Stale = had at least one error AND last success is older than 2× interval.
  const isStale = (() => {
    if (!error) return false;
    if (!lastUpdatedAt) return true;
    const ageMs = Date.now() - lastUpdatedAt.getTime();
    return ageMs > 2 * intervalMs;
  })();

  return {
    data,
    error,
    isStale,
    lastUpdatedAt,
    lastAttemptAt,
    refresh: fetchOnce,
  };
}

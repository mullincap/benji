"use client";

/**
 * Persistent sidebar collapsed state across all sections (compiler, indexer,
 * manager, trader). Stored in localStorage under a single key so a collapse
 * in any one section persists when the user navigates to another section
 * (and across page reloads). Default = expanded (false).
 *
 * Pattern (matches what the per-section layouts had before, just shared):
 *
 *   const [collapsed, setCollapsed] = useSidebarCollapsed();
 *
 * SSR safety: the hook returns `false` during the server render and the
 * first client render, then synchronizes from localStorage in a useEffect.
 * This avoids hydration mismatches; a briefly-flashed expanded state on
 * load is acceptable and matches the previous useState(false) behavior.
 */
import { useCallback, useEffect, useState } from "react";

const STORAGE_KEY = "benji.sidebar.collapsed";

export function useSidebarCollapsed(): [boolean, (next: boolean | ((prev: boolean) => boolean)) => void] {
  const [collapsed, setCollapsedState] = useState<boolean>(false);

  // Hydrate from localStorage on mount.
  useEffect(() => {
    try {
      const v = window.localStorage.getItem(STORAGE_KEY);
      if (v === "1") setCollapsedState(true);
      else if (v === "0") setCollapsedState(false);
    } catch {
      // localStorage unavailable (private browsing, SSR) — keep default.
    }
  }, []);

  // Persist on change. Wrapped so callers can pass a function-updater
  // (preserves the existing `setCollapsed(v => !v)` pattern).
  const setCollapsed = useCallback(
    (next: boolean | ((prev: boolean) => boolean)) => {
      setCollapsedState((prev) => {
        const resolved = typeof next === "function" ? next(prev) : next;
        try {
          window.localStorage.setItem(STORAGE_KEY, resolved ? "1" : "0");
        } catch {
          // ignore storage errors
        }
        return resolved;
      });
    },
    [],
  );

  return [collapsed, setCollapsed];
}

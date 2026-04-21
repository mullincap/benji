"use client";

import { useEffect, useRef } from "react";
import type { AvailableAlloc } from "./AllocationFilter";

/**
 * Auto-default the AllocationFilter selection to the allocation with the
 * largest capital_usd on first load. Fires exactly once — if the user
 * manually picks a different option (including switching back to "all")
 * it stays on their choice.
 *
 * Uses capital_usd (configured allocation size) rather than live equity
 * because that's what's already in the dropdown options. If we want
 * PnL-adjusted equity later, we'd need to extend the summary response.
 */
export function useLargestAllocationDefault(
  current: "all" | string,
  setValue: (next: "all" | string) => void,
  options: AvailableAlloc[] | undefined,
) {
  const appliedRef = useRef(false);
  useEffect(() => {
    if (appliedRef.current) return;
    if (!options || options.length === 0) return;
    if (current !== "all") {
      appliedRef.current = true;
      return;
    }
    const largest = [...options].sort(
      (a, b) => (b.capital_usd ?? 0) - (a.capital_usd ?? 0),
    )[0];
    if (largest) {
      setValue(largest.allocation_id);
      appliedRef.current = true;
    }
  }, [current, options, setValue]);
}

/**
 * Admin module visual tokens.
 *
 * Locked per the admin spec:
 * - Module accent: amber #f0a500 (distinct from the five canonical
 *   module colors). Already maps to `var(--amber)` in globals.css.
 * - "Soft" amber for hover/selection states: rgba(240,165,0,0.12).
 * - "Bright" amber for primary CTA hover: #ffba24.
 *
 * Components reference these by name rather than re-declaring the hex
 * inline so a future palette tweak only edits this file.
 */

export const ADMIN_ACCENT = "var(--amber)" as const;        // #f0a500
export const AMBER_SOFT = "rgba(240, 165, 0, 0.12)" as const;
export const AMBER_BRIGHT = "#ffba24" as const;

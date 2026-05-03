/**
 * AdminTable — typed wrapper around a styled HTML table matching the
 * mockup. Generic in the row type so consumers retain type-safety on
 * the cell render functions.
 *
 * Empty-state handling: when `rows` is empty AND `loading` is false,
 * renders a centered dim message instead of a broken empty table.
 */

import type { ReactNode } from "react";

export type Column<T> = {
  /** Header label. Will be rendered uppercase in the styled th. */
  header: ReactNode;
  /** Cell render function. */
  render: (row: T, index: number) => ReactNode;
  /** Right-align the column (numbers). */
  alignRight?: boolean;
  /** Fixed/preferred width — accepts CSS length string. */
  width?: string;
  /** Small key for stable React keys when header is a node. */
  key?: string;
};

type Props<T> = {
  columns: Column<T>[];
  rows: T[];
  /** Show "Loading…" placeholder rows while true. */
  loading?: boolean;
  /** Click handler — wires up cursor:pointer + onClick on each row. */
  onRowClick?: (row: T) => void;
  /** Empty-state message when not loading and rows.length === 0. */
  emptyMessage?: ReactNode;
  /** Optional row key extractor — defaults to index. */
  rowKey?: (row: T, index: number) => string;
};

export default function AdminTable<T>({
  columns,
  rows,
  loading,
  onRowClick,
  emptyMessage = "Nothing here yet.",
  rowKey,
}: Props<T>) {
  return (
    <table
      style={{
        width: "100%",
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderCollapse: "collapse",
        borderRadius: 2,
        overflow: "hidden",
      }}
    >
      <thead>
        <tr>
          {columns.map((col, i) => (
            <th
              key={col.key || `h${i}`}
              style={{
                textAlign: col.alignRight ? "right" : "left",
                padding: "10px 14px",
                color: "var(--t3)",
                fontSize: 9,
                letterSpacing: "0.16em",
                textTransform: "uppercase",
                borderBottom: "1px solid var(--line)",
                background: "var(--bg3)",
                fontWeight: 700,
                width: col.width,
              }}
            >
              {col.header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {loading ? (
          <tr>
            <td
              colSpan={columns.length}
              style={{
                padding: "24px 14px",
                textAlign: "center",
                color: "var(--t3)",
                fontSize: 12,
              }}
            >
              Loading…
            </td>
          </tr>
        ) : rows.length === 0 ? (
          <tr>
            <td
              colSpan={columns.length}
              style={{
                padding: "32px 14px",
                textAlign: "center",
                color: "var(--t3)",
                fontSize: 12,
              }}
            >
              {emptyMessage}
            </td>
          </tr>
        ) : (
          rows.map((row, ri) => (
            <tr
              key={rowKey ? rowKey(row, ri) : ri}
              onClick={onRowClick ? () => onRowClick(row) : undefined}
              style={{
                borderBottom: "1px solid var(--line)",
                transition: "background 0.08s ease",
                cursor: onRowClick ? "pointer" : "default",
              }}
              onMouseEnter={(e) => {
                if (onRowClick) e.currentTarget.style.background = "var(--bg3)";
              }}
              onMouseLeave={(e) => {
                if (onRowClick) e.currentTarget.style.background = "transparent";
              }}
            >
              {columns.map((col, ci) => (
                <td
                  key={col.key || `c${ri}-${ci}`}
                  style={{
                    padding: "12px 14px",
                    color: "var(--t0)",
                    fontSize: 12,
                    verticalAlign: "middle",
                    textAlign: col.alignRight ? "right" : "left",
                  }}
                >
                  {col.render(row, ri)}
                </td>
              ))}
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}

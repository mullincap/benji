/**
 * PasswordStrengthMeter — custom 4-tier strength scorer.
 *
 * Replacement for zxcvbn (which is a 400KB+ dep). Scoring is intentionally
 * simple and conservative: length-tier + character-class diversity, with
 * a hard cap to "Weak" if the password contains a known-common substring.
 *
 * Server-side validation must mirror this exactly (see
 * backend/app/api/routes/auth.py — to be added in the backend phase).
 *
 * Score tiers:
 *   0  empty
 *   1  Weak    (contains common substring, OR <8 chars, OR <2 char classes)
 *   2  Fair    (8–11 chars, ≥2 classes)
 *   3  Good    (12–15 chars, ≥3 classes)
 *   4  Strong  (≥16 chars, ≥3 classes) OR (≥12 chars, all 4 classes)
 *
 * Public minimum for v1 is score ≥ 3 ("Good") — matches the
 * "≥12 chars, mixed case, number, symbol" rule on the accept-invite screen.
 */

const COMMON_SUBSTRINGS: readonly string[] = [
  "password", "qwerty", "admin", "123456", "letmein",
  "welcome", "monkey", "dragon", "master", "login",
  "abc123", "iloveyou", "sunshine", "princess", "football",
  "baseball", "shadow", "superman", "mullincap", "3m3m",
];

type Score = 0 | 1 | 2 | 3 | 4;

export type StrengthResult = {
  score: Score;
  label: "" | "Weak" | "Fair" | "Good" | "Strong";
  meetsMinimum: boolean;
};

const LABELS = ["", "Weak", "Fair", "Good", "Strong"] as const;
const MINIMUM_SCORE: Score = 3;

export function scorePassword(password: string): StrengthResult {
  if (!password) {
    return { score: 0, label: "", meetsMinimum: false };
  }

  const lower = password.toLowerCase();
  const classes =
    Number(/[a-z]/.test(password)) +
    Number(/[A-Z]/.test(password)) +
    Number(/[0-9]/.test(password)) +
    Number(/[^A-Za-z0-9]/.test(password));

  // Common-substring penalty wins over everything else.
  if (COMMON_SUBSTRINGS.some((c) => lower.includes(c))) {
    return { score: 1, label: "Weak", meetsMinimum: false };
  }

  let score: Score;
  if (password.length < 8 || classes < 2) {
    score = 1;
  } else if (password.length < 12) {
    score = 2;
  } else if (password.length < 16) {
    score = classes >= 4 ? 4 : 3;
  } else {
    score = classes >= 3 ? 4 : 3;
  }

  return {
    score,
    label: LABELS[score],
    meetsMinimum: score >= MINIMUM_SCORE,
  };
}

type Props = {
  password: string;
  /** Optional helper text shown to the right of the strength label. */
  helper?: string;
};

const BAR_COLORS: Record<Score, [string, string, string, string]> = {
  0: ["var(--line)", "var(--line)", "var(--line)", "var(--line)"],
  1: ["var(--red)", "var(--line)", "var(--line)", "var(--line)"],
  2: ["var(--amber)", "var(--amber)", "var(--line)", "var(--line)"],
  3: ["var(--green)", "var(--green)", "var(--green)", "var(--line)"],
  4: ["var(--green)", "var(--green)", "var(--green)", "var(--green)"],
};

const LABEL_COLORS: Record<Score, string> = {
  0: "var(--t3)",
  1: "var(--red)",
  2: "var(--amber)",
  3: "var(--green)",
  4: "var(--green)",
};

export default function PasswordStrengthMeter({ password, helper }: Props) {
  const { score, label } = scorePassword(password);
  const bars = BAR_COLORS[score];

  return (
    <div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: 4,
          marginTop: 8,
          marginBottom: 6,
        }}
      >
        {bars.map((color, i) => (
          <span
            key={i}
            style={{ height: 3, background: color, borderRadius: 1, transition: "background 0.15s ease" }}
          />
        ))}
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          fontSize: 10,
          color: "var(--t3)",
          letterSpacing: "0.06em",
          textTransform: "uppercase",
        }}
      >
        <span>{helper || "Strength"}</span>
        <span style={{ color: LABEL_COLORS[score] }}>{label || "—"}</span>
      </div>
    </div>
  );
}

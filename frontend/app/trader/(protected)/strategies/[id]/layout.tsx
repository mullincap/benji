import type { Metadata } from "next";

// Dynamic title — uses the slug from the URL (e.g. "alts_main"). The
// strategy's display name lives in audit.strategies.display_name and
// would render nicer ("ALTS MAIN") but fetching it server-side requires
// a DB round-trip we'd duplicate against what the client component
// already does. Slug is meaningful enough for the tab title.
type Props = { params: Promise<{ id: string }> };

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { id } = await params;
  return { title: id };
}

export default function StrategyDetailLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}

/**
 * frontend/app/trader/_lib/exchange-info.ts
 * ==========================================
 * Per-exchange metadata used by both wizards (ExchangeLinkWizard and
 * SetupWizard) for surfacing the right URLs, wallet terminology, and
 * API-key creation walkthroughs to the user.
 *
 * Keyed by exchange slug (lowercase).
 *
 * Two lookup helpers:
 *   resolveExchangeInfo(name) — by display name OR slug; falls back to
 *     a usable generic shape when the exchange isn't in the map.
 *   EXCHANGE_INFO[slug]      — direct lookup when the caller already
 *     has the slug in hand.
 *
 * Adding a new exchange: drop a new entry here, both wizards pick it
 * up automatically. The generic-fallback shape below is what's used
 * in the meantime.
 */

export type ExchangeInfo = {
  /** Canonical Assets/Wallet view — covers deposits AND inter-wallet
   *  transfers from one entry point. Used by the empty-balance panel
   *  on the SYNC CAPITAL wizard's Allocate step. */
  accountUrl: string;
  /** The exchange's actual UI label for its USDT-M futures wallet
   *  ("USDT-M wallet" on BloFin, "USDⓈ-M Futures wallet" on Binance).
   *  Used in the empty-balance copy so the user can find the right
   *  wallet without translation. */
  futuresWalletLabel: string;
  /** Display-cased name ("BloFin", "Binance") for banner / button
   *  copy. Different from the slug used as the map key. */
  displayName: string;
  /** Direct link to the API-key management page where the user
   *  creates the key the wizard then asks them to paste. Bottom CTA
   *  on the "Need help creating your API keys?" expandable. */
  apiKeyManagementUrl: string;
  /** CTA label for the link above. Per-exchange so the displayed
   *  text matches the exchange's own UI terminology. */
  apiKeyInstructionsLabel: string;
  /** Step-by-step instructions for creating an API key with the
   *  right permissions. Rendered as an ordered list inside the
   *  collapsible expandable. */
  apiKeyInstructions: string[];
};

export const EXCHANGE_INFO: Record<string, ExchangeInfo> = {
  blofin: {
    accountUrl: "https://blofin.com/assets/overview",
    futuresWalletLabel: "USDT-M wallet",
    displayName: "BloFin",
    apiKeyManagementUrl: "https://blofin.com/account/apis",
    apiKeyInstructionsLabel: "Open BloFin API Management →",
    apiKeyInstructions: [
      "Log into BloFin, go to Account → API Management",
      "Click \"Create API,\" set a name (e.g. \"3M trading\")",
      "Set permissions: enable \"Read\" and \"Trade.\" Leave \"Transfer\" / \"Withdraw\" OFF.",
      "Copy your API Key + Secret Key, paste them above",
    ],
  },
  binance: {
    accountUrl: "https://www.binance.com/en/my/wallet/account/overview",
    futuresWalletLabel: "USDⓈ-M Futures wallet",
    displayName: "Binance",
    apiKeyManagementUrl: "https://www.binance.com/en/my/settings/api-management",
    apiKeyInstructionsLabel: "Open Binance API Management →",
    apiKeyInstructions: [
      "Log into Binance, go to Account → API Management",
      "Click \"Create API,\" choose \"System generated\"",
      "Set permissions: enable \"Reading\" and \"Enable Futures.\" Leave \"Enable Withdrawals\" OFF.",
      "Copy your API Key + Secret Key, paste them above",
    ],
  },
};

/**
 * Resolve an EXCHANGE_INFO entry from a display name or slug. Returns
 * a generic fallback when the input doesn't match a mapped exchange,
 * so callers can always render something usable.
 *
 * Generic fallback shape:
 *   accountUrl              "#"           — disables nav until mapped
 *   futuresWalletLabel      "futures wallet"
 *   displayName             capitalized slug, or "your exchange" for null
 *   apiKeyManagementUrl     "#"
 *   apiKeyInstructions      [] (empty — caller should detect and skip)
 *   apiKeyInstructionsLabel "" (empty)
 *
 * Callers using the instructions array should check for length > 0
 * before rendering the expandable; the empty array is a "silently
 * degrade" signal.
 */
export function resolveExchangeInfo(exchangeName: string | null): ExchangeInfo {
  if (exchangeName) {
    const slug = exchangeName.toLowerCase();
    const hit = EXCHANGE_INFO[slug];
    if (hit) return hit;
    return {
      accountUrl: "#",
      futuresWalletLabel: "futures wallet",
      displayName: exchangeName.charAt(0).toUpperCase() + exchangeName.slice(1).toLowerCase(),
      apiKeyManagementUrl: "#",
      apiKeyInstructionsLabel: "",
      apiKeyInstructions: [],
    };
  }
  return {
    accountUrl: "#",
    futuresWalletLabel: "futures wallet",
    displayName: "your exchange",
    apiKeyManagementUrl: "#",
    apiKeyInstructionsLabel: "",
    apiKeyInstructions: [],
  };
}

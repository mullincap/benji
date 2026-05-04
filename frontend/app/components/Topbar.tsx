'use client';

import { useEffect, useRef, useState, useCallback } from 'react';
import { useRouter, usePathname } from 'next/navigation';

import { useAuth } from '../lib/auth';

// ─── Theme definitions ───────────────────────────────────────────────────────

type ModuleKey = 'compiler' | 'indexer' | 'simulator' | 'allocator' | 'manager' | 'admin';

interface ThemeDef {
  label: string;
  colors: Record<ModuleKey, string>;
  disabled?: boolean;
}

// Admin's accent is locked to amber #F0A500 across every theme — it's a
// distinct surface from the 5 canonical modules and shouldn't recolor on
// theme switch. The theme dropdown's 5-swatch preview deliberately
// omits admin so the user-facing theme picker still reads as "5 module
// colors".
const ADMIN_ACCENT = '#F0A500';

const THEMES: Record<string, ThemeDef> = {
  spectrum: {
    label: 'Spectrum',
    colors: {
      compiler: '#00E87A',
      indexer: '#38B4FF',
      simulator: '#C47DFF',
      allocator: '#FFB830',
      manager: '#FF5E5E',
      admin: ADMIN_ACCENT,
    },
  },
  terminal: {
    label: 'Terminal Glow',
    colors: {
      compiler: '#00D4AA',
      indexer: '#39F084',
      simulator: '#D4E84A',
      allocator: '#F0A500',
      manager: '#E060FF',
      admin: ADMIN_ACCENT,
    },
  },
  institutional: {
    label: 'Institutional',
    colors: {
      compiler: '#4EC994',
      indexer: '#5BA3D9',
      simulator: '#9B7FE8',
      allocator: '#C9A84C',
      manager: '#C96060',
      admin: ADMIN_ACCENT,
    },
  },
  electric: {
    label: 'Electric',
    colors: {
      compiler: '#00C2FF',
      indexer: '#00E5C8',
      simulator: '#39FF85',
      allocator: '#A78BFF',
      manager: '#7B5FFF',
      admin: ADMIN_ACCENT,
    },
  },
  oxide: {
    label: 'Oxide',
    colors: { compiler: '', indexer: '', simulator: '', allocator: '', manager: '', admin: ADMIN_ACCENT },
    disabled: true,
  },
};

const THEME_KEYS = Object.keys(THEMES);
const STORAGE_KEY = '3m-theme';
const TARGET_KEY = '3m-theme-target';
const BASE_KEY = '3m-base-theme';
type ThemeTarget = 'none' | 'text' | 'fill' | 'mono';

// ─── Base theme definitions ──────────────────────────────────────────────────

interface BaseThemeDef {
  label: string;
  swatch: string;
  surface: string;
  panel: string;
  card: string;
  raised: string;
  border: string;
}

const BASE_THEMES: Record<string, BaseThemeDef> = {
  abyss:    { label: 'Abyss',    swatch: '#0D1520', surface: '#080C10', panel: '#0D1520', card: '#131F30', raised: '#1C2C42', border: '#253850' },
  vault:    { label: 'Vault',    swatch: '#131316', surface: '#0C0C0E', panel: '#131316', card: '#1C1C21', raised: '#26262D', border: '#32323B' },
  carbon:   { label: 'Carbon',   swatch: '#181610', surface: '#0E0D0B', panel: '#181610', card: '#221F17', raised: '#2E2A1F', border: '#3C3628' },
  slate:    { label: 'Slate',    swatch: '#13171C', surface: '#0B0D10', panel: '#13171C', card: '#1A1F27', raised: '#232934', border: '#2E3542' },
  moss:     { label: 'Moss',     swatch: '#0F1812', surface: '#090E0B', panel: '#0F1812', card: '#15221A', raised: '#1D2E22', border: '#273D2D' },
  obsidian: { label: 'Obsidian', swatch: '#16121E', surface: '#0D0B10', panel: '#16121E', card: '#1F1A2B', raised: '#2A2338', border: '#362E48' },
};

const BASE_THEME_KEYS = Object.keys(BASE_THEMES);

function applyBaseTheme(id: string) {
  const bt = BASE_THEMES[id] ?? BASE_THEMES.vault;
  const root = document.documentElement;
  root.style.setProperty('--base-surface', bt.surface);
  root.style.setProperty('--base-panel', bt.panel);
  root.style.setProperty('--base-card', bt.card);
  root.style.setProperty('--base-raised', bt.raised);
  root.style.setProperty('--base-border', bt.border);
}

// ─── Module items ────────────────────────────────────────────────────────────

const MODULES: Array<{ key: ModuleKey; icon: string; href: string; pill: string; shortcut: string }> = [
  { key: 'compiler',  icon: '</>', href: '/compiler',  pill: 'COMP',  shortcut: '1' },
  { key: 'indexer',   icon: '⌕',  href: '/indexer',   pill: 'INDX',  shortcut: '2' },
  { key: 'simulator', icon: '◴',  href: '/simulator', pill: 'SIM',   shortcut: '3' },
  { key: 'allocator', icon: '⚙',  href: '/trader',    pill: 'ALLOC', shortcut: '4' },
  { key: 'manager',   icon: '▣',  href: '/manager',   pill: 'MGR',   shortcut: '5' },
  // Admin slot is filtered out for non-admin users at render time. It
  // stays at index 5 so ⌘6 maps consistently when the user is admin.
  { key: 'admin',     icon: '⚡', href: '/admin',     pill: 'ADMIN', shortcut: '6' },
];

// ─── Helpers ─────────────────────────────────────────────────────────────────

function getActiveModule(pathname: string): ModuleKey | null {
  const match = MODULES.find((m) => m.href && pathname.startsWith(m.href));
  return match?.key ?? null;
}

function resolveAccent(themeId: string, moduleKey: ModuleKey | null): string {
  if (!moduleKey) return '';
  const theme = THEMES[themeId] ?? THEMES.spectrum;
  return theme.colors[moduleKey];
}

function applyAccent(themeId: string, moduleKey: ModuleKey | null) {
  const color = resolveAccent(themeId, moduleKey);
  document.documentElement.style.setProperty('--module-accent', color || 'var(--t0)');
}

// ─── Divider component ──────────────────────────────────────────────────────

function Divider() {
  return (
    <div style={{
      width: 1, height: 16, background: 'var(--line)', margin: '0 6px', flexShrink: 0,
    }} />
  );
}

// ─── User-menu helpers ───────────────────────────────────────────────────────

type AuthUserLike = { email: string; first_name: string | null; last_name: string | null };

function userInitials(u: AuthUserLike): string {
  const f = (u.first_name || '').trim();
  const l = (u.last_name || '').trim();
  if (f && l) return (f[0] + l[0]).toUpperCase();
  if (f) return f.slice(0, 2).toUpperCase();
  // Fallback to first 2 letters of the local part of the email — covers the
  // window between user creation and the first profile save where first/last
  // can still be null on legacy rows.
  const local = (u.email || '').split('@')[0] || '?';
  return (local[0] + (local[1] || local[0])).toUpperCase();
}

function userDisplayName(u: AuthUserLike): string {
  const f = (u.first_name || '').trim();
  const l = (u.last_name || '').trim();
  if (f && l) return `${f} ${l}`;
  if (f) return f;
  return u.email;
}

// ─── Component ───────────────────────────────────────────────────────────────

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || '';

export default function Topbar() {
  const [modulesOpen, setModulesOpen] = useState(false);
  const [themeOpen, setThemeOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [themeId, setThemeId] = useState('spectrum');
  const [themeTarget, setThemeTarget] = useState<ThemeTarget>('text');
  const [baseThemeId, setBaseThemeId] = useState('vault');
  const [equity, setEquity] = useState<number | null>(null);
  const modulesRef = useRef<HTMLDivElement | null>(null);
  const themeRef = useRef<HTMLDivElement | null>(null);
  const userMenuRef = useRef<HTMLDivElement | null>(null);
  const router = useRouter();
  const pathname = usePathname();
  const { user, signout } = useAuth();

  const activeModule = getActiveModule(pathname);
  const theme = THEMES[themeId] ?? THEMES.spectrum;
  const accentColor = resolveAccent(themeId, activeModule);

  // Load theme + target + base from localStorage on mount
  useEffect(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored && THEMES[stored]) setThemeId(stored);
    const target = localStorage.getItem(TARGET_KEY);
    if (target === 'none' || target === 'text' || target === 'fill' || target === 'mono') setThemeTarget(target);
    const base = localStorage.getItem(BASE_KEY);
    if (base && BASE_THEMES[base]) { setBaseThemeId(base); applyBaseTheme(base); }
  }, []);

  // Update --module-accent whenever theme or pathname changes
  useEffect(() => {
    applyAccent(themeId, activeModule);
  }, [themeId, activeModule]);

  // Click outside / escape handlers
  useEffect(() => {
    function syncFullscreen() {
      setIsFullscreen(!!document.fullscreenElement);
    }
    function onDocPointerDown(e: MouseEvent) {
      if (modulesRef.current && !modulesRef.current.contains(e.target as Node)) {
        setModulesOpen(false);
      }
      if (themeRef.current && !themeRef.current.contains(e.target as Node)) {
        setThemeOpen(false);
      }
      if (userMenuRef.current && !userMenuRef.current.contains(e.target as Node)) {
        setUserMenuOpen(false);
      }
    }
    function onEsc(e: KeyboardEvent) {
      if (e.key === 'Escape') {
        setModulesOpen(false);
        setThemeOpen(false);
        setUserMenuOpen(false);
      }
    }
    document.addEventListener('mousedown', onDocPointerDown);
    document.addEventListener('keydown', onEsc);
    document.addEventListener('fullscreenchange', syncFullscreen);
    syncFullscreen();
    return () => {
      document.removeEventListener('mousedown', onDocPointerDown);
      document.removeEventListener('keydown', onEsc);
      document.removeEventListener('fullscreenchange', syncFullscreen);
    };
  }, []);

  // Total exchange equity — only renders when the auth-protected API call
  // succeeds (i.e. user is logged in). Polls every 30s; clears on 401.
  useEffect(() => {
    let cancelled = false;
    async function fetchEquity() {
      try {
        const res = await fetch(`${API_BASE}/api/allocator/exchanges`, { credentials: 'include' });
        if (!res.ok) { if (!cancelled) setEquity(null); return; }
        const data = (await res.json()) as { exchanges: Array<{ status: string; balance: number }> };
        if (cancelled) return;
        const total = data.exchanges
          .filter((e) => e.status === 'active')
          .reduce((s, e) => s + (e.balance ?? 0), 0);
        setEquity(total);
      } catch {
        if (!cancelled) setEquity(null);
      }
    }
    fetchEquity();
    const id = setInterval(fetchEquity, 30000);
    return () => { cancelled = true; clearInterval(id); };
  }, [pathname]);

  // Keyboard shortcuts: ⌘1–5 navigate to modules; ⌘6 to /admin
  // (admin-only — non-admins ignore ⌘6 to avoid the bumpy redirect
  // through the admin layout's own deny path).
  const isAdmin = user?.is_admin === true;
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (!e.metaKey && !e.ctrlKey) return;
      const idx = parseInt(e.key, 10);
      if (idx >= 1 && idx <= 5) {
        e.preventDefault();
        const mod = MODULES[idx - 1];
        if (mod?.href) router.push(mod.href);
      } else if (idx === 6 && isAdmin) {
        e.preventDefault();
        router.push('/admin');
      }
    }
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [router, isAdmin]);

  const selectTheme = useCallback((id: string) => {
    if (THEMES[id]?.disabled) return;
    setThemeId(id);
    localStorage.setItem(STORAGE_KEY, id);
  }, []);

  const selectTarget = useCallback((t: ThemeTarget) => {
    setThemeTarget(t);
    localStorage.setItem(TARGET_KEY, t);
  }, []);

  const selectBaseTheme = useCallback((id: string) => {
    setBaseThemeId(id);
    localStorage.setItem(BASE_KEY, id);
    applyBaseTheme(id);
  }, []);

  async function toggleFullscreen() {
    try {
      if (document.fullscreenElement) {
        await document.exitFullscreen();
      } else {
        await document.documentElement.requestFullscreen();
      }
    } catch {
      // Ignore browser-level fullscreen rejections
    }
  }

  function getThemePreviewColor(id: string): string {
    const t = THEMES[id];
    if (!t || t.disabled) return 'var(--t2)';
    return activeModule ? t.colors[activeModule] : t.colors.simulator;
  }

  const activeModuleName = activeModule
    ? MODULES.find((m) => m.key === activeModule)?.key.toUpperCase() ?? ''
    : '';

  const navClass = themeTarget === 'fill' ? 'navbar--fill-mode' : themeTarget === 'mono' ? 'navbar--mono-mode' : '';

  return (
    <div
      className={navClass}
      style={{
        height: 46,
        background: 'var(--bg1)',
        borderBottom: '1px solid var(--line)',
        padding: '0 16px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexShrink: 0,
      }}
    >
      {/* ─── Left side: 3M · MODULE ▾ dropdown + pill row ────────────────────── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 0 }}>
      <div ref={modulesRef} style={{ position: 'relative' }}>
        <button
          onClick={() => { setModulesOpen((v) => !v); setThemeOpen(false); }}
          style={{
            background: 'transparent',
            border: 'none',
            cursor: 'pointer',
            display: 'inline-flex',
            alignItems: 'center',
            gap: 0,
            padding: 0,
            fontFamily: 'var(--font-space-mono), Space Mono, monospace',
          }}
        >
          <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--t0)' }}>3M</span>
          {activeModuleName && (
            <>
              <span style={{ fontSize: 9, color: 'var(--t3)', margin: '0 6px' }}>&middot;</span>
              <span style={{
                fontSize: 11,
                fontWeight: 700,
                color: themeTarget === 'fill' ? 'var(--t0)'
                  : (themeTarget === 'text' || themeTarget === 'mono') ? (accentColor || 'var(--t0)')
                  : 'var(--t0)',
              }}>
                {activeModuleName}
              </span>
            </>
          )}
          <span style={{ fontSize: 8, color: 'var(--t3)', marginLeft: 5 }}>
            {modulesOpen ? '\u25B4' : '\u25BE'}
          </span>
        </button>

        {modulesOpen && (
          <div
            className="navbar-theme-panel"
            style={{
              position: 'absolute',
              left: 0,
              top: 34,
              minWidth: 220,
              background: 'var(--bg0)',
              border: '1px solid var(--line)',
              borderRadius: 6,
              padding: 6,
              zIndex: 60,
              boxShadow: '0 10px 24px rgba(0,0,0,0.35)',
            }}
          >
            {MODULES.filter((item) => item.key !== 'admin' || isAdmin).map((item) => {
              const isActive = pathname.startsWith(item.href);
              const itemAccent = resolveAccent(themeId, item.key);
              return (
                <button
                  key={item.key}
                  onClick={() => {
                    setModulesOpen(false);
                    router.push(item.href);
                  }}
                  style={{
                    width: '100%',
                    height: 30,
                    border: 'none',
                    background: isActive ? 'var(--bg2)' : 'transparent',
                    textAlign: 'left',
                    padding: '0 10px',
                    borderRadius: 4,
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    gap: 10,
                    fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                  }}
                  onMouseEnter={(e) => { if (!isActive) e.currentTarget.style.background = 'var(--bg2)'; }}
                  onMouseLeave={(e) => { if (!isActive) e.currentTarget.style.background = 'transparent'; }}
                >
                  {/* Accent dot */}
                  <span style={{
                    width: 6, height: 6, borderRadius: '50%',
                    background: itemAccent || 'var(--t3)',
                    flexShrink: 0,
                  }} />
                  {/* Module name */}
                  <span style={{
                    flex: 1,
                    fontSize: 9,
                    fontWeight: 700,
                    letterSpacing: '0.08em',
                    textTransform: 'uppercase',
                    color: isActive ? 'var(--t0)' : 'var(--t3)',
                  }}>
                    {item.key}
                  </span>
                  {/* Shortcut badge */}
                  <span style={{
                    fontSize: 8,
                    fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                    color: 'var(--t3)',
                    background: 'var(--bg2)',
                    border: '1px solid var(--line)',
                    borderRadius: 3,
                    padding: '1px 5px',
                  }}>
                    {'\u2318'}{item.shortcut}
                  </span>
                </button>
              );
            })}
          </div>
        )}
      </div>

        <Divider />

        {/* ─── Module pill row ──────────────────────────────────────────────── */}
        {MODULES.filter((item) => item.key !== 'admin' || isAdmin).map((item) => {
          const isActive = pathname.startsWith(item.href);
          const pillAccent = resolveAccent(themeId, item.key);
          return (
            <button
              key={item.key}
              className={isActive ? 'navbar-pill-active' : ''}
              onClick={() => router.push(item.href)}
              style={{
                fontSize: 8,
                fontWeight: 700,
                letterSpacing: '0.06em',
                padding: '4px 8px',
                borderRadius: 4,
                border: isActive
                  ? `0.5px solid ${pillAccent}4D`   // 30% opacity
                  : '0.5px solid transparent',
                background: isActive
                  ? `${pillAccent}1A`                // 10% opacity
                  : 'transparent',
                color: isActive ? pillAccent : 'var(--t3)',
                cursor: 'pointer',
                fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                transition: 'all 0.15s ease',
              }}
              onMouseEnter={(e) => {
                if (!isActive) {
                  e.currentTarget.style.color = 'var(--t2)';
                  e.currentTarget.style.background = 'var(--bg2)';
                }
              }}
              onMouseLeave={(e) => {
                if (!isActive) {
                  e.currentTarget.style.color = 'var(--t3)';
                  e.currentTarget.style.background = 'transparent';
                }
              }}
            >
              {item.pill}
            </button>
          );
        })}
      </div>

      {/* ─── Right side ─────────────────────────────────────────────────────── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 0 }}>

        {/* ADMIN MODE pill — visible only inside /admin/*. The
            distinct amber pill makes it visually obvious when an admin
            is operating with elevated scope versus their regular
            module workflow. */}
        {pathname.startsWith('/admin') && isAdmin && (
          <>
            <span
              title="Admin operations are audit-logged"
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 6,
                padding: '4px 10px',
                background: 'rgba(240, 165, 0, 0.12)',
                border: '1px solid var(--amber)',
                borderRadius: 2,
                color: 'var(--amber)',
                fontSize: 9,
                letterSpacing: '0.16em',
                textTransform: 'uppercase',
                fontWeight: 700,
                marginRight: 8,
              }}
            >
              <span
                style={{
                  width: 6,
                  height: 6,
                  borderRadius: '50%',
                  background: 'var(--amber)',
                  animation: 'pulse-dot 2s ease-in-out infinite',
                }}
              />
              Admin Mode
            </span>
            <Divider />
          </>
        )}

        {/* Total exchange equity — only when authed */}
        {equity != null && (
          <>
            <span
              title="Total equity across connected exchanges"
              style={{
                marginRight: 8, padding: '0 6px',
                fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                fontSize: 11, color: 'var(--t0)', fontWeight: 400,
              }}
            >${Math.round(equity).toLocaleString('en-US')}</span>
            <Divider />
          </>
        )}

        {/* Fullscreen toggle — borderless */}
        <button
          className="navbar-btn"
          type="button"
          onClick={() => { void toggleFullscreen(); }}
          title={isFullscreen ? 'Exit fullscreen' : 'Enter fullscreen'}
          aria-label={isFullscreen ? 'Exit fullscreen' : 'Enter fullscreen'}
          style={{
            width: 28, height: 28,
            border: 'none',
            borderRadius: 3,
            background: 'transparent',
            color: isFullscreen ? 'var(--t0)' : 'var(--t3)',
            fontSize: 12,
            cursor: 'pointer',
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            marginRight: 4,
          }}
        >
          {isFullscreen ? '\u2922' : '\u26F6'}
        </button>

        {/* Theme selector — borderless */}
        <div ref={themeRef} style={{ position: 'relative' }}>
          <button
            className="navbar-btn"
            onClick={() => { setThemeOpen((v) => !v); setModulesOpen(false); }}
            style={{
              height: 28,
              padding: '0 8px',
              border: 'none',
              borderRadius: 3,
              background: 'transparent',
              color: 'var(--t3)',
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: '0.08em',
              textTransform: 'uppercase',
              cursor: 'pointer',
              display: 'inline-flex',
              alignItems: 'center',
              gap: 5,
            }}
          >
            {accentColor && (
              <span style={{
                width: 6, height: 6, borderRadius: '50%',
                background: accentColor, flexShrink: 0,
              }} />
            )}
            Theme
          </button>
          {themeOpen && (
            <div
              className="navbar-theme-panel"
              style={{
                position: 'absolute',
                right: 0,
                top: 34,
                minWidth: 160,
                background: 'var(--bg2)',
                border: '1px solid var(--line2)',
                borderRadius: 3,
                padding: 6,
                zIndex: 60,
                boxShadow: '0 10px 24px rgba(0,0,0,0.35)',
              }}
            >
              {THEME_KEYS.map((id) => {
                const t = THEMES[id];
                const isActive = id === themeId;
                const isDisabled = t.disabled;
                return (
                  <button
                    key={id}
                    onClick={() => { if (!isDisabled) { selectTheme(id); setThemeOpen(false); } }}
                    style={{
                      width: '100%',
                      height: 30,
                      border: 'none',
                      background: isActive ? 'var(--bg3)' : 'transparent',
                      color: isDisabled ? 'var(--t2)' : 'var(--t1)',
                      textAlign: 'left',
                      padding: '0 8px',
                      fontSize: 10,
                      borderRadius: 2,
                      cursor: isDisabled ? 'default' : 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: 8,
                      opacity: isDisabled ? 0.4 : 1,
                    }}
                    onMouseEnter={(e) => { if (!isActive && !isDisabled) (e.currentTarget).style.background = 'var(--bg3)'; }}
                    onMouseLeave={(e) => { if (!isActive && !isDisabled) (e.currentTarget).style.background = 'transparent'; }}
                  >
                    <div style={{ display: 'flex', gap: 2, flexShrink: 0 }}>
                      {(['compiler', 'indexer', 'simulator', 'allocator', 'manager'] as ModuleKey[]).map((m) => (
                        <span
                          key={m}
                          style={{
                            width: 8, height: 8, borderRadius: 2,
                            background: isDisabled ? 'var(--bg4)' : t.colors[m],
                          }}
                        />
                      ))}
                    </div>
                    <span style={{ flex: 1 }}>{t.label}</span>
                    {isActive && <span style={{ color: 'var(--green)', fontSize: 11 }}>{'\u2713'}</span>}
                    {isDisabled && <span style={{ fontSize: 8, color: 'var(--t2)', letterSpacing: '0.06em' }}>SOON</span>}
                  </button>
                );
              })}

              {/* Color target toggle */}
              <div style={{ borderTop: '1px solid var(--line)', marginTop: 6, paddingTop: 8, padding: '8px 8px 4px' }}>
                <div className="theme-label" style={{ fontSize: 8, color: 'var(--t2)', letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 6 }}>COLOR TARGET</div>
                <div style={{ display: 'flex', border: '1px solid var(--line)', borderRadius: 4, overflow: 'hidden' }}>
                  {(['none', 'text', 'fill', 'mono'] as ThemeTarget[]).map((t) => (
                    <button
                      key={t}
                      className={themeTarget === t ? 'theme-target-btn-active' : 'theme-target-btn'}
                      onClick={() => selectTarget(t)}
                      style={{
                        flex: 1, padding: '4px 0',
                        fontSize: 9, fontWeight: 700, letterSpacing: '0.08em',
                        textTransform: 'uppercase',
                        background: themeTarget === t ? 'var(--bg4)' : 'transparent',
                        color: themeTarget === t ? 'var(--t0)' : 'var(--t3)',
                        border: 'none', cursor: 'pointer',
                      }}
                    >{t}</button>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>

        <Divider />

        {/* User menu — initials trigger + Account / Sign out dropdown.
            Replaces the standalone SIGN OUT button so users have one
            persistent identity surface (initials confirm "you're signed
            in as the right account") without burying sign-out where
            J can't find it for multi-account testing.
            Admin users get an amber initials block; everyone else is
            allocator-purple. */}
        {user && (
          <div ref={userMenuRef} style={{ position: 'relative' }}>
            <button
              type="button"
              onClick={() => {
                setUserMenuOpen((v) => !v);
                setModulesOpen(false);
                setThemeOpen(false);
              }}
              aria-haspopup="menu"
              aria-expanded={userMenuOpen}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 8,
                padding: '4px 8px 4px 4px',
                border: `1px solid ${userMenuOpen ? (user.is_admin ? 'var(--amber)' : '#A78BFF') : 'var(--line)'}`,
                borderRadius: 3,
                background: userMenuOpen ? (user.is_admin ? 'rgba(240, 165, 0, 0.08)' : 'rgba(167, 139, 255, 0.08)') : 'transparent',
                cursor: 'pointer',
                fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                transition: 'all 0.15s ease',
              }}
              onMouseEnter={(e) => {
                if (!userMenuOpen) {
                  e.currentTarget.style.borderColor = 'var(--line2)';
                  e.currentTarget.style.background = 'var(--bg2)';
                }
              }}
              onMouseLeave={(e) => {
                if (!userMenuOpen) {
                  e.currentTarget.style.borderColor = 'var(--line)';
                  e.currentTarget.style.background = 'transparent';
                }
              }}
            >
              <span
                aria-hidden="true"
                style={{
                  width: 22, height: 22,
                  borderRadius: 2,
                  background: user.is_admin ? 'var(--amber)' : '#A78BFF',
                  color: user.is_admin ? '#1a0f00' : '#0d0518',
                  fontSize: 9,
                  fontWeight: 700,
                  letterSpacing: '0.04em',
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  flexShrink: 0,
                }}
              >
                {userInitials(user)}
              </span>
              <span style={{
                fontSize: 8,
                color: userMenuOpen ? (user.is_admin ? 'var(--amber)' : '#A78BFF') : 'var(--t3)',
                transition: 'transform 0.15s ease',
                transform: userMenuOpen ? 'rotate(180deg)' : 'rotate(0deg)',
              }}>
                {'▾'}
              </span>
            </button>

            {userMenuOpen && (
              <div
                role="menu"
                style={{
                  position: 'absolute',
                  right: 0,
                  top: 34,
                  minWidth: 220,
                  background: 'var(--bg1)',
                  border: '1px solid var(--line2)',
                  borderRadius: 3,
                  zIndex: 60,
                  boxShadow: '0 12px 40px rgba(0,0,0,0.7)',
                  overflow: 'hidden',
                }}
              >
                <div style={{ padding: '14px 16px', borderBottom: '1px solid var(--line)' }}>
                  <div style={{ color: 'var(--t0)', fontSize: 13, fontWeight: 700, marginBottom: 2 }}>
                    {userDisplayName(user)}
                  </div>
                  <div style={{ color: 'var(--t3)', fontSize: 11 }}>{user.email}</div>
                </div>
                <div style={{ padding: '4px 0' }}>
                  <button
                    type="button"
                    role="menuitem"
                    onClick={() => { setUserMenuOpen(false); router.push('/account'); }}
                    onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg2)'; e.currentTarget.style.color = 'var(--t0)'; }}
                    onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--t1)'; }}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      width: '100%',
                      padding: '9px 16px',
                      border: 0,
                      background: 'transparent',
                      color: 'var(--t1)',
                      fontFamily: 'inherit',
                      fontSize: 11,
                      letterSpacing: '0.08em',
                      textTransform: 'uppercase',
                      cursor: 'pointer',
                      textAlign: 'left',
                      transition: 'all 0.1s ease',
                    }}
                  >
                    Account
                  </button>
                  <div style={{ height: 1, background: 'var(--line)', margin: '4px 0' }} />
                  <button
                    type="button"
                    role="menuitem"
                    onClick={() => { setUserMenuOpen(false); void signout(); }}
                    onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg2)'; e.currentTarget.style.color = 'var(--red)'; }}
                    onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--t3)'; }}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      width: '100%',
                      padding: '9px 16px',
                      border: 0,
                      background: 'transparent',
                      color: 'var(--t3)',
                      fontFamily: 'inherit',
                      fontSize: 11,
                      letterSpacing: '0.08em',
                      textTransform: 'uppercase',
                      cursor: 'pointer',
                      textAlign: 'left',
                      transition: 'all 0.1s ease',
                    }}
                  >
                    Sign out
                  </button>
                </div>
              </div>
            )}
          </div>
        )}

      </div>
    </div>
  );
}

'use client';

import { useEffect, useRef, useState, useCallback } from 'react';
import { useRouter, usePathname } from 'next/navigation';

// ─── Theme definitions ───────────────────────────────────────────────────────

type ModuleKey = 'compiler' | 'indexer' | 'simulator' | 'allocator' | 'manager';

interface ThemeDef {
  label: string;
  colors: Record<ModuleKey, string>;
  disabled?: boolean;
}

const THEMES: Record<string, ThemeDef> = {
  spectrum: {
    label: 'Spectrum',
    colors: {
      compiler: '#00E87A',
      indexer: '#38B4FF',
      simulator: '#C47DFF',
      allocator: '#FFB830',
      manager: '#FF5E5E',
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
    },
  },
  oxide: {
    label: 'Oxide',
    colors: { compiler: '', indexer: '', simulator: '', allocator: '', manager: '' },
    disabled: true,
  },
  polar: {
    label: 'Polar',
    colors: { compiler: '', indexer: '', simulator: '', allocator: '', manager: '' },
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
  swatch: string; // panel-level color for the preview swatch
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

const MODULES: Array<{ key: ModuleKey; icon: string; href?: string }> = [
  { key: 'compiler', icon: '</>' },
  { key: 'indexer', icon: '⌕' },
  { key: 'simulator', icon: '◴', href: '/simulator' },
  { key: 'allocator', icon: '⚙', href: '/trader' },
  { key: 'manager', icon: '▣' },
];

// ─── Helpers ─────────────────────────────────────────────────────────────────

function getActiveModule(pathname: string): ModuleKey | null {
  const match = MODULES.find((m) => m.href && pathname.startsWith(m.href));
  return match?.key ?? null;
}

function applyAccent(themeId: string, moduleKey: ModuleKey | null) {
  const theme = THEMES[themeId] ?? THEMES.spectrum;
  const color = moduleKey ? theme.colors[moduleKey] : '';
  document.documentElement.style.setProperty('--module-accent', color || 'var(--t0)');
}

// ─── Component ───────────────────────────────────────────────────────────────

export default function Topbar() {
  const [modulesOpen, setModulesOpen] = useState(false);
  const [themeOpen, setThemeOpen] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [themeId, setThemeId] = useState('spectrum');
  const [themeTarget, setThemeTarget] = useState<ThemeTarget>('text');
  const [baseThemeId, setBaseThemeId] = useState('vault');
  const modulesRef = useRef<HTMLDivElement | null>(null);
  const themeRef = useRef<HTMLDivElement | null>(null);
  const router = useRouter();
  const pathname = usePathname();

  const activeModule = getActiveModule(pathname);
  const theme = THEMES[themeId] ?? THEMES.spectrum;
  const accentColor = activeModule ? theme.colors[activeModule] : '';

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
    }
    function onEsc(e: KeyboardEvent) {
      if (e.key === 'Escape') { setModulesOpen(false); setThemeOpen(false); }
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

  // Preview swatch color — use the allocator color as the representative dot for each theme
  function getThemePreviewColor(id: string): string {
    const t = THEMES[id];
    if (!t || t.disabled) return 'var(--t2)';
    // Show a gradient of all 5 module colors — but for a single swatch, use the active module or simulator
    return activeModule ? t.colors[activeModule] : t.colors.simulator;
  }

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
      {/* Logo — accent colored module name */}
      <span className="navbar-title" style={{ fontSize: 14, fontWeight: 700, letterSpacing: '0.05em' }}>
        <span>3M </span>
        <span style={{
          color: themeTarget === 'fill' ? undefined
            : (themeTarget === 'text' || themeTarget === 'mono') ? (accentColor || 'var(--t0)')
            : 'var(--t0)',
        }}>
          {activeModule?.toUpperCase() || 'CAPITAL'}
        </span>
      </span>

      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>

        {/* Fullscreen toggle — icon only */}
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
            background: isFullscreen ? 'var(--bg3)' : 'transparent',
            color: isFullscreen ? 'var(--t0)' : 'var(--t2)',
            fontSize: 12,
            cursor: 'pointer',
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          {isFullscreen ? '\u2922' : '\u26F6'}
        </button>

        {/* Theme selector */}
        <div ref={themeRef} style={{ position: 'relative' }}>
          <button
            className="navbar-btn"
            onClick={() => { setThemeOpen((v) => !v); setModulesOpen(false); }}
            style={{
              height: 28,
              padding: '0 10px',
              border: '1px solid var(--line2)',
              borderRadius: 3,
              background: 'var(--bg1)',
              color: 'var(--t1)',
              fontSize: 9,
              letterSpacing: '0.08em',
              textTransform: 'uppercase',
              cursor: 'pointer',
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
            }}
          >
            {/* Accent dot */}
            {accentColor && (
              <span style={{
                width: 6, height: 6, borderRadius: '50%',
                background: accentColor,
                flexShrink: 0,
              }} />
            )}
            Theme
          </button>
          {themeOpen && (
            <div
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
                    {/* 5-color swatch strip */}
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
                <div style={{ fontSize: 8, color: 'var(--t2)', letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 6 }}>COLOR TARGET</div>
                <div style={{ display: 'flex', border: '1px solid var(--line)', borderRadius: 4, overflow: 'hidden' }}>
                  {(['none', 'text', 'fill', 'mono'] as ThemeTarget[]).map((t) => (
                    <button
                      key={t}
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

        {/* Modules dropdown */}
        <div ref={modulesRef} style={{ position: 'relative' }}>
          <button
            className="navbar-btn"
            onClick={() => { setModulesOpen((v) => !v); setThemeOpen(false); }}
            style={{
              height: 28,
              padding: '0 10px',
              border: '1px solid var(--line2)',
              borderRadius: 3,
              background: 'var(--bg1)',
              color: 'var(--t1)',
              fontSize: 9,
              letterSpacing: '0.08em',
              textTransform: 'uppercase',
              cursor: 'pointer',
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
            }}
          >
            <span style={{ opacity: 0.85 }}>☰</span>
            {activeModule ?? 'Modules'} {modulesOpen ? '▴' : '▾'}
          </button>
          {modulesOpen && (
            <div
              style={{
                position: 'absolute',
                right: 0,
                top: 34,
                minWidth: 140,
                background: 'var(--bg2)',
                border: '1px solid var(--line2)',
                borderRadius: 3,
                padding: 4,
                zIndex: 60,
                boxShadow: '0 10px 24px rgba(0,0,0,0.35)',
              }}
            >
              {MODULES.map((item) => {
                const isActive = item.href && pathname.startsWith(item.href);
                const itemAccent = theme.colors[item.key];
                return (
                  <button
                    key={item.key}
                    onClick={() => {
                      setModulesOpen(false);
                      if (item.href) router.push(item.href);
                    }}
                    style={{
                      width: '100%',
                      height: 28,
                      border: 'none',
                      background: isActive ? 'var(--bg3)' : 'transparent',
                      color: isActive ? itemAccent : 'var(--t1)',
                      textAlign: 'left',
                      padding: '0 8px',
                      fontSize: 10,
                      textTransform: 'capitalize',
                      borderRadius: 2,
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: 8,
                    }}
                    onMouseEnter={(e) => { if (!isActive) (e.currentTarget).style.background = 'var(--bg3)'; }}
                    onMouseLeave={(e) => { if (!isActive) (e.currentTarget).style.background = 'transparent'; }}
                  >
                    <span
                      style={{
                        width: 14,
                        textAlign: 'center',
                        color: isActive ? itemAccent : 'var(--t2)',
                        fontSize: 10,
                      }}
                    >
                      {item.icon}
                    </span>
                    <span>{item.key}</span>
                  </button>
                );
              })}
            </div>
          )}
        </div>

      </div>
    </div>
  );
}

import { useCallback, useEffect, useState } from 'react';

type Theme = 'system' | 'light' | 'dark';

const STORAGE_KEY = 'flowcus:theme';

function getStored(): Theme {
  const v = localStorage.getItem(STORAGE_KEY);
  if (v === 'light' || v === 'dark' || v === 'system') return v;
  return 'system';
}

function applyTheme(theme: Theme) {
  const resolved = theme === 'system'
    ? (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light')
    : theme;
  document.documentElement.setAttribute('data-theme', resolved);
}

export function ThemeSwitcher() {
  const [theme, setTheme] = useState<Theme>(getStored);

  const cycle = useCallback(() => {
    setTheme((prev) => {
      const next: Theme = prev === 'system' ? 'light' : prev === 'light' ? 'dark' : 'system';
      localStorage.setItem(STORAGE_KEY, next);
      applyTheme(next);
      return next;
    });
  }, []);

  // Apply on mount + listen for system changes
  useEffect(() => {
    applyTheme(theme);
    const mq = window.matchMedia('(prefers-color-scheme: dark)');
    const handler = () => { if (theme === 'system') applyTheme('system'); };
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, [theme]);

  const icon = theme === 'light' ? '\u2600' : theme === 'dark' ? '\u263E' : '\u25D1';
  const label = theme === 'system' ? 'System' : theme === 'light' ? 'Light' : 'Dark';

  return (
    <button className="theme-switcher" onClick={cycle} title={`Theme: ${label} \u2014 click to cycle`}>
      {icon}
    </button>
  );
}

/** Call once on app startup before React renders, to avoid flash of wrong theme. */
export function initTheme() {
  applyTheme(getStored());
}

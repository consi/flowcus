import { useCallback, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';

interface AboutDialogProps {
  open: boolean;
  onClose: () => void;
  version?: string;
}

export function AboutDialog({ open, onClose, version }: AboutDialogProps) {
  const modalRef = useRef<HTMLDivElement>(null);

  const handleOverlayClick = useCallback(
    (e: React.MouseEvent) => {
      if (modalRef.current && !modalRef.current.contains(e.target as Node)) {
        onClose();
      }
    },
    [onClose],
  );

  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [open, onClose]);

  if (!open) return null;

  return createPortal(
    <div className="settings-overlay" onClick={handleOverlayClick}>
      <div className="about-modal" ref={modalRef}>
        <div className="settings-header">
          <h2>About Flowcus</h2>
          <button className="settings-close" onClick={onClose} title="Close">
            {'\u00D7'}
          </button>
        </div>
        <div className="about-body">
          <div className="about-logo">
            <svg viewBox="208 208 290 290" xmlns="http://www.w3.org/2000/svg" width="200" height="200">
              <g transform="translate(340,340)" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="-20" cy="-20" r="100" strokeWidth="10" />
                <line x1="62" y1="62" x2="135" y2="135" strokeWidth="18" />
                <g fill="currentColor" stroke="none">
                  <circle cx="10" cy="-80" r="7" />
                  <circle cx="-10" cy="-80" r="4.5" opacity="0.5" />
                  <circle cx="-23" cy="-80" r="2.5" opacity="0.2" />
                  <circle cx="-50" cy="-48" r="7" />
                  <circle cx="-70" cy="-48" r="4.5" opacity="0.5" />
                  <circle cx="-83" cy="-48" r="2.5" opacity="0.2" />
                  <circle cx="42" cy="-52" r="7" />
                  <circle cx="22" cy="-52" r="4.5" opacity="0.5" />
                  <circle cx="9" cy="-52" r="2.5" opacity="0.2" />
                  <circle cx="-28" cy="-18" r="7" />
                  <circle cx="-48" cy="-18" r="4.5" opacity="0.5" />
                  <circle cx="-61" cy="-18" r="2.5" opacity="0.2" />
                  <circle cx="50" cy="-22" r="7" />
                  <circle cx="30" cy="-22" r="4.5" opacity="0.5" />
                  <circle cx="17" cy="-22" r="2.5" opacity="0.2" />
                  <circle cx="-58" cy="12" r="7" />
                  <circle cx="-78" cy="12" r="4.5" opacity="0.5" />
                  <circle cx="30" cy="8" r="7" />
                  <circle cx="10" cy="8" r="4.5" opacity="0.5" />
                  <circle cx="-3" cy="8" r="2.5" opacity="0.2" />
                  <circle cx="2" cy="40" r="7" />
                  <circle cx="-18" cy="40" r="4.5" opacity="0.5" />
                  <circle cx="-31" cy="40" r="2.5" opacity="0.2" />
                  <circle cx="-15" cy="62" r="7" />
                  <circle cx="-35" cy="62" r="4.5" opacity="0.5" />
                </g>
              </g>
            </svg>
          </div>
          <p className="about-description">
            Standalone collector and browser for IPFIX and NetFlow v5/v9 network flows.
            Captures, stores, and queries flow data in a single binary.
          </p>
          {version && <p className="about-version">Version {version}</p>}
          <div className="about-links">
            <a href="https://github.com/consi" target="_blank" rel="noopener noreferrer">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0 0 24 12c0-6.63-5.37-12-12-12z" />
              </svg>
              GitHub
            </a>
            <a href="https://www.linkedin.com/in/marek-wajdzik" target="_blank" rel="noopener noreferrer">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 0 1-2.063-2.065 2.064 2.064 0 1 1 2.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z" />
              </svg>
              LinkedIn
            </a>
            <a href="mailto:marek@jest.pro">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <rect x="2" y="4" width="20" height="16" rx="2" />
                <path d="M22 7l-10 7L2 7" />
              </svg>
              marek@jest.pro
            </a>
          </div>
          <p className="about-copyright">Marek Wajdzik &copy; 2026</p>
        </div>
      </div>
    </div>,
    document.body,
  );
}

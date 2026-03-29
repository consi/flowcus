import { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import {
  fetchSettings,
  fetchSettingsSchema,
  fetchSettingsDefaults,
  fetchHealth,
  saveSettings,
  restartServer,
  type SettingsSchema,
  type SettingField,
  type SettingsValidation,
} from './api';

// --- Helpers ---

function formatBytes(bytes: number): string {
  if (bytes >= 1073741824) return `${(bytes / 1073741824).toFixed(1)} GB`;
  if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
}

function formatDuration(secs: number): string {
  if (secs >= 86400) return `${(secs / 86400).toFixed(1)}d`;
  if (secs >= 3600) return `${(secs / 3600).toFixed(1)}h`;
  if (secs >= 60) return `${(secs / 60).toFixed(0)}m`;
  return `${secs}s`;
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return false;
  if (typeof a !== 'object') return false;
  const aObj = a as Record<string, unknown>;
  const bObj = b as Record<string, unknown>;
  const keys = new Set([...Object.keys(aObj), ...Object.keys(bObj)]);
  for (const key of keys) {
    if (!deepEqual(aObj[key], bObj[key])) return false;
  }
  return true;
}

// --- Field rendering ---

interface FieldInputProps {
  field: SettingField;
  value: unknown;
  onChange: (value: unknown) => void;
}

function FieldInput({ field, value, onChange }: FieldInputProps) {
  const ctrl = field.control;

  switch (ctrl.type) {
    case 'bool':
      return (
        <label className="settings-toggle">
          <input
            type="checkbox"
            checked={Boolean(value)}
            onChange={(e) => onChange(e.target.checked)}
          />
          <span className="settings-toggle-slider" />
        </label>
      );

    case 'select':
      return (
        <select
          value={String(value ?? '')}
          onChange={(e) => onChange(e.target.value)}
        >
          {ctrl.options?.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
      );

    case 'slider':
      return (
        <div className="slider-container">
          <input
            type="range"
            min={ctrl.min ?? 0}
            max={ctrl.max ?? 100}
            step={ctrl.step ?? 1}
            value={Number(value ?? 0)}
            onChange={(e) => onChange(Number(e.target.value))}
          />
          <span className="slider-value">{String(value)}</span>
        </div>
      );

    case 'bytes':
      return (
        <div className="input-with-unit">
          <input
            type="number"
            min={ctrl.min}
            max={ctrl.max}
            step={ctrl.step ?? 1}
            value={Number(value ?? 0)}
            onChange={(e) => onChange(Number(e.target.value))}
          />
          <span className="input-unit">{formatBytes(Number(value ?? 0))}</span>
        </div>
      );

    case 'duration':
      return (
        <div className="input-with-unit">
          <input
            type="number"
            min={ctrl.min_secs ?? ctrl.min}
            max={ctrl.max_secs ?? ctrl.max}
            step={ctrl.step ?? 1}
            value={Number(value ?? 0)}
            onChange={(e) => onChange(Number(e.target.value))}
          />
          <span className="input-unit">{formatDuration(Number(value ?? 0))}</span>
        </div>
      );

    case 'number':
      return (
        <div className="input-with-unit">
          <input
            type="number"
            min={ctrl.min}
            max={ctrl.max}
            step={ctrl.step ?? 1}
            value={Number(value ?? 0)}
            onChange={(e) => onChange(Number(e.target.value))}
          />
          {ctrl.unit && <span className="input-unit">{ctrl.unit}</span>}
        </div>
      );

    case 'text':
    default:
      return (
        <input
          type="text"
          value={String(value ?? '')}
          onChange={(e) => onChange(e.target.value)}
        />
      );
  }
}

// --- Settings Field ---

interface SettingsFieldRowProps {
  field: SettingField;
  value: unknown;
  onChange: (value: unknown) => void;
  error?: string;
  warning?: string;
}

function SettingsFieldRow({ field, value, onChange, error, warning }: SettingsFieldRowProps) {
  return (
    <div className="settings-field">
      <div className="settings-field-label">
        {field.label}
        {field.restart_required && (
          <span className="settings-restart-badge">needs restart</span>
        )}
      </div>
      <div className="settings-field-input">
        <FieldInput field={field} value={value} onChange={onChange} />
      </div>
      {error && <div className="settings-field-error">{error}</div>}
      {warning && <div className="settings-field-warning">{warning}</div>}
      {field.description && (
        <div className="settings-field-description">{field.description}</div>
      )}
      {field.guidance && (
        <div className="settings-field-guidance">{field.guidance}</div>
      )}
    </div>
  );
}

// --- Main component ---

interface SettingsPanelProps {
  open: boolean;
  onClose: () => void;
}

type ConfigState = Record<string, Record<string, unknown>>;

export function SettingsPanel({ open, onClose }: SettingsPanelProps) {
  const [schema, setSchema] = useState<SettingsSchema | null>(null);
  const [config, setConfig] = useState<ConfigState | null>(null);
  const [savedConfig, setSavedConfig] = useState<ConfigState | null>(null);
  const [activeSection, setActiveSection] = useState<string>('');
  const [saving, setSaving] = useState(false);
  const [validation, setValidation] = useState<SettingsValidation | null>(null);
  const [restartRequired, setRestartRequired] = useState<string[]>([]);
  const [restarting, setRestarting] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  const modalRef = useRef<HTMLDivElement>(null);

  // Load schema + config when opened
  useEffect(() => {
    if (!open) return;
    setLoadError(null);

    Promise.all([fetchSettingsSchema(), fetchSettings()])
      .then(([s, c]) => {
        setSchema(s);
        setConfig(structuredClone(c));
        setSavedConfig(structuredClone(c));
        if (s.sections.length > 0 && !activeSection) {
          setActiveSection(s.sections[0].key);
        }
      })
      .catch((err) => {
        setLoadError(err instanceof Error ? err.message : String(err));
      });
  }, [open]);

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [open, onClose]);

  // Dirty tracking
  const isDirty = config !== null && savedConfig !== null && !deepEqual(config, savedConfig);

  const handleFieldChange = useCallback(
    (section: string, key: string, value: unknown) => {
      setConfig((prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          [section]: { ...prev[section], [key]: value },
        };
      });
    },
    [],
  );

  const handleSave = useCallback(async () => {
    if (!config) return;
    setSaving(true);
    setValidation(null);
    try {
      const res = await saveSettings(config);
      setConfig(structuredClone(res.config));
      setSavedConfig(structuredClone(res.config));
      setValidation(res.validation);
      if (res.restart_required.length > 0) {
        setRestartRequired(res.restart_required);
      }
    } catch (err: unknown) {
      if (err && typeof err === 'object' && 'errors' in err) {
        // Validation errors from the server
        setValidation(err as SettingsValidation);
      }
    } finally {
      setSaving(false);
    }
  }, [config]);

  const handleResetDefaults = useCallback(async () => {
    if (!confirm('Reset all settings to their default values?')) return;
    try {
      const defaults = await fetchSettingsDefaults();
      setConfig(structuredClone(defaults));
    } catch {
      // silently fail
    }
  }, []);

  const handleRestart = useCallback(async () => {
    setRestarting(true);
    try {
      await restartServer();
    } catch {
      // Server will drop the connection during restart, which is expected
    }

    // Wait at least 1 second, then poll health
    const startTime = Date.now();
    const poll = () => {
      setTimeout(async () => {
        try {
          await fetchHealth();
          if (Date.now() - startTime >= 1000) {
            window.location.reload();
          } else {
            // Wait a bit more to meet the 1-second minimum
            setTimeout(() => window.location.reload(), 1000 - (Date.now() - startTime));
          }
        } catch {
          poll();
        }
      }, 500);
    };
    poll();
  }, []);

  const handleOverlayClick = useCallback(
    (e: React.MouseEvent) => {
      if (modalRef.current && !modalRef.current.contains(e.target as Node)) {
        onClose();
      }
    },
    [onClose],
  );

  // Find active section data
  const currentSection = schema?.sections.find((s) => s.key === activeSection);

  // Validation lookup helpers
  const getFieldError = (section: string, field: string): string | undefined =>
    validation?.errors.find((e) => e.section === section && e.field === field)?.message;
  const getFieldWarning = (section: string, field: string): string | undefined =>
    validation?.warnings.find((w) => w.section === section && w.field === field)?.message;

  if (!open) return null;

  return createPortal(
    <>
      <div className="settings-overlay" onClick={handleOverlayClick}>
        <div className="settings-modal" ref={modalRef}>
          <div className="settings-header">
            <h2>Settings</h2>
            <button className="settings-close" onClick={onClose} title="Close">
              {'\u00D7'}
            </button>
          </div>

          {restartRequired.length > 0 && (
            <div className="settings-restart-banner">
              <span>
                Some changes require a server restart: {restartRequired.join(', ')}
              </span>
              <button
                className="settings-btn settings-btn-ghost"
                onClick={handleRestart}
              >
                Restart
              </button>
            </div>
          )}

          {loadError ? (
            <div className="settings-loading">Failed to load settings: {loadError}</div>
          ) : !schema || !config ? (
            <div className="settings-loading">Loading settings...</div>
          ) : (
            <div className="settings-body">
              <nav className="settings-nav">
                {schema.sections.map((section) => (
                  <button
                    key={section.key}
                    className={`settings-nav-item ${activeSection === section.key ? 'active' : ''}`}
                    onClick={() => setActiveSection(section.key)}
                  >
                    {section.label}
                  </button>
                ))}
              </nav>

              <div className="settings-content">
                {currentSection && (
                  <>
                    {currentSection.description && (
                      <div className="settings-section-desc">
                        {currentSection.description}
                      </div>
                    )}
                    {currentSection.fields.filter((f) => !f.key.startsWith('unprocessed_')).map((field) => (
                      <SettingsFieldRow
                        key={field.key}
                        field={field}
                        value={config[field.section]?.[field.key] ?? field.default_value}
                        onChange={(v) => handleFieldChange(field.section, field.key, v)}
                        error={getFieldError(field.section, field.key)}
                        warning={getFieldWarning(field.section, field.key)}
                      />
                    ))}
                  </>
                )}
              </div>
            </div>
          )}

          <div className="settings-footer">
            <div className="settings-footer-left">
              <button
                className="settings-btn settings-btn-danger"
                onClick={handleResetDefaults}
              >
                Reset to Defaults
              </button>
            </div>
            <div className="settings-footer-right">
              <button
                className="settings-btn settings-btn-ghost"
                onClick={handleRestart}
              >
                Restart Server
              </button>
              <button className="settings-btn settings-btn-ghost" onClick={onClose}>
                Cancel
              </button>
              <button
                className="settings-btn settings-btn-primary"
                disabled={!isDirty || saving}
                onClick={handleSave}
              >
                {saving ? 'Saving...' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      </div>

      {restarting && (
        <div className="restart-overlay">
          <div className="restart-spinner" />
          <div className="restart-overlay-text">Restarting server...</div>
        </div>
      )}
    </>,
    document.body,
  );
}

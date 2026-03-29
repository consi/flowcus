import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import type { QueryColumn } from './api';
import { selectVisibleColumns } from './formatters';

interface ColumnConfigProps {
  columns: QueryColumn[];
  visibleColumns: string[];
  onChange: (columns: string[]) => void;
}

export function ColumnConfig({ columns, visibleColumns, onChange }: ColumnConfigProps) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const triggerRef = useRef<HTMLButtonElement>(null);
  const popoverRef = useRef<HTMLDivElement>(null);
  const [popoverStyle, setPopoverStyle] = useState<React.CSSProperties>({});

  // Position the popover relative to the trigger button, and reposition on scroll
  useEffect(() => {
    if (!open || !triggerRef.current) return;

    const reposition = () => {
      const el = triggerRef.current;
      if (!el) return;
      const rect = el.getBoundingClientRect();
      const popoverW = 340;
      const popoverMaxH = 420;

      let left = rect.left;
      let top = rect.bottom + 4;

      if (left + popoverW > window.innerWidth - 8) {
        left = window.innerWidth - popoverW - 8;
      }
      if (left < 8) left = 8;
      if (top + popoverMaxH > window.innerHeight - 8) {
        top = rect.top - popoverMaxH - 4;
        if (top < 8) top = 8;
      }

      setPopoverStyle({ top, left });
    };

    reposition();

    // Reposition on scroll/resize so the popover tracks the trigger
    window.addEventListener('scroll', reposition, true);
    window.addEventListener('resize', reposition);
    return () => {
      window.removeEventListener('scroll', reposition, true);
      window.removeEventListener('resize', reposition);
    };
  }, [open]);

  // Close on outside click or Escape
  useEffect(() => {
    if (!open) return;
    const handleClick = (e: MouseEvent) => {
      const target = e.target as Node;
      if (
        popoverRef.current && !popoverRef.current.contains(target) &&
        triggerRef.current && !triggerRef.current.contains(target)
      ) {
        setOpen(false);
      }
    };
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', handleClick);
    document.addEventListener('keydown', handleKey);
    return () => {
      document.removeEventListener('mousedown', handleClick);
      document.removeEventListener('keydown', handleKey);
    };
  }, [open]);

  const visibleSet = useMemo(() => new Set(visibleColumns), [visibleColumns]);

  const filteredColumns = useMemo(() => {
    if (!search) return columns;
    const q = search.toLowerCase();
    return columns.filter((c) => c.name.toLowerCase().includes(q));
  }, [columns, search]);

  const toggleColumn = useCallback(
    (name: string) => {
      if (visibleSet.has(name)) {
        onChange(visibleColumns.filter((c) => c !== name));
      } else {
        onChange([...visibleColumns, name]);
      }
    },
    [visibleColumns, visibleSet, onChange],
  );

  const selectAll = useCallback(() => {
    onChange(columns.map((c) => c.name));
  }, [columns, onChange]);

  const resetToDefault = useCallback(() => {
    const defaultIndices = selectVisibleColumns(columns);
    onChange(defaultIndices.map((i) => columns[i].name));
  }, [columns, onChange]);

  return (
    <>
      <button
        ref={triggerRef}
        className={`column-config-trigger ${open ? 'active' : ''}`}
        onClick={() => { setOpen(!open); setSearch(''); }}
        title="Configure visible columns"
      >{'\u2699'}</button>

      {open && createPortal(
        <div className="column-config-popover" ref={popoverRef} style={popoverStyle}>
          <div className="column-config-header">
            <span className="column-config-title">Columns</span>
            <span className="column-config-count">
              {visibleColumns.length}/{columns.length}
            </span>
          </div>
          <input
            type="text"
            className="column-config-search"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search columns..."
            autoFocus
          />
          <div className="column-config-list">
            {filteredColumns.map((col) => (
              <label key={col.name} className="column-config-item">
                <input
                  type="checkbox"
                  checked={visibleSet.has(col.name)}
                  onChange={() => toggleColumn(col.name)}
                />
                <span className="column-config-name">{col.name}</span>
                <span className="column-config-type">{col.type}</span>
              </label>
            ))}
            {filteredColumns.length === 0 && (
              <div className="column-config-empty">No matching columns</div>
            )}
          </div>
          <div className="column-config-actions">
            <button onClick={selectAll}>Select all</button>
            <button onClick={resetToDefault}>Reset</button>
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}

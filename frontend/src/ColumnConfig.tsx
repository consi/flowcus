import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import type { QueryColumn } from './api';
import { DEFAULT_COLUMNS } from './formatters';

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
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [dragOverIndex, setDragOverIndex] = useState<number | null>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const touchActiveRef = useRef(false);

  const isMobile = typeof window !== 'undefined' && 'ontouchstart' in window;

  // Position the popover relative to the trigger button, and reposition on scroll
  useEffect(() => {
    if (!open || !triggerRef.current) return;

    const reposition = () => {
      // On mobile, go full-screen
      if (window.innerWidth <= 480) {
        setPopoverStyle({ top: 0, left: 0, width: '100vw', height: '100vh', maxHeight: '100vh', borderRadius: 0 });
        return;
      }
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

  // Build ordered list: visible columns first (in their order), then unchecked columns
  const orderedColumns = useMemo(() => {
    const colMap = new Map(columns.map((c) => [c.name, c]));
    const visible: QueryColumn[] = [];
    for (const name of visibleColumns) {
      const col = colMap.get(name);
      if (col) visible.push(col);
    }
    const unchecked = columns.filter((c) => !visibleSet.has(c.name));
    return { visible, unchecked };
  }, [columns, visibleColumns, visibleSet]);

  const filteredVisible = useMemo(() => {
    if (!search) return orderedColumns.visible;
    const q = search.toLowerCase();
    return orderedColumns.visible.filter((c) => c.name.toLowerCase().includes(q));
  }, [orderedColumns.visible, search]);

  const filteredUnchecked = useMemo(() => {
    if (!search) return orderedColumns.unchecked;
    const q = search.toLowerCase();
    return orderedColumns.unchecked.filter((c) => c.name.toLowerCase().includes(q));
  }, [orderedColumns.unchecked, search]);

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
    const rest = columns.filter((c) => !visibleSet.has(c.name)).map((c) => c.name);
    onChange([...visibleColumns, ...rest]);
  }, [columns, visibleColumns, visibleSet, onChange]);

  const resetToDefault = useCallback(() => {
    const availableSet = new Set(columns.map((c) => c.name));
    onChange(DEFAULT_COLUMNS.filter((name) => availableSet.has(name)));
  }, [columns, onChange]);

  // Commit reorder
  const commitReorder = useCallback((fromIdx: number, toIdx: number) => {
    if (fromIdx === toIdx) return;
    const next = [...visibleColumns];
    const [moved] = next.splice(fromIdx, 1);
    next.splice(toIdx, 0, moved);
    onChange(next);
  }, [visibleColumns, onChange]);

  // HTML5 drag-and-drop handlers (desktop)
  const handleDragStart = useCallback((e: React.DragEvent, index: number) => {
    setDragIndex(index);
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData('text/plain', String(index));
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent, index: number) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
    setDragOverIndex(index);
  }, []);

  const handleDragEnd = useCallback(() => {
    if (dragIndex !== null && dragOverIndex !== null) {
      commitReorder(dragIndex, dragOverIndex);
    }
    setDragIndex(null);
    setDragOverIndex(null);
  }, [dragIndex, dragOverIndex, commitReorder]);

  const handleDragLeave = useCallback(() => {
    setDragOverIndex(null);
  }, []);

  // Touch drag handlers (mobile) — attached to the drag handle
  const getVisibleIndexFromTouch = useCallback((touchY: number): number | null => {
    const list = listRef.current;
    if (!list) return null;
    const items = list.querySelectorAll<HTMLElement>('.column-config-item[data-vi]');
    for (const item of items) {
      const rect = item.getBoundingClientRect();
      if (touchY >= rect.top && touchY <= rect.bottom) {
        const vi = parseInt(item.dataset.vi!, 10);
        return isNaN(vi) ? null : vi;
      }
    }
    return null;
  }, []);

  const handleTouchStart = useCallback((e: React.TouchEvent, index: number) => {
    e.preventDefault();
    e.stopPropagation();
    touchActiveRef.current = true;
    setDragIndex(index);
    setDragOverIndex(index);
  }, []);

  const handleTouchMove = useCallback((e: React.TouchEvent) => {
    if (!touchActiveRef.current) return;
    e.preventDefault();
    const touch = e.touches[0];
    const overIdx = getVisibleIndexFromTouch(touch.clientY);
    if (overIdx !== null) {
      setDragOverIndex(overIdx);
    }
  }, [getVisibleIndexFromTouch]);

  const handleTouchEnd = useCallback(() => {
    if (!touchActiveRef.current) return;
    touchActiveRef.current = false;
    if (dragIndex !== null && dragOverIndex !== null) {
      commitReorder(dragIndex, dragOverIndex);
    }
    setDragIndex(null);
    setDragOverIndex(null);
  }, [dragIndex, dragOverIndex, commitReorder]);

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
            {isMobile && (
              <button className="column-config-close" onClick={() => setOpen(false)}>{'\u2715'}</button>
            )}
          </div>
          <input
            type="text"
            className="column-config-search"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search columns..."
            autoFocus={!isMobile}
          />
          <div className="column-config-list" ref={listRef}>
            {filteredVisible.map((col) => {
              const vi = visibleColumns.indexOf(col.name);
              const isDragging = dragIndex === vi;
              const isDragOver = dragOverIndex === vi && dragIndex !== vi;
              return (
                <label
                  key={col.name}
                  className={`column-config-item${isDragging ? ' dragging' : ''}${isDragOver ? ' drag-over' : ''}`}
                  data-vi={vi}
                  draggable={!search && !isMobile}
                  onDragStart={(e) => handleDragStart(e, vi)}
                  onDragOver={(e) => handleDragOver(e, vi)}
                  onDragEnd={handleDragEnd}
                  onDragLeave={handleDragLeave}
                >
                  <input
                    type="checkbox"
                    checked
                    onChange={() => toggleColumn(col.name)}
                  />
                  <span className="column-config-name">{col.name}</span>
                  <span className="column-config-type">{col.type}</span>
                  {!search && (
                    <span
                      className="column-config-drag-handle"
                      title="Drag to reorder"
                      onTouchStart={(e) => handleTouchStart(e, vi)}
                      onTouchMove={handleTouchMove}
                      onTouchEnd={handleTouchEnd}
                    >{'\u2807'}</span>
                  )}
                </label>
              );
            })}
            {filteredUnchecked.map((col) => (
              <label key={col.name} className="column-config-item">
                <input
                  type="checkbox"
                  checked={false}
                  onChange={() => toggleColumn(col.name)}
                />
                <span className="column-config-name">{col.name}</span>
                <span className="column-config-type">{col.type}</span>
              </label>
            ))}
            {filteredVisible.length === 0 && filteredUnchecked.length === 0 && (
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

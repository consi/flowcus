import { useCallback, useEffect, useRef, useState } from 'react';
import { tokenize, tokenClass } from './tokenizer';
import { useCompletions, type Completion } from './useCompletions';

interface QueryEditorProps {
  onExecute: (query: string) => void;
  loading: boolean;
  error: { error: string; position?: number; length?: number } | null;
}

export function QueryEditor({ onExecute, loading, error }: QueryEditorProps) {
  const [query, setQuery] = useState('last 1h | top 10 by sum(bytes)');
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const overlayRef = useRef<HTMLDivElement>(null);
  const completionRef = useRef<HTMLDivElement>(null);
  const { getCompletions } = useCompletions();

  const [completions, setCompletions] = useState<Completion[]>([]);
  const [completionWordStart, setCompletionWordStart] = useState(0);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [showCompletions, setShowCompletions] = useState(false);
  const [completionPos, setCompletionPos] = useState({ top: 0, left: 0 });

  const updateCompletions = useCallback(
    (text: string, cursorPos: number) => {
      const { items, wordStart } = getCompletions(text, cursorPos);
      setCompletions(items);
      setCompletionWordStart(wordStart);
      setSelectedIndex(0);
      setShowCompletions(items.length > 0);
    },
    [getCompletions],
  );

  const applyCompletion = useCallback(
    (item: Completion) => {
      const textarea = textareaRef.current;
      if (!textarea) return;
      const before = query.slice(0, completionWordStart);
      const after = query.slice(textarea.selectionStart);
      const suffix = item.kind === 'function' ? '(' : ' ';
      const newQuery = before + item.label + suffix + after;
      const newPos = completionWordStart + item.label.length + suffix.length;
      setQuery(newQuery);
      setShowCompletions(false);
      // Restore cursor position after React re-render
      requestAnimationFrame(() => {
        textarea.focus();
        textarea.setSelectionRange(newPos, newPos);
      });
    },
    [query, completionWordStart],
  );

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLTextAreaElement>) => {
      const text = e.target.value;
      setQuery(text);
      updateCompletions(text, e.target.selectionStart);
    },
    [updateCompletions],
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      // Ctrl+Enter or Cmd+Enter to execute
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        onExecute(query);
        setShowCompletions(false);
        return;
      }

      if (!showCompletions) return;

      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setSelectedIndex((i) => Math.min(i + 1, completions.length - 1));
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        setSelectedIndex((i) => Math.max(i - 1, 0));
      } else if (e.key === 'Tab' || e.key === 'Enter') {
        if (completions.length > 0) {
          e.preventDefault();
          applyCompletion(completions[selectedIndex]);
        }
      } else if (e.key === 'Escape') {
        setShowCompletions(false);
      }
    },
    [showCompletions, completions, selectedIndex, applyCompletion, onExecute, query],
  );

  // Position the completions dropdown near the caret
  const updateCaretPosition = useCallback(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    // Create a mirror to measure caret position
    const mirror = document.createElement('div');
    const style = window.getComputedStyle(textarea);
    mirror.style.position = 'absolute';
    mirror.style.visibility = 'hidden';
    mirror.style.whiteSpace = 'pre-wrap';
    mirror.style.wordWrap = 'break-word';
    mirror.style.font = style.font;
    mirror.style.padding = style.padding;
    mirror.style.border = style.border;
    mirror.style.width = style.width;
    mirror.style.lineHeight = style.lineHeight;
    mirror.style.letterSpacing = style.letterSpacing;

    const textBefore = query.slice(0, textarea.selectionStart);
    mirror.textContent = textBefore;
    const span = document.createElement('span');
    span.textContent = '|';
    mirror.appendChild(span);
    document.body.appendChild(mirror);

    const rect = textarea.getBoundingClientRect();
    const spanRect = span.getBoundingClientRect();
    const mirrorRect = mirror.getBoundingClientRect();

    setCompletionPos({
      top: spanRect.top - mirrorRect.top + rect.top + parseInt(style.lineHeight || '20') + 4 - textarea.scrollTop,
      left: spanRect.left - mirrorRect.left + rect.left - textarea.scrollLeft,
    });

    document.body.removeChild(mirror);
  }, [query]);

  useEffect(() => {
    if (showCompletions) {
      updateCaretPosition();
    }
  }, [showCompletions, updateCaretPosition]);

  // Sync scroll between textarea and overlay
  const handleScroll = useCallback(() => {
    if (textareaRef.current && overlayRef.current) {
      overlayRef.current.scrollTop = textareaRef.current.scrollTop;
      overlayRef.current.scrollLeft = textareaRef.current.scrollLeft;
    }
  }, []);

  // Build highlighted overlay
  const renderHighlight = useCallback(() => {
    const tokens = tokenize(query);
    return tokens.map((token, i) => {
      const cls = tokenClass(token.type);
      // Check if this token overlaps with an error position
      const hasError =
        error?.position !== undefined &&
        error.position !== null &&
        token.start < (error.position + (error.length ?? 1)) &&
        token.end > error.position;

      const className = [cls, hasError ? 'fql-error' : ''].filter(Boolean).join(' ');

      if (className) {
        return (
          <span key={i} className={className}>
            {token.value}
          </span>
        );
      }
      return token.value;
    });
  }, [query, error]);

  return (
    <div className="query-editor">
      <div className="query-editor-container">
        <div ref={overlayRef} className="query-editor-overlay" aria-hidden="true">
          <pre>{renderHighlight()}{'\n'}</pre>
        </div>
        <textarea
          ref={textareaRef}
          className="query-editor-textarea"
          value={query}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onScroll={handleScroll}
          onClick={() => {
            if (textareaRef.current) {
              updateCompletions(query, textareaRef.current.selectionStart);
            }
          }}
          spellCheck={false}
          autoComplete="off"
          autoCorrect="off"
          autoCapitalize="off"
          placeholder="Type an FQL query... (e.g. last 1h | top 10 by sum(bytes))"
          rows={4}
        />
      </div>

      {error && (
        <div className="query-error">
          <span className="query-error-icon">!</span>
          {error.error}
          {error.position !== undefined && (
            <span className="query-error-pos"> (at position {error.position})</span>
          )}
        </div>
      )}

      <div className="query-editor-actions">
        <button
          className="execute-btn"
          onClick={() => onExecute(query)}
          disabled={loading || query.trim().length === 0}
        >
          {loading ? (
            <>
              <span className="spinner" />
              Running...
            </>
          ) : (
            'Execute'
          )}
        </button>
        <span className="shortcut-hint">Ctrl+Enter</span>
      </div>

      {showCompletions && completions.length > 0 && (
        <div
          ref={completionRef}
          className="completions-dropdown"
          style={{ top: completionPos.top, left: completionPos.left }}
        >
          {completions.map((item, i) => (
            <div
              key={item.label}
              className={`completion-item ${i === selectedIndex ? 'selected' : ''}`}
              onMouseDown={(e) => {
                e.preventDefault();
                applyCompletion(item);
              }}
              onMouseEnter={() => setSelectedIndex(i)}
            >
              <span className={`completion-kind completion-kind-${item.kind}`}>
                {item.kind.slice(0, 3)}
              </span>
              <span className="completion-label">{item.label}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

use crate::ast::{
    AggCall, AggExpr, AggFunc, CompareOp, Duration, DurationPart, DurationUnit, FilterExpr,
    GroupByKey, IpDirection, IpFilter, IpValue, NumericFilter, NumericValue, PortDirection,
    PortFilter, PortValue, ProtoFilter, ProtoValue, Query, SelectExpr, SelectField,
    SelectFieldExpr, Stage, StringFilterExpr, StringOp, TimeRange,
};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Error, Clone, Serialize, Deserialize)]
#[serde(crate = "serde")]
pub enum ParseError {
    #[error("unexpected end of input at position {pos}")]
    UnexpectedEof { pos: usize },

    #[error("unexpected token `{token}` at position {pos}: {message}")]
    UnexpectedToken {
        token: String,
        pos: usize,
        message: String,
    },

    #[error("invalid number `{value}` at position {pos}")]
    InvalidNumber { value: String, pos: usize },

    #[error("invalid duration at position {pos}: {message}")]
    InvalidDuration { pos: usize, message: String },

    #[error("parse error at position {pos}: {message}")]
    General { pos: usize, message: String },
}

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Token {
    text: String,
    pos: usize,
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // Skip whitespace
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // Skip comments (# to end of line)
        if bytes[i] == b'#' {
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        // Quoted string
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            let mut s = String::new();
            while i < len && bytes[i] != b'"' {
                if bytes[i] == b'\\' && i + 1 < len {
                    i += 1;
                    match bytes[i] {
                        b'n' => s.push('\n'),
                        b't' => s.push('\t'),
                        b'\\' => s.push('\\'),
                        b'"' => s.push('"'),
                        c => {
                            s.push('\\');
                            s.push(c as char);
                        }
                    }
                } else {
                    s.push(bytes[i] as char);
                }
                i += 1;
            }
            if i < len {
                i += 1; // skip closing quote
            }
            tokens.push(Token {
                text: format!("\"{s}\""),
                pos: start,
            });
            continue;
        }

        // Two-char operators: !=, !~, >=, <=, +-,  ..
        if i + 1 < len {
            let two = &input[i..i + 2];
            match two {
                "!=" | "!~" | ">=" | "<=" | "+-" | ".." => {
                    tokens.push(Token {
                        text: two.to_string(),
                        pos: i,
                    });
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }

        // Single-char punctuation
        match bytes[i] {
            b'|' | b'(' | b')' | b',' | b'>' | b'<' | b'=' | b'~' | b'/' => {
                tokens.push(Token {
                    text: (bytes[i] as char).to_string(),
                    pos: i,
                });
                i += 1;
                continue;
            }
            _ => {}
        }

        // Word or number (including IPs, CIDRs, dates, durations, wildcards, ranges)
        let start = i;
        while i < len
            && !bytes[i].is_ascii_whitespace()
            && !matches!(
                bytes[i],
                b'|' | b'(' | b')' | b',' | b'>' | b'<' | b'=' | b'~'
            )
        {
            // Stop at `/` only if it's not part of a CIDR or date
            if bytes[i] == b'/' {
                // Check if this looks like a CIDR prefix length or path — keep going if
                // next char is a digit (CIDR) or preceded by IP-like chars
                if i + 1 < len && bytes[i + 1].is_ascii_digit() {
                    // It's CIDR notation — consume the slash and digits
                    i += 1;
                    while i < len && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                    break;
                }
                break;
            }
            i += 1;
        }
        if i > start {
            tokens.push(Token {
                text: input[start..i].to_string(),
                pos: start,
            });
        }
    }

    tokens
}

// ---------------------------------------------------------------------------
// Parser state
// ---------------------------------------------------------------------------

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_text(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(|t| t.text.as_str())
    }

    fn current_pos(&self) -> usize {
        self.tokens
            .get(self.pos)
            .map_or_else(|| self.last_pos(), |t| t.pos)
    }

    fn last_pos(&self) -> usize {
        self.tokens.last().map_or(0, |t| t.pos + t.text.len())
    }

    fn advance(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let tok = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(tok)
        } else {
            None
        }
    }

    fn expect(&mut self, expected: &str) -> Result<Token, ParseError> {
        match self.advance() {
            Some(tok) if tok.text.eq_ignore_ascii_case(expected) => Ok(tok),
            Some(tok) => Err(ParseError::UnexpectedToken {
                token: tok.text.clone(),
                pos: tok.pos,
                message: format!("expected `{expected}`"),
            }),
            None => Err(ParseError::UnexpectedEof {
                pos: self.last_pos(),
            }),
        }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn check(&self, text: &str) -> bool {
        self.peek_text()
            .is_some_and(|t| t.eq_ignore_ascii_case(text))
    }

    fn eat(&mut self, text: &str) -> bool {
        if self.check(text) {
            self.advance();
            true
        } else {
            false
        }
    }

    // -----------------------------------------------------------------------
    // Top-level parse
    // -----------------------------------------------------------------------

    fn parse_query(&mut self) -> Result<Query, ParseError> {
        let time_range = self.parse_time_range()?;
        let mut stages = Vec::new();

        while self.eat("|") {
            stages.push(self.parse_stage()?);
        }

        if !self.at_end() {
            let tok = self.peek().unwrap();
            return Err(ParseError::UnexpectedToken {
                token: tok.text.clone(),
                pos: tok.pos,
                message: "expected `|` or end of input".to_string(),
            });
        }

        Ok(Query { time_range, stages })
    }

    // -----------------------------------------------------------------------
    // Time range
    // -----------------------------------------------------------------------

    fn parse_time_range(&mut self) -> Result<TimeRange, ParseError> {
        // Default if nothing looks like a time range
        if self.at_end() || self.check("|") {
            return Ok(TimeRange::Relative {
                duration: Duration {
                    parts: vec![DurationPart {
                        value: 1,
                        unit: DurationUnit::Hours,
                    }],
                },
                offset: None,
            });
        }

        if self.check("last") {
            return self.parse_relative_time();
        }

        if self.check("at") {
            return self.parse_point_in_time();
        }

        if self.check("(") {
            return self.parse_combined_time();
        }

        // Try absolute: starts with a date-like token (YYYY-MM-DD...)
        if let Some(text) = self.peek_text() {
            if looks_like_date(text) {
                return self.parse_absolute_time();
            }
        }

        // If the first token doesn't look like a time range, assume default `last 1h`
        // and let the stages handle it. But first check if it looks like a filter keyword.
        if self.is_filter_start() {
            return Ok(TimeRange::Relative {
                duration: Duration {
                    parts: vec![DurationPart {
                        value: 1,
                        unit: DurationUnit::Hours,
                    }],
                },
                offset: None,
            });
        }

        // Try to parse as duration (could be just a bare duration)
        Ok(TimeRange::Relative {
            duration: Duration {
                parts: vec![DurationPart {
                    value: 1,
                    unit: DurationUnit::Hours,
                }],
            },
            offset: None,
        })
    }

    fn parse_relative_time(&mut self) -> Result<TimeRange, ParseError> {
        self.expect("last")?;
        let duration = self.parse_duration()?;

        // Check for recurring: daily, weekly, every
        if self.check("daily") || self.check("weekly") || self.check("every") {
            let base = TimeRange::Relative {
                duration: duration.clone(),
                offset: None,
            };
            let kind = self.parse_recurring_kind()?;
            return Ok(TimeRange::Recurring {
                base: Box::new(base),
                kind,
            });
        }

        let offset = if self.eat("offset") {
            Some(self.parse_duration()?)
        } else {
            None
        };

        Ok(TimeRange::Relative { duration, offset })
    }

    fn parse_recurring_kind(&mut self) -> Result<crate::ast::RecurringKind, ParseError> {
        if self.eat("daily") {
            let start_time = self
                .advance()
                .ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?
                .text;
            // Expect the `..` to be attached or separate
            let (start_time, end_time) = self.parse_dotdot_range_strings(start_time)?;
            return Ok(crate::ast::RecurringKind::Daily {
                start_time,
                end_time,
            });
        }
        if self.eat("weekly") {
            let weekday = self
                .advance()
                .ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?
                .text;
            let start_time = self
                .advance()
                .ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?
                .text;
            let (start_time, end_time) = self.parse_dotdot_range_strings(start_time)?;
            return Ok(crate::ast::RecurringKind::Weekly {
                weekday,
                start_time,
                end_time,
            });
        }
        if self.eat("every") {
            let dur = self.parse_duration()?;
            return Ok(crate::ast::RecurringKind::Every(dur));
        }
        Err(ParseError::General {
            pos: self.current_pos(),
            message: "expected `daily`, `weekly`, or `every`".to_string(),
        })
    }

    fn parse_dotdot_range_strings(
        &mut self,
        first: String,
    ) -> Result<(String, String), ParseError> {
        // The range may look like "09:00..10:00" as one token or "09:00" ".." "10:00"
        if let Some(idx) = first.find("..") {
            let start = first[..idx].to_string();
            let end = first[idx + 2..].to_string();
            if end.is_empty() {
                let end_tok = self.advance().ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?;
                return Ok((start, end_tok.text));
            }
            return Ok((start, end));
        }
        self.expect("..")?;
        let end_tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;
        Ok((first, end_tok.text))
    }

    fn parse_point_in_time(&mut self) -> Result<TimeRange, ParseError> {
        self.expect("at")?;
        let datetime = self
            .advance()
            .ok_or(ParseError::UnexpectedEof {
                pos: self.last_pos(),
            })?
            .text;
        let window = if self.check("+-") {
            self.advance();
            let dur = self.parse_duration()?;
            crate::ast::PointWindow::PlusMinus(dur)
        } else if self.check("+") {
            // This won't happen as `+` isn't a token by itself usually.
            // Duration tokens like `+10m` handled differently.
            self.advance();
            let dur = self.parse_duration()?;
            crate::ast::PointWindow::Plus(dur)
        } else if self.check("-") {
            self.advance();
            let dur = self.parse_duration()?;
            crate::ast::PointWindow::Minus(dur)
        } else {
            // Default window
            crate::ast::PointWindow::PlusMinus(Duration {
                parts: vec![DurationPart {
                    value: 5,
                    unit: DurationUnit::Minutes,
                }],
            })
        };
        Ok(TimeRange::PointInTime { datetime, window })
    }

    fn parse_absolute_time(&mut self) -> Result<TimeRange, ParseError> {
        let first = self.advance().unwrap().text;

        // Check if the token itself contains `..`
        if let Some(idx) = first.find("..") {
            let start = first[..idx].to_string();
            let end_part = &first[idx + 2..];
            let end = if end_part.is_empty() {
                self.advance()
                    .ok_or(ParseError::UnexpectedEof {
                        pos: self.last_pos(),
                    })?
                    .text
            } else {
                end_part.to_string()
            };
            return Ok(TimeRange::Absolute { start, end });
        }

        // Might be `date .. date` as separate tokens
        if self.check("..") {
            self.advance();
            let end = self
                .advance()
                .ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?
                .text;
            return Ok(TimeRange::Absolute { start: first, end });
        }

        // Single date = full day
        Ok(TimeRange::Absolute {
            start: first.clone(),
            end: first,
        })
    }

    fn parse_combined_time(&mut self) -> Result<TimeRange, ParseError> {
        self.expect("(")?;
        let mut ranges = vec![self.parse_time_range()?];
        while self.eat(",") {
            ranges.push(self.parse_time_range()?);
        }
        self.expect(")")?;
        Ok(TimeRange::Combined(ranges))
    }

    // -----------------------------------------------------------------------
    // Duration
    // -----------------------------------------------------------------------

    fn parse_duration(&mut self) -> Result<Duration, ParseError> {
        let tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;
        parse_duration_str(&tok.text, tok.pos)
    }

    // -----------------------------------------------------------------------
    // Stages
    // -----------------------------------------------------------------------

    fn parse_stage(&mut self) -> Result<Stage, ParseError> {
        if self.check("select") {
            return self.parse_select_stage();
        }
        if self.check("group") {
            return self.parse_group_by_stage().map(Stage::Aggregate);
        }
        if self.check("top") || self.check("bottom") {
            return self.parse_top_n_stage().map(Stage::Aggregate);
        }
        if self.check("sort") {
            return self.parse_sort_stage().map(Stage::Aggregate);
        }
        if self.check("limit") {
            return self.parse_limit_stage().map(Stage::Aggregate);
        }
        // Default: filter expression
        self.parse_filter_expr().map(Stage::Filter)
    }

    // -----------------------------------------------------------------------
    // Filter expression (boolean with precedence: NOT > AND > OR)
    // -----------------------------------------------------------------------

    fn parse_filter_expr(&mut self) -> Result<FilterExpr, ParseError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<FilterExpr, ParseError> {
        let mut left = self.parse_and_expr()?;
        while self.eat("or") {
            let right = self.parse_and_expr()?;
            left = FilterExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<FilterExpr, ParseError> {
        let mut left = self.parse_not_expr()?;
        while self.eat("and") {
            let right = self.parse_not_expr()?;
            left = FilterExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<FilterExpr, ParseError> {
        if self.check("not") && !self.is_next_in() {
            self.advance();
            let inner = self.parse_not_expr()?;
            return Ok(FilterExpr::Not(Box::new(inner)));
        }
        self.parse_primary_filter()
    }

    /// Check if the token after "not" is "in" (for `not in` field filter).
    fn is_next_in(&self) -> bool {
        self.tokens
            .get(self.pos + 1)
            .is_some_and(|t| t.text.eq_ignore_ascii_case("in"))
    }

    fn parse_primary_filter(&mut self) -> Result<FilterExpr, ParseError> {
        // Parenthesized
        if self.check("(") {
            self.advance();
            let expr = self.parse_filter_expr()?;
            self.expect(")")?;
            return Ok(expr);
        }

        let text = self
            .peek_text()
            .ok_or(ParseError::UnexpectedEof {
                pos: self.last_pos(),
            })?
            .to_ascii_lowercase();

        match text.as_str() {
            "src" | "dst" | "ip" => self.parse_ip_filter(),
            "sport" | "dport" | "port" => self.parse_port_filter(),
            "proto" => self.parse_proto_filter(),
            _ => self.parse_field_or_numeric_filter(),
        }
    }

    // -----------------------------------------------------------------------
    // IP filter
    // -----------------------------------------------------------------------

    fn parse_ip_filter(&mut self) -> Result<FilterExpr, ParseError> {
        let dir_tok = self.advance().unwrap();
        let direction = match dir_tok.text.to_ascii_lowercase().as_str() {
            "src" => IpDirection::Src,
            "dst" => IpDirection::Dst,
            _ => IpDirection::Any,
        };

        let negated = self.eat("not");

        // `in (...)` list
        if self.check("in") {
            self.advance();
            let list = self.parse_paren_string_list()?;
            return Ok(FilterExpr::Ip(IpFilter {
                direction,
                negated,
                value: IpValue::List(list),
            }));
        }

        let tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;

        // Check for parenthesized list without `in` keyword:
        // `dst not (10.0.0.0/8, ...)`
        if tok.text == "(" {
            let list = self.parse_string_list_inner()?;
            self.expect(")")?;
            return Ok(FilterExpr::Ip(IpFilter {
                direction,
                negated,
                value: IpValue::List(list),
            }));
        }

        let value = if tok.text.contains('*') || tok.text.contains('-') && tok.text.contains('.') {
            // Check if it's a wildcard like 10.*.*.1 vs a range like 10.1-5.*.*
            IpValue::Wildcard(tok.text)
        } else if tok.text.contains('/') {
            IpValue::Cidr(tok.text)
        } else {
            IpValue::Addr(tok.text)
        };

        Ok(FilterExpr::Ip(IpFilter {
            direction,
            negated,
            value,
        }))
    }

    // -----------------------------------------------------------------------
    // Port filter
    // -----------------------------------------------------------------------

    fn parse_port_filter(&mut self) -> Result<FilterExpr, ParseError> {
        let dir_tok = self.advance().unwrap();
        let direction = match dir_tok.text.to_ascii_lowercase().as_str() {
            "sport" => PortDirection::Src,
            "dport" => PortDirection::Dst,
            _ => PortDirection::Any,
        };

        let negated = self.eat("not");

        let tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;

        let value = self.parse_port_value(&tok.text, tok.pos)?;

        // Check for comma-separated list continuation
        let value = if self.check(",") {
            let mut items = match value {
                PortValue::List(items) => items,
                other => vec![other],
            };
            while self.eat(",") {
                let next = self.advance().ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?;
                items.push(self.parse_port_value(&next.text, next.pos)?);
            }
            PortValue::List(items)
        } else {
            value
        };

        Ok(FilterExpr::Port(PortFilter {
            direction,
            negated,
            value,
        }))
    }

    fn parse_port_value(&self, text: &str, pos: usize) -> Result<PortValue, ParseError> {
        // Named port?
        if text.chars().all(|c| c.is_ascii_alphabetic()) {
            return Ok(PortValue::Named(text.to_string()));
        }

        // Range: 80-90 or 1024-
        if let Some(idx) = text.find('-') {
            let start: u16 = text[..idx].parse().map_err(|_| ParseError::InvalidNumber {
                value: text.to_string(),
                pos,
            })?;
            let rest = &text[idx + 1..];
            if rest.is_empty() {
                return Ok(PortValue::OpenRange(start));
            }
            let end: u16 = rest.parse().map_err(|_| ParseError::InvalidNumber {
                value: text.to_string(),
                pos,
            })?;
            return Ok(PortValue::Range(start, end));
        }

        // Single number
        let n: u16 = text.parse().map_err(|_| ParseError::InvalidNumber {
            value: text.to_string(),
            pos,
        })?;
        Ok(PortValue::Single(n))
    }

    // -----------------------------------------------------------------------
    // Proto filter
    // -----------------------------------------------------------------------

    fn parse_proto_filter(&mut self) -> Result<FilterExpr, ParseError> {
        self.advance(); // consume "proto"
        let negated = self.eat("not");

        let tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;

        let first_val = parse_proto_value(&tok.text);

        // Comma-separated list?
        let value = if self.check(",") {
            let mut items = vec![first_val];
            while self.eat(",") {
                let next = self.advance().ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?;
                items.push(parse_proto_value(&next.text));
            }
            ProtoValue::List(items)
        } else {
            first_val
        };

        Ok(FilterExpr::Proto(ProtoFilter { negated, value }))
    }

    // -----------------------------------------------------------------------
    // Field / numeric / string filter
    // -----------------------------------------------------------------------

    fn parse_field_or_numeric_filter(&mut self) -> Result<FilterExpr, ParseError> {
        let field_tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;
        let field = field_tok.text.clone();

        let op_tok = self.peek().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;

        let op_text = op_tok.text.clone();
        let op_pos = op_tok.pos;

        match op_text.as_str() {
            "~" => {
                self.advance();
                let val = self.parse_string_value()?;
                return Ok(FilterExpr::StringFilter(StringFilterExpr {
                    field,
                    op: StringOp::Regex,
                    value: val,
                }));
            }
            "!~" => {
                self.advance();
                let val = self.parse_string_value()?;
                return Ok(FilterExpr::StringFilter(StringFilterExpr {
                    field,
                    op: StringOp::NotRegex,
                    value: val,
                }));
            }
            "=" => {
                self.advance();
                let val_tok = self.advance().ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?;
                if val_tok.text.starts_with('"') {
                    return Ok(FilterExpr::StringFilter(StringFilterExpr {
                        field,
                        op: StringOp::Eq,
                        value: unquote(&val_tok.text),
                    }));
                }
                // Numeric
                let nv = parse_numeric_value(&val_tok.text, val_tok.pos)?;
                return Ok(FilterExpr::Numeric(NumericFilter {
                    field,
                    op: CompareOp::Eq,
                    value: nv,
                }));
            }
            "!=" => {
                self.advance();
                let val_tok = self.advance().ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?;
                if val_tok.text.starts_with('"') {
                    return Ok(FilterExpr::StringFilter(StringFilterExpr {
                        field,
                        op: StringOp::Ne,
                        value: unquote(&val_tok.text),
                    }));
                }
                let nv = parse_numeric_value(&val_tok.text, val_tok.pos)?;
                return Ok(FilterExpr::Numeric(NumericFilter {
                    field,
                    op: CompareOp::Ne,
                    value: nv,
                }));
            }
            ">" | ">=" | "<" | "<=" => {
                self.advance();
                let val_tok = self.advance().ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?;
                let op = match op_text.as_str() {
                    ">" => CompareOp::Gt,
                    ">=" => CompareOp::Ge,
                    "<" => CompareOp::Lt,
                    "<=" => CompareOp::Le,
                    _ => unreachable!(),
                };
                let nv = parse_numeric_value(&val_tok.text, val_tok.pos)?;
                return Ok(FilterExpr::Numeric(NumericFilter {
                    field,
                    op,
                    value: nv,
                }));
            }
            "in" => {
                self.advance();
                let list = self.parse_paren_string_list()?;
                return Ok(FilterExpr::StringFilter(StringFilterExpr {
                    field,
                    op: StringOp::In,
                    value: list.join(","),
                }));
            }
            "not" => {
                // `field not in (...)`
                if self
                    .tokens
                    .get(self.pos + 1)
                    .is_some_and(|t| t.text.eq_ignore_ascii_case("in"))
                {
                    self.advance(); // not
                    self.advance(); // in
                    let list = self.parse_paren_string_list()?;
                    return Ok(FilterExpr::StringFilter(StringFilterExpr {
                        field,
                        op: StringOp::NotIn,
                        value: list.join(","),
                    }));
                }
            }
            _ => {}
        }

        Err(ParseError::UnexpectedToken {
            token: op_text,
            pos: op_pos,
            message: format!("expected comparison operator after `{field}`"),
        })
    }

    fn parse_string_value(&mut self) -> Result<String, ParseError> {
        let tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;
        Ok(unquote(&tok.text))
    }

    fn parse_paren_string_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect("(")?;
        let list = self.parse_string_list_inner()?;
        self.expect(")")?;
        Ok(list)
    }

    fn parse_string_list_inner(&mut self) -> Result<Vec<String>, ParseError> {
        let mut items = Vec::new();
        let tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;
        items.push(unquote(&tok.text));
        while self.eat(",") {
            let tok = self.advance().ok_or(ParseError::UnexpectedEof {
                pos: self.last_pos(),
            })?;
            items.push(unquote(&tok.text));
        }
        Ok(items)
    }

    // -----------------------------------------------------------------------
    // Select stage
    // -----------------------------------------------------------------------

    fn parse_select_stage(&mut self) -> Result<Stage, ParseError> {
        self.expect("select")?;

        if self.check("*") {
            self.advance();
            if self.eat("except") {
                self.expect("(")?;
                let mut fields = Vec::new();
                let tok = self.advance().ok_or(ParseError::UnexpectedEof {
                    pos: self.last_pos(),
                })?;
                fields.push(tok.text);
                while self.eat(",") {
                    let tok = self.advance().ok_or(ParseError::UnexpectedEof {
                        pos: self.last_pos(),
                    })?;
                    fields.push(tok.text);
                }
                self.expect(")")?;
                return Ok(Stage::Select(SelectExpr::AllExcept(fields)));
            }
            return Ok(Stage::Select(SelectExpr::All));
        }

        let mut fields = vec![self.parse_select_field()?];
        while self.eat(",") {
            // Stop if next token is a pipe or keyword that starts a new stage
            if self.at_end() || self.check("|") {
                break;
            }
            fields.push(self.parse_select_field()?);
        }

        Ok(Stage::Select(SelectExpr::Fields(fields)))
    }

    fn parse_select_field(&mut self) -> Result<SelectField, ParseError> {
        let tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;
        let name = tok.text;

        // Check for binary op: `bytes / packets as avg_pkt_size`
        if self.check("/") || self.check("*") || self.check("+") || self.check("-") {
            let op_tok = self.advance().unwrap();
            let right_tok = self.advance().ok_or(ParseError::UnexpectedEof {
                pos: self.last_pos(),
            })?;
            let op = match op_tok.text.as_str() {
                "/" => crate::ast::ArithOp::Div,
                "*" => crate::ast::ArithOp::Mul,
                "+" => crate::ast::ArithOp::Add,
                "-" => crate::ast::ArithOp::Sub,
                _ => unreachable!(),
            };
            let alias = if self.eat("as") {
                Some(
                    self.advance()
                        .ok_or(ParseError::UnexpectedEof {
                            pos: self.last_pos(),
                        })?
                        .text,
                )
            } else {
                None
            };
            return Ok(SelectField {
                expr: SelectFieldExpr::BinaryOp {
                    left: name,
                    op,
                    right: right_tok.text,
                },
                alias,
            });
        }

        let alias = if self.eat("as") {
            Some(
                self.advance()
                    .ok_or(ParseError::UnexpectedEof {
                        pos: self.last_pos(),
                    })?
                    .text,
            )
        } else {
            None
        };

        Ok(SelectField {
            expr: SelectFieldExpr::Field(name),
            alias,
        })
    }

    // -----------------------------------------------------------------------
    // Aggregation stages
    // -----------------------------------------------------------------------

    fn parse_group_by_stage(&mut self) -> Result<AggExpr, ParseError> {
        self.expect("group")?;
        self.expect("by")?;

        let mut keys = vec![self.parse_group_by_key()?];
        while self.eat(",") {
            // Stop if next is pipe, end, or an agg function
            if self.at_end() || self.check("|") {
                break;
            }
            keys.push(self.parse_group_by_key()?);
        }

        // Aggregate functions come after `|` or directly
        let mut functions = Vec::new();
        if self.eat("|") {
            functions.push(self.parse_agg_call()?);
            while self.eat(",") {
                functions.push(self.parse_agg_call()?);
            }
        }

        // Also allow agg functions without pipe separator (e.g. `group by src | sum(bytes)`)
        // Already handled above with `|`.

        Ok(AggExpr::GroupBy { keys, functions })
    }

    fn parse_group_by_key(&mut self) -> Result<GroupByKey, ParseError> {
        let tok = self.peek().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;

        // Time bucket: looks like a duration (e.g., `5m`, `1h`)
        if looks_like_duration(&tok.text) {
            let tok = self.advance().unwrap();
            let dur = parse_duration_str(&tok.text, tok.pos)?;
            return Ok(GroupByKey::TimeBucket(dur));
        }

        let field_tok = self.advance().unwrap();
        let field = field_tok.text.clone();

        // Subnet prefix: `src /24`
        if self.check("/") {
            self.advance();
            let prefix_tok = self.advance().ok_or(ParseError::UnexpectedEof {
                pos: self.last_pos(),
            })?;
            let prefix_len: u8 =
                prefix_tok
                    .text
                    .parse()
                    .map_err(|_| ParseError::InvalidNumber {
                        value: prefix_tok.text.clone(),
                        pos: prefix_tok.pos,
                    })?;
            return Ok(GroupByKey::Subnet { field, prefix_len });
        }

        Ok(GroupByKey::Field(field))
    }

    fn parse_top_n_stage(&mut self) -> Result<AggExpr, ParseError> {
        let bottom = self.check("bottom");
        self.advance(); // "top" or "bottom"

        let n_tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;
        let n: u64 = n_tok.text.parse().map_err(|_| ParseError::InvalidNumber {
            value: n_tok.text.clone(),
            pos: n_tok.pos,
        })?;

        self.expect("by")?;
        let by = self.parse_agg_call()?;

        Ok(AggExpr::TopN { n, by, bottom })
    }

    fn parse_sort_stage(&mut self) -> Result<AggExpr, ParseError> {
        self.expect("sort")?;
        let by = self.parse_agg_call()?;
        let descending = if self.eat("desc") {
            true
        } else {
            self.eat("asc");
            false
        };
        Ok(AggExpr::Sort { by, descending })
    }

    fn parse_limit_stage(&mut self) -> Result<AggExpr, ParseError> {
        self.expect("limit")?;
        let tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;
        let n: u64 = tok.text.parse().map_err(|_| ParseError::InvalidNumber {
            value: tok.text.clone(),
            pos: tok.pos,
        })?;
        Ok(AggExpr::Limit(n))
    }

    fn parse_agg_call(&mut self) -> Result<AggCall, ParseError> {
        let func_tok = self.advance().ok_or(ParseError::UnexpectedEof {
            pos: self.last_pos(),
        })?;
        let func = parse_agg_func_name(&func_tok.text, func_tok.pos)?;

        self.expect("(")?;
        let field = if self.check(")") {
            None
        } else {
            let f = self.advance().ok_or(ParseError::UnexpectedEof {
                pos: self.last_pos(),
            })?;
            Some(f.text)
        };
        self.expect(")")?;

        Ok(AggCall { func, field })
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn is_filter_start(&self) -> bool {
        let Some(text) = self.peek_text() else {
            return false;
        };
        let lower = text.to_ascii_lowercase();
        matches!(
            lower.as_str(),
            "src" | "dst" | "ip" | "sport" | "dport" | "port" | "proto" | "not" | "flags" | "("
        ) || {
            // Any identifier followed by a comparison operator
            self.tokens.get(self.pos + 1).is_some_and(|t| {
                matches!(
                    t.text.as_str(),
                    "=" | "!=" | ">" | ">=" | "<" | "<=" | "~" | "!~" | "in"
                )
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Free helper functions
// ---------------------------------------------------------------------------

fn looks_like_date(s: &str) -> bool {
    // Starts with 4 digits and a dash: `2024-...`
    s.len() >= 5 && s.as_bytes()[0..4].iter().all(|b| b.is_ascii_digit()) && s.as_bytes()[4] == b'-'
}

fn looks_like_duration(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    // Must start with a digit and end with a duration unit letter
    s.as_bytes()[0].is_ascii_digit()
        && s.as_bytes()
            .last()
            .is_some_and(|&b| matches!(b, b's' | b'm' | b'h' | b'd' | b'w' | b'M'))
}

fn parse_duration_str(s: &str, pos: usize) -> Result<Duration, ParseError> {
    let mut parts = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        // Parse digits
        let num_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == num_start {
            return Err(ParseError::InvalidDuration {
                pos,
                message: format!("expected digit in `{s}`"),
            });
        }
        let value: u64 = s[num_start..i]
            .parse()
            .map_err(|_| ParseError::InvalidDuration {
                pos,
                message: format!("invalid number in `{s}`"),
            })?;

        // Parse unit
        if i >= bytes.len() {
            return Err(ParseError::InvalidDuration {
                pos,
                message: format!("expected unit suffix in `{s}`"),
            });
        }
        let unit = match bytes[i] {
            b's' => DurationUnit::Seconds,
            b'm' => DurationUnit::Minutes,
            b'h' => DurationUnit::Hours,
            b'd' => DurationUnit::Days,
            b'w' => DurationUnit::Weeks,
            b'M' => DurationUnit::Months,
            c => {
                return Err(ParseError::InvalidDuration {
                    pos,
                    message: format!("unknown duration unit `{}`", c as char),
                });
            }
        };
        i += 1;
        parts.push(DurationPart { value, unit });
    }

    if parts.is_empty() {
        return Err(ParseError::InvalidDuration {
            pos,
            message: format!("empty duration `{s}`"),
        });
    }

    Ok(Duration { parts })
}

fn parse_proto_value(s: &str) -> ProtoValue {
    if let Ok(n) = s.parse::<u8>() {
        ProtoValue::Number(n)
    } else {
        ProtoValue::Named(s.to_ascii_lowercase())
    }
}

fn parse_numeric_value(s: &str, pos: usize) -> Result<NumericValue, ParseError> {
    // Check for suffix: K, M, G, T, Ki, Mi, Gi
    let bytes = s.as_bytes();
    let mut num_end = bytes.len();

    // Find where digits end
    for (i, &b) in bytes.iter().enumerate() {
        if !b.is_ascii_digit() && b != b'.' {
            num_end = i;
            break;
        }
    }

    if num_end == 0 {
        return Err(ParseError::InvalidNumber {
            value: s.to_string(),
            pos,
        });
    }

    let suffix = &s[num_end..];
    if suffix.is_empty() {
        let n: u64 = s.parse().map_err(|_| ParseError::InvalidNumber {
            value: s.to_string(),
            pos,
        })?;
        return Ok(NumericValue::Integer(n));
    }

    let num_part: u64 = s[..num_end]
        .parse()
        .map_err(|_| ParseError::InvalidNumber {
            value: s.to_string(),
            pos,
        })?;

    Ok(NumericValue::WithSuffix(num_part, suffix.to_string()))
}

fn parse_agg_func_name(s: &str, pos: usize) -> Result<AggFunc, ParseError> {
    match s.to_ascii_lowercase().as_str() {
        "sum" => Ok(AggFunc::Sum),
        "avg" => Ok(AggFunc::Avg),
        "min" => Ok(AggFunc::Min),
        "max" => Ok(AggFunc::Max),
        "count" => Ok(AggFunc::Count),
        "uniq" => Ok(AggFunc::Uniq),
        "p50" => Ok(AggFunc::P50),
        "p95" => Ok(AggFunc::P95),
        "p99" => Ok(AggFunc::P99),
        "stddev" => Ok(AggFunc::Stddev),
        "rate" => Ok(AggFunc::Rate),
        "first" => Ok(AggFunc::First),
        "last" => Ok(AggFunc::Last),
        _ => Err(ParseError::UnexpectedToken {
            token: s.to_string(),
            pos,
            message: "expected aggregate function name".to_string(),
        }),
    }
}

fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parse an FQL query string into a `Query` AST.
///
/// # Errors
///
/// Returns `ParseError` if the input is not valid FQL.
pub fn parse(input: &str) -> Result<Query, ParseError> {
    let tokens = tokenize(input);
    let mut parser = Parser::new(tokens);
    parser.parse_query()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    #[test]
    fn test_parse_simple_time_range() {
        let q = parse("last 1h").unwrap();
        match &q.time_range {
            TimeRange::Relative { duration, offset } => {
                assert_eq!(duration.parts.len(), 1);
                assert_eq!(duration.parts[0].value, 1);
                assert_eq!(duration.parts[0].unit, DurationUnit::Hours);
                assert!(offset.is_none());
            }
            _ => panic!("expected Relative time range"),
        }
        assert!(q.stages.is_empty());
    }

    #[test]
    fn test_parse_time_with_offset() {
        let q = parse("last 1h offset 1d").unwrap();
        match &q.time_range {
            TimeRange::Relative { duration, offset } => {
                assert_eq!(duration.parts[0].value, 1);
                assert_eq!(duration.parts[0].unit, DurationUnit::Hours);
                let off = offset.as_ref().unwrap();
                assert_eq!(off.parts[0].value, 1);
                assert_eq!(off.parts[0].unit, DurationUnit::Days);
            }
            _ => panic!("expected Relative time range with offset"),
        }
    }

    #[test]
    fn test_parse_ip_filter() {
        let q = parse("last 1h | src 10.0.0.0/8").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Filter(FilterExpr::Ip(f)) => {
                assert_eq!(f.direction, IpDirection::Src);
                assert!(!f.negated);
                match &f.value {
                    IpValue::Cidr(c) => assert_eq!(c, "10.0.0.0/8"),
                    _ => panic!("expected CIDR"),
                }
            }
            _ => panic!("expected IP filter stage"),
        }
    }

    #[test]
    fn test_parse_port_filter() {
        let q = parse("last 1h | dport 80,443").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Filter(FilterExpr::Port(f)) => {
                assert_eq!(f.direction, PortDirection::Dst);
                assert!(!f.negated);
                match &f.value {
                    PortValue::List(items) => {
                        assert_eq!(items.len(), 2);
                    }
                    _ => panic!("expected port list, got {:?}", f.value),
                }
            }
            _ => panic!("expected Port filter stage"),
        }
    }

    #[test]
    fn test_parse_compound_filter() {
        let q = parse("last 1h | src 10.0.0.0/8 and dport 80").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Filter(FilterExpr::And(left, right)) => {
                assert!(matches!(left.as_ref(), FilterExpr::Ip(_)));
                assert!(matches!(right.as_ref(), FilterExpr::Port(_)));
            }
            _ => panic!("expected And filter"),
        }
    }

    #[test]
    fn test_parse_top_n() {
        let q = parse("last 1h | top 10 by sum(bytes)").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Aggregate(AggExpr::TopN { n, by, bottom }) => {
                assert_eq!(*n, 10);
                assert!(!bottom);
                assert_eq!(by.func, AggFunc::Sum);
                assert_eq!(by.field.as_deref(), Some("bytes"));
            }
            _ => panic!("expected TopN aggregate"),
        }
    }

    #[test]
    fn test_parse_group_by() {
        let q = parse("last 1h | group by src, dport | sum(bytes)").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Aggregate(AggExpr::GroupBy { keys, functions }) => {
                assert_eq!(keys.len(), 2);
                assert!(matches!(&keys[0], GroupByKey::Field(f) if f == "src"));
                assert!(matches!(&keys[1], GroupByKey::Field(f) if f == "dport"));
                assert_eq!(functions.len(), 1);
                assert_eq!(functions[0].func, AggFunc::Sum);
            }
            _ => panic!("expected GroupBy aggregate"),
        }
    }

    #[test]
    fn test_parse_full_query() {
        let q = parse(
            "last 1h | src 10.0.0.0/8 and proto tcp | select src, dst, bytes | top 10 by sum(bytes)",
        )
        .unwrap();
        assert_eq!(q.stages.len(), 3);
        assert!(matches!(&q.stages[0], Stage::Filter(_)));
        assert!(matches!(&q.stages[1], Stage::Select(_)));
        assert!(matches!(
            &q.stages[2],
            Stage::Aggregate(AggExpr::TopN { .. })
        ));
    }

    #[test]
    fn test_parse_string_regex() {
        let q = parse(r#"last 1h | dnsQueryName ~ ".*evil.*""#).unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Filter(FilterExpr::StringFilter(f)) => {
                assert_eq!(f.field, "dnsQueryName");
                assert_eq!(f.op, StringOp::Regex);
                assert_eq!(f.value, ".*evil.*");
            }
            _ => panic!("expected StringFilter"),
        }
    }

    #[test]
    fn test_parse_named_port() {
        let q = parse("last 1h | dport dns").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Filter(FilterExpr::Port(f)) => {
                assert_eq!(f.direction, PortDirection::Dst);
                assert!(matches!(&f.value, PortValue::Named(n) if n == "dns"));
            }
            _ => panic!("expected Port filter with named port"),
        }
    }

    #[test]
    fn test_parse_numeric_with_suffix() {
        let q = parse("last 1h | bytes > 1M").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Filter(FilterExpr::Numeric(f)) => {
                assert_eq!(f.field, "bytes");
                assert_eq!(f.op, CompareOp::Gt);
                match &f.value {
                    NumericValue::WithSuffix(n, s) => {
                        assert_eq!(*n, 1);
                        assert_eq!(s, "M");
                    }
                    _ => panic!("expected WithSuffix"),
                }
            }
            _ => panic!("expected Numeric filter"),
        }
    }

    #[test]
    fn test_parse_proto_filter() {
        let q = parse("last 1h | proto tcp").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Filter(FilterExpr::Proto(f)) => {
                assert!(!f.negated);
                assert!(matches!(&f.value, ProtoValue::Named(n) if n == "tcp"));
            }
            _ => panic!("expected Proto filter"),
        }
    }

    #[test]
    fn test_parse_combined_duration() {
        let q = parse("last 1h30m").unwrap();
        match &q.time_range {
            TimeRange::Relative { duration, .. } => {
                assert_eq!(duration.parts.len(), 2);
                assert_eq!(duration.parts[0].value, 1);
                assert_eq!(duration.parts[0].unit, DurationUnit::Hours);
                assert_eq!(duration.parts[1].value, 30);
                assert_eq!(duration.parts[1].unit, DurationUnit::Minutes);
            }
            _ => panic!("expected Relative"),
        }
    }

    #[test]
    fn test_parse_absolute_date_range() {
        let q = parse("2024-03-15..2024-03-20").unwrap();
        match &q.time_range {
            TimeRange::Absolute { start, end } => {
                assert_eq!(start, "2024-03-15");
                assert_eq!(end, "2024-03-20");
            }
            _ => panic!("expected Absolute time range"),
        }
    }

    #[test]
    fn test_parse_limit() {
        let q = parse("last 1h | limit 100").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Aggregate(AggExpr::Limit(n)) => assert_eq!(*n, 100),
            _ => panic!("expected Limit"),
        }
    }

    #[test]
    fn test_parse_sort() {
        let q = parse("last 1h | sort sum(bytes) desc").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Aggregate(AggExpr::Sort { by, descending }) => {
                assert_eq!(by.func, AggFunc::Sum);
                assert_eq!(by.field.as_deref(), Some("bytes"));
                assert!(descending);
            }
            _ => panic!("expected Sort"),
        }
    }

    #[test]
    fn test_parse_negated_ip() {
        let q = parse("last 1h | src not 192.168.0.0/16").unwrap();
        match &q.stages[0] {
            Stage::Filter(FilterExpr::Ip(f)) => {
                assert!(f.negated);
                assert_eq!(f.direction, IpDirection::Src);
            }
            _ => panic!("expected negated IP filter"),
        }
    }

    #[test]
    fn test_parse_or_filter() {
        let q = parse("last 1h | dport 80 or dport 443").unwrap();
        assert_eq!(q.stages.len(), 1);
        assert!(matches!(&q.stages[0], Stage::Filter(FilterExpr::Or(_, _))));
    }

    #[test]
    fn test_parse_not_filter() {
        let q = parse("last 1h | not proto icmp").unwrap();
        assert_eq!(q.stages.len(), 1);
        match &q.stages[0] {
            Stage::Filter(FilterExpr::Not(inner)) => {
                assert!(matches!(inner.as_ref(), FilterExpr::Proto(_)));
            }
            _ => panic!("expected Not filter"),
        }
    }

    #[test]
    fn test_parse_port_range() {
        let q = parse("last 1h | sport 1024-65535").unwrap();
        match &q.stages[0] {
            Stage::Filter(FilterExpr::Port(f)) => {
                assert_eq!(f.direction, PortDirection::Src);
                assert!(matches!(&f.value, PortValue::Range(1024, 65535)));
            }
            _ => panic!("expected port range"),
        }
    }

    #[test]
    fn test_parse_group_by_subnet() {
        let q = parse("last 1h | group by dst /24 | sum(bytes)").unwrap();
        match &q.stages[0] {
            Stage::Aggregate(AggExpr::GroupBy { keys, .. }) => {
                assert!(
                    matches!(&keys[0], GroupByKey::Subnet { field, prefix_len } if field == "dst" && *prefix_len == 24)
                );
            }
            _ => panic!("expected group by subnet"),
        }
    }

    #[test]
    fn test_parse_group_by_time_bucket() {
        let q = parse("last 1h | group by 5m | sum(bytes)").unwrap();
        match &q.stages[0] {
            Stage::Aggregate(AggExpr::GroupBy { keys, .. }) => {
                assert!(matches!(&keys[0], GroupByKey::TimeBucket(_)));
            }
            _ => panic!("expected group by time bucket"),
        }
    }

    #[test]
    fn test_serialization_roundtrip() {
        let q = parse("last 1h | src 10.0.0.0/8 and proto tcp | top 10 by sum(bytes)").unwrap();
        let json = serde_json::to_string(&q).unwrap();
        let q2: Query = serde_json::from_str(&json).unwrap();
        // Check it didn't lose structure
        assert_eq!(q2.stages.len(), 2);
    }

    #[test]
    fn test_parse_default_time_range() {
        // No time range — should default to last 1h
        let q = parse("").unwrap();
        match &q.time_range {
            TimeRange::Relative { duration, offset } => {
                assert_eq!(duration.parts[0].value, 1);
                assert_eq!(duration.parts[0].unit, DurationUnit::Hours);
                assert!(offset.is_none());
            }
            _ => panic!("expected default Relative"),
        }
    }
}

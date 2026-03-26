use serde::{Deserialize, Serialize};

/// A complete FQL query: optional time range followed by pipeline stages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub time_range: TimeRange,
    pub stages: Vec<Stage>,
}

// ---------------------------------------------------------------------------
// Time ranges
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeRange {
    /// `last 1h`, `last 1h30m`
    Relative {
        duration: Duration,
        offset: Option<Duration>,
    },
    /// `2024-03-15`, `2024-03-15T08:00..2024-03-15T17:00`
    Absolute { start: String, end: String },
    /// `at 2024-03-15T14:30 +-5m`
    PointInTime {
        datetime: String,
        window: PointWindow,
    },
    /// `last 7d daily 09:00..10:00`
    Recurring {
        base: Box<TimeRange>,
        kind: RecurringKind,
    },
    /// `(last 1h, last 1h offset 1d)`
    Combined(Vec<TimeRange>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PointWindow {
    /// `+-5m`
    PlusMinus(Duration),
    /// `+10m`
    Plus(Duration),
    /// `-1h`
    Minus(Duration),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecurringKind {
    /// `daily 09:00..10:00`
    Daily {
        start_time: String,
        end_time: String,
    },
    /// `weekly mon 08:00..17:00`
    Weekly {
        weekday: String,
        start_time: String,
        end_time: String,
    },
    /// `every 1h`
    Every(Duration),
}

// ---------------------------------------------------------------------------
// Duration
// ---------------------------------------------------------------------------

/// A compound duration like `1h30m` or `2d12h`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Duration {
    pub parts: Vec<DurationPart>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurationPart {
    pub value: u64,
    pub unit: DurationUnit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurationUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
    Weeks,
    Months,
}

// ---------------------------------------------------------------------------
// Pipeline stages
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Stage {
    Filter(FilterExpr),
    Select(SelectExpr),
    Aggregate(AggExpr),
}

// ---------------------------------------------------------------------------
// Filter expressions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterExpr {
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
    Ip(IpFilter),
    Port(PortFilter),
    Proto(ProtoFilter),
    Numeric(NumericFilter),
    StringFilter(StringFilterExpr),
    Field(FieldFilter),
}

// ---------------------------------------------------------------------------
// IP filter
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpFilter {
    pub direction: IpDirection,
    pub negated: bool,
    pub value: IpValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IpDirection {
    Src,
    Dst,
    Any,
    /// Explicit column name for fields that aren't src/dst (e.g. NAT, nexthop).
    Named(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpValue {
    /// `10.0.0.1` or `2001:db8::1`
    Addr(String),
    /// `10.0.0.0/8` or `2001:db8::/32`
    Cidr(String),
    /// `in (10.0.0.1, 10.0.0.2)`
    List(Vec<String>),
    /// `10.*.*.1`
    Wildcard(String),
}

// ---------------------------------------------------------------------------
// Port filter
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortFilter {
    pub direction: PortDirection,
    pub negated: bool,
    pub value: PortValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PortDirection {
    Src,
    Dst,
    Any,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PortValue {
    /// Single port number
    Single(u16),
    /// `80-90`
    Range(u16, u16),
    /// `80,443,8080` or `80-90,443`
    List(Vec<PortValue>),
    /// `http`, `dns`, etc.
    Named(String),
    /// `1024-` (open-ended)
    OpenRange(u16),
}

// ---------------------------------------------------------------------------
// Protocol filter
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtoFilter {
    pub negated: bool,
    pub value: ProtoValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtoValue {
    /// `tcp`, `udp`, `icmp`, `gre`
    Named(String),
    /// `47`
    Number(u8),
    /// `tcp,udp`
    List(Vec<ProtoValue>),
}

// ---------------------------------------------------------------------------
// Numeric filter
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericFilter {
    pub field: String,
    pub op: CompareOp,
    pub value: NumericValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NumericValue {
    /// Plain integer
    Integer(u64),
    /// Number with suffix: `1M` = 1_000_000
    WithSuffix(u64, String),
}

// ---------------------------------------------------------------------------
// String filter
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringFilterExpr {
    pub field: String,
    pub op: StringOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StringOp {
    /// `=`
    Eq,
    /// `!=`
    Ne,
    /// `~`
    Regex,
    /// `!~`
    NotRegex,
    /// `in`
    In,
    /// `not in`
    NotIn,
}

// ---------------------------------------------------------------------------
// Generic field filter (IPFIX IE by name)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldFilter {
    pub field: String,
    pub op: CompareOp,
    pub value: FieldValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldValue {
    Integer(u64),
    String(String),
    List(Vec<FieldValue>),
}

// ---------------------------------------------------------------------------
// Select expression
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectExpr {
    /// `select field1, field2, ...`
    Fields(Vec<SelectField>),
    /// `select *`
    All,
    /// `select * except (f1, f2)`
    AllExcept(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectField {
    pub expr: SelectFieldExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectFieldExpr {
    /// Simple field name
    Field(String),
    /// `bytes / packets` — computed
    BinaryOp {
        left: String,
        op: ArithOp,
        right: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

// ---------------------------------------------------------------------------
// Aggregation expressions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggExpr {
    /// `group by src, dport | sum(bytes)`
    GroupBy {
        keys: Vec<GroupByKey>,
        functions: Vec<AggCall>,
    },
    /// `top 10 by sum(bytes)`
    TopN { n: u64, by: AggCall, bottom: bool },
    /// `sort sum(bytes) desc`
    Sort { by: AggCall, descending: bool },
    /// `limit 100`
    Limit(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupByKey {
    /// Plain field name
    Field(String),
    /// `src /24` — subnet prefix
    Subnet { field: String, prefix_len: u8 },
    /// `5m` — time bucket
    TimeBucket(Duration),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCall {
    pub func: AggFunc,
    pub field: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunc {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Uniq,
    P50,
    P95,
    P99,
    Stddev,
    Rate,
    First,
    Last,
}

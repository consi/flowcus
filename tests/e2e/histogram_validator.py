"""
Histogram validator for e2e testing.

Executes histogram requests against /api/stats/histogram and validates
SSE responses against expected values computed from the query API.

Cross-validates histogram results against the query engine to ensure
both paths agree, regardless of how many flows have been flushed.
"""

import json
import time
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Callable, Optional

from ipfix_generator import FlowRecord, PROTO_TCP, PROTO_UDP, PROTO_ICMP


@dataclass
class HistogramResult:
    """Parsed histogram SSE response (final event)."""
    buckets: list[dict]  # [{timestamp, count}, ...]
    total_rows: int
    time_range: dict  # {start, end}
    bucket_ms: int
    done: bool
    all_events: list[dict]  # all SSE events (for progressive testing)
    http_status: int = 200
    error: Optional[str] = None

    @property
    def bucket_count(self) -> int:
        return len(self.buckets)

    @property
    def bucket_sum(self) -> int:
        return sum(b.get("count", 0) for b in self.buckets)

    @property
    def nonzero_buckets(self) -> list[dict]:
        return [b for b in self.buckets if b.get("count", 0) > 0]


def execute_histogram(
    api_base: str,
    time_range: dict,
    filters: list[dict] | None = None,
    logic: str = "and",
    buckets: int = 60,
) -> HistogramResult:
    """Execute a histogram request and parse the SSE response."""
    url = f"{api_base}/api/stats/histogram"
    payload = json.dumps({
        "time_range": time_range,
        "filters": filters or [],
        "logic": logic,
        "buckets": buckets,
    }).encode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode()
            events = _parse_sse_events(text)

            if not events:
                return HistogramResult(
                    buckets=[], total_rows=0, time_range={},
                    bucket_ms=0, done=False, all_events=[],
                    http_status=resp.status,
                    error="No SSE events in response",
                )

            final = _find_final_event(events)

            # Check for error events from the backend
            if "error" in final:
                return HistogramResult(
                    buckets=[], total_rows=0, time_range={},
                    bucket_ms=0, done=True, all_events=events,
                    http_status=resp.status,
                    error=f"Backend error: {final['error']}",
                )

            return HistogramResult(
                buckets=final.get("buckets", []),
                total_rows=final.get("total_rows", 0),
                time_range=final.get("time_range", {}),
                bucket_ms=final.get("bucket_ms", 0),
                done=final.get("done", False),
                all_events=events,
                http_status=resp.status,
            )
    except urllib.error.HTTPError as e:
        return HistogramResult(
            buckets=[], total_rows=0, time_range={},
            bucket_ms=0, done=False, all_events=[],
            http_status=e.code,
            error=str(e),
        )
    except Exception as e:
        return HistogramResult(
            buckets=[], total_rows=0, time_range={},
            bucket_ms=0, done=False, all_events=[],
            http_status=0,
            error=str(e),
        )


def _parse_sse_events(text: str) -> list[dict]:
    """Parse SSE data: lines from a text/event-stream response."""
    events = []
    for block in text.split("\n\n"):
        for line in block.split("\n"):
            trimmed = line.rstrip("\r")
            if trimmed.startswith("data:"):
                json_str = trimmed[5:].lstrip(" ")
                try:
                    events.append(json.loads(json_str))
                except json.JSONDecodeError:
                    pass
    return events


def _find_final_event(events: list[dict]) -> dict:
    """Find the last event with done=true, or the last event."""
    for ev in reversed(events):
        if ev.get("done") is True:
            return ev
    return events[-1] if events else {}


def _query_count(api_base: str, fql: str) -> int:
    """Execute an FQL query and return the total count."""
    from query_validator import execute_query
    r = execute_query(api_base, fql)
    return r.total


def _query_agg_count(api_base: str, fql: str) -> int:
    """Execute an FQL aggregation query and extract the count."""
    from query_validator import execute_query
    r = execute_query(api_base, fql)
    if r.rows:
        for i, col in enumerate(r.columns):
            if "count" in col.lower():
                try:
                    return int(r.rows[0][i])
                except (IndexError, ValueError, TypeError):
                    pass
    return 0


# ---------------------------------------------------------------------------
# Histogram test case builders
# ---------------------------------------------------------------------------

def build_histogram_test_cases(
    ingested_flows: list[FlowRecord],
    export_time: int,
    api_base: str,
) -> list[tuple[str, dict, Callable[[HistogramResult], dict]]]:
    """
    Build histogram test cases.

    Tests are split into two categories:
    - Structural: validate SSE format, bucket layout, coverage
    - Cross-validation: compare histogram totals against query API

    Returns list of (test_name, request_params, validator_fn).
    """
    tr = {
        "type": "absolute",
        "start": _unix_to_iso(export_time - 3600),
        "end": _unix_to_iso(export_time + 3600),
    }

    cases = []

    # ===================================================================
    # STRUCTURAL TESTS (no dependency on ingested data counts)
    # ===================================================================

    # --- 1. Valid SSE structure ---
    def validate_structure(result: HistogramResult) -> dict:
        has_buckets = len(result.buckets) > 0
        has_time_range = bool(result.time_range.get("start")) and bool(result.time_range.get("end"))
        has_bucket_ms = result.bucket_ms > 0
        is_done = result.done
        return {
            "expected_valid_structure": True,
            "actual_has_buckets": has_buckets,
            "actual_has_time_range": has_time_range,
            "actual_has_bucket_ms": has_bucket_ms,
            "actual_done": is_done,
            "match": has_buckets and has_time_range and has_bucket_ms and is_done,
        }

    cases.append(("hist_valid_structure", {"time_range": tr, "filters": []}, validate_structure))

    # --- 2. Bucket count sum equals total_rows (no double counting) ---
    def validate_bucket_sum(result: HistogramResult) -> dict:
        return {
            "expected_sum": result.total_rows,
            "actual_sum": result.bucket_sum,
            "match": result.bucket_sum == result.total_rows,
        }

    cases.append(("hist_bucket_sum_equals_total", {"time_range": tr, "filters": []}, validate_bucket_sum))

    # --- 3. Buckets are uniformly spaced ---
    def validate_uniform_spacing(result: HistogramResult) -> dict:
        if len(result.buckets) < 2:
            return {"match": True, "actual_bucket_count": len(result.buckets)}
        bs = result.bucket_ms
        mismatches = []
        for i in range(1, len(result.buckets)):
            t0 = result.buckets[i - 1]["timestamp"]
            t1 = result.buckets[i]["timestamp"]
            if t1 - t0 != bs:
                mismatches.append({"index": i, "gap": t1 - t0, "expected": bs})
        return {
            "expected_spacing": bs,
            "actual_mismatches": len(mismatches),
            "match": len(mismatches) == 0,
        }

    cases.append(("hist_uniform_spacing", {"time_range": tr, "filters": []}, validate_uniform_spacing))

    # --- 4. Buckets cover the queried time range ---
    def validate_time_coverage(result: HistogramResult) -> dict:
        if not result.buckets:
            return {"match": False, "actual_bucket_count": 0}
        t_start = result.time_range.get("start", 0)
        t_end = result.time_range.get("end", 0)
        first_ts = result.buckets[0]["timestamp"]
        last_ts = result.buckets[-1]["timestamp"]
        last_end = last_ts + result.bucket_ms
        return {
            "expected_first_le_start": True,
            "actual_first_ts": first_ts,
            "actual_time_start": t_start,
            "actual_last_end": last_end,
            "actual_time_end": t_end,
            "match": first_ts <= t_start and last_end >= t_end,
        }

    cases.append(("hist_time_coverage", {"time_range": tr, "filters": []}, validate_time_coverage))

    # --- 5. Each bucket has valid fields ---
    def validate_bucket_fields(result: HistogramResult) -> dict:
        invalid = []
        for i, b in enumerate(result.buckets):
            if "timestamp" not in b or "count" not in b:
                invalid.append(i)
            elif not isinstance(b["count"], (int, float)):
                invalid.append(i)
        return {
            "expected_all_valid": True,
            "actual_invalid_count": len(invalid),
            "match": len(invalid) == 0 and len(result.buckets) > 0,
        }

    cases.append(("hist_bucket_fields_valid", {"time_range": tr, "filters": []}, validate_bucket_fields))

    # --- 6. All non-zero buckets contain the export_time ---
    def validate_data_placement(result: HistogramResult) -> dict:
        if not result.buckets:
            return {"match": False}
        bs = result.bucket_ms
        other_nonzero = [
            b for b in result.buckets
            if b["count"] > 0 and not (b["timestamp"] <= export_time < b["timestamp"] + bs)
        ]
        return {
            "expected_no_stray_data": True,
            "actual_stray_buckets": len(other_nonzero),
            "match": len(other_nonzero) == 0,
        }

    cases.append(("hist_data_in_correct_bucket", {"time_range": tr, "filters": []}, validate_data_placement))

    # --- 7. Empty range returns zero ---
    def validate_empty_range(result: HistogramResult) -> dict:
        return {
            "expected_total": 0,
            "actual_total": result.total_rows,
            "actual_bucket_sum": result.bucket_sum,
            "match": result.total_rows == 0 and result.bucket_sum == 0,
        }

    empty_tr = {
        "type": "absolute",
        "start": "2000-01-01T00:00:00Z",
        "end": "2000-01-01T00:05:00Z",
    }
    cases.append(("hist_empty_range_zero", {"time_range": empty_tr, "filters": []}, validate_empty_range))

    # --- 8. Progressive events have monotonically increasing totals ---
    def validate_progressive(result: HistogramResult) -> dict:
        if len(result.all_events) < 2:
            return {"match": True, "actual_event_count": len(result.all_events)}
        totals = [ev.get("total_rows", 0) for ev in result.all_events if "total_rows" in ev]
        if len(totals) < 2:
            return {"match": True, "actual_event_count": len(totals)}
        monotonic = all(totals[i] <= totals[i + 1] for i in range(len(totals) - 1))
        return {
            "expected_monotonic": True,
            "actual_monotonic": monotonic,
            "actual_totals": totals,
            "match": monotonic,
        }

    cases.append(("hist_progressive_monotonic", {"time_range": tr, "filters": []}, validate_progressive))

    # --- 9. Final SSE event has done=true ---
    def validate_done_flag(result: HistogramResult) -> dict:
        has_done_event = any(ev.get("done") is True for ev in result.all_events)
        return {
            "expected_done": True,
            "actual_done": has_done_event,
            "match": has_done_event,
        }

    cases.append(("hist_done_flag_present", {"time_range": tr, "filters": []}, validate_done_flag))

    # --- 10. All bucket counts are non-negative ---
    def validate_nonneg(result: HistogramResult) -> dict:
        negatives = [b for b in result.buckets if b.get("count", 0) < 0]
        return {
            "expected_no_negatives": True,
            "actual_negative_count": len(negatives),
            "match": len(negatives) == 0,
        }

    cases.append(("hist_no_negative_counts", {"time_range": tr, "filters": []}, validate_nonneg))

    # ===================================================================
    # CROSS-VALIDATION: histogram vs query API
    # ===================================================================

    # --- 11. Histogram total matches query total ---
    def validate_hist_vs_query(result: HistogramResult) -> dict:
        query_total = _query_count(api_base, "last 2h")
        return {
            "expected_query_total": query_total,
            "actual_hist_total": result.total_rows,
            "match": result.total_rows == query_total,
        }

    cases.append(("hist_total_matches_query", {"time_range": tr, "filters": []}, validate_hist_vs_query))

    # --- 12. Filtered histogram: TCP total matches query ---
    def validate_tcp_cross(result: HistogramResult) -> dict:
        query_total = _query_count(api_base, "last 2h | proto tcp")
        return {
            "expected_query_total": query_total,
            "actual_hist_total": result.total_rows,
            "match": result.total_rows == query_total,
        }

    cases.append(("hist_filter_tcp_matches_query", {
        "time_range": tr,
        "filters": [{"field": "protocolIdentifier", "op": "eq", "value": PROTO_TCP}],
    }, validate_tcp_cross))

    # --- 13. Filtered histogram: UDP total matches query ---
    def validate_udp_cross(result: HistogramResult) -> dict:
        query_total = _query_count(api_base, "last 2h | proto udp")
        return {
            "expected_query_total": query_total,
            "actual_hist_total": result.total_rows,
            "match": result.total_rows == query_total,
        }

    cases.append(("hist_filter_udp_matches_query", {
        "time_range": tr,
        "filters": [{"field": "protocolIdentifier", "op": "eq", "value": PROTO_UDP}],
    }, validate_udp_cross))

    # --- 14. Filtered histogram: dport 80 matches query ---
    def validate_dport80_cross(result: HistogramResult) -> dict:
        query_total = _query_count(api_base, "last 2h | dport 80")
        return {
            "expected_query_total": query_total,
            "actual_hist_total": result.total_rows,
            "match": result.total_rows == query_total,
        }

    cases.append(("hist_filter_dport80_matches_query", {
        "time_range": tr,
        "filters": [{"field": "destinationTransportPort", "op": "eq", "value": 80}],
    }, validate_dport80_cross))

    # --- 15. Filtered histogram: src IP matches query ---
    def validate_src_cross(result: HistogramResult) -> dict:
        query_total = _query_count(api_base, "last 2h | src 10.0.0.1")
        return {
            "expected_query_total": query_total,
            "actual_hist_total": result.total_rows,
            "match": result.total_rows == query_total,
        }

    cases.append(("hist_filter_src_matches_query", {
        "time_range": tr,
        "filters": [{"field": "sourceIPv4Address", "op": "eq", "value": "10.0.0.1"}],
    }, validate_src_cross))

    # --- 16. Filtered histogram: bytes > 1000 matches query ---
    def validate_bytes_cross(result: HistogramResult) -> dict:
        query_total = _query_count(api_base, "last 2h | bytes > 1000")
        return {
            "expected_query_total": query_total,
            "actual_hist_total": result.total_rows,
            "match": result.total_rows == query_total,
        }

    cases.append(("hist_filter_bytes_gt_matches_query", {
        "time_range": tr,
        "filters": [{"field": "octetDeltaCount", "op": "gt", "value": 1000}],
    }, validate_bytes_cross))

    # --- 17. Filter reduces total ---
    def validate_filter_reduces(result: HistogramResult) -> dict:
        unfiltered = execute_histogram(api_base, tr, filters=[])
        return {
            "expected_less_than": unfiltered.total_rows,
            "actual_filtered": result.total_rows,
            "match": result.total_rows < unfiltered.total_rows,
        }

    cases.append(("hist_filter_reduces_total", {
        "time_range": tr,
        "filters": [{"field": "protocolIdentifier", "op": "eq", "value": PROTO_TCP}],
    }, validate_filter_reduces))

    # --- 18. Filtered bucket sum == filtered total ---
    def validate_filtered_bucket_sum(result: HistogramResult) -> dict:
        return {
            "expected_sum": result.total_rows,
            "actual_sum": result.bucket_sum,
            "match": result.bucket_sum == result.total_rows,
        }

    cases.append(("hist_filtered_bucket_sum", {
        "time_range": tr,
        "filters": [{"field": "protocolIdentifier", "op": "eq", "value": PROTO_TCP}],
    }, validate_filtered_bucket_sum))

    # --- 19. Narrow range returns <= wide range ---
    def validate_narrow_range(result: HistogramResult) -> dict:
        wide = execute_histogram(api_base, tr, filters=[])
        return {
            "expected_le_wide": wide.total_rows,
            "actual_narrow": result.total_rows,
            "match": result.total_rows <= wide.total_rows,
        }

    narrow_tr = {
        "type": "absolute",
        "start": _unix_to_iso(export_time - 30),
        "end": _unix_to_iso(export_time + 30),
    }
    cases.append(("hist_narrow_range_subset", {"time_range": narrow_tr, "filters": []}, validate_narrow_range))

    # --- 20. Histogram has data (positive total) ---
    def validate_has_data(result: HistogramResult) -> dict:
        return {
            "expected_positive_total": True,
            "actual_total": result.total_rows,
            "match": result.total_rows > 0,
        }

    cases.append(("hist_has_data", {"time_range": tr, "filters": []}, validate_has_data))

    return cases


def build_histogram_consistency_test_cases(
    api_base: str,
    export_time: int,
) -> list[tuple[str, dict, Callable[[HistogramResult], dict]]]:
    """
    Build histogram consistency tests for phases 4-5 (post-bulk/merge).
    """
    tr = {
        "type": "absolute",
        "start": _unix_to_iso(export_time - 3600),
        "end": _unix_to_iso(export_time + 3600),
    }

    cases = []

    # --- 1. Bucket sum == total_rows ---
    def validate_sum_eq_total(result: HistogramResult) -> dict:
        return {
            "expected_sum": result.total_rows,
            "actual_sum": result.bucket_sum,
            "match": result.bucket_sum == result.total_rows and result.total_rows > 0,
        }

    cases.append(("hist_consistency_sum_eq_total", {"time_range": tr, "filters": []}, validate_sum_eq_total))

    # --- 2. Histogram total > 0 (data survived) ---
    def validate_data_exists(result: HistogramResult) -> dict:
        return {
            "expected_min": 100,
            "actual_total": result.total_rows,
            "match": result.total_rows >= 100,
        }

    cases.append(("hist_consistency_data_survived", {"time_range": tr, "filters": []}, validate_data_exists))

    # --- 3. Histogram total matches query total ---
    def validate_hist_query_agree(result: HistogramResult) -> dict:
        query_total = _query_count(api_base, "last 2h")
        return {
            "expected_query_total": query_total,
            "actual_hist_total": result.total_rows,
            "match": result.total_rows == query_total,
        }

    cases.append(("hist_consistency_matches_query", {"time_range": tr, "filters": []}, validate_hist_query_agree))

    # --- 4. Filtered histogram TCP matches query ---
    def validate_tcp_cross(result: HistogramResult) -> dict:
        query_total = _query_count(api_base, "last 2h | proto tcp")
        return {
            "expected_query_total": query_total,
            "actual_hist_total": result.total_rows,
            "match": result.total_rows == query_total,
        }

    cases.append(("hist_consistency_tcp_matches_query", {
        "time_range": tr,
        "filters": [{"field": "protocolIdentifier", "op": "eq", "value": PROTO_TCP}],
    }, validate_tcp_cross))

    # --- 5. Idempotent: same request twice returns same total ---
    def validate_idempotent(result: HistogramResult) -> dict:
        second = execute_histogram(api_base, tr, filters=[])
        return {
            "expected_total": result.total_rows,
            "actual_total": second.total_rows,
            "match": result.total_rows == second.total_rows,
        }

    cases.append(("hist_consistency_idempotent", {"time_range": tr, "filters": []}, validate_idempotent))

    # --- 6. Buckets sorted ---
    def validate_sorted(result: HistogramResult) -> dict:
        if len(result.buckets) < 2:
            return {"match": True}
        timestamps = [b["timestamp"] for b in result.buckets]
        is_sorted = all(timestamps[i] < timestamps[i + 1] for i in range(len(timestamps) - 1))
        return {
            "expected_sorted": True,
            "actual_sorted": is_sorted,
            "match": is_sorted,
        }

    cases.append(("hist_consistency_sorted", {"time_range": tr, "filters": []}, validate_sorted))

    # --- 7. Done flag present ---
    def validate_done(result: HistogramResult) -> dict:
        return {
            "expected_done": True,
            "actual_done": result.done,
            "match": result.done,
        }

    cases.append(("hist_consistency_done", {"time_range": tr, "filters": []}, validate_done))

    # --- 8. dport 80 filtered histogram matches query ---
    def validate_dport_cross(result: HistogramResult) -> dict:
        query_total = _query_count(api_base, "last 2h | dport 80")
        return {
            "expected_query_total": query_total,
            "actual_hist_total": result.total_rows,
            "match": result.total_rows == query_total,
        }

    cases.append(("hist_consistency_dport80_matches_query", {
        "time_range": tr,
        "filters": [{"field": "destinationTransportPort", "op": "eq", "value": 80}],
    }, validate_dport_cross))

    # --- 9. Filtered bucket sum == filtered total ---
    def validate_filtered_sum(result: HistogramResult) -> dict:
        return {
            "expected_sum": result.total_rows,
            "actual_sum": result.bucket_sum,
            "match": result.bucket_sum == result.total_rows,
        }

    cases.append(("hist_consistency_filtered_sum", {
        "time_range": tr,
        "filters": [{"field": "protocolIdentifier", "op": "eq", "value": PROTO_TCP}],
    }, validate_filtered_sum))

    # --- 10. No negative bucket counts ---
    def validate_nonneg(result: HistogramResult) -> dict:
        negatives = [b for b in result.buckets if b.get("count", 0) < 0]
        return {
            "expected_no_negatives": True,
            "actual_negative_count": len(negatives),
            "match": len(negatives) == 0,
        }

    cases.append(("hist_consistency_no_negatives", {"time_range": tr, "filters": []}, validate_nonneg))

    return cases


class HistogramValidator:
    """Runs histogram test cases and collects verdicts."""

    def __init__(self, api_base: str):
        self.api_base = api_base
        self.verdicts: list[dict] = []

    def run_test_suite(
        self,
        test_cases: list[tuple[str, dict, Callable]],
        phase: str,
        storage_state: dict,
    ) -> list[dict]:
        """Run all histogram test cases and return verdicts."""
        phase_verdicts = []
        for test_name, request_params, validator in test_cases:
            start = time.time()

            tr = request_params.get("time_range", {})
            filters = request_params.get("filters", [])
            result = execute_histogram(self.api_base, tr, filters=filters)
            duration_ms = (time.time() - start) * 1000

            if result.error:
                verdict = {
                    "test_name": test_name,
                    "phase": phase,
                    "passed": False,
                    "expected": {"no_error": True},
                    "actual": {"error": result.error, "http_status": result.http_status},
                    "storage_state": storage_state,
                    "details": f"Histogram request failed: {result.error}",
                    "duration_ms": duration_ms,
                }
            else:
                comparison = validator(result)
                passed = comparison.get("match", False)
                verdict = {
                    "test_name": test_name,
                    "phase": phase,
                    "passed": passed,
                    "expected": {k: v for k, v in comparison.items() if k.startswith("expected")},
                    "actual": {k: v for k, v in comparison.items() if k.startswith("actual")},
                    "storage_state": storage_state,
                    "details": "" if passed else f"Mismatch: {comparison}",
                    "duration_ms": duration_ms,
                }

            phase_verdicts.append(verdict)
            self.verdicts.append(verdict)

            status = "PASS" if verdict["passed"] else "FAIL"
            print(f"  [{status}] {test_name}")
            if not verdict["passed"]:
                print(f"         Expected: {verdict['expected']}")
                print(f"         Actual:   {verdict['actual']}")

        return phase_verdicts

    def summary(self) -> dict:
        total = len(self.verdicts)
        passed = sum(1 for v in self.verdicts if v["passed"])
        failed = total - passed
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": f"{passed/total*100:.1f}%" if total > 0 else "N/A",
            "by_phase": self._by_phase(),
        }

    def _by_phase(self) -> dict:
        phases: dict[str, dict] = {}
        for v in self.verdicts:
            p = v["phase"]
            if p not in phases:
                phases[p] = {"total": 0, "passed": 0, "failed": 0}
            phases[p]["total"] += 1
            if v["passed"]:
                phases[p]["passed"] += 1
            else:
                phases[p]["failed"] += 1
        return phases

    def all_verdicts(self) -> list[dict]:
        return self.verdicts


def _unix_to_iso(ts: int) -> str:
    """Convert Unix timestamp to ISO 8601 string."""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

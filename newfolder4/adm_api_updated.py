import logging
import time
from datetime import datetime, timedelta, date
from collections import defaultdict
from sqlalchemy import text
from database.connectors import get_adm_engine, get_atls_engine

"""
adm_api.py
- Optimized with pre-grouping (for large UNION results ~50k rows)
- Stage-specific marker_type_cd filtering (prevents cross-stage leakage)
- Robust ATLS↔ADM matching with canonicalization of keys
- Rich diagnostics & timings (DB time, grouping time, per-stage processing time)
- Detailed ATLS logs (separate Accounting vs Pricing original_message_id lists)
- Optional ADM fallback when ATLS message IDs are missing but stage markers exist
"""

logger = logging.getLogger(__name__)

# ----------------------------------
# Tunables
# ----------------------------------
LONG_RUNNING_THRESHOLD_MINUTES = 30
ENABLE_ADM_FALLBACK_WHEN_ATLS_EMPTY = True  # If True, evaluate status from ADM markers when ATLS IDs are missing

# Canonical region mapping if systems differ on labels/case
REGION_MAP = {
    "GLOBAL": "GLOBAL",
    "Global": "GLOBAL",
    "Amer": "AMER",
    "AMER": "AMER",
    "EMEA": "EMEA",
    "APAC": "APAC",
}

WORKFLOW_ORDER = {
    'trading_ars': 1, 'pricing_ars': 2, 'pricing_marker': 3,
    'pricing_raw': 4, 'pricing_enrich': 5, 'pricing_roll': 6, 'pricing_mart': 7,
    'eod_ars': 8, 'eod': 9, 'eod_marker': 10, 'eod_raw': 11,
    'eod_enrich': 12, 'eod_roll': 13, 'eod_mart': 14, 'eod_final': 15,
    'asof_events': 16, 'asof_marker': 17, 'aod': 18, 'aod_marker': 19,
    'aod_raw': 20, 'aod_enrich': 21, 'aod_roll': 22, 'aod_mart': 23, 'aod_final': 24,
    'sod_ars': 25, 'sod': 26, 'sod_marker': 27, 'sod_raw': 28,
    'sod_enrich': 29, 'sod_roll': 30, 'sod_mart': 31, 'sod_final': 32
}

EXPECTED_SUBJECTS = {
    "eodpx_raw": ["valuation_prices"],
    "eodpx_enrich": ["valuation_prices"],
    "eodpx_roll": ["valuation_prices"],
    "eodpx_mart": ["valuation_prices"],
    "eodpx_final": ["valuation_prices"],

    "eod_raw": ["positions", "taxlots", "transactions", "cash_settlements", "disposal_lots"],
    "eod_enrich": ["positions", "taxlots", "transactions", "cash_settlements", "disposal_lots"],
    "eod_roll": ["positions", "taxlots", "transactions", "cash_settlements", "disposal_lots"],
    "eod_mart": ["positions", "taxlots", "transactions", "cash_settlements", "disposal_lots"],
    "eod_final": ["positions", "taxlots", "transactions", "cash_settlements", "disposal_lots"],

    "aod_raw": ["positions", "taxlots"],
    "aod_enrich": ["positions", "taxlots"],
    "aod_roll": ["positions", "taxlots"],
    "aod_mart": ["positions", "taxlots"],
    "aod_final": ["positions", "taxlots"],

    "sod_raw": ["positions", "taxlots", "transactions", "cash_settlements", "disposal_lots"],
    "sod_enrich": ["positions", "taxlots", "transactions", "cash_settlements", "disposal_lots"],
    "sod_roll": ["positions", "taxlots", "transactions", "cash_settlements", "disposal_lots"],
    "sod_mart": ["positions", "taxlots", "transactions", "cash_settlements", "disposal_lots"],
    "sod_final": ["positions", "taxlots", "transactions", "cash_settlements", "disposal_lots"]
}

# Stage → marker_type_cd mapping (strict filtering per stage)
STAGE_MARKER_MAP = {
    # EOD
    ("EOD", "RAW"): "eodRegionSubjectAreaRawLoadComplete",
    ("EOD", "ENRICH"): "eodRegionSubjectAreaEnriched",
    ("EOD", "ROLL"): "eodRegionSubjectAreaRollupComplete",
    ("EOD", "MART"): "eodRegionMartLoadComplete",

    # SOD
    ("SOD", "RAW"): "sodRegionSubjectAreaRawLoadComplete",
    ("SOD", "ENRICH"): "sodRegionSubjectAreaEnriched",
    ("SOD", "ROLL"): "sodRegionSubjectAreaRollupComplete",
    ("SOD", "MART"): "sodRegionSubjectAreaMartLoadComplete",

    # AOD
    ("AOD", "RAW"): "asOfRegionSubjectAreaRawLoadComplete",
    ("AOD", "ENRICH"): "asOfRegionSubjectAreaEnriched",
    ("AOD", "ROLL"): "asOfRegionSubjectAreaRollupComplete",
    ("AOD", "MART"): "asOfRegionSubjectAreaMartLoadComplete",
    ("AOD", "FINAL"): "asOfRegionsStatementsPublished",  # plus allow eodAllRegionStatementsPublished in workflow filter
}

# ----------------------------------
# Utilities
# ----------------------------------

def _norm_client(v: str) -> str:
    return (v or "").strip().upper()


def _norm_region(v: str) -> str:
    if v is None:
        return ""
    # exact map first, then UPPER fallback
    return REGION_MAP.get(v, REGION_MAP.get(v.upper(), v.strip().upper()))


def _norm_snapshot(v: str) -> str:
    return (v or "").strip().upper()


def _norm_date(d) -> str:
    """Accepts date/datetime/str and returns YYYY-MM-DD string."""
    if d is None:
        return ""
    if isinstance(d, str):
        # keep only date portion if timestamp-like
        try:
            return str(datetime.fromisoformat(d).date())
        except Exception:
            # maybe already 'YYYY-MM-DD'
            return d[:10]
    if isinstance(d, datetime):
        return str(d.date())
    if isinstance(d, date):
        return str(d)
    return str(d)

# ----------------------------------
# SQL builder
# ----------------------------------

def get_combined_workflow_sql(business_date: str, sod_date: str) -> str:
    return f"""
SELECT
    m.created_at AS last_updated,
    m.client_cd,
    m.processing_region_cd,
    m.snapshot_type_cd,
    m.marker_type_cd,
    m.subject_area_cd,
    m.original_message_id,
    CAST(m.business_dt AS date) AS business_dt,
    'success' AS status
FROM markers m
WHERE m.business_dt IN ('{business_date}', '{sod_date}')

UNION ALL

SELECT
    fm.created_at AS last_updated,
    fm.marker->'header'->>'party_cd' AS client_cd,
    fm.marker->'header'->>'processing_region_cd' AS processing_region_cd,
    fm.marker->'payload'->>'snapshot_type_cd' AS snapshot_type_cd,
    fm.marker_type AS marker_type_cd,
    fm.marker->'payload'->>'subject_area_cd' AS subject_area_cd,
    fm.original_message_id,
    CAST(fm.marker->'payload'->>'business_date' AS date) AS business_dt,
    'success' AS status
FROM final_markers fm
WHERE CAST(fm.marker->'payload'->>'business_date' AS date) IN ('{business_date}', '{sod_date}')

UNION ALL

SELECT
    el.created_at AS last_updated,
    m.client_cd,
    m.processing_region_cd,
    m.snapshot_type_cd,
    el.service_nm AS marker_type_cd,
    el.table_nm AS subject_area_cd,
    el.original_message_id,
    CAST(m.business_dt AS date) AS business_dt,
    'failed' AS status
FROM error_logs el
JOIN markers m ON el.original_message_id = m.original_message_id
WHERE m.business_dt IN ('{business_date}', '{sod_date}')
"""

# ----------------------------------
# Evaluators
# ----------------------------------

def evaluate_workflow_status(subjects_found, expected_subjects, first_event_time):
    if not subjects_found:
        return "pending"
    missing = set(expected_subjects) - set(subjects_found)
    if not missing:
        return "completed"
    elif first_event_time:
        age = (datetime.now() - first_event_time).total_seconds() / 60
        if age > LONG_RUNNING_THRESHOLD_MINUTES:
            return "long_running"
        return "inprogress"
    return "pending"


def evaluate_aod_stage(rows, total_count):
    positions_ids = {r["original_message_id"] for r in rows if r["subject_area_cd"] == "positions"}
    taxlots_ids = {r["original_message_id"] for r in rows if r["subject_area_cd"] == "taxlots"}

    pos_count, tax_count = len(positions_ids), len(taxlots_ids)

    if total_count == 0:
        return "pending", pos_count, tax_count
    if pos_count == total_count and tax_count == total_count:
        return "completed", pos_count, tax_count
    elif pos_count or tax_count:
        return "inprogress", pos_count, tax_count
    return "pending", pos_count, tax_count


def evaluate_aod_final(rows, total_count):
    positions_ids = {
        r["original_message_id"]
        for r in rows
        if r["marker_type_cd"] == "asOfRegionsStatementsPublished" and r["subject_area_cd"] == "positions"
    }
    taxlots_ids = {
        r["original_message_id"]
        for r in rows
        if r["marker_type_cd"] == "asOfRegionsStatementsPublished" and r["subject_area_cd"] == "taxlots"
    }
    global_marker = any(r["marker_type_cd"] == "eodAllRegionStatementsPublished" for r in rows)

    pos_count, tax_count = len(positions_ids), len(taxlots_ids)

    if total_count == 0:
        return "pending", pos_count, tax_count
    if pos_count == total_count and tax_count == total_count and global_marker:
        return "completed", pos_count, tax_count
    elif pos_count or tax_count or global_marker:
        return "inprogress", pos_count, tax_count
    return "pending", pos_count, tax_count

# ----------------------------------
# ATLS message IDs (with diagnostics)
# ----------------------------------

def get_atls_message_ids(business_date: str, sod_date: str):
    """Return a mapping of (client, region) → list of dicts with id, snapshot, business_dt, source.
    Also logs separate counts and sample IDs per source (Accounting vs Pricing).
    Keys and dates are canonicalized to reduce prod mismatches.
    """
    message_ids_map = defaultdict(list)

    # Helper to collect + log per-source
    def _fetch_source(conn, table, params):
        q = text(f"""
            SELECT client_cd, processing_region_cd, snapshot_type_cd, business_dt, original_message_id
            FROM {table}
            WHERE business_dt IN (:business_date, :sod_date)
        """)
        t0 = time.time()
        rows = conn.execute(q, params).mappings().all()
        dur = time.time() - t0
        logger.info(f"ATLS fetch from {table}: {len(rows)} rows in {dur:.2f}s for {params}")
        return rows

    try:
        with get_atls_engine().connect() as conn:
            params = {"business_date": business_date, "sod_date": sod_date}
            acc_rows = _fetch_source(conn, "accounting_events", params)
            prc_rows = _fetch_source(conn, "pricing_events", params)

            # Normalize & load to map with source labels
            def _ingest(rows, source):
                for r in rows:
                    client = _norm_client(r["client_cd"])
                    region = _norm_region(r["processing_region_cd"])
                    snap = _norm_snapshot(r["snapshot_type_cd"])
                    bdt = _norm_date(r["business_dt"])
                    oid = r["original_message_id"]
                    if not oid:
                        continue
                    message_ids_map[(client, region)].append({
                        "id": oid,
                        "snapshot": snap,
                        "business_dt": bdt,
                        "source": source,
                    })

            _ingest(acc_rows, "ACCOUNTING")
            _ingest(prc_rows, "PRICING")

            # Log summary per (client, region, snapshot, date)
            summary = defaultdict(lambda: {"ACCOUNTING": [], "PRICING": []})
            for (client, region), items in message_ids_map.items():
                for it in items:
                    key = (client, region, it["snapshot"], it["business_dt"]) 
                    summary[key][it["source"].upper()].append(it["id"])

            # Print a concise summary
            for (client, region, snap, bdt), src_map in summary.items():
                acc_ids = src_map["ACCOUNTING"]
                prc_ids = src_map["PRICING"]
                logger.info(
                    "ATLS IDs [%s|%s|%s|%s] accounting=%d%s pricing=%d%s",
                    client, region, snap, bdt,
                    len(acc_ids), f" sample={acc_ids[:3]}" if acc_ids else "",
                    len(prc_ids), f" sample={prc_ids[:3]}" if prc_ids else "",
                )
    except Exception as e:
        logger.error(f"Error getting message IDs from ATLS: {e}")

    return message_ids_map

# ----------------------------------
# Row grouping (ADM+final_markers+error_logs)
# ----------------------------------

def group_rows_by_key(all_rows):
    grouped = defaultdict(list)
    for r in all_rows:
        key = (
            _norm_client(r["client_cd"]),
            _norm_region(r["processing_region_cd"]),
            _norm_snapshot(r["snapshot_type_cd"]),
            _norm_date(r["business_dt"]),
        )
        grouped[key].append(r)
    return grouped

# ----------------------------------
# Main
# ----------------------------------

def calculate_sod_date(business_date: str) -> str:
    business_dt = datetime.strptime(business_date, "%Y-%m-%d")
    sod_date = business_dt + timedelta(days=1)
    while sod_date.weekday() >= 5:  # skip weekends
        sod_date += timedelta(days=1)
    return sod_date.strftime("%Y-%m-%d")


def get_combined_workflow_sql_rows(business_date: str, sod_date: str):
    sql = get_combined_workflow_sql(business_date, sod_date)
    t0 = time.time()
    with get_adm_engine().connect() as conn:
        result = conn.execute(text(sql))
        rows = [dict(row) for row in result.mappings()]
    dur = time.time() - t0
    logger.info("ADM combined SQL fetched %d rows in %.2fs", len(rows), dur)
    return rows


def get_combined_workflow_status(clients_regions, business_date):
    sod_date = calculate_sod_date(business_date)
    logger.info("Running ADM workflow for business_date=%s, sod_date=%s", business_date, sod_date)

    # ATLS IDs (with logs per source)
    atls_message_map = get_atls_message_ids(business_date, sod_date)

    # ADM rows
    all_rows = get_combined_workflow_sql_rows(business_date, sod_date)

    # Group once
    t_group = time.time()
    grouped_rows = group_rows_by_key(all_rows)
    logger.info("Row grouping completed in %.2fs. Groups=%d", time.time() - t_group, len(grouped_rows))

    workflows = []
    t_process = time.time()

    for client_in, region_in in clients_regions:
        # Canonicalize keys for lookup
        client = _norm_client(client_in)
        region = _norm_region(region_in)

        message_entries = atls_message_map.get((client, region), [])

        for workflow_type, expected_subjects in EXPECTED_SUBJECTS.items():
            stage_start = time.time()

            snapshot = _norm_snapshot(workflow_type.split("_")[0])
            stage = workflow_type.split("_")[1].upper()
            target_date = sod_date if snapshot == "SOD" else business_date
            key = (client, region, snapshot, target_date)

            # Filter ATLS message ids for this (client, region, snapshot, date)
            message_items = [
                m for m in message_entries
                if m["snapshot"] == snapshot and m["business_dt"] == target_date
            ]
            message_ids = [m["id"] for m in message_items]

            # Log per-stage key and ATLS IDs (split by source)
            acc_ids = [m["id"] for m in message_items if m.get("source") == "ACCOUNTING"]
            prc_ids = [m["id"] for m in message_items if m.get("source") == "PRICING"]
            logger.debug(
                "ATLS key [%s|%s|%s|%s] → total=%d acc=%d%s prc=%d%s",
                client, region, snapshot, target_date,
                len(message_ids), len(acc_ids), f" sample={acc_ids[:3]}" if acc_ids else "",
                len(prc_ids), f" sample={prc_ids[:3]}" if prc_ids else "",
            )

            candidate_rows = grouped_rows.get(key, [])
            marker_filter = STAGE_MARKER_MAP.get((snapshot, stage))

            if snapshot == "AOD" and stage == "FINAL":
                workflow_rows = [
                    r for r in candidate_rows
                    if (not message_ids or r["original_message_id"] in message_ids
                        or r["marker_type_cd"] == "eodAllRegionStatementsPublished")
                ]
            else:
                workflow_rows = [
                    r for r in candidate_rows
                    if (not message_ids or r["original_message_id"] in message_ids)
                    and (not marker_filter or r["marker_type_cd"] == marker_filter)
                ]

            # If ATLS has no IDs but ADM has stage markers, optionally fallback
            if ENABLE_ADM_FALLBACK_WHEN_ATLS_EMPTY and not message_ids and workflow_rows:
                adm_ids = list({r.get("original_message_id") for r in workflow_rows if r.get("original_message_id")})
                if adm_ids:
                    logger.warning(
                        "ATLS empty, using ADM fallback IDs for [%s|%s|%s|%s]: %s",
                        client, region, snapshot + "_" + stage, target_date, adm_ids[:3] + (["..."] if len(adm_ids) > 3 else [])
                    )
                    message_ids = adm_ids

            # Subjects only from expected set
            subjects_found = list({
                r["subject_area_cd"] for r in workflow_rows if r["subject_area_cd"] in expected_subjects
            })
            missing = list(set(expected_subjects) - set(subjects_found))

            # Timestamps for long-running
            first_event_time = min([r["last_updated"] for r in workflow_rows if r.get("last_updated")], default=None)

            # Decide status
            if snapshot == "AOD":
                total_count = len(message_ids)
                if stage == "FINAL":
                    status, pos_count, tax_count = evaluate_aod_final(workflow_rows, total_count)
                else:
                    status, pos_count, tax_count = evaluate_aod_stage(workflow_rows, total_count)
            else:
                # Even if message_ids is empty (fallback case), evaluate from subjects
                status = evaluate_workflow_status(subjects_found, expected_subjects, first_event_time)
                pos_count, tax_count, total_count = None, None, None

                # Failure detection from error_logs slice
                failed_rows = [r for r in workflow_rows if r.get("status") == "failed"]
                if failed_rows:
                    service_nm_list = [(fr.get("marker_type_cd") or "").lower() for fr in failed_rows]
                    if stage == "RAW" and any("raw" in s for s in service_nm_list):
                        status = "failed"
                    elif stage == "ENRICH" and any("enrich" in s for s in service_nm_list):
                        status = "failed"
                    elif stage in ("ROLL", "ROLLUP") and any(("roll" in s or "rollup" in s) for s in service_nm_list):
                        status = "failed"
                    elif stage == "MART" and any("mart" in s for s in service_nm_list):
                        status = "failed"
                    elif stage == "FINAL" and any("final" in s for s in service_nm_list):
                        status = "failed"

            # Assemble response entry
            last_updated = max([r["last_updated"] for r in workflow_rows if r.get("last_updated")], default=None)
            first_oid = None
            if message_ids:
                first_oid = message_ids[0]
            else:
                # If still empty, try to surface an ID from ADM rows
                first_oid = next((r.get("original_message_id") for r in workflow_rows if r.get("original_message_id")), None)

            workflows.append({
                "client_cd": client,
                "processing_region_cd": region,
                "workflow_type": workflow_type,
                "snapshot_type_cd": snapshot,
                "status": status,
                "status_with_long_running": status,
                "subjects_found": subjects_found,
                "last_updated": last_updated,
                "business_dt": target_date,
                "original_message_id": first_oid,
                "total_count": total_count,
                "positions_count": pos_count if snapshot == "AOD" else None,
                "taxlots_count": tax_count if snapshot == "AOD" else None,
            })

            # Per-stage summary timing/log
            elapsed = time.time() - stage_start
            logger.debug(
                "Stage [%s|%s|%s] status=%s subjects=%s missing=%s rows=%d elapsed=%.3fs",
                client, region, workflow_type, status, subjects_found, missing, len(workflow_rows), elapsed
            )

    total_elapsed = time.time() - t_process
    logger.info("Python workflow processing completed in %.2fs. Total workflows=%d", total_elapsed, len(workflows))

    workflows.sort(key=lambda w: WORKFLOW_ORDER.get(w["workflow_type"], 999))
    return workflows

# adm_api.py
import logging
import time
from datetime import datetime, timedelta, date
from collections import defaultdict
from sqlalchemy import text
from database.connectors import get_adm_engine, get_atls_engine

"""
adm_api.py

Updated to avoid duplicate combined-SQL DB calls:
- After get_combined_workflow_status fetches combined rows (all_rows),
  it populates a short-lived in-memory cache (_COMBINED_ROWS_CACHE) keyed by
  (client, region, business_date).
- get_all_reporting_loaders_status will reuse cached all_rows when available,
  avoiding a second DB call for the same client/region/business_date.
- If cache miss, get_all_reporting_loaders_status falls back to calling
  get_combined_workflow_sql_rows() (single DB call).
No other workflow logic changes.
"""

logger = logging.getLogger(__name__)

# ----------------------------------
# Tunables
# ----------------------------------
LONG_RUNNING_THRESHOLD_MINUTES = 30
ENABLE_ADM_FALLBACK_WHEN_ATLS_EMPTY = True  # If True, evaluate status from ADM markers when ATLS IDs are missing

# Cache TTL for combined rows (seconds). Keep short to avoid stale data.
_COMBINED_ROWS_CACHE_TTL_SECONDS = 20
_COMBINED_ROWS_CACHE = {}  # key -> {"ts": epoch_seconds, "rows": [...]}

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

# Stage → marker_type_cd filtering
STAGE_MARKER_FILTERS = {
    ("EOD", "RAW"): "eodRegionSubjectAreaRawLoadComplete",
    ("EOD", "ENRICH"): "eodRegionSubjectAreaEnriched",
    ("EOD", "ROLL"): "eodRegionSubjectAreaRollupComplete",
    ("EOD", "MART"): {
        "eodRegionPositionsMartLoadComplete",
        "eodRegionTaxlotsMartLoadComplete",
        "eodRegionTransactionsMartLoadComplete",
        "eodRegionDisposalLotsMartLoadComplete",
        "eodRegionCashSettlementsMartLoadComplete",
    },

    ("SOD", "RAW"): "sodRegionSubjectAreaRawLoadComplete",
    ("SOD", "ENRICH"): "sodRegionSubjectAreaEnriched",
    ("SOD", "ROLL"): "sodRegionSubjectAreaRollupComplete",
    ("SOD", "MART"): {
        "sodRegionPositionsMartLoadComplete",
        "sodRegionTaxlotsMartLoadComplete",
        "sodRegionTransactionsMartLoadComplete",
        "sodRegionDisposalLotsMartLoadComplete",
        "sodRegionCashSettlementsMartLoadComplete",
    },

    ("AOD", "RAW"): "asOfRegionSubjectAreaRawLoadComplete",
    ("AOD", "ENRICH"): "asOfRegionSubjectAreaEnriched",
    ("AOD", "ROLL"): "asOfRegionSubjectAreaRollupComplete",
    ("AOD", "MART"): "asOfRegionSubjectAreaMartLoadComplete",
    ("AOD", "FINAL"): "asOfRegionsStatementsPublished",
}

# --------------------------------------------------------------------
# Reporting static mapping (from your old code)
# --------------------------------------------------------------------
REPORTING_STATIC_MARKERS = {
    'EODPX': [
        ('eodpxReportingPricingLoadComplete', 'reporting_valuation_prices')
    ],
    'EOD': [
        ('eodReportingIncomeReceivedLoadComplete', 'reporting_income_received_results'),
        ('eodReportingSecurityMasterLoadComplete', 'reporting_security_masters'),
        ('eodReportingPendingTradesLoadComplete', 'pending_trades'),
        ('eodReportingDisposalLotsLoadComplete', 'reporting_disposal_lots'),
        ('eodReportingPositionsNAVLoadComplete', 'reporting_account_nava'),
        ('eodReportingAccountSummaryByAssetTypeLoadComplete', 'reporting_account_summary_by_asset_types'),
        ('eodReportingLedgerCashBalancesLoadComplete', 'reporting_ledger_cash_balances'),
        ('eodReportingPositionsLoadComplete', 'reporting_positions'),
        ('eodReportingTaxlotsLoadComplete', 'reporting_taxlots'),
        ('eodReportingCashStatementLoadComplete', 'reporting_cash_statements'),
        ('eodReportingGLCategoriesLoadForPositionsComplete', 'position_gl_categories'),
        ('eodReportingPendingFxTradesLoadComplete', 'reporting_pending_fx_trades'),
        ('eodReportingPendingEXSPOTTradesLoadComplete', 'reporting_pending_fx_trades'),
        ('eodReportingIncomeReceivableLoadComplete', 'reporting_income_receivables'),
        ('eodReportingTotalNetAssetsAndCashFlowsComplete', 'reporting_transactions_cash_flows'),
        ('eodReportingAccountMasterLoadComplete', 'reporting_account_masters'),
        ('eodReportingEarnedIncomeLoadComplete', 'reporting_earned_income_results'),
        ('eodReportingOTCETDRecordsLoadComplete', 'reporting_otcs_etds'),
        ('eodReportingTransactionsSettlementsLoadComplete', 'reporting_transactions_settlements')
    ],
    'AOD': [
        ('aodReportingPendingTradesLoadComplete', 'pending_trades'),
        ('aodReportingDisposalLotsLoadComplete', 'reporting_disposal_lots'),
        ('aodReportingPositionsNAVLoadComplete', 'reporting_account_nava'),
        ('aodReportingAccountSummaryByAssetTypeLoadComplete', 'reporting_account_summary_by_asset_types'),
        ('aodReportingLedgerCashBalancesLoadComplete', 'reporting_ledger_cash_balances'),
        ('aodReportingPositionsLoadComplete', 'reporting_positions'),
        ('aodReportingTaxlotsLoadComplete', 'reporting_taxlots')
    ],
    'SOD': [
        ('sodReportingPositionsNAVLoadComplete', 'reporting_account_nava'),
        ('sodReportingAccountSummaryByAssetTypeLoadComplete', 'reporting_account_summary_by_asset_types'),
        ('sodReportingLedgerCashBalancesLoadComplete', 'reporting_ledger_cash_balances'),
        ('sodReportingPositionsLoadComplete', 'reporting_positions'),
        ('sodReportingTaxlotsLoadComplete', 'reporting_taxlots'),
        ('sodReportingCashStatementLoadComplete', 'reporting_cash_statements'),
        ('sodReportingGLCategoriesLoadForPositionsComplete', 'position_gl_categories'),
        ('sodReportingPendingFxTradesLoadComplete', 'reporting_pending_fx_trades'),
        ('sodReportingPendingEXSPOTTradesLoadComplete', 'reporting_pending_fx_trades'),
        ('sodReportingIncomeReceivableLoadComplete', 'reporting_income_receivables'),
        ('sodReportingTransactionsSettlementsLoadComplete', 'reporting_transactions_settlements'),
        ('sodpxReportingPricingLoadComplete', 'reporting_valuation_prices')
    ],
}

# ----------------------------------
# Utilities
# ----------------------------------


def _norm_client(v: str) -> str:
    return (v or "").strip().upper()


def _norm_region(v: str) -> str:
    if v is None:
        return ""
    return REGION_MAP.get(v, REGION_MAP.get(v.upper(), v.strip().upper()))


def _norm_snapshot(v: str) -> str:
    return (v or "").strip().upper()


def _norm_date(d) -> str:
    """Accepts date/datetime/str and returns YYYY-MM-DD string."""
    if d is None:
        return ""
    if isinstance(d, str):
        try:
            return str(datetime.fromisoformat(d).date())
        except Exception:
            return d[:10]
    if isinstance(d, datetime):
        return str(d.date())
    if isinstance(d, date):
        return str(d)
    return str(d)


def _cache_put_combined_rows(client, region, business_date, rows):
    """Store combined rows into short-lived cache for reuse."""
    key = (client or "").upper(), (region or "").upper(), business_date
    _COMBINED_ROWS_CACHE[key] = {"ts": time.time(), "rows": rows}
    logger.debug("Cached combined rows for key=%s (rows=%d)", key, len(rows))


def _cache_get_combined_rows(client, region, business_date):
    """Return cached combined rows or None if expired/missing."""
    key = (client or "").upper(), (region or "").upper(), business_date
    rec = _COMBINED_ROWS_CACHE.get(key)
    if not rec:
        return None
    if time.time() - rec["ts"] > _COMBINED_ROWS_CACHE_TTL_SECONDS:
        del _COMBINED_ROWS_CACHE[key]
        logger.debug("Combined rows cache expired for key=%s", key)
        return None
    logger.debug("Reusing cached combined rows for key=%s (rows=%d)", key, len(rec["rows"]))
    return rec["rows"]

# ----------------------------------
# SQL builder
# ----------------------------------


def get_combined_workflow_sql_all(business_date: str, sod_date: str) -> str:
    return get_combined_workflow_sql_base(business_date, sod_date)


def get_combined_workflow_sql_for_client_region(business_date: str, sod_date: str, client: str, region: str) -> str:
    client_val = client.strip() if client else None
    region_val = region.strip() if region else None

    conds_m = [f"m.business_dt IN ('{business_date}', '{sod_date}')"]
    if client_val:
        conds_m.append(f"UPPER(m.client_cd) = UPPER('{client_val}')")
    if region_val:
        conds_m.append(f"UPPER(m.processing_region_cd) = UPPER('{region_val}')")
    base_m = " AND ".join(conds_m)

    conds_fm = [f"CAST(fm.marker->'payload'->>'business_date' AS date) IN ('{business_date}', '{sod_date}')"]
    if client_val:
        conds_fm.append(f"UPPER(fm.marker->'header'->>'party_cd') = UPPER('{client_val}')")
    if region_val:
        conds_fm.append(f"UPPER(fm.marker->'header'->>'processing_region_cd') = UPPER('{region_val}')")
    base_fm = " AND ".join(conds_fm)

    return get_combined_workflow_sql_base_with_bases(base_m, base_fm)


def get_combined_workflow_sql_base(business_date: str, sod_date: str) -> str:
    base_m = f"m.business_dt IN ('{business_date}', '{sod_date}')"
    base_fm = f"CAST(fm.marker->'payload'->>'business_date' AS date) IN ('{business_date}', '{sod_date}')"
    return get_combined_workflow_sql_base_with_bases(base_m, base_fm)


def _build_base_r_from_base_m(base_m: str) -> str:
    """
    Convert base_m (which references m.*) into base_r for reporting_loaders_markers (r.*).
    This replaces table alias references: m. -> r. and UPPER(m....) -> UPPER(r....).
    """
    base_r = base_m.replace("m.", "r.")
    base_r = base_r.replace("UPPER(m.", "UPPER(r.")
    base_r = base_r.replace("lower(m.", "lower(r.")
    return base_r


def get_combined_workflow_sql_base_with_bases(base_m: str, base_fm: str) -> str:
    """
    Combined union SQL for markers, final_markers, error_logs (joined to markers), reporting_loaders_markers.
    NOTE: base_m and base_fm are the WHERE conditions for markers/error_logs and final_markers respectively.
    """
    base_r = _build_base_r_from_base_m(base_m)

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
WHERE {base_m}

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
WHERE {base_fm}

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
WHERE {base_m}

UNION ALL

SELECT
    r.created_at AS last_updated,
    r.client_cd,
    r.processing_region_cd,
    r.snapshot_type_cd,
    r.marker_type_cd,
    r.subject_area_cd,
    r.original_message_id,
    CAST(r.business_dt AS date) AS business_dt,
    'success' AS status
FROM reporting_loaders_markers r
WHERE {base_r}
"""


# ----------------------------------
# Evaluators (unchanged)
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
# ATLS message IDs
# ----------------------------------


def get_atls_message_ids(business_date: str, sod_date: str):
    message_ids_map = defaultdict(list)

    def _fetch_source(conn, table, params):
        q = text(f"""
            SELECT client_cd, processing_region_cd, snapshot_type_cd, business_dt, original_message_id
            FROM {table}
            WHERE business_dt IN (:business_date, :sod_date)
        """)
        t0 = time.time()
        rows = conn.execute(q, params).mappings().all()
        dur = time.time() - t0
        logger.info("ATLS fetch from %s: %d rows in %.2fs for %s", table, len(rows), dur, params)
        return rows

    try:
        with get_atls_engine().connect() as conn:
            params = {"business_date": business_date, "sod_date": sod_date}
            acc_rows = _fetch_source(conn, "accounting_events", params)
            prc_rows = _fetch_source(conn, "pricing_events", params)

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

            summary = defaultdict(lambda: {"ACCOUNTING": [], "PRICING": []})
            for (client, region), items in message_ids_map.items():
                for it in items:
                    key = (client, region, it["snapshot"], it["business_dt"])
                    summary[key][it["source"].upper()].append(it["id"])

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
        logger.error("Error getting message IDs from ATLS: %s", e)

    return message_ids_map

# ----------------------------------
# Row grouping
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


def get_combined_workflow_sql_rows(business_date: str, sod_date: str, client: str = None, region: str = None):
    # Decide which SQL to call: all vs filtered
    if client and region:
        sql = get_combined_workflow_sql_for_client_region(business_date, sod_date, client, region)
        mode = 'filtered'
    else:
        sql = get_combined_workflow_sql_all(business_date, sod_date)
        mode = 'all'

    # Log the exact SQL for debugging (trimmed) so we can paste into psql if needed
    try:
        sql_preview = "\n" + (sql if len(sql) < 2000 else sql[:2000] + "\n... (truncated)")
        logger.debug("Executing ADM SQL (mode=%s): %s", mode, sql_preview)
    except Exception:
        logger.debug("Executing ADM SQL (mode=%s)", mode)

    t0 = time.time()
    with get_adm_engine().connect() as conn:
        result = conn.execute(text(sql))
        rows = [dict(row) for row in result.mappings()]
    dur = time.time() - t0
    logger.info("ADM combined SQL fetched %d rows in %.2fs (mode=%s, client=%s, region=%s)", len(rows), dur, mode, client, region)
    return rows


def get_combined_workflow_status(clients_regions, business_date):
    sod_date = calculate_sod_date(business_date)
    logger.info("Running ADM workflow for business_date=%s, sod_date=%s", business_date, sod_date)

    # If a single (client, region) is requested, apply SQL-side filters
    client_filter = region_filter = None
    if len(clients_regions) == 1:
        client_filter, region_filter = clients_regions[0]
        client_filter = _norm_client(client_filter) if client_filter else None
        region_filter = _norm_region(region_filter) if region_filter else None
        logger.info("Using SQL filters client=%s region=%s", client_filter, region_filter)
        # IMPORTANT: override the input clients_regions so we only process the requested pair
        clients_regions = [(client_filter, region_filter)]
        logger.debug("Processing restricted to clients_regions=%s", clients_regions)

    # ATLS IDs (with logs per source)
    atls_message_map = get_atls_message_ids(business_date, sod_date)

    # ADM rows (filtered at SQL level when single client/region requested)
    all_rows = get_combined_workflow_sql_rows(business_date, sod_date, client_filter, region_filter)

    # Cache combined rows for reuse by reporting helper (avoid duplicate DB hit)
    try:
        # cache for each client/region key we will process
        for (c, r) in clients_regions:
            _cache_put_combined_rows(c, r, business_date, all_rows)
    except Exception:
        logger.exception("Error caching combined rows")

    # --- diagnostics: how many reporting loader rows arrived for this fetch
    try:
        reporting_rows = [
            r for r in all_rows
            if (r.get('marker_type_cd') or '').lower().find('reporting') >= 0
            or (r.get('subject_area_cd') or '').startswith('reporting_')
        ]
        logger.info("Diagnostic: reporting_rows fetched=%d (sample types=%s)",
                    len(reporting_rows),
                    list({ (r.get('marker_type_cd'), r.get('subject_area_cd')) for r in reporting_rows } )[:6])
    except Exception:
        logger.exception("Diagnostic: error enumerating reporting rows")

    # Group once
    t_group = time.time()
    grouped_rows = group_rows_by_key(all_rows)
    logger.info("Row grouping completed in %.2fs. Groups=%d", time.time() - t_group, len(grouped_rows))

    # If we ran in filtered mode (single client/region), prune grouped_rows to that key set
    if client_filter and region_filter:
        keys_before = len(grouped_rows)
        pruned = {k: v for k, v in grouped_rows.items() if k[0] == client_filter and k[1] == region_filter}
        logger.debug("Pruned grouped_rows from %d to %d entries for filtered client/region", keys_before, len(pruned))
        grouped_rows = defaultdict(list, pruned)

    workflows = []
    t_process = time.time()

    for client_in, region_in in clients_regions:
        client = _norm_client(client_in)
        region = _norm_region(region_in)

        message_entries = atls_message_map.get((client, region), [])

        for workflow_type, expected_subjects in EXPECTED_SUBJECTS.items():
            stage_start = time.time()

            snapshot = _norm_snapshot(workflow_type.split("_")[0])
            stage = workflow_type.split("_")[1].upper()
            target_date = sod_date if snapshot == "SOD" else business_date
            key = (client, region, snapshot, target_date)

            message_items = [
                m for m in message_entries
                if m["snapshot"] == snapshot and m["business_dt"] == target_date
            ]
            message_ids = [m["id"] for m in message_items]

            acc_ids = [m["id"] for m in message_items if m.get("source") == "ACCOUNTING"]
            prc_ids = [m["id"] for m in message_items if m.get("source") == "PRICING"]
            logger.debug(
                "ATLS key [%s|%s|%s|%s] → total=%d acc=%d%s prc=%d%s",
                client, region, snapshot, target_date,
                len(message_ids), len(acc_ids), f" sample={acc_ids[:3]}" if acc_ids else "",
                len(prc_ids), f" sample={prc_ids[:3]}" if prc_ids else "",
            )

            candidate_rows = grouped_rows.get(key, [])
            marker_filter = STAGE_MARKER_FILTERS.get((snapshot, stage))

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
                    and (
                        not marker_filter or (
                            isinstance(marker_filter, set) and r["marker_type_cd"] in marker_filter
                        ) or (
                            isinstance(marker_filter, str) and r["marker_type_cd"] == marker_filter
                        )
                    )
                ]

            if ENABLE_ADM_FALLBACK_WHEN_ATLS_EMPTY and not message_ids and workflow_rows:
                adm_ids = list({r.get("original_message_id") for r in workflow_rows if r.get("original_message_id")})
                if adm_ids:
                    logger.warning(
                        "ATLS empty, using ADM fallback IDs for [%s|%s|%s|%s]: %s",
                        client, region, snapshot + "_" + stage, target_date, adm_ids[:3] + (["..."] if len(adm_ids) > 3 else [])
                    )
                    message_ids = adm_ids

            subjects_found = list({
                r["subject_area_cd"] for r in workflow_rows if r["subject_area_cd"] in expected_subjects
            })
            missing = list(set(expected_subjects) - set(subjects_found))

            first_event_time = min([r.get("last_updated") for r in workflow_rows if r.get("last_updated")], default=None)

            if snapshot == "AOD":
                total_count = len(message_ids)
                if stage == "FINAL":
                    status, pos_count, tax_count = evaluate_aod_final(workflow_rows, total_count)
                else:
                    status, pos_count, tax_count = evaluate_aod_stage(workflow_rows, total_count)
            else:
                status = evaluate_workflow_status(subjects_found, expected_subjects, first_event_time)
                pos_count, tax_count, total_count = None, None, None

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

            last_updated = max([r.get("last_updated") for r in workflow_rows if r.get("last_updated")], default=None)
            first_oid = message_ids[0] if message_ids else next((r.get("original_message_id") for r in workflow_rows if r.get("original_message_id")), None)

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

            elapsed = time.time() - stage_start
            logger.debug(
                "Stage [%s|%s|%s] status=%s subjects=%s missing=%s rows=%d elapsed=%.3fs",
                client, region, workflow_type, status, subjects_found, missing, len(workflow_rows), elapsed
            )

    total_elapsed = time.time() - t_process
    logger.info("Python workflow processing completed in %.2fs. Total workflows=%d", total_elapsed, len(workflows))

    workflows.sort(key=lambda w: WORKFLOW_ORDER.get(w["workflow_type"], 999))
    return workflows

# --------------------------------------------------------------------
# Reporting helpers (reuse cached combined rows when available)
# --------------------------------------------------------------------


def _collect_reporting_rows_from_all_rows(all_rows):
    """
    Returns a dict keyed by (client, region, snapshot, business_dt, marker_type_cd, subject_area_cd)
    mapping to the row dict from the combined SQL that came from reporting_loaders_markers.
    """
    reporting_rows = {}
    reporting_subjects = {
        'reporting_income_received_results', 'reporting_security_masters', 'pending_trades',
        'reporting_disposal_lots', 'reporting_account_nava', 'reporting_account_summary_by_asset_types',
        'reporting_ledger_cash_balances', 'reporting_positions', 'reporting_taxlots',
        'reporting_cash_statements', 'position_gl_categories', 'reporting_pending_fx_trades',
        'reporting_income_receivables', 'reporting_transactions_cash_flows',
        'reporting_account_masters', 'reporting_earned_income_results', 'reporting_otcs_etds',
        'reporting_transactions_settlements', 'reporting_valuation_prices'
    }

    for r in all_rows:
        subj = (r.get('subject_area_cd') or '').strip()
        mtype = (r.get('marker_type_cd') or '').strip()
        # simple heuristics to identify reporting rows coming from reporting_loaders_markers
        if (mtype.lower().startswith('eod') and 'reporting' in mtype.lower()) or \
           (mtype.lower().startswith('sod') and 'reporting' in mtype.lower()) or \
           (mtype.lower().startswith('aod') and 'reporting' in mtype.lower()) or \
           subj in reporting_subjects:
            key = (
                _norm_client(r.get('client_cd')),
                _norm_region(r.get('processing_region_cd')),
                _norm_snapshot(r.get('snapshot_type_cd')),
                _norm_date(r.get('business_dt')),
                mtype,
                subj
            )
            existing = reporting_rows.get(key)
            if not existing or (r.get('last_updated') and existing.get('last_updated') and r['last_updated'] > existing['last_updated']):
                reporting_rows[key] = r
    return reporting_rows


def get_all_reporting_loaders_status(client, region, business_date, *, _reuse_combined_rows=None):
    """
    Return reporting loader statuses for all snapshots.
    Behavior:
      - If _reuse_combined_rows is provided (list of rows), the helper will use it (no DB call).
      - Else tries to reuse short-lived cache (populated by get_combined_workflow_status). If cache hit, reuses it.
      - On cache miss, will call get_combined_workflow_sql_rows(...) to fetch combined rows (single DB call).
    Returns a dict: snapshot -> list of rows in the same shape as your old function.
    """
    client_norm = _norm_client(client)
    region_norm = _norm_region(region)
    sod_date = calculate_sod_date(business_date)

    # 1) If caller supplied combined rows directly, use them
    if _reuse_combined_rows is not None:
        all_rows = _reuse_combined_rows
        logger.debug("get_all_reporting_loaders_status: using _reuse_combined_rows supplied by caller (rows=%d)", len(all_rows))
    else:
        # 2) try short-lived cache
        cached = _cache_get_combined_rows(client_norm, region_norm, business_date)
        if cached is not None:
            all_rows = cached
            logger.debug("get_all_reporting_loaders_status: reused cached combined rows (rows=%d)", len(all_rows))
        else:
            # 3) fallback: fetch combined rows now (single DB call)
            logger.info("get_all_reporting_loaders_status: cache miss, fetching combined SQL rows from DB for %s|%s|%s", client_norm, region_norm, business_date)
            sod_date = calculate_sod_date(business_date)
            all_rows = get_combined_workflow_sql_rows(business_date, sod_date, client_norm, region_norm)
            # put into cache for potential subsequent reuse
            _cache_put_combined_rows(client_norm, region_norm, business_date, all_rows)

    # collect reporting rows
    reporting_lookup = _collect_reporting_rows_from_all_rows(all_rows)

    results_by_snapshot = {}
    for snapshot_type, static_markers in REPORTING_STATIC_MARKERS.items():
        snap_norm = _norm_snapshot(snapshot_type)
        snapshot_results = []
        for marker_type, subject_area in static_markers:
            matched_row = None
            # try exact key
            key = (client_norm, region_norm, snap_norm, business_date, marker_type, subject_area)
            matched_row = reporting_lookup.get(key)
            # fallback: match by marker_type only
            if not matched_row:
                for (c, rg, s, bd, mtype, subj), row in reporting_lookup.items():
                    if c == client_norm and rg == region_norm and s == snap_norm and bd == business_date and mtype == marker_type:
                        matched_row = row
                        break
            snapshot_results.append({
                'client_cd': client_norm,
                'processing_region_cd': region_norm,
                'business_dt': business_date,
                'snapshot_type_cd': snap_norm,
                'marker_type_cd': marker_type,
                'subject_area_cd': subject_area,
                'created_at': matched_row.get('last_updated') if matched_row else None,
                'status': 'completed' if matched_row else 'pending'
            })
        results_by_snapshot[snap_norm] = snapshot_results

    return results_by_snapshot

import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from collections import defaultdict
from database.connectors import get_adm_engine, get_atls_engine
import time

logger = logging.getLogger(__name__)
LONG_RUNNING_THRESHOLD_MINUTES = 30

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

# stage → marker_type_cd mapping
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
    ("SOD", "MART"): "sodRegionMartLoadComplete",

    # AOD
    ("AOD", "RAW"): "asOfRegionSubjectAreaRawLoadComplete",
    ("AOD", "ENRICH"): "asOfRegionSubjectAreaEnriched",
    ("AOD", "ROLL"): "asOfRegionSubjectAreaRollupComplete",
    ("AOD", "MART"): "asOfRegionSubjectAreaMartLoadComplete",
    ("AOD", "FINAL"): "asOfRegionsStatementsPublished",
}

# ----------------------------------------------------------------------
# SQL builder
# ----------------------------------------------------------------------
def get_combined_workflow_sql(business_date, sod_date):
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

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def calculate_sod_date(business_date: str) -> str:
    business_dt = datetime.strptime(business_date, "%Y-%m-%d")
    sod_date = business_dt + timedelta(days=1)
    while sod_date.weekday() >= 5:  # skip weekends
        sod_date += timedelta(days=1)
    return sod_date.strftime("%Y-%m-%d")


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


def get_atls_message_ids(business_date, sod_date):
    message_ids_map = {}
    try:
        with get_atls_engine().connect() as conn:
            query = text("""
                SELECT client_cd, processing_region_cd, snapshot_type_cd, business_dt, original_message_id
                FROM accounting_events
                WHERE business_dt IN (:business_date, :sod_date)
                UNION
                SELECT client_cd, processing_region_cd, snapshot_type_cd, business_dt, original_message_id
                FROM pricing_events
                WHERE business_dt IN (:business_date, :sod_date)
            """)
            results = conn.execute(query, {"business_date": business_date, "sod_date": sod_date}).mappings().all()
            for row in results:
                key = (row["client_cd"], row["processing_region_cd"])
                message_ids_map.setdefault(key, []).append({
                    "id": row["original_message_id"],
                    "snapshot": row["snapshot_type_cd"],
                    "business_dt": str(row["business_dt"])
                })
    except Exception as e:
        logger.error(f"Error getting message IDs from ATLS: {e}")
    return message_ids_map


def group_rows_by_key(all_rows):
    grouped = defaultdict(list)
    for r in all_rows:
        key = (
            r["client_cd"],
            r["processing_region_cd"],
            r["snapshot_type_cd"].upper(),
            str(r["business_dt"])
        )
        grouped[key].append(r)
    return grouped

def get_combined_workflow_status(clients_regions, business_date):
    sod_date = calculate_sod_date(business_date)
    logger.info(f"Running combined ADM workflow query for business_date={business_date}, sod_date={sod_date}")

    atls_message_map = get_atls_message_ids(business_date, sod_date)
    combined_sql = get_combined_workflow_sql(business_date, sod_date)

    try:
        # ✅ DB timing
        t0 = time.time()
        with get_adm_engine().connect() as conn:
            result = conn.execute(text(combined_sql))
            all_rows = [dict(row) for row in result.mappings()]
        db_time = time.time() - t0
        logger.info(f"DB fetch completed in {db_time:.2f} seconds. Retrieved {len(all_rows)} rows.")

        # ✅ Pre-grouping timing
        t1 = time.time()
        grouped_rows = group_rows_by_key(all_rows)
        grouping_time = time.time() - t1
        logger.info(f"Row grouping completed in {grouping_time:.2f} seconds. Groups: {len(grouped_rows)}")

        workflows = []

        # ✅ Workflow processing timing
        t2 = time.time()

        for client, region in clients_regions:
            message_entries = atls_message_map.get((client, region), [])

            for workflow_type, expected_subjects in EXPECTED_SUBJECTS.items():
                stage_start = time.time()

                snapshot = workflow_type.split("_")[0].upper()
                stage = workflow_type.split("_")[1].upper()
                target_date = sod_date if snapshot == "SOD" else business_date
                key = (client, region, snapshot, target_date)

                message_ids = [
                    m["id"] for m in message_entries
                    if m["snapshot"].upper() == snapshot and m["business_dt"] == target_date
                ]

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

                subjects_found = list({
                    r["subject_area_cd"]
                    for r in workflow_rows
                    if r["subject_area_cd"] in expected_subjects
                })
                missing = list(set(expected_subjects) - set(subjects_found))

                first_event_time = min([r["last_updated"] for r in workflow_rows if r["last_updated"]], default=None)
                total_count = len(message_ids)

                if snapshot == "AOD":
                    if stage == "FINAL":
                        status, pos_count, tax_count = evaluate_aod_final(workflow_rows, total_count)
                    else:
                        status, pos_count, tax_count = evaluate_aod_stage(workflow_rows, total_count)
                else:
                    if not message_ids:
                        status = "pending"
                    else:
                        status = evaluate_workflow_status(subjects_found, expected_subjects, first_event_time)

                    failed_rows = [r for r in workflow_rows if r["status"] == "failed"]
                    if failed_rows:
                        for fr in failed_rows:
                            service_nm = (fr.get("marker_type_cd") or "").lower()
                            if stage == "RAW" and "raw" in service_nm:
                                status = "failed"
                            elif stage == "ENRICH" and "enrich" in service_nm:
                                status = "failed"
                            elif stage in ("ROLL", "ROLLUP") and ("roll" in service_nm or "rollup" in service_nm):
                                status = "failed"
                            elif stage == "MART" and "mart" in service_nm:
                                status = "failed"
                            elif stage == "FINAL" and "final" in service_nm:
                                status = "failed"

                    pos_count, tax_count = None, None

                workflows.append({
                    "client_cd": client,
                    "processing_region_cd": region,
                    "workflow_type": workflow_type,
                    "snapshot_type_cd": snapshot,
                    "status": status,
                    "status_with_long_running": status,
                    "subjects_found": subjects_found,
                    "last_updated": max([r["last_updated"] for r in workflow_rows if r["last_updated"]], default=None),
                    "business_dt": target_date,
                    "original_message_id": message_ids[0] if message_ids else None,
                    "total_count": total_count if snapshot == "AOD" else None,
                    "positions_count": pos_count if snapshot == "AOD" else None,
                    "taxlots_count": tax_count if snapshot == "AOD" else None
                })

                # ✅ Per stage summary log
                elapsed = time.time() - stage_start
                logger.debug(
                    f"[{client}|{region}|{workflow_type}] status={status}, "
                    f"subjects_found={subjects_found}, missing={missing}, "
                    f"rows={len(workflow_rows)}, elapsed={elapsed:.3f}s"
                )

        process_time = time.time() - t2
        logger.info(f"Python workflow processing completed in {process_time:.2f} seconds. Total workflows={len(workflows)}")

        return workflows

    except Exception as e:
        logger.error(f"Error in combined workflow status: {e}", exc_info=True)
        return []

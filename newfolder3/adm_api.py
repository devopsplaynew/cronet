from sqlalchemy import text
from database.connectors import get_adm_engine, get_atls_engine
import logging
import os
from datetime import datetime, timedelta
import json
import re

logger = logging.getLogger(__name__)

# ------------------- Validation helpers -------------------
def validate_message_id(message_id):
    if message_id is None:
        return True
    if isinstance(message_id, list):
        for mid in message_id:
            if not isinstance(mid, str) or not re.match(r'^[a-zA-Z0-9_\-:]{1,100}$', mid):
                raise ValueError(f"Invalid message ID format: {mid}")
        return True
    if not isinstance(message_id, str) or not re.match(r'^[a-zA-Z0-9_\-:]{1,100}$', message_id):
        raise ValueError(f"Invalid message ID format: {message_id}")
    return True

def validate_date_format(date_string):
    if not isinstance(date_string, str):
        raise ValueError("Date must be a string")
    datetime.strptime(date_string, '%Y-%m-%d')
    return True

# ------------------- Query helpers -------------------
def load_adm_query(query_name):
    try:
        query_file = os.path.join(os.path.dirname(__file__), '../sql/adm_queries.sql')
        with open(query_file, 'r') as f:
            content = f.read()
        if query_name == 'combined_workflow_status':
            return content.strip()
        queries = content.split(';')
        for query in queries:
            if query.strip().startswith(f'-- {query_name}'):
                return query.strip()
        return None
    except Exception as e:
        logger.error(f"Error loading query {query_name}: {str(e)}")
        return None

def execute_with_timeout(conn, query, params=None, timeout=180):
    try:
        if params:
            result = conn.execute(text(query), params)
        else:
            result = conn.execute(text(query))
        return result
    except Exception as e:
        if "timeout" in str(e).lower() or "canceling statement" in str(e).lower():
            logger.error(f"Query timed out after {timeout} seconds")
            raise Exception(f"Query execution timed out after {timeout} seconds")
        raise e

def format_message_ids(message_ids):
    if isinstance(message_ids, list):
        if not message_ids:
            return "''"
        # just return comma-separated quoted values
        return ", ".join([f"'{msg_id}'" for msg_id in message_ids])
    elif isinstance(message_ids, str):
        try:
            parsed_ids = json.loads(message_ids)
            if isinstance(parsed_ids, list):
                return ", ".join([f"'{msg_id}'" for msg_id in parsed_ids])
            else:
                return f"'{message_ids}'"
        except json.JSONDecodeError:
            return f"'{message_ids}'"
    else:
        return "''"


# ------------------- Combined workflow -------------------
def get_combined_workflow_status(message_id, message_ids=None, total_count=None, parent_message_id=None, filter_workflow_types=None):
    try:
        query_text = load_adm_query('combined_workflow_status')
        if not query_text:
            logger.error("Combined workflow status query not found")
            return []

        formatted_message_id = format_message_ids(message_id) if message_id else "''"
        formatted_message_ids = format_message_ids(message_ids) if message_ids else "''"
        total_count_val = total_count or 0
        parent_message_id_val = parent_message_id or ''

        formatted_query = query_text.format(
            message_id=formatted_message_id,
            message_ids_placeholder=formatted_message_ids,
            total_count=total_count_val,
            parent_message_id=parent_message_id_val
        )

        workflows = []
        with get_adm_engine().connect() as conn:
            result = execute_with_timeout(conn, formatted_query)
            for row in result.fetchall():
                workflow_type_full = row[0]
                status = row[1]
                last_updated = row[2]
                started_at = row[3] if len(row) > 3 else None

                workflow_type = workflow_type_full.replace('_status', '')

                if filter_workflow_types and workflow_type not in filter_workflow_types:
                    continue

                if status in ('pending', 'inprogress') and started_at:
                    time_diff = (datetime.now() - started_at).total_seconds() / 60
                    if time_diff > 30:
                        status_with_long_running = 'long_running'
                    else:
                        status_with_long_running = 'inprogress'
                else:
                    status_with_long_running = status

                workflow_data = {
                    'workflow_type': workflow_type,
                    'status': status,
                    'status_with_long_running': status_with_long_running,
                    'last_updated': last_updated,
                    'started_at': started_at,
                    # add counts if present in row
                    'total_count': row[4] if len(row) > 4 else None,
                    'positions_count': row[5] if len(row) > 5 else None,
                    'taxlots_count': row[6] if len(row) > 6 else None,
                    'asof_statements_count': row[5] if len(row) > 5 else None,
                    'eod_all_statements_count': row[6] if len(row) > 6 else None,
                }


                workflows.append(workflow_data)

        return workflows

    except Exception as e:
        logger.error(f"Error getting combined workflow status: {str(e)}")
        return []

# ------------------- Batch functions -------------------
def _append_pending_workflows(workflows, client, region, business_date, wf_types, snapshot_type):
    logger.info(f"[{snapshot_type}] {client}/{region} → no message IDs in ATLS, skipping ADM, marking as pending")
    for wf_type in wf_types:
        workflows.append({
            "client_cd": client,
            "processing_region_cd": region,
            "workflow_type": wf_type,
            "status": "pending",
            "status_with_long_running": "pending",
            "last_updated": None,
            "business_dt": business_date,
            "original_message_id": None,
            "original_message_ids": []
        })

def get_batch_pricing_workflow_status(clients_regions, business_date):
    workflows = []

    placeholders = ",".join([f"(:c{i}, :r{i})" for i in range(len(clients_regions))])
    query = f"""
        SELECT client_cd, processing_region_cd, original_message_id
        FROM pricing_events
        WHERE business_dt = :business_date
          AND (client_cd, processing_region_cd) IN ({placeholders})
    """

    params = {'business_date': business_date}
    for i, (c, r) in enumerate(clients_regions):
        params[f"c{i}"] = c
        params[f"r{i}"] = r

    with get_atls_engine().connect() as conn:
        rows = [dict(row) for row in execute_with_timeout(conn, query, params).mappings()]

    grouped = {}
    for row in rows:
        key = (row['client_cd'], row['processing_region_cd'])
        grouped.setdefault(key, []).append(row['original_message_id'])

    for client, region in clients_regions:
        ids = grouped.get((client, region), [])
        if not ids:
            _append_pending_workflows(workflows, client, region, business_date,
                                      ['pricing_raw','pricing_enrich','pricing_roll','pricing_mart'], "PRICING")
            continue

        logger.info(f"[PRICING] {client}/{region} → message_ids={ids}")
        adm_wfs = get_combined_workflow_status(ids[0],
                                               filter_workflow_types=['pricing_raw','pricing_enrich','pricing_roll','pricing_mart'])
        for wf in adm_wfs:
            wf.update({'client_cd': client, 'processing_region_cd': region,
                       'business_dt': business_date, 'original_message_id': ids[0]})
            workflows.append(wf)

    logger.info(f"[PRICING] Final workflows count={len(workflows)}")
    return workflows


def get_batch_eod_workflow_status(clients_regions, business_date):
    workflows = []

    placeholders = ",".join([f"(:c{i}, :r{i})" for i in range(len(clients_regions))])
    query = f"""
        SELECT client_cd, processing_region_cd, original_message_id, parent_original_message_id
        FROM accounting_events
        WHERE business_dt = :business_date
          AND snapshot_type_cd = 'EOD'
          AND (client_cd, processing_region_cd) IN ({placeholders})
    """

    params = {'business_date': business_date}
    for i, (c, r) in enumerate(clients_regions):
        params[f"c{i}"] = c
        params[f"r{i}"] = r

    with get_atls_engine().connect() as conn:
        rows = [dict(row) for row in execute_with_timeout(conn, query, params).mappings()]

    grouped = {}
    parent_map = {}
    for row in rows:
        key = (row['client_cd'], row['processing_region_cd'])
        grouped.setdefault(key, []).append(row['original_message_id'])
        parent_map[key] = row.get('parent_original_message_id')

    for client, region in clients_regions:
        ids = grouped.get((client, region), [])
        parent_id = parent_map.get((client, region))
        if not ids:
            _append_pending_workflows(workflows, client, region, business_date,
                                      ['eod_raw','eod_enrich','eod_roll','eod_mart','eod_final'], "EOD")
            continue

        logger.info(f"[EOD] {client}/{region} → message_ids={ids}, parent_id={parent_id}")
        adm_wfs = get_combined_workflow_status(ids[0],
                                               filter_workflow_types=['eod_raw','eod_enrich','eod_roll','eod_mart','eod_final'])
        for wf in adm_wfs:
            wf.update({'client_cd': client, 'processing_region_cd': region,
                       'business_dt': business_date})
            workflows.append(wf)

    logger.info(f"[EOD] Final workflows count={len(workflows)}")
    return workflows


def get_batch_aod_workflow_status(clients_regions, business_date):
    workflows = []

    placeholders = ",".join([f"(:c{i}, :r{i})" for i in range(len(clients_regions))])
    query = f"""
        SELECT client_cd, processing_region_cd, original_message_id, parent_original_message_id
        FROM accounting_events
        WHERE business_dt = :business_date
          AND snapshot_type_cd = 'AOD'
          AND (client_cd, processing_region_cd) IN ({placeholders})
    """

    params = {'business_date': business_date}
    for i, (c, r) in enumerate(clients_regions):
        params[f"c{i}"] = c
        params[f"r{i}"] = r

    with get_atls_engine().connect() as conn:
        rows = [dict(row) for row in execute_with_timeout(conn, query, params).mappings()]

    grouped = {}
    parent_map = {}
    for row in rows:
        key = (row['client_cd'], row['processing_region_cd'])
        grouped.setdefault(key, []).append(row['original_message_id'])
        parent_map[key] = row.get('parent_original_message_id')

    for client, region in clients_regions:
        ids = grouped.get((client, region), [])
        parent_id = parent_map.get((client, region))
        if not ids:
            _append_pending_workflows(workflows, client, region, business_date,
                                      ['aod_raw','aod_enrich','aod_roll','aod_mart','aod_final'], "AOD")
            continue

        logger.info(f"[AOD] {client}/{region} → message_ids={ids}, parent_id={parent_id}")
        adm_wfs = get_combined_workflow_status(ids,
                                               message_ids=ids,
                                               total_count=len(ids),
                                               parent_message_id=parent_id,
                                               filter_workflow_types=['aod_raw','aod_enrich','aod_roll','aod_mart','aod_final'])
        for wf in adm_wfs:
            wf.update({'client_cd': client, 'processing_region_cd': region,
                       'business_dt': business_date})
            workflows.append(wf)

    logger.info(f"[AOD] Final workflows count={len(workflows)}")
    return workflows


def get_batch_sod_workflow_status(clients_regions, business_date):
    workflows = []

    sod_date = datetime.strptime(business_date, '%Y-%m-%d').date() + timedelta(days=1)
    while sod_date.weekday() >= 5:
        sod_date += timedelta(days=1)
    sod_date_str = sod_date.strftime('%Y-%m-%d')

    placeholders = ",".join([f"(:c{i}, :r{i})" for i in range(len(clients_regions))])
    query = f"""
        SELECT client_cd, processing_region_cd, original_message_id, parent_original_message_id
        FROM accounting_events
        WHERE business_dt = :sod_date
          AND snapshot_type_cd = 'SOD'
          AND (client_cd, processing_region_cd) IN ({placeholders})
    """

    params = {'sod_date': sod_date_str}
    for i, (c, r) in enumerate(clients_regions):
        params[f"c{i}"] = c
        params[f"r{i}"] = r

    with get_atls_engine().connect() as conn:
        rows = [dict(row) for row in execute_with_timeout(conn, query, params).mappings()]

    grouped = {}
    parent_map = {}
    for row in rows:
        key = (row['client_cd'], row['processing_region_cd'])
        grouped.setdefault(key, []).append(row['original_message_id'])
        parent_map[key] = row.get('parent_original_message_id')

    for client, region in clients_regions:
        ids = grouped.get((client, region), [])
        parent_id = parent_map.get((client, region))
        if not ids:
            _append_pending_workflows(workflows, client, region, sod_date_str,
                                      ['sod_raw','sod_enrich','sod_roll','sod_mart','sod_final'], "SOD")
            continue

        logger.info(f"[SOD] {client}/{region} → message_ids={ids}, parent_id={parent_id}")
        adm_wfs = get_combined_workflow_status(ids[0],
                                               filter_workflow_types=['sod_raw','sod_enrich','sod_roll','sod_mart','sod_final'])
        for wf in adm_wfs:
            wf.update({'client_cd': client, 'processing_region_cd': region,
                       'business_dt': sod_date_str})
            workflows.append(wf)

    logger.info(f"[SOD] Final workflows count={len(workflows)}")
    return workflows

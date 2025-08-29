from sqlalchemy import text
from database.connectors import get_adm_engine, get_atls_engine
import logging
import os
from datetime import datetime
import json

logger = logging.getLogger(__name__)

def load_adm_query(query_name):
    """Helper function to load SQL queries from adm_queries.sql file"""
    try:
        query_file = os.path.join(os.path.dirname(__file__), '../sql/adm_queries.sql')
        with open(query_file, 'r') as f:
            content = f.read()

        # For combined query, we'll use the entire file
        if query_name == 'combined_workflow_status':
            return content.strip()

        # Extract the specific query by name (for backward compatibility)
        queries = content.split(';')
        for query in queries:
            if query.strip().startswith(f'-- {query_name}'):
                return query.strip()
        return None
    except Exception as e:
        logger.error(f"Error loading query {query_name}: {str(e)}")
        return None

def format_message_ids(message_ids):
    """Properly format message IDs for SQL queries"""
    logger.info(f"DEBUG: Formatting message_ids: {message_ids} (type: {type(message_ids)})")
    
    if isinstance(message_ids, list):
        if not message_ids:
            logger.info("DEBUG: Empty list, returning ''")
            return "''"
        
        formatted = ", ".join([f"'{msg_id}'" for msg_id in message_ids])
        logger.info(f"DEBUG: Formatted list: {formatted}")
        return formatted
        
    elif isinstance(message_ids, str):
        try:
            # Check if it's a JSON string
            parsed_ids = json.loads(message_ids)
            if isinstance(parsed_ids, list):
                formatted = ", ".join([f"'{msg_id}'" for msg_id in parsed_ids])
                logger.info(f"DEBUG: Formatted JSON list: {formatted}")
                return formatted
            else:
                logger.info(f"DEBUG: Formatted string: '{message_ids}'")
                return f"'{message_ids}'"
        except json.JSONDecodeError:
            logger.info(f"DEBUG: Formatted plain string: '{message_ids}'")
            return f"'{message_ids}'"
    else:
        logger.info("DEBUG: Empty message_ids, returning ''")
        return "''"
def get_batch_pricing_workflow_status(clients_regions, business_date):
    """Get pricing workflow statuses for multiple clients in batch"""
    try:
        # Get all message IDs for all clients in a single query
        message_ids_by_client = {}

        for client, region in clients_regions:
            message_ids_query = """
                SELECT original_message_id
                FROM pricing_events
                WHERE client_cd = :client
                  AND processing_region_cd = :region
                  AND business_dt = :business_date
            """

            with get_atls_engine().connect() as conn:
                result = conn.execute(text(message_ids_query), {
                    'client': client,
                    'region': region,
                    'business_date': business_date
                })
                message_ids = [row[0] for row in result.fetchall()]
                message_ids_by_client[(client, region)] = message_ids
                
                # DEBUG: Log the message IDs found
                logger.info(f"DEBUG: Found message IDs for {client}/{region}: {message_ids}")

        # Process all message IDs in batch
        all_workflows = []
        for (client, region), message_ids in message_ids_by_client.items():
            if message_ids:
                # Use combined query for all message IDs
                workflows = get_combined_workflow_status(
                    message_id=message_ids[0],  # Pass the first message ID
                    filter_workflow_types=['pricing_raw', 'pricing_enrich', 'pricing_roll', 'pricing_mart']
                )
                
                logger.info(f"DEBUG: ADM returned {len(workflows)} workflows for {client}/{region}")
                
                for workflow in workflows:
                    workflow.update({
                        'client_cd': client,
                        'processing_region_cd': region,
                        'business_dt': business_date,
                        'original_message_id': message_ids[0]  # Ensure message ID is included
                    })
                    all_workflows.append(workflow)
            else:
                # If no message IDs found, add pending statuses
                for workflow_type in ['pricing_raw', 'pricing_enrich', 'pricing_roll', 'pricing_mart']:
                    all_workflows.append({
                        "client_cd": client,
                        "processing_region_cd": region,
                        "workflow_type": workflow_type,
                        "status": "pending",
                        "status_with_long_running": "pending",
                        "last_updated": None,
                        "business_dt": business_date,
                        "original_message_id": None
                    })

        return all_workflows

    except Exception as e:
        logger.error(f"Error getting batch pricing workflow status: {str(e)}")
        return []        
def get_batch_eod_workflow_status(clients_regions, business_date):
    """Get EOD workflow statuses for multiple clients in batch"""
    try:
        all_workflows = []

        for client, region in clients_regions:
            workflows = get_workflow_statuses_from_adm(
                client, region, business_date, 'EOD',
                ['eod_raw', 'eod_enrich', 'eod_roll', 'eod_mart', 'eod_final'],
                get_eod_workflow_status
            )
            all_workflows.extend(workflows)

        return all_workflows

    except Exception as e:
        logger.error(f"Error getting batch EOD workflow status: {str(e)}")
        return []

def get_batch_aod_workflow_status(clients_regions, business_date):
    """Get AOD workflow statuses for multiple clients in batch"""
    try:
        all_workflows = []

        for client, region in clients_regions:
            workflows = get_workflow_statuses_from_adm(
                client, region, business_date, 'AOD',
                ['aod_raw', 'aod_enrich', 'aod_roll', 'aod_mart', 'aod_final'],
                get_aod_workflow_status
            )
            all_workflows.extend(workflows)

        return all_workflows

    except Exception as e:
        logger.error(f"Error getting batch AOD workflow status: {str(e)}")
        return []

def get_batch_sod_workflow_status(clients_regions, business_date):
    """Get SOD workflow statuses for multiple clients in batch"""
    try:
        all_workflows = []

        for client, region in clients_regions:
            workflows = get_workflow_statuses_from_adm(
                client, region, business_date, 'SOD',
                ['sod_raw', 'sod_enrich', 'sod_roll', 'sod_mart', 'sod_final'],
                get_sod_workflow_status
            )
            all_workflows.extend(workflows)

        return all_workflows

    except Exception as e:
        logger.error(f"Error getting batch SOD workflow status: {str(e)}")
        return []

def get_workflow_statuses_from_adm(client, region, business_date, snapshot_type, workflow_types, get_status_function):
    """Generic function to get workflow statuses from ADM for batch processing"""
    from app import calculate_sod_date

    sod_date = calculate_sod_date(business_date)

    # Determine the date to use based on snapshot type
    query_date = sod_date if snapshot_type == 'SOD' else business_date

    message_ids_query = """
        SELECT original_message_id
        FROM accounting_events
        WHERE client_cd = :client
          AND processing_region_cd = :region
          AND business_dt = :query_date
          AND snapshot_type_cd = :snapshot_type
    """

    with get_atls_engine().connect() as conn:
        result = conn.execute(text(message_ids_query), {
            'client': client,
            'region': region,
            'query_date': query_date,
            'snapshot_type': snapshot_type
        })
        message_ids = [row[0] for row in result.fetchall()]
        
        logger.info(f"DEBUG: Found {len(message_ids)} AOD message IDs for {client}/{region}: {message_ids}")

    workflows = []

    # Get actual statuses using the provided function
    adm_workflows = get_status_function(message_ids)
    for workflow in adm_workflows:
        workflow.update({
            'client_cd': client,
            'processing_region_cd': region,
            'business_dt': query_date
        })
        workflows.append(workflow)

    # If no workflows were returned (no message IDs or no data), return pending statuses
    if not workflows:
        logger.info(f"DEBUG: No AOD workflows found for {client}/{region}, creating pending statuses")
        for workflow_type in workflow_types:
            workflows.append({
                "client_cd": client,
                "processing_region_cd": region,
                "workflow_type": workflow_type,
                "status": "pending",
                "status_with_long_running": "pending",
                "last_updated": None,
                "business_dt": query_date,
                "original_message_ids": message_ids  # Include message IDs for debugging
            })

    return workflows
def get_combined_workflow_status(message_id, message_ids=None, total_count=None, parent_message_id=None, filter_workflow_types=None):
    """Get all workflow statuses in a single query using the combined SQL"""
    try:
        logger.info(f"DEBUG: Combined query called with message_id={message_id}, message_ids={message_ids}, total_count={total_count}")
        
        # Load the combined query
        query_text = load_adm_query('combined_workflow_status')
        if not query_text:
            logger.error("Combined workflow status query not found")
            return []

        # Format parameters
        formatted_message_id = format_message_ids(message_id) if message_id else "''"
        formatted_message_ids = format_message_ids(message_ids) if message_ids else "''"
        total_count_val = total_count or 0
        parent_message_id_val = parent_message_id or ''

        logger.info(f"DEBUG: Formatted params - message_id: {formatted_message_id}, message_ids: {formatted_message_ids}, total_count: {total_count_val}")

        # Replace placeholders
        formatted_query = query_text.format(
            message_id=formatted_message_id,
            message_ids_placeholder=formatted_message_ids,
            total_count=total_count_val,
            parent_message_id=parent_message_id_val
        )

        logger.info(f"DEBUG: Executing ADM query for AOD workflows")
        
        workflows = []
        with get_adm_engine().connect() as conn:
            result = conn.execute(text(formatted_query))
            
            for row in result.fetchall():
                logger.info(f"DEBUG: ADM returned row: {row}")
                workflow_type_full = row[0]  # e.g., 'pricing_raw_status'
                status = row[1]
                last_updated = row[2]
                started_at = row[3] if len(row) > 3 else None

                # Extract base workflow type (remove '_status' suffix)
                workflow_type = workflow_type_full.replace('_status', '')

                # Filter by workflow types if specified
                if filter_workflow_types and workflow_type not in filter_workflow_types:
                    continue

                # Calculate status_with_long_running
                if status == 'pending' and started_at:
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
                    'started_at': started_at
                }

                # Add message IDs based on workflow type
                if workflow_type.startswith(('aod_', 'aod_final')):
                    workflow_data['original_message_ids'] = message_ids
                    workflow_data['parent_message_id'] = parent_message_id
                else:
                    workflow_data['original_message_id'] = message_id

                # Add progress counts for AOD workflows
                if workflow_type_full in ['aod_raw_status', 'aod_enrich_status', 'aod_roll_status', 'aod_mart_status']:
                    if len(row) > 6:
                        workflow_data.update({
                            'total_count': row[4],
                            'positions_count': row[5],
                            'taxlots_count': row[6]
                        })
                elif workflow_type_full == 'aod_final_status':
                    if len(row) > 6:
                        workflow_data.update({
                            'total_count': row[4],
                            'asof_statements_count': row[5],
                            'eod_all_statements_count': row[6]
                        })

                workflows.append(workflow_data)

        return workflows

    except Exception as e:
        logger.error(f"Error getting combined workflow status: {str(e)}")
        return []

def get_pricing_workflow_status(message_id):
    """Get pricing workflow statuses using combined query"""
    return get_combined_workflow_status(
        message_id=message_id,
        filter_workflow_types=['pricing_raw', 'pricing_enrich', 'pricing_roll', 'pricing_mart']
    )

def get_eod_workflow_status(message_id):
    """Get EOD workflow statuses using combined query"""
    return get_combined_workflow_status(
        message_id=message_id,
        filter_workflow_types=['eod_raw', 'eod_enrich', 'eod_roll', 'eod_mart', 'eod_final']
    )

def get_sod_workflow_status(message_id):
    """Get SOD workflow statuses using combined query"""
    return get_combined_workflow_status(
        message_id=message_id,
        filter_workflow_types=['sod_raw', 'sod_enrich', 'sod_roll', 'sod_mart', 'sod_final']
    )

def get_aod_workflow_status(message_ids):
    """Get AOD workflow statuses using combined query"""
    if not message_ids:
        logger.info("DEBUG: get_aod_workflow_status called with empty message_ids")
        return []
        
    try:
        # Get parent_original_message_id from ATLS database
        parent_message_id = get_aod_parent_message_id(message_ids)
        total_count = len(message_ids) if isinstance(message_ids, list) else 1
        
        logger.info(f"DEBUG: AOD processing - message_ids: {message_ids}, total_count: {total_count}, parent_message_id: {parent_message_id}")
        
        return get_combined_workflow_status(
            message_id=None,
            message_ids=message_ids,
            total_count=total_count,
            parent_message_id=parent_message_id,
            filter_workflow_types=['aod_raw', 'aod_enrich', 'aod_roll', 'aod_mart', 'aod_final']
        )
        
    except Exception as e:
        logger.error(f"Error getting ADM AOD workflow status: {str(e)}")
        return []
        
def get_aod_parent_message_id(message_ids):
    """Get the parent_original_message_id from accounting_events for AOD workflows"""
    try:
        if not message_ids:
            return None
            
        # Format message IDs for SQL query
        message_ids_str = format_message_ids(message_ids)
        
        # Query to get parent_original_message_id from accounting_events
        query = """
            SELECT DISTINCT parent_original_message_id
            FROM accounting_events
            WHERE original_message_id IN ({})
              AND snapshot_type_cd = 'AOD'
            LIMIT 1
        """.format(message_ids_str)
        
        with get_atls_engine().connect() as conn:
            result = conn.execute(text(query))
            row = result.fetchone()
            return row[0] if row else None
            
    except Exception as e:
        logger.error(f"Error getting AOD parent message ID: {str(e)}")
        return None
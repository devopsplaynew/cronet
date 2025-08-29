from sqlalchemy import text
from database.connectors import get_adm_engine, get_atls_engine
import logging
import os
from datetime import datetime
import json
import re  # Add this import
from datetime import timedelta  # Add this import


logger = logging.getLogger(__name__)

# Add the missing validate_message_id function
def validate_message_id(message_id):
    """Validate message ID format"""
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

def validate_client_region_input(client, region):
    """Simple validation to replace the circular import"""
    if not isinstance(client, str) or not isinstance(region, str):
        raise ValueError("Client and region must be strings")
    return True

def validate_date_format(date_string):
    """Simple validation to replace the circular import"""
    if not isinstance(date_string, str):
        raise ValueError("Date must be a string")
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date: {date_string}")
    return True

def load_adm_query(query_name):
    """Helper function to load SQL queries from adm_queries.sql file"""
    try:
        query_file = os.path.join(os.path.dirname(__file__), '../sql/adm_queries.sql')
        with open(query_file, 'r') as f:
            content = f.read()

        # For combined query, we'll use the entire file
        if query_name == 'combined_workflow_status':
            return content.strip()

        # Extract the specific query by name
        queries = content.split(';')
        for query in queries:
            if query.strip().startswith(f'-- {query_name}'):
                return query.strip()
        return None
    except Exception as e:
        logger.error(f"Error loading query {query_name}: {str(e)}")
        return None

def execute_with_timeout(conn, query, params=None, timeout=180):
    """Execute query with timeout handling"""
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
    """Properly format message IDs for SQL queries - KEEPING ORIGINAL FORMAT"""
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
                result = execute_with_timeout(conn, message_ids_query, {
                    'client': client,
                    'region': region,
                    'business_date': business_date
                })
                message_ids = [row[0] for row in result.fetchall()]
                message_ids_by_client[(client, region)] = message_ids
                
                logger.info(f"DEBUG: Found message IDs for {client}/{region}: {message_ids}")

        # Process all message IDs in batch
        all_workflows = []
        for (client, region), message_ids in message_ids_by_client.items():
            if message_ids:
                workflows = get_combined_workflow_status(
                    message_id=message_ids[0],
                    filter_workflow_types=['pricing_raw', 'pricing_enrich', 'pricing_roll', 'pricing_mart']
                )
                
                logger.info(f"DEBUG: ADM returned {len(workflows)} workflows for {client}/{region}")
                
                for workflow in workflows:
                    workflow.update({
                        'client_cd': client,
                        'processing_region_cd': region,
                        'business_dt': business_date,
                        'original_message_id': message_ids[0]
                    })
                    all_workflows.append(workflow)
            else:
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
        if "timed out" in str(e):
            logger.error("Batch pricing workflow status query timed out after 3 minutes")
        else:
            logger.error(f"Error getting batch pricing workflow status: {str(e)}")
        return []        

def get_combined_workflow_status(message_id, message_ids=None, total_count=None, parent_message_id=None, filter_workflow_types=None):
    """Get all workflow statuses in a single query using the combined SQL"""
    try:
        logger.info(f"DEBUG: Combined query called with message_id={message_id}, message_ids={message_ids}, total_count={total_count}")
        
        query_text = load_adm_query('combined_workflow_status')
        if not query_text:
            logger.error("Combined workflow status query not found")
            return []

        # Format parameters using original string formatting
        formatted_message_id = format_message_ids(message_id) if message_id else "''"
        formatted_message_ids = format_message_ids(message_ids) if message_ids else "''"
        total_count_val = total_count or 0
        parent_message_id_val = parent_message_id or ''

        logger.info(f"DEBUG: Formatted params - message_id: {formatted_message_id}, message_ids: {formatted_message_ids}, total_count: {total_count_val}")

        # Replace placeholders using original string formatting
        formatted_query = query_text.format(
            message_id=formatted_message_id,
            message_ids_placeholder=formatted_message_ids,
            total_count=total_count_val,
            parent_message_id=parent_message_id_val
        )

        logger.info(f"DEBUG: Executing ADM query for AOD workflows")
        
        workflows = []
        with get_adm_engine().connect() as conn:
            result = execute_with_timeout(conn, formatted_query)
            
            for row in result.fetchall():
                logger.info(f"DEBUG: ADM returned row: {row}")
                workflow_type_full = row[0]
                status = row[1]
                last_updated = row[2]
                started_at = row[3] if len(row) > 3 else None

                workflow_type = workflow_type_full.replace('_status', '')

                if filter_workflow_types and workflow_type not in filter_workflow_types:
                    continue

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

                if workflow_type.startswith(('aod_', 'aod_final')):
                    workflow_data['original_message_ids'] = message_ids
                    workflow_data['parent_message_id'] = parent_message_id
                else:
                    workflow_data['original_message_id'] = message_id

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
        if "timed out" in str(e):
            logger.error("Combined workflow status query timed out after 3 minutes")
        else:
            logger.error(f"Error getting combined workflow status: {str(e)}")
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
    # Calculate SOD date for SOD workflows
    query_date = business_date
    if snapshot_type == 'SOD':
        try:
            from datetime import datetime
            business_dt = datetime.strptime(business_date, '%Y-%m-%d').date()
            sod_date = business_dt + timedelta(days=1)
            while sod_date.weekday() >= 5:  # Skip weekends (5=Saturday, 6=Sunday)
                sod_date += timedelta(days=1)
            query_date = sod_date.strftime('%Y-%m-%d')
            logger.info(f"DEBUG: SOD date calculated: {business_date} -> {query_date}")
        except Exception as e:
            logger.error(f"Error calculating SOD date: {e}")
            query_date = business_date

    message_ids_query = """
        SELECT original_message_id
        FROM accounting_events
        WHERE client_cd = :client
          AND processing_region_cd = :region
          AND business_dt = :query_date
          AND snapshot_type_cd = :snapshot_type
    """

    try:
        with get_atls_engine().connect() as conn:
            result = conn.execute(text(message_ids_query), {
                'client': client,
                'region': region,
                'query_date': query_date,
                'snapshot_type': snapshot_type
            })
            message_ids = [row[0] for row in result.fetchall()]
            
            logger.info(f"DEBUG: Found {len(message_ids)} {snapshot_type} message IDs for {client}/{region}: {message_ids}")

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
            logger.info(f"DEBUG: No {snapshot_type} workflows found for {client}/{region}, creating pending statuses")
            for workflow_type in workflow_types:
                workflows.append({
                    "client_cd": client,
                    "processing_region_cd": region,
                    "workflow_type": workflow_type,
                    "status": "pending",
                    "status_with_long_running": "pending",
                    "last_updated": None,
                    "business_dt": query_date,
                    "original_message_ids": message_ids
                })

        return workflows
        
    except Exception as e:
        logger.error(f"Error getting {snapshot_type} workflow statuses for {client}/{region}: {e}")
        # Return pending statuses on error
        workflows = []
        for workflow_type in workflow_types:
            workflows.append({
                "client_cd": client,
                "processing_region_cd": region,
                "workflow_type": workflow_type,
                "status": "pending",
                "status_with_long_running": "pending",
                "last_updated": None,
                "business_dt": query_date,
                "original_message_ids": []
            })
        return workflows
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
        validate_message_id(message_ids)
        
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
            
        validate_message_id(message_ids)
        
        # Query to get parent_original_message_id from accounting_events using parameter binding
        query = """
            SELECT DISTINCT parent_original_message_id
            FROM accounting_events
            WHERE original_message_id = ANY(:message_ids)
              AND snapshot_type_cd = 'AOD'
            LIMIT 1
        """
        
        with get_atls_engine().connect() as conn:
            result = execute_with_timeout(
                conn,
                query,
                {'message_ids': message_ids}
            )
            row = result.fetchone()
            return row[0] if row else None
            
    except TimeoutError:
        logger.error("AOD parent message ID query timed out after 3 minutes")
        return None
    except Exception as e:
        logger.error(f"Error getting AOD parent message ID: {str(e)}")
        return None
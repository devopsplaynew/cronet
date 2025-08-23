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
        
        # Extract the specific query by name
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
    if isinstance(message_ids, list):
        # If it's a list, join with commas and quote each ID
        return ", ".join([f"'{msg_id}'" for msg_id in message_ids])
    elif isinstance(message_ids, str):
        # If it's already a string, check if it's JSON-like
        try:
            # Try to parse as JSON array
            parsed_ids = json.loads(message_ids)
            if isinstance(parsed_ids, list):
                return ", ".join([f"'{msg_id}'" for msg_id in parsed_ids])
            else:
                return f"'{message_ids}'"
        except json.JSONDecodeError:
            # If not JSON, treat as single ID
            return f"'{message_ids}'"
    else:
        return "''"

def get_pricing_workflow_status(message_id):
    """Get pricing workflow statuses from ADM database using queries from adm_queries.sql"""
    if not message_id:
        return []
        
    try:
        workflows = []
        
        # Check status for each workflow type using the proper queries
        for workflow_type, query_name in [
            ('pricing_raw', 'pricing_raw_status'),
            ('pricing_enrich', 'pricing_enrich_status'),
            ('pricing_roll', 'pricing_roll_status'),
            ('pricing_mart', 'pricing_mart_status')
        ]:
            query_text = load_adm_query(query_name)
            if not query_text:
                logger.error(f"Query {query_name} not found in adm_queries.sql")
                continue
                
            # Format the query with the message_id parameter
            formatted_message_id = format_message_ids(message_id)
            formatted_query = query_text.format(message_id=formatted_message_id)
            
            with get_adm_engine().connect() as conn:
                result = conn.execute(text(formatted_query))
                row = result.fetchone()
                
                if row:
                    status = row[0]
                    last_updated = row[1]
                    started_at = row[2] if len(row) > 2 else None
                    
                    # Calculate status_with_long_running
                    if status == 'pending' and started_at:
                        time_diff = (datetime.now() - started_at).total_seconds() / 60
                        if time_diff > 30:
                            status_with_long_running = 'long_running'
                        else:
                            status_with_long_running = 'inprogress'
                    else:
                        status_with_long_running = status
                    
                    workflows.append({
                        'workflow_type': workflow_type,
                        'status': status,
                        'status_with_long_running': status_with_long_running,
                        'last_updated': last_updated,
                        'original_message_id': message_id,
                        'started_at': started_at
                    })
        
        return workflows
        
    except Exception as e:
        logger.error(f"Error getting ADM pricing workflow status: {str(e)}")
        return []
        
def get_eod_workflow_status(message_id):
    """Get EOD workflow statuses from ADM database"""
    if not message_id:
        return []
        
    try:
        workflows = []
        
        # Check status for each workflow type
        for workflow_type, query_name in [
            ('eod_raw', 'eod_raw_status'),
            ('eod_enrich', 'eod_enrich_status'),
            ('eod_roll', 'eod_roll_status'),
            ('eod_mart', 'eod_mart_status'),
            ('eod_final', 'eod_final_status')  # Added eod_final
        ]:
            query_text = load_adm_query(query_name)
            if not query_text:
                logger.error(f"Query {query_name} not found in adm_queries.sql")
                continue
                
            formatted_message_id = format_message_ids(message_id)
            formatted_query = query_text.format(message_id=formatted_message_id)
            
            with get_adm_engine().connect() as conn:
                result = conn.execute(text(formatted_query))
                row = result.fetchone()
                
                if row:
                    status = row[0]
                    last_updated = row[1]
                    started_at = row[2] if len(row) > 2 else None
                    
                    # Calculate status_with_long_running
                    if status == 'pending' and started_at:
                        time_diff = (datetime.now() - started_at).total_seconds() / 60
                        if time_diff > 30:
                            status_with_long_running = 'long_running'
                        else:
                            status_with_long_running = 'inprogress'
                    else:
                        status_with_long_running = status
                    
                    workflows.append({
                        'workflow_type': workflow_type,
                        'status': status,
                        'status_with_long_running': status_with_long_running,
                        'last_updated': last_updated,
                        'original_message_id': message_id,
                        'started_at': started_at
                    })
        
        return workflows
        
    except Exception as e:
        logger.error(f"Error getting ADM EOD workflow status: {str(e)}")
        return []
        
def get_sod_workflow_status(message_id):
    """Get SOD workflow statuses from ADM database"""
    if not message_id:
        return []
        
    try:
        workflows = []
        
        # Check status for each workflow type
        for workflow_type, query_name in [
            ('sod_raw', 'sod_raw_status'),
            ('sod_enrich', 'sod_enrich_status'),
            ('sod_roll', 'sod_roll_status'),
            ('sod_mart', 'sod_mart_status'),
            ('sod_final', 'sod_final_status')
        ]:
            query_text = load_adm_query(query_name)
            if not query_text:
                logger.error(f"Query {query_name} not found in adm_queries.sql")
                continue
                
            formatted_message_id = format_message_ids(message_id)
            formatted_query = query_text.format(message_id=formatted_message_id)
            
            with get_adm_engine().connect() as conn:
                result = conn.execute(text(formatted_query))
                row = result.fetchone()
                
                if row:
                    status = row[0]
                    last_updated = row[1]
                    started_at = row[2] if len(row) > 2 else None
                    
                    # Calculate status_with_long_running
                    if status == 'pending' and started_at:
                        time_diff = (datetime.now() - started_at).total_seconds() / 60
                        if time_diff > 30:
                            status_with_long_running = 'long_running'
                        else:
                            status_with_long_running = 'inprogress'
                    else:
                        status_with_long_running = status
                    
                    workflows.append({
                        'workflow_type': workflow_type,
                        'status': status,
                        'status_with_long_running': status_with_long_running,
                        'last_updated': last_updated,
                        'original_message_id': message_id,
                        'started_at': started_at
                    })
        
        return workflows
    except Exception as e:
        logger.error(f"Error getting ADM SOD workflow status: {str(e)}")
        return []
        
def get_aod_workflow_status(message_ids):
    """Get AOD workflow statuses from ADM database"""
    if not message_ids:
        return []
        
    try:
        workflows = []
        
        # Format message IDs for SQL query
        message_ids_str = format_message_ids(message_ids)
        total_count = len(message_ids) if isinstance(message_ids, list) else 1
        
        # Get parent_original_message_id from ATLS database
        parent_message_id = get_aod_parent_message_id(message_ids)
        
        # Check status for each AOD workflow type
        for workflow_type, query_name, service_name in [
            ('aod_raw', 'aod_raw_status', 'aod_raw_loader'),
            ('aod_enrich', 'aod_enrich_status', 'aod_enrichment_service'),
            ('aod_roll', 'aod_roll_status', 'aod_rollup_service'),
            ('aod_mart', 'aod_mart_status', 'aod_mart_loader'),
            ('aod_final', 'aod_final_status', 'aod_final_publisher')
        ]:
            query_text = load_adm_query(query_name)
            if not query_text:
                logger.error(f"Query {query_name} not found in adm_queries.sql")
                continue
                
            # Replace placeholders with actual values
            formatted_query = query_text.format(
                message_ids_placeholder=message_ids_str,
                total_count=total_count,
                parent_message_id=parent_message_id or ''
            )
            
            with get_adm_engine().connect() as conn:
                result = conn.execute(text(formatted_query))
                row = result.fetchone()
                
                if row:
                    status = row[0]
                    last_updated = row[1]
                    started_at = row[2] if len(row) > 2 else None
                    
                    # Extract progress counts based on workflow type
                    if workflow_type in ['aod_raw', 'aod_enrich', 'aod_roll', 'aod_mart']:
                        # For regular AOD workflows: total:positions:taxlots
                        total_count_val = row[3] if len(row) > 3 else total_count
                        positions_count = row[4] if len(row) > 4 else 0
                        taxlots_count = row[5] if len(row) > 5 else 0
                        progress_data = {
                            'total_count': total_count_val,
                            'positions_count': positions_count,
                            'taxlots_count': taxlots_count
                        }
                    elif workflow_type == 'aod_final':
                        # For final workflow: total:asof_statements:eod_all_statements
                        total_count_val = row[3] if len(row) > 3 else total_count
                        asof_statements_count = row[4] if len(row) > 4 else 0
                        eod_all_statements_count = row[5] if len(row) > 5 else 0
                        progress_data = {
                            'total_count': total_count_val,
                            'asof_statements_count': asof_statements_count,
                            'eod_all_statements_count': eod_all_statements_count
                        }
                    else:
                        progress_data = {}
                    
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
                        'original_message_ids': message_ids,
                        'parent_message_id': parent_message_id,
                        'started_at': started_at
                    }
                    workflow_data.update(progress_data)
                    workflows.append(workflow_data)
        
        return workflows
        
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
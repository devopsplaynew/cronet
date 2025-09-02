from sqlalchemy import text
from datetime import datetime, timedelta
from database.connectors import get_atls_engine
import os
import logging

logger = logging.getLogger(__name__)

def load_query(query_name):
    """Helper function to load SQL queries from file"""
    try:
        query_file = os.path.join(os.path.dirname(__file__), '../sql/atls_queries.sql')
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

def get_batch_workflow_status(clients_regions, workflow_types, business_date):
    business_date = str(business_date)
    """Get workflow status for multiple clients/regions in a single batch"""
    sod_date = business_date
    
    try:
        if not clients_regions:
            return []

        # Calculate SOD date for SOD workflows
        if any(wf in workflow_types for wf in ['sod_ars', 'sod', 'sod_marker']):
            try:
                business_dt = datetime.strptime(business_date, '%Y-%m-%d').date()
                sod_date = business_dt + timedelta(days=1)
                while sod_date.weekday() >= 5:  # Skip weekends
                    sod_date += timedelta(days=1)
                sod_date = sod_date.strftime('%Y-%m-%d')
            except:
                sod_date = business_date

        # Load the batch query
        query_text = load_query('batch_workflow_status')
        if not query_text:
            logger.error("Batch workflow status query not found, falling back to individual queries")
            return get_individual_workflow_statuses(clients_regions, workflow_types, business_date, sod_date)

        # Format client/region list for SQL
        client_region_list = ",".join([f"('{client}','{region}')" for client, region in clients_regions])
        
        # Format the query with parameters (maintaining original format for now)
        formatted_query = query_text.format(
            business_date=business_date,
            sod_date=sod_date,
            client_region_list=client_region_list
        )

        # Execute the batch query with timeout
        with get_atls_engine().connect() as conn:
            result = execute_with_timeout(conn, formatted_query)
            batch_results = [dict(row) for row in result.mappings()]

        # Filter results and add pending statuses (same as original)
        filtered_results = [
            result for result in batch_results 
            if result['workflow_type'] in workflow_types
        ]

        all_combinations = set()
        for client, region in clients_regions:
            for workflow_type in workflow_types:
                all_combinations.add((client, region, workflow_type))

        for result in filtered_results:
            combo = (result['client_cd'], result['processing_region_cd'], result['workflow_type'])
            if combo in all_combinations:
                all_combinations.remove(combo)

        for client, region, workflow_type in all_combinations:
            filtered_results.append({
                'client_cd': client,
                'processing_region_cd': region,
                'workflow_type': workflow_type,
                'status': 'pending',
                'status_with_long_running': 'pending',
                'last_updated': None,
                'business_dt': business_date
            })

        return filtered_results

    except Exception as e:
        if "timed out" in str(e):
            logger.error("Batch workflow status query timed out after 3 minutes")
        else:
            logger.error(f"Error getting batch workflow status: {str(e)}")
        return get_individual_workflow_statuses(clients_regions, workflow_types, business_date, sod_date)

def get_individual_workflow_statuses(clients_regions, workflow_types, business_date, sod_date):
    """Fallback to individual queries if batch query fails"""
    all_status_data = []
    for workflow_type in workflow_types:
        query = load_query(workflow_type)
        if not query:
            continue

        for client, region in clients_regions:
            try:
                formatted_query = query.format(
                    client=client,
                    region=region,
                    business_date=business_date,
                    sod_date=sod_date
                )

                with get_atls_engine().connect() as conn:
                    result = execute_with_timeout(conn, formatted_query)
                    row = result.fetchone()

                    if row:
                        status_data = dict(zip(result.keys(), row))
                        all_status_data.append(status_data)
                    else:
                        all_status_data.append(default_workflow_status(client, region, workflow_type, business_date))
            
            except Exception as e:
                if "timed out" in str(e):
                    logger.warning(f"Query timeout for {client}/{region}/{workflow_type}")
                else:
                    logger.error(f"Error getting status for {client}/{region}/{workflow_type}: {str(e)}")
                all_status_data.append(default_workflow_status(client, region, workflow_type, business_date))

    return all_status_data

def default_workflow_status(client, region, workflow_type, business_date):
    """Return default status when no query is found"""
    return {
        'client_cd': client,
        'processing_region_cd': region,
        'workflow_type': workflow_type,
        'status': 'pending',
        'status_with_long_running': 'pending',
        'last_updated': None,
        'business_dt': business_date,
        'adm_status': None,
        'adm_last_updated': None,
        'original_message_id': None
    }
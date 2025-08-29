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

def get_batch_workflow_status(clients_regions, workflow_types, business_date):
    """Get workflow status for multiple clients/regions in a single batch"""
    try:
        if not clients_regions:
            return []

        # Format client/region list for SQL - use proper tuple format
        client_region_tuples = []
        for client, region in clients_regions:
            # Handle any special characters or quotes
            client_clean = client.replace("'", "''")
            region_clean = region.replace("'", "''")
            client_region_tuples.append(f"('{client_clean}','{region_clean}')")
        
        client_region_list = ",".join(client_region_tuples)
        
        # Calculate SOD date for SOD workflows
        sod_date = None
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

        # Format the query with parameters
        formatted_query = query_text.format(
            business_date=business_date,
            sod_date=sod_date or business_date,
            client_region_list=client_region_list
        )

        # Execute the batch query
        with get_atls_engine().connect() as conn:
            result = conn.execute(text(formatted_query))
            batch_results = [dict(row) for row in result.mappings()]

        # Filter results to only include requested workflow types
        filtered_results = [
            result for result in batch_results 
            if result['workflow_type'] in workflow_types
        ]

        # For any missing client/region/workflow combinations, add pending status
        all_combinations = set()
        for client, region in clients_regions:
            for workflow_type in workflow_types:
                all_combinations.add((client, region, workflow_type))

        # Remove combinations that already have results
        for result in filtered_results:
            combo = (result['client_cd'], result['processing_region_cd'], result['workflow_type'])
            if combo in all_combinations:
                all_combinations.remove(combo)

        # Add pending status for missing combinations
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
        logger.error(f"Error getting batch workflow status: {str(e)}")
        # Fall back to individual queries
        return get_individual_workflow_statuses(clients_regions, workflow_types, business_date, sod_date or business_date)

def get_individual_workflow_statuses(clients_regions, workflow_types, business_date, sod_date):
    """Fallback to individual queries if batch query fails"""
    all_status_data = []
    for workflow_type in workflow_types:
        query = load_query(workflow_type)
        if not query:
            continue

        for client, region in clients_regions:
            formatted_query = query.format(
                client=client,
                region=region,
                business_date=business_date,
                sod_date=sod_date or business_date
            )

            with get_atls_engine().connect() as conn:
                result = conn.execute(text(formatted_query))
                row = result.fetchone()

                if row:
                    status_data = dict(zip(result.keys(), row))
                    all_status_data.append(status_data)
                else:
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
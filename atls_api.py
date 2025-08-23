from sqlalchemy import text
from datetime import datetime, timedelta
from database.connectors import get_atls_engine
import os

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
        print(f"Error loading query {query_name}: {str(e)}")
        return None

def get_workflow_status(client, region, workflow_type, business_date):
    """Get workflow status from ATLS database"""
    try:
        query = load_query(workflow_type)
        if not query:
            return default_workflow_status(client, region, workflow_type, business_date)
        
        # Calculate SOD date for workflows that need it
        sod_date = None
        if workflow_type in ['sod_ars', 'sod', 'sod_marker']:
            business_dt = datetime.strptime(business_date, '%Y-%m-%d').date()
            sod_date = business_dt + timedelta(days=1)
            while sod_date.weekday() >= 5:  # Skip weekends
                sod_date += timedelta(days=1)
            sod_date = sod_date.strftime('%Y-%m-%d')
        
        # Format the query with parameters
        formatted_query = query.format(
            client=client,
            region=region,
            business_date=business_date,
            sod_date=sod_date or business_date
        )
        
        with get_atls_engine().connect() as conn:
            result = conn.execute(text(formatted_query))
            
            # Get column names from the result
            columns = result.keys()
            
            # Fetch the first row
            row = result.fetchone()
            
            if row:
                # Ensure we have matching columns and values
                if len(columns) != len(row):
                    logger.error(f"Column/value mismatch in {workflow_type} query. Columns: {len(columns)}, Values: {len(row)}")
                    return default_workflow_status(client, region, workflow_type, business_date)
                
                # Create dictionary from column names and row values
                status_data = dict(zip(columns, row))
                
                # Ensure we have all required fields
                required_fields = ['client_cd', 'processing_region_cd', 'workflow_type', 
                                  'status', 'status_with_long_running', 'last_updated', 
                                  'business_dt']
                
                for field in required_fields:
                    if field not in status_data:
                        status_data[field] = default_workflow_status(client, region, workflow_type, business_date)[field]
                
                return status_data
            
            return default_workflow_status(client, region, workflow_type, business_date)
            
    except Exception as e:
        logger.error(f"Error getting workflow status for {workflow_type}: {str(e)}")
        return default_workflow_status(client, region, workflow_type, business_date)

def get_original_message_id(client, region, business_date, workflow_type):
    """Get original message ID for a workflow"""
    try:
        if workflow_type in ['pricing_ars', 'trading_ars', 'eod_ars', 'sod_ars']:
            trigger_type = {
                'pricing_ars': 'opsRegionEODPricingSignoff',
                'trading_ars': 'opsRegionEODTradingSignoff',
                'eod_ars': 'opsRegionEodSignoff',
                'sod_ars': 'sodRegionGlobalProcessTrigger'
            }.get(workflow_type)
            
            query = load_query('ars_original_message_id')
            if not query:
                return None
                
            formatted_query = query.format(
                client=client,
                region=region,
                business_date=business_date,
                trigger_type=trigger_type
            )
        elif workflow_type == 'pricing_marker':
            query = load_query('pricing_original_message_id')
            if not query:
                return None
                
            formatted_query = query.format(
                client=client,
                region=region,
                business_date=business_date
            )
        else:
            return None
            
        with get_atls_engine().connect() as conn:
            result = conn.execute(text(formatted_query))
            return result.scalar()
            
    except Exception as e:
        print(f"Error getting original message ID: {str(e)}")
        return None

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
        # Add any additional fields that might be expected
        'adm_status': None,
        'adm_last_updated': None,
        'original_message_id': None
    }
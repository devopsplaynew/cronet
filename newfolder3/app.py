from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
import logging
from api.atls_api import get_batch_workflow_status
from database.connectors import get_atls_engine, get_adm_engine
from api.adm_api import get_batch_pricing_workflow_status, get_batch_eod_workflow_status, get_batch_sod_workflow_status, get_batch_aod_workflow_status
from sqlalchemy import text
from functools import lru_cache
import time
from functools import wraps
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Define workflow order for UI display
WORKFLOW_ORDER = {
    'trading_ars': 1,
    'pricing_ars': 2,
    'pricing_marker': 3,
    'pricing_raw': 4,
    'pricing_enrich': 5,
    'pricing_roll': 6,
    'pricing_mart': 7,
    'eod_ars': 8,
    'eod': 9,
    'eod_marker': 10,
    'eod_raw': 11,
    'eod_enrich': 12,
    'eod_roll': 13,
    'eod_mart': 14,
    'eod_final': 15,
    'asof_events': 16,
    'asof_marker': 17,
    'aod': 18,
    'aod_marker': 19,
    'aod_raw': 20,
    'aod_enrich': 21,
    'aod_roll': 22,
    'aod_mart': 23,
    'aod_final': 24,
    'sod_ars': 25,
    'sod': 26,
    'sod_marker': 27,
    'sod_raw': 28,
    'sod_enrich': 29,
    'sod_roll': 30,
    'sod_mart': 31,
    'sod_final': 32
}

# Simple cache implementation
class SimpleCache:
    def __init__(self):
        self.cache = {}
        self.default_ttl = 300  # 5 minutes default TTL
    
    def get(self, key):
        if key in self.cache:
            value, expiry = self.cache[key]
            if expiry > datetime.now():
                return value
            else:
                del self.cache[key]
        return None
    
    def set(self, key, value, ttl=None):
        expiry = datetime.now() + timedelta(seconds=ttl or self.default_ttl)
        self.cache[key] = (value, expiry)
    
    def clear(self):
        self.cache = {}

# Initialize cache
cache = SimpleCache()

# Cache decorator
def cached(ttl=300):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Create a cache key based on function name and arguments
            key_parts = [f.__name__] + [str(arg) for arg in args] + [f"{k}={v}" for k, v in kwargs.items()]
            cache_key = hashlib.md5("|".join(key_parts).encode()).hexdigest()
            
            # Check cache
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Execute function if not cached
            result = f(*args, **kwargs)
            
            # Cache the result
            cache.set(cache_key, result, ttl)
            
            return result
        return decorated_function
    return decorator

def sort_workflows(workflows):
    """Sort workflows according to the defined order"""
    return sorted(workflows, key=lambda x: WORKFLOW_ORDER.get(x['workflow_type'], 999))

def calculate_sod_date(business_date):
    """Calculate SOD date (next business day)"""
    business_dt = datetime.strptime(business_date, '%Y-%m-%d').date()
    sod_date = business_dt + timedelta(days=1)
    while sod_date.weekday() >= 5:  # Skip weekends
        sod_date += timedelta(days=1)
    return sod_date.strftime('%Y-%m-%d')

@cached(ttl=300)  # Cache for 5 minutes
def get_all_clients_regions():
    """Get all distinct client/region combinations"""
    query = """
        SELECT DISTINCT client_cd, processing_region_cd
        FROM (
            SELECT client_cd, processing_region_cd FROM ars_events
            UNION
            SELECT client_cd, processing_region_cd FROM pricing_events
            UNION
            SELECT client_cd, processing_region_cd FROM accounting_events
        ) AS combined
        ORDER BY client_cd, processing_region_cd
    """

    with get_atls_engine().connect() as conn:
        result = conn.execute(text(query))
        return [(row['client_cd'], row['processing_region_cd']) for row in result.mappings()]
        
@app.route('/api/batch_status')
@cached(ttl=60)  # Cache for 1 minute
def get_batch_status():
    business_date = request.args.get('business_date')
    client_filter = request.args.get('client')  # Get client filter from request
    
    if not business_date:
        return jsonify({"status": "error", "message": "business_date parameter is required"}), 400

    try:
        # Get all client/region combinations
        clients_regions = get_all_clients_regions()
        
        # Filter by client if specified
        if client_filter and client_filter != 'ALL':
            clients_regions = [(client, region) for client, region in clients_regions if client == client_filter]

        # Get ATLS workflows in single batch query
        atls_workflow_types = [
            'trading_ars', 'pricing_ars', 'pricing_marker',
            'eod_ars', 'eod', 'eod_marker',
            'asof_events', 'asof_marker', 'aod', 'aod_marker',
            'sod_ars', 'sod', 'sod_marker'
        ]

        atls_statuses = get_batch_workflow_status(clients_regions, atls_workflow_types, business_date)

        # Get ADM workflows
        pricing_statuses = get_batch_pricing_workflow_status(clients_regions, business_date)
        eod_statuses = get_batch_eod_workflow_status(clients_regions, business_date)
        aod_statuses = get_batch_aod_workflow_status(clients_regions, business_date)
        sod_statuses = get_batch_sod_workflow_status(clients_regions, business_date)

        # Combine all statuses
        all_statuses = atls_statuses + pricing_statuses + eod_statuses + aod_statuses + sod_statuses

        # Sort and return
        sorted_response = sort_workflows(all_statuses)

        return jsonify({
            "status": "success",
            "data": sorted_response,
            "timestamp": datetime.now().isoformat(),
            "business_date": business_date,
            "client_filter": client_filter if client_filter else 'ALL'
        })

    except Exception as e:
        logger.error(f"Error in batch status: {str(e)}")
        return jsonify({"status": "error", "message": "Could not retrieve batch status"}), 500
        
@app.route('/')
@app.route('/dashboard')
def dashboard():
    return render_template('batch_status_workflow.html')

@app.route('/details/<client>/<region>')
def details(client, region):
    business_date = request.args.get('business_date', datetime.now().strftime('%Y-%m-%d'))

    try:
        # Get all workflows for this client/region in batch
        clients_regions = [(client, region)]

        # Get standard workflow statuses in batch
        standard_workflows = [
            'trading_ars', 'pricing_ars', 'pricing_marker',
            'eod_ars', 'eod', 'eod_marker',
            'asof_events', 'asof_marker', 'aod', 'aod_marker',
            'sod_ars', 'sod', 'sod_marker'
        ]

        standard_statuses = get_batch_workflow_status(
            clients_regions, standard_workflows, business_date
        )

        # Get pricing workflow statuses in batch
        pricing_statuses = get_batch_pricing_workflow_status(clients_regions, business_date)

        # Get EOD workflow statuses in batch
        eod_statuses = get_batch_eod_workflow_status(clients_regions, business_date)

        # Get AOD workflow statuses in batch
        aod_statuses = get_batch_aod_workflow_status(clients_regions, business_date)

        # Get SOD workflow statuses in batch
        sod_statuses = get_batch_sod_workflow_status(clients_regions, business_date)

        # Combine all workflows
        all_workflows = standard_statuses + pricing_statuses + eod_statuses + aod_statuses + sod_statuses

        # Sort workflows according to the defined order
        sorted_workflows = sort_workflows(all_workflows)

        sod_date = calculate_sod_date(business_date)

        # Get reporting loaders status for different snapshot types
        reporting_loaders_eodpx = get_reporting_loaders_status(client, region, business_date, 'EODPX')
        reporting_loaders_eod = get_reporting_loaders_status(client, region, business_date, 'EOD')
        reporting_loaders_aod = get_reporting_loaders_status(client, region, business_date, 'AOD')
        reporting_loaders_sod = get_reporting_loaders_status(client, region, sod_date, 'SOD')

        # Combine all reporting loaders by snapshot type
        reporting_loaders = {
            'EODPX': reporting_loaders_eodpx,
            'EOD': reporting_loaders_eod,
            'AOD': reporting_loaders_aod,
            'SOD': reporting_loaders_sod
        }

        return render_template(
            'details.html',
            client=client,
            region=region,
            business_date=business_date,
            sod_date=sod_date,
            workflows=sorted_workflows,
            reporting_loaders=reporting_loaders
        )

    except Exception as e:
        logger.error(f"Error in details route: {str(e)}")
        return render_template(
            'details.html',
            client=client,
            region=region,
            business_date=business_date,
            workflows=[],
            reporting_loaders={}
        )

@app.route('/api/pricing_workflows')
def pricing_workflows():
    business_date = request.args.get('business_date')
    client = request.args.get('client')
    region = request.args.get('region')

    if not all([business_date, client, region]):
        return jsonify({
            "status": "error",
            "message": "business_date, client, and region parameters are required",
            "timestamp": datetime.now().isoformat()
        }), 400

    try:
        # First get message IDs from ATLS
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

        # Then get workflow statuses from ADM for each message ID using combined query
        workflows = []
        for message_id in message_ids:
            adm_workflows = get_combined_workflow_status(message_id=message_id)
            for workflow in adm_workflows:
                # Only include pricing workflows
                if workflow['workflow_type'].startswith('pricing_'):
                    workflow.update({
                        'client_cd': client,
                        'processing_region_cd': region,
                        'business_dt': business_date
                    })
                    workflows.append(workflow)

        # Sort workflows by order
        sorted_workflows = sort_workflows(workflows)

        return jsonify({
            "status": "success",
            "data": sorted_workflows,
            "timestamp": datetime.now().isoformat(),
            "business_date": business_date
        })

    except Exception as e:
        logger.error(f"Error getting pricing workflows: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Could not retrieve pricing workflows",
            "timestamp": datetime.now().isoformat()
        }), 500

def get_reporting_loaders_status(client, region, business_date, snapshot_type):
    """Get reporting loaders status from ADM database"""
    try:
        # Define the static set of marker_type_cd and subject_area_cd pairs for each snapshot type
        static_markers = {
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
            ]
        }
        
        # Get the appropriate static markers for the snapshot type
        markers = static_markers.get(snapshot_type, [])
        
        # Query to get existing markers from the ADM database
        query = """
            SELECT client_cd, processing_region_cd, business_dt, 
                   snapshot_type_cd, subject_area_cd, marker_type_cd, created_at
            FROM reporting_loaders_markers
            WHERE client_cd = :client
              AND processing_region_cd = :region
              AND business_dt = :business_date
              AND snapshot_type_cd = :snapshot_type
        """
        
        # Execute the query against ADM database
        with get_adm_engine().connect() as conn:
            result = conn.execute(text(query), {
                'client': client,
                'region': region,
                'business_date': business_date,
                'snapshot_type': snapshot_type
            })
            db_markers = {row['marker_type_cd']: row for row in result.mappings()}
        
        # Create the result set with all static markers
        results = []
        for marker_type, subject_area in markers:
            if marker_type in db_markers:
                # Marker exists in database
                results.append({
                    'client_cd': client,
                    'processing_region_cd': region,
                    'business_dt': business_date,
                    'snapshot_type_cd': snapshot_type,
                    'marker_type_cd': marker_type,
                    'subject_area_cd': subject_area,
                    'created_at': db_markers[marker_type]['created_at'],
                    'status': 'completed'
                })
            else:
                # Marker doesn't exist in database
                results.append({
                    'client_cd': client,
                    'processing_region_cd': region,
                    'business_dt': business_date,
                    'snapshot_type_cd': snapshot_type,
                    'marker_type_cd': marker_type,
                    'subject_area_cd': subject_area,
                    'created_at': None,
                    'status': 'pending'
                })
        
        return results
        
    except Exception as e:
        logger.error(f"Error getting reporting loaders status: {str(e)}")
        # Return empty results in case of error
        return []

@app.route('/api/volume_trends')
def volume_trends_api():
    client = request.args.get('client')
    region = request.args.get('region', 'AMER')
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))

    if not client:
        return jsonify({"status": "error", "message": "client parameter is required"}), 400

    try:
        data = get_volume_trends(client, region, start_date, end_date)
        return jsonify({
            "status": "success",
            "client": client,
            "region": region,
            "start_date": start_date,
            "end_date": end_date,
            "data": data
        })
    except Exception as e:
        logger.error(f"Error in volume_trends_api: {str(e)}")
        return jsonify({"status": "error", "message": "Could not fetch volume trends"}), 500

@app.route('/volume_trends')
@app.route('/volume_trends/<region>')
def all_volume_trends(region="ALL"):
    # default: last 15 days
    default_end = datetime.now().date()
    default_start = default_end - timedelta(days=14)

    start_date = request.args.get('start_date', default_start.strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', default_end.strftime('%Y-%m-%d'))

    try:
        data = get_all_volume_trends(region, start_date, end_date)
        return render_template(
            'volume_trends_all.html',
            region=region,
            start_date=start_date,
            end_date=end_date,
            data=data
        )
    except Exception as e:
        logger.error(f"Error in all_volume_trends: {str(e)}")
        return render_template('volume_trends_all.html', region=region, data=[])

def get_all_volume_trends(region, start_date, end_date):
    """Get volume trends for all clients, all regions (or a specific one)"""
    base_query = """
        SELECT 
            business_dt as business_dt,
            client_cd,
            processing_region_cd,
            snapshot_type_cd,
            SUM(records_expected_ct) as total_records
        FROM volume_check
        WHERE business_dt BETWEEN :start_date AND :end_date
    """
    params = {"start_date": start_date, "end_date": end_date}

    if region != "ALL":
        base_query += " AND processing_region_cd = :region"
        params["region"] = region

    base_query += """
        GROUP BY business_dt, client_cd, processing_region_cd, snapshot_type_cd
        ORDER BY business_dt, client_cd, processing_region_cd, snapshot_type_cd
    """

    with get_atls_engine().connect() as conn:
        result = conn.execute(text(base_query), params)
        return [dict(row) for row in result.mappings()]

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error("server_error", error=str(error))
    return jsonify({'error': 'Internal server error'}), 500

@app.route('/health')
def health_check():
    try:
        # Test database connections
        with get_atls_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        with get_adm_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
            
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'databases': ['atls', 'adm'],
            'version': '1.0.0'
        })
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

@app.route('/clear_cache')
def clear_cache():
    """Clear all caches"""
    cache.clear()
    get_all_clients_regions.cache_clear() if hasattr(get_all_clients_regions, 'cache_clear') else None
    return jsonify({"status": "success", "message": "Cache cleared"})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
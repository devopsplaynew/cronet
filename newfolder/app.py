from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
import logging
from api.atls_api import get_batch_workflow_status
from database.connectors import get_atls_engine, get_adm_engine
from api.adm_api import get_batch_pricing_workflow_status, get_batch_eod_workflow_status, get_batch_sod_workflow_status, get_batch_aod_workflow_status
from sqlalchemy import text
from functools import lru_cache
import time

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
def get_batch_status():
    business_date = request.args.get('business_date')
    if not business_date:
        return jsonify({"status": "error", "message": "business_date parameter is required"}), 400

    try:
        # Get all client/region combinations
        clients_regions = get_all_clients_regions()

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
            "business_date": business_date
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
        
def get_pricing_marker_processing_times(client, region, start_date, end_date):
    """Get pricing marker completion times with optimized query"""
    query = """
        SELECT 
            pe.business_dt,
            MIN(pe.created_at) as event_created,
            MIN(pm.created_at) as marker_created,
            EXTRACT(EPOCH FROM (MIN(pm.created_at) - MIN(pe.created_at))) as processing_seconds
        FROM pricing_events pe
        JOIN pricing_markers pm ON pe.id = pm.pricing_event_id
        WHERE pe.client_cd = :client
          AND pe.processing_region_cd = :region
          AND pe.business_dt BETWEEN :start_date AND :end_date
          AND pm.marker_type_cd = 'eodPXRegionSubjectAreaTransformed'
        GROUP BY pe.business_dt
        ORDER BY pe.business_dt DESC
        LIMIT 7
    """
    
    with get_atls_engine().connect() as conn:
        result = conn.execute(text(query), {
            'client': client,
            'region': region,
            'start_date': start_date,
            'end_date': end_date
        })
        return [dict(row) for row in result.mappings()]

def get_eod_processing_times(client, region, start_date, end_date):
    """Get EOD workflow processing times with optimized query"""
    query = """
        SELECT 
            business_dt,
            MIN(created_at) as workflow_start,
            MAX(created_at) as workflow_end,
            EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) as processing_seconds
        FROM accounting_events
        WHERE client_cd = :client
          AND processing_region_cd = :region
          AND business_dt BETWEEN :start_date AND :end_date
          AND snapshot_type_cd = 'EOD'
        GROUP BY business_dt
        ORDER BY business_dt DESC
        LIMIT 7
    """
    
    with get_atls_engine().connect() as conn:
        result = conn.execute(text(query), {
            'client': client,
            'region': region,
            'start_date': start_date,
            'end_date': end_date
        })
        return [dict(row) for row in result.mappings()]

def get_ars_processing_times(client, region, start_date, end_date):
    """Get ARS workflow processing times with optimized query"""
    query = """
        SELECT 
            business_dt,
            trigger_marker_type_cd,
            MIN(created_at) as workflow_start,
            MAX(created_at) as workflow_end,
            EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) as processing_seconds
        FROM ars_events
        WHERE client_cd = :client
          AND processing_region_cd = :region
          AND business_dt BETWEEN :start_date AND :end_date
          AND trigger_marker_type_cd IN (
              'opsRegionEODTradingSignoff', 
              'opsRegionEODPricingSignoff',
              'opsRegionEodSignoff',
              'sodRegionGlobalProcessTrigger'
          )
        GROUP BY business_dt, trigger_marker_type_cd
        ORDER BY business_dt DESC, trigger_marker_type_cd
        LIMIT 21  -- 7 days * 3 types max
    """
    
    with get_atls_engine().connect() as conn:
        result = conn.execute(text(query), {
            'client': client,
            'region': region,
            'start_date': start_date,
            'end_date': end_date
        })
        return [dict(row) for row in result.mappings()]

def get_aod_processing_times(client, region, start_date, end_date):
    """Get AOD workflow processing times with optimized query"""
    query = """
        SELECT 
            business_dt,
            MIN(created_at) as workflow_start,
            MAX(created_at) as workflow_end,
            EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) as processing_seconds
        FROM accounting_events
        WHERE client_cd = :client
          AND processing_region_cd = :region
          AND business_dt BETWEEN :start_date AND :end_date
          AND snapshot_type_cd = 'AOD'
        GROUP BY business_dt
        ORDER BY business_dt DESC
        LIMIT 7
    """
    
    with get_atls_engine().connect() as conn:
        result = conn.execute(text(query), {
            'client': client,
            'region': region,
            'start_date': start_date,
            'end_date': end_date
        })
        return [dict(row) for row in result.mappings()]

def get_sod_processing_times(client, region, start_date, end_date):
    """Get SOD workflow processing times with optimized query"""
    query = """
        SELECT 
            business_dt,
            MIN(created_at) as workflow_start,
            MAX(created_at) as workflow_end,
            EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) as processing_seconds
        FROM accounting_events
        WHERE client_cd = :client
          AND processing_region_cd = :region
          AND business_dt BETWEEN :start_date AND :end_date
          AND snapshot_type_cd = 'SOD'
        GROUP BY business_dt
        ORDER BY business_dt DESC
        LIMIT 7
    """
    
    with get_atls_engine().connect() as conn:
        result = conn.execute(text(query), {
            'client': client,
            'region': region,
            'start_date': start_date,
            'end_date': end_date
        })
        return [dict(row) for row in result.mappings()]        

def get_workflow_processing_times(client, region, start_date, end_date):
    """Get processing times for all workflows over date range"""
    processing_data = {}
    
    # Convert dates to strings for SQL queries
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    try:
        # Get pricing marker processing times
        pricing_marker_times = get_pricing_marker_processing_times(client, region, start_date_str, end_date_str)
        processing_data['pricing_marker'] = pricing_marker_times
        
        # Get EOD processing times
        eod_times = get_eod_processing_times(client, region, start_date_str, end_date_str)
        processing_data['eod'] = eod_times
        
        # Get AOD processing times
        aod_times = get_aod_processing_times(client, region, start_date_str, end_date_str)
        processing_data['aod'] = aod_times
        
        # Get SOD processing times
        sod_times = get_sod_processing_times(client, region, start_date_str, end_date_str)
        processing_data['sod'] = sod_times
        
        # Get ARS processing times
        ars_times = get_ars_processing_times(client, region, start_date_str, end_date_str)
        processing_data['ars'] = ars_times
        
    except Exception as e:
        logger.error(f"Error getting processing times: {str(e)}")
    
    return processing_data

# ---- Caching function ----
@lru_cache(maxsize=32)
def get_cached_processing_times(client, region, days=7):
    """Get processing times with caching (5 min refresh)"""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days-1)
    return get_workflow_processing_times(client, region, start_date, end_date)

# ---- Manual cache clearing endpoint ----
@app.route('/clear_processing_cache')
def clear_processing_cache():
    """Clear processing times cache"""
    get_cached_processing_times.cache_clear()
    return jsonify({"status": "success", "message": "Cache cleared"})

# ---- Processing times route ----
@app.route('/processing_times/<client>/<region>')
def processing_times(client, region):
    """Show processing times for workflows over last 7 days"""
    try:
        processing_data = get_cached_processing_times(client, region, 7)
        return render_template(
            'processing_times.html',
            client=client,
            region=region,
            processing_data=processing_data,
            days=7
        )
    except Exception as e:
        logger.error(f"Error in processing_times route: {str(e)}")
        return render_template(
            'processing_times.html',
            client=client,
            region=region,
            processing_data={},
            days=7
        )
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

@app.route('/metrics')
def metrics():
    from prometheus_client import generate_latest, Counter, Gauge
    
    REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests')
    PROCESSING_TIME = Gauge('request_processing_seconds', 'Time spent processing request')
    
    REQUEST_COUNT.inc()
    return generate_latest()
    
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

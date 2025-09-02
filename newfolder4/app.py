from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
import logging
from api.atls_api import get_batch_workflow_status
from database.connectors import get_atls_engine, get_adm_engine
from api.adm_api import get_combined_workflow_status   # only one ADM entry point
from sqlalchemy import text
from functools import lru_cache
import time
from functools import wraps
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Workflow order    
WORKFLOW_ORDER = {
    'trading_ars': 1, 'pricing_ars': 2, 'pricing_marker': 3,
    'eodpx_raw': 4, 'eodpx_enrich': 5, 'eodpx_roll': 6, 'eodpx_mart': 7, 'eodpx_final': 8,
    'eod_ars': 9, 'eod': 10, 'eod_marker': 11, 'eod_raw': 12,
    'eod_enrich': 13, 'eod_roll': 14, 'eod_mart': 15, 'eod_final': 16,
    'asof_events': 17, 'asof_marker': 18, 'aod': 19, 'aod_marker': 20,
    'aod_raw': 21, 'aod_enrich': 22, 'aod_roll': 23, 'aod_mart': 24, 'aod_final': 25,
    'sod_ars': 26, 'sod': 27, 'sod_marker': 28, 'sod_raw': 29,
    'sod_enrich': 30, 'sod_roll': 31, 'sod_mart': 32, 'sod_final': 33
}

# ----------------------------------------------------------------------
# Simple in-memory cache
class SimpleCache:
    def __init__(self):
        self.cache = {}
        self.default_ttl = 300
    
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

cache = SimpleCache()

# Cache decorator
def cached(ttl=300):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            key_parts = [f.__name__] + [str(arg) for arg in args] + [f"{k}={v}" for k, v in kwargs.items()]
            cache_key = hashlib.md5("|".join(key_parts).encode()).hexdigest()
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            result = f(*args, **kwargs)
            cache.set(cache_key, result, ttl)
            return result
        return decorated_function
    return decorator

# ----------------------------------------------------------------------
def sort_workflows(workflows):
    return sorted(workflows, key=lambda x: WORKFLOW_ORDER.get(x['workflow_type'], 999))

def calculate_sod_date(business_date):
    business_dt = datetime.strptime(business_date, '%Y-%m-%d')
    sod_date = business_dt + timedelta(days=1)
    while sod_date.weekday() >= 5:
        sod_date += timedelta(days=1)
    return sod_date.strftime('%Y-%m-%d')

# ----------------------------------------------------------------------
@cached(ttl=300)
def get_all_clients_regions():
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

# ----------------------------------------------------------------------
@app.route('/api/batch_status')
@cached(ttl=60)
def get_batch_status():
    business_date = request.args.get("business_date")
    client_filter = request.args.get("client")

    if not business_date:
        return jsonify({"status": "error", "message": "business_date required"}), 400

    try:
        # Get client/region pairs from ATLS
        clients_regions = get_all_clients_regions()

        if client_filter and client_filter != "ALL":
            clients_regions = [(c, r) for c, r in clients_regions if c == client_filter]

        # ATLS workflows
        atls_workflow_types = [
            'trading_ars', 'pricing_ars', 'pricing_marker',
            'eod_ars', 'eod', 'eod_marker',
            'asof_events', 'asof_marker', 'aod', 'aod_marker',
            'sod_ars', 'sod', 'sod_marker'
        ]
        atls_statuses = get_batch_workflow_status(clients_regions, atls_workflow_types, business_date)

        # ADM workflows (now a single entry point)
        adm_statuses = get_combined_workflow_status(clients_regions, business_date)

        all_statuses = atls_statuses + adm_statuses
        sorted_response = sort_workflows(all_statuses)

        return jsonify({
            "status": "success",
            "data": sorted_response,
            "timestamp": datetime.now().isoformat(),
            "business_date": business_date,
            "client_filter": client_filter if client_filter else "ALL"
        })

    except Exception as e:
        logger.error(f"Error in batch status: {str(e)}")
        return jsonify({"status": "error", "message": "Could not retrieve batch status"}), 500


@app.route('/')
@app.route('/dashboard')
def dashboard():
    return render_template("batch_status_workflow.html")


@app.route('/details/<client>/<region>')
def details(client, region):
    business_date = request.args.get("business_date", datetime.now().strftime("%Y-%m-%d"))

    try:
        clients_regions = [(client, region)]

        standard_workflows = [
            'trading_ars', 'pricing_ars', 'pricing_marker',
            'eod_ars', 'eod', 'eod_marker',
            'asof_events', 'asof_marker', 'aod', 'aod_marker',
            'sod_ars', 'sod', 'sod_marker'
        ]

        standard_statuses = get_batch_workflow_status(clients_regions, standard_workflows, business_date)

        adm_statuses = get_combined_workflow_status(clients_regions, business_date)

        all_workflows = standard_statuses + adm_statuses
        sorted_workflows = sort_workflows(all_workflows)

        sod_date = calculate_sod_date(business_date)

        return render_template(
            "details.html",
            client=client,
            region=region,
            business_date=business_date,
            sod_date=sod_date,
            workflows=sorted_workflows,
            reporting_loaders={}   # keep it simple, no changes yet
        )

    except Exception as e:
        logger.error(f"Error in details route: {str(e)}")
        return render_template("details.html", client=client, region=region,
                               business_date=business_date,
                               workflows=[], reporting_loaders={})
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error("server_error", exc_info=True)
    return jsonify({'error': 'Internal server error'}), 500

@app.route('/health')
def health_check():
    try:
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
    cache.clear()
    return jsonify({"status": "success", "message": "Cache cleared"})

# ----------------------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
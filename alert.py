import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import pytz
from tabulate import tabulate
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys

# Timezone setup
EST = pytz.timezone('US/Eastern')

# Configuration
DB_CONFIG = {
    'host': 'your_postgres_host',
    'database': 'your_database',
    'user': 'your_username',
    'password': 'your_password',
    'port': '5432'
}

EMAIL_CONFIG = {
    'smtp_server': 'your.smtp.server',
    'smtp_port': 587,
    'sender_email': 'alerts@yourdomain.com',
    'sender_password': 'yourpassword',
    'recipient_emails': ['recipient@domain.com']
}

def get_db_connection():
    """Establish PostgreSQL connection with error handling"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port']
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        sys.exit(1)

def fetch_data():
    """Fetch data from PostgreSQL with proper error handling"""
    query = """
    SELECT 
        client_cd,
        processing_region_cd,
        snapshot_type_cd,
        business_dt,
        original_message_id,
        marker_type,
        max AS processing_time,
        count
    FROM your_table_name
    WHERE marker_type IN ('accounting_events', 'edRegionsubjectareaTransformed')
    ORDER BY client_cd, processing_region_cd, snapshot_type_cd, business_dt, original_message_id, marker_type
    """
    
    try:
        with get_db_connection() as conn:
            # Use pandas with explicit column names to prevent tuple index errors
            df = pd.read_sql_query(query, conn)
            
            # Convert timestamps to EST
            if not df.empty and 'processing_time' in df.columns:
                df['processing_time'] = pd.to_datetime(df['processing_time']).dt.tz_localize('UTC').dt.tz_convert(EST)
            
            return df
            
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return pd.DataFrame()  # Return empty dataframe on error

def process_data(df):
    """Process data with robust error handling"""
    alerts = {}
    
    if df.empty:
        print("Warning: No data returned from database")
        return []
    
    try:
        current_est = get_current_est()
        
        # Verify required columns exist
        required_columns = ['client_cd', 'processing_region_cd', 'snapshot_type_cd',
                           'business_dt', 'original_message_id', 'marker_type',
                           'processing_time', 'count']
        
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            print(f"Error: Missing columns in data - {missing_cols}")
            return []
        
        # Group data safely
        grouped = df.groupby(['client_cd', 'processing_region_cd', 'snapshot_type_cd',
                            'business_dt', 'original_message_id'])
        
        for group_key, group_df in grouped:
            try:
                msg_id = group_key[4]
                accounting_row = group_df[group_df['marker_type'] == 'accounting_events']
                
                if not accounting_row.empty:
                    accounting_time = pd.to_datetime(accounting_row['processing_time'].iloc[0])
                    duration = (current_est - accounting_time).total_seconds() / 60
                    
                    if duration > 30:
                        transformed_rows = group_df[
                            group_df['marker_type'].str.contains('Transformed', case=False, na=False)
                        ]
                        
                        if transformed_rows.empty:
                            alerts[msg_id] = create_alert(group_key, duration, 'Missing transformed marker')
                        else:
                            snapshot_type = group_key[2]
                            required_count = 6 if snapshot_type in ['EOD', 'SOD'] else 2 if snapshot_type == 'AOD' else None
                            
                            for _, row in transformed_rows.iterrows():
                                transformed_time = pd.to_datetime(row['processing_time'])
                                transform_duration = (transformed_time - accounting_time).total_seconds() / 60
                                
                                if (required_count is not None and 
                                    row['count'] != required_count and 
                                    transform_duration > 30):
                                    alerts[msg_id] = create_alert(
                                        group_key, 
                                        transform_duration, 
                                        f'Count mismatch (expected {required_count}, got {row["count"]})'
                                    )
            except Exception as e:
                print(f"Error processing group {group_key}: {str(e)}")
                continue
                
    except Exception as e:
        print(f"Processing error: {str(e)}")
        
    return list(alerts.values())

def create_alert(group_key, duration, issue):
    """Helper to create consistent alert structure"""
    return {
        'client': group_key[0],
        'region': group_key[1],
        'snapshot': group_key[2],
        'business_dt': group_key[3],
        'original_message_id': group_key[4],
        'duration': f"{int(duration)}mins",
        'issue': issue,
        'alert_time': get_current_est().strftime('%Y-%m-%d %H:%M:%S %Z')
    }

def get_current_est():
    return datetime.now(EST)

# ... [rest of the functions remain the same: create_html_alert, send_email, main] ...

if __name__ == "__main__":
    main()

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
    """Fetch data from PostgreSQL with proper column handling"""
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
            # Explicitly handle column names to prevent errors
            df = pd.read_sql_query(query, conn)
            
            # Ensure all expected columns exist
            expected_columns = ['client_cd', 'processing_region_cd', 'snapshot_type_cd',
                              'business_dt', 'original_message_id', 'marker_type',
                              'processing_time', 'count']
            
            # Add missing columns with None values
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Convert timestamps to EST if processing_time exists
            if 'processing_time' in df.columns and not df['processing_time'].empty:
                df['processing_time'] = pd.to_datetime(df['processing_time']).dt.tz_localize('UTC').dt.tz_convert(EST)
            
            return df
            
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return pd.DataFrame(columns=expected_columns)  # Return empty dataframe with expected columns

def process_data(df):
    """Process data with robust column handling"""
    alerts = {}
    
    if df.empty:
        print("Warning: No data returned from database")
        return []
    
    try:
        current_est = get_current_est()
        
        # Verify we have the minimum required columns
        required_columns = ['client_cd', 'processing_region_cd', 'snapshot_type_cd',
                          'business_dt', 'original_message_id', 'marker_type']
        
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            print(f"Error: Missing required columns - {missing_cols}")
            return []
        
        # Group data safely
        grouped = df.groupby(['client_cd', 'processing_region_cd', 'snapshot_type_cd',
                            'business_dt', 'original_message_id'])
        
        for group_key, group_df in grouped:
            try:
                msg_id = group_key[4]
                accounting_row = group_df[group_df['marker_type'] == 'accounting_events']
                
                if not accounting_row.empty:
                    # Safely get processing time with fallback
                    accounting_time = None
                    if 'processing_time' in accounting_row.columns and not accounting_row['processing_time'].isnull().all():
                        accounting_time = pd.to_datetime(accounting_row['processing_time'].iloc[0])
                    
                    if accounting_time is None:
                        print(f"Warning: Missing processing time for {msg_id}")
                        continue
                        
                    duration = (current_est - accounting_time).total_seconds() / 60
                    
                    if duration > 30:
                        transformed_rows = group_df[
                            group_df['marker_type'].str.contains('Transformed', case=False, na=False)
                        ] if 'marker_type' in group_df.columns else pd.DataFrame()
                        
                        if transformed_rows.empty:
                            alerts[msg_id] = create_alert(group_key, duration, 'Missing transformed marker')
                        else:
                            snapshot_type = group_key[2]
                            required_count = 6 if snapshot_type in ['EOD', 'SOD'] else 2 if snapshot_type == 'AOD' else None
                            
                            for _, row in transformed_rows.iterrows():
                                transformed_time = pd.to_datetime(row['processing_time']) if 'processing_time' in row else None
                                if transformed_time is None:
                                    continue
                                    
                                transform_duration = (transformed_time - accounting_time).total_seconds() / 60
                                
                                if (required_count is not None and 
                                    'count' in row and 
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

def create_html_alert(alerts):
    """Generate HTML table for alerts"""
    if not alerts:
        return "<p>No alerts to display</p>"
    
    html = """
    <html>
    <head>
    <style>
    body {
        font-family: Arial, sans-serif;
        margin: 20px;
    }
    h2 {
        color: #2c3e50;
    }
    .alert-table {
        border-collapse: collapse;
        width: 100%;
        margin-top: 15px;
    }
    .alert-table th {
        background-color: #3498db;
        color: white;
        text-align: left;
        padding: 10px;
    }
    .alert-table td {
        padding: 8px;
        border-bottom: 1px solid #ddd;
    }
    .alert-table tr:nth-child(even) {
        background-color: #f2f2f2;
    }
    .alert-time {
        color: #7f8c8d;
        font-size: 0.9em;
        margin-bottom: 15px;
    }
    </style>
    </head>
    <body>
    <h2>Data Processing Alerts</h2>
    <div class="alert-time">Generated at: {alert_time}</div>
    <table class="alert-table">
        <tr>
            <th>Client</th>
            <th>Region</th>
            <th>Type</th>
            <th>Business Date</th>
            <th>Message ID</th>
            <th>Duration</th>
            <th>Issue</th>
        </tr>
    """.format(alert_time=get_current_est().strftime('%Y-%m-%d %H:%M:%S %Z'))

    for alert in alerts:
        html += f"""
        <tr>
            <td>{alert['client']}</td>
            <td>{alert['region']}</td>
            <td>{alert['snapshot']}</td>
            <td>{alert['business_dt']}</td>
            <td>{alert['original_message_id']}</td>
            <td>{alert['duration']}</td>
            <td>{alert['issue']}</td>
        </tr>
        """
    
    html += """
    </table>
    </body>
    </html>
    """
    return html

def send_email(alerts):
    """Send HTML formatted email alert"""
    if not alerts:
        print("No alerts to send")
        return
    
    html_content = create_html_alert(alerts)
    
    msg = MIMEMultipart()
    msg['Subject'] = f"Data Alert - {len(alerts)} issue(s) detected"
    msg['From'] = EMAIL_CONFIG['sender_email']
    msg['To'] = ", ".join(EMAIL_CONFIG['recipient_emails'])
    
    msg.attach(MIMEText(html_content, 'html'))
    
    try:
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.send_message(msg)
        print("Alert email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

def main():
    print("=== Data Processing Alert System ===")
    print(f"Current EST: {get_current_est().strftime('%Y-%m-%d %H:%M:%S %Z')}\n")
    
    # Fetch data
    df = fetch_data()
    
    # Process data
    alerts = process_data(df)
    
    if alerts:
        print(f"\nFound {len(alerts)} alert(s):")
        print(tabulate(
            [[a['client'], a['region'], a['snapshot'], a['business_dt'], 
             a['original_message_id'], a['duration'], a['issue']] for a in alerts],
            headers=['Client', 'Region', 'Type', 'Date', 'Message ID', 'Duration', 'Issue'],
            tablefmt='grid'
        ))
        
        # Print HTML preview
        print("\nHTML Email Preview:")
        print(create_html_alert(alerts))
        
        # Uncomment to actually send email
        # send_email(alerts)
    else:
        print("No alerts generated - all conditions normal")

if __name__ == "__main__":
    main()

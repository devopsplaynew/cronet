import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import pytz
import smtplib
from email.mime.text import MIMEText
from tabulate import tabulate

# Timezone setup
EST = pytz.timezone('US/Eastern')

# Database connection configuration
DB_CONFIG = {
    'server': 'your_server_name',
    'database': 'your_database_name',
    'username': 'your_username',
    'password': 'your_password',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

# Email configuration for alerts
EMAIL_CONFIG = {
    'smtp_server': 'your_smtp_server',
    'smtp_port': 587,
    'sender_email': 'alerts@yourdomain.com',
    'sender_password': 'your_email_password',
    'recipient_emails': ['recipient1@domain.com']
}


def get_current_est():
    """Get current time in EST"""
    return datetime.now(EST)

def get_db_connection():
    """Establish database connection"""
    conn_str = f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']}"
    return pyodbc.connect(conn_str)

def fetch_data():
    """Fetch data from database using EST timestamps"""
    current_est = get_current_est()
    est_date = current_est.strftime('%Y-%m-%d')
    
    query = f"""
    SELECT 
        client_cd,
        processing_region_cd,
        snapshot_type_cd,
        business_dt,
        original_message_id,
        marker_type,
        max,
        count
    FROM your_table_name
    WHERE marker_type IN ('accounting_events', 'edRegionsubjectareaTransformed')
    AND CONVERT(date, SWITCHOFFSET(TODATETIMEOFFSET(max, DATEPART(tz, SYSDATETIMEOFFSET())), 1) = '{est_date}'
    ORDER BY client_cd, processing_region_cd, snapshot_type_cd, business_dt, original_message_id, marker_type
    """
    
    with get_db_connection() as conn:
        df = pd.read_sql(query, conn)
        if not df.empty and 'max' in df.columns:
            df['max'] = pd.to_datetime(df['max']).dt.tz_localize('UTC').dt.tz_convert(EST)
        return df

def process_data(df):
    """Process data with 30-minute threshold and deduplication"""
    alerts = {}
    current_est = get_current_est()
    
    grouped = df.groupby(['client_cd', 'processing_region_cd', 'snapshot_type_cd', 
                         'business_dt', 'original_message_id'])
    
    for group_key, group_df in grouped:
        msg_id = group_key[4]
        accounting_row = group_df[group_df['marker_type'] == 'accounting_events']
        
        if not accounting_row.empty:
            accounting_time = pd.to_datetime(accounting_row['max'].iloc[0]).tz_localize(EST)
            duration = (current_est - accounting_time).total_seconds() / 60
            
            if duration > 30:
                transformed_rows = group_df[group_df['marker_type'].str.contains('Transformed', case=False, na=False)]
                
                if transformed_rows.empty:
                    alerts[msg_id] = {
                        'client': group_key[0],
                        'region': group_key[1],
                        'snapshot': group_key[2],
                        'business_dt': group_key[3],
                        'original_message_id': msg_id,
                        'duration': f"{int(duration)}mins",
                        'issue': 'Missing transformed marker',
                        'alert_time': current_est.strftime('%Y-%m-%d %H:%M:%S %Z')
                    }
                else:
                    snapshot_type = group_key[2]
                    required_count = 6 if snapshot_type in ['EOD', 'SOD'] else 2 if snapshot_type == 'AOD' else None
                    
                    for _, row in transformed_rows.iterrows():
                        transformed_time = pd.to_datetime(row['max']).tz_localize(EST)
                        transform_duration = (transformed_time - accounting_time).total_seconds() / 60
                        
                        if required_count is not None and row['count'] != required_count and transform_duration > 30:
                            alerts[msg_id] = {
                                'client': group_key[0],
                                'region': group_key[1],
                                'snapshot': group_key[2],
                                'business_dt': group_key[3],
                                'original_message_id': msg_id,
                                'duration': f"{int(transform_duration)}mins",
                                'issue': f'Count mismatch (expected {required_count}, got {row["count"]})',
                                'alert_time': current_est.strftime('%Y-%m-%d %H:%M:%S %Z')
                            }
    
    return list(alerts.values())

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
    
    # Create DataFrame from test data
    df = pd.DataFrame(TEST_DATA)
    
    # Process data to get alerts
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
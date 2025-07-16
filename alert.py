import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import pytz
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys

# Timezone setup
EST = pytz.timezone('US/Eastern')

# Database configurations
DB_CONFIG_1 = {
    'host': 'your_db1_host',
    'database': 'your_db1_name',
    'user': 'your_username',
    'password': 'your_password',
    'port': '5432'
}

DB_CONFIG_2 = {
    'host': 'your_db2_host',
    'database': 'your_db2_name',
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

def get_current_est():
    return datetime.now(EST)

def get_db_connection(db_config):
    try:
        return psycopg2.connect(**db_config)
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        sys.exit(1)

def fetch_data(db_config, query):
    try:
        with get_db_connection(db_config) as conn:
            df = pd.read_sql_query(query, conn)
            if not df.empty and 'processing_time' in df.columns:
                df['processing_time'] = pd.to_datetime(df['processing_time']).dt.tz_localize('UTC').dt.tz_convert(EST)
            return df
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return pd.DataFrame()

def get_required_counts(snapshot_type):
    """Returns (transformed_count, martload_count) based on snapshot type"""
    if snapshot_type in ['EOD', 'SOD']:
        return (6, 5)
    elif snapshot_type == 'AOD':
        return (2, 2)
    elif snapshot_type == 'EODGL':
        return (1, 1)
    return (None, None)

def process_data(db1_df, db2_df):
    alerts = {}
    current_est = get_current_est()
    
    db1_grouped = db1_df.groupby(['client_cd', 'processing_region_cd', 'snapshot_type_cd',
                                 'business_dt', 'original_message_id'])
    
    for group_key, group_df in db1_grouped:
        msg_id = group_key[4]
        snapshot_type = group_key[2]
        accounting_row = group_df[group_df['marker_type'] == 'accounting_events']
        
        if not accounting_row.empty:
            accounting_time = pd.to_datetime(accounting_row['processing_time'].iloc[0])
            duration = (current_est - accounting_time).total_seconds() / 3600  # in hours
            
            # Get required counts for this snapshot type
            req_transformed, req_martload = get_required_counts(snapshot_type)
            
            # Check transformed marker in DB1
            transformed_rows = group_df[group_df['marker_type'].str.contains('Transformed', case=False, na=False)]
            transformed_ok = True
            transformed_issues = []
            
            if transformed_rows.empty:
                if duration > 0.5:  # 30 mins
                    transformed_ok = False
                    transformed_issues.append('Missing transformed marker')
            else:
                transform_time = pd.to_datetime(transformed_rows['processing_time'].iloc[0])
                transform_duration = (transform_time - accounting_time).total_seconds() / 60
                
                if transform_duration > 30:
                    transformed_ok = False
                    transformed_issues.append(f'Transformed late ({transform_duration:.0f} mins)')
                
                if req_transformed is not None and transformed_rows['count'].iloc[0] != req_transformed:
                    transformed_ok = False
                    transformed_issues.append(f'Transformed count mismatch ({transformed_rows["count"].iloc[0]}/{req_transformed})')
            
            # Check martload in DB2
            mart_row = db2_df[
                (db2_df['original_message_id'] == msg_id) &
                (db2_df['marker_type'] == 'martloadcomplete')
            ]
            martload_issues = []
            
            if duration > 1:  # Only check martload after 1 hour
                if mart_row.empty:
                    martload_issues.append('Missing martload completion')
                else:
                    mart_time = pd.to_datetime(mart_row['processing_time'].iloc[0])
                    mart_duration = (mart_time - accounting_time).total_seconds() / 3600
                    
                    if mart_duration > 1:
                        martload_issues.append(f'Martload late ({mart_duration:.1f}h)')
                    
                    if req_martload is not None and mart_row['count'].iloc[0] != req_martload:
                        martload_issues.append(f'Martload count mismatch ({mart_row["count"].iloc[0]}/{req_martload})')
            
            # Combine all issues
            all_issues = transformed_issues + martload_issues
            if all_issues:
                alerts[msg_id] = {
                    'client': group_key[0],
                    'region': group_key[1],
                    'snapshot': snapshot_type,
                    'business_dt': group_key[3],
                    'original_message_id': msg_id,
                    'duration': f"{duration:.1f}h",
                    'issue': ' | '.join(all_issues),
                    'alert_time': current_est.strftime('%Y-%m-%d %H:%M:%S %Z'),
                    'transformed_status': 'OK' if transformed_ok else 'ISSUE',
                    'martload_status': 'OK' if not martload_issues else 'ISSUE'
                }
    
    return list(alerts.values())

def create_html_alert(alerts):
    if not alerts:
        return "<p>No alerts to display</p>"
    
    html = """<html>
<body>
<h3>Data Processing Alerts</h3>
<p>Generated at: {alert_time}</p>
<table border="1" cellpadding="5" cellspacing="0">
    <tr>
        <th>Client</th>
        <th>Region</th>
        <th>Type</th>
        <th>Date</th>
        <th>Message ID</th>
        <th>Duration</th>
        <th>Transformed</th>
        <th>Martload</th>
        <th>Issues</th>
    </tr>""".format(alert_time=get_current_est().strftime('%Y-%m-%d %H:%M:%S %Z'))

    for alert in alerts:
        html += f"""
    <tr>
        <td>{alert['client']}</td>
        <td>{alert['region']}</td>
        <td>{alert['snapshot']}</td>
        <td>{alert['business_dt']}</td>
        <td>{alert['original_message_id']}</td>
        <td>{alert['duration']}</td>
        <td>{alert['transformed_status']}</td>
        <td>{alert['martload_status']}</td>
        <td>{alert['issue']}</td>
    </tr>"""
    
    html += """</table></body></html>"""
    return html

# [Keep send_email() and main() functions from previous version]

if __name__ == "__main__":
    main()

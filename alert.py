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

def process_data(db1_df, db2_df):
    alerts = {}
    current_est = get_current_est()
    
    db1_grouped = db1_df.groupby(['client_cd', 'processing_region_cd', 'snapshot_type_cd',
                                 'business_dt', 'original_message_id'])
    
    for group_key, group_df in db1_grouped:
        msg_id = group_key[4]
        accounting_row = group_df[group_df['marker_type'] == 'accounting_events']
        
        if not accounting_row.empty:
            accounting_time = pd.to_datetime(accounting_row['processing_time'].iloc[0])
            duration = (current_est - accounting_time).total_seconds() / 3600  # in hours
            
            # Check transformed marker in DB1
            transformed_rows = group_df[group_df['marker_type'].str.contains('Transformed', case=False, na=False)]
            transformed_ok = not transformed_rows.empty and (pd.to_datetime(transformed_rows['processing_time'].iloc[0]) - accounting_time).total_seconds() / 60 <= 30
            
            # Check martload in DB2
            mart_row = db2_df[
                (db2_df['original_message_id'] == msg_id) &
                (db2_df['marker_type'] == 'martloadcomplete')
            ]
            
            alert_reasons = []
            
            # Only check martload if accounting event is >1 hour old
            if duration > 1:
                if mart_row.empty:
                    alert_reasons.append('Missing martload completion')
                else:
                    mart_time = pd.to_datetime(mart_row['processing_time'].iloc[0])
                    mart_duration = (mart_time - accounting_time).total_seconds() / 3600
                    
                    if mart_duration > 1:
                        alert_reasons.append(f'Martload late ({mart_duration:.1f}h)')
                    if mart_row['count'].iloc[0] < 5:
                        alert_reasons.append(f'Martload count low ({mart_row["count"].iloc[0]}/5)')
            
            # Create alert if any issues found (even if transformed is OK)
            if alert_reasons:
                alerts[msg_id] = {
                    'client': group_key[0],
                    'region': group_key[1],
                    'snapshot': group_key[2],
                    'business_dt': group_key[3],
                    'original_message_id': msg_id,
                    'duration': f"{duration:.1f}h",
                    'issue': ' | '.join(alert_reasons),
                    'alert_time': current_est.strftime('%Y-%m-%d %H:%M:%S %Z'),
                    'transformed_status': 'OK' if transformed_ok else 'Missing'
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
        <td>{alert['issue']}</td>
    </tr>"""
    
    html += """</table></body></html>"""
    return html

def send_email(alerts):
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
    print("=== Combined Database Monitoring ===")
    
    # DB1 Query
    db1_query = """SELECT client_cd, processing_region_cd, snapshot_type_cd, business_dt,
                          original_message_id, marker_type, max AS processing_time, count
                   FROM your_table_name
                   WHERE marker_type IN ('accounting_events', 'edRegionsubjectareaTransformed')"""
    
    # DB2 Query
    db2_query = """SELECT client_cd, processing_region_cd, snapshot_type_cd, business_dt,
                          original_message_id, marker_type, max AS processing_time, count
                   FROM your_martload_table
                   WHERE marker_type = 'martloadcomplete'"""
    
    # Fetch data
    db1_df = fetch_data(DB_CONFIG_1, db1_query)
    db2_df = fetch_data(DB_CONFIG_2, db2_query)
    
    # Process data
    alerts = process_data(db1_df, db2_df)
    
    if alerts:
        print(f"\nFound {len(alerts)} alert(s)")
        send_email(alerts)
    else:
        print("No alerts generated")

if __name__ == "__main__":
    main()

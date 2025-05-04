import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../input/setup.env')

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Email configuration
SMTP_CONFIG = {
    'server': os.getenv('SMTP_HOST'),
    'port': int(os.getenv('SMTP_PORT')),
    'username': os.getenv('SMTP_USERNAME'),
    'password': os.getenv('SMTP_PASSWORD'),
    'from': os.getenv('ALERT_FROM_EMAIL'),
    'to': os.getenv('ALERT_TO_EMAILS').split(',')
}

def get_data():
    """Fetch data from PostgreSQL database"""
    import psycopg2
    query = """
    SELECT client_id as client, region, COUNT(*) as count
    FROM marker_status
    WHERE status = 'published'
    GROUP BY client_id, region
    ORDER BY count DESC
    """
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql(query, conn)
    return df

def create_bar_chart(df, filename='chart.png'):
    """Create enhanced bar chart visualization"""
    plt.style.use('seaborn')
    
    # Group by client and region
    pivot_df = df.pivot_table(index='client', columns='region', values='count', fill_value=0)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    pivot_df.plot(kind='bar', ax=ax, width=0.8, edgecolor='white', linewidth=0.5)
    
    # Customize chart
    ax.set_title('Published Markers by Client and Region', pad=20, fontsize=16)
    ax.set_xlabel('Client', labelpad=10)
    ax.set_ylabel('Count', labelpad=10)
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    ax.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Add value labels
    for p in ax.patches:
        ax.annotate(f"{int(p.get_height())}", 
                   (p.get_x() + p.get_width() / 2., p.get_height()),
                   ha='center', va='center', 
                   xytext=(0, 5), 
                   textcoords='offset points')
    
    plt.tight_layout()
    plt.savefig(filename, dpi=120, bbox_inches='tight')
    plt.close()
    return filename

def send_alert(high_count_df, chart_path=None):
    """Send email alert with table of high counts and chart attachment"""
    msg = MIMEMultipart()
    msg['From'] = SMTP_CONFIG['from']
    msg['To'] = ', '.join(SMTP_CONFIG['to'])
    msg['Subject'] = f"🚨 High Marker Count Alert ({len(high_count_df)} rows > 50)"
    
    # Create HTML table
    html_table = high_count_df.to_html(index=False, classes='data-table', border=1)
    
    # HTML email body
    html = f"""
    <html>
      <head>
        <style>
          body {{ font-family: Arial, sans-serif; }}
          h2 {{ color: #d9534f; }}
          .data-table {{
            border-collapse: collapse;
            width: 100%;
            margin: 15px 0;
          }}
          .data-table th, .data-table td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
          }}
          .data-table th {{
            background-color: #f2f2f2;
          }}
          .data-table tr:nth-child(even) {{
            background-color: #f9f9f9;
          }}
          .footer {{ margin-top: 20px; color: #777; }}
        </style>
      </head>
      <body>
        <h2>High Marker Count Alert</h2>
        <p>The following client/region combinations have more than 50 published markers:</p>
        {html_table}
        <div class="footer">
          <p>Generated at: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    """
    
    if chart_path:
        html += '<h3>Visualization</h3><img src="cid:chart">'
    
    html += "</body></html>"
    
    msg.attach(MIMEText(html, 'html'))
    
    # Attach chart if available
    if chart_path:
        with open(chart_path, 'rb') as img:
            chart = MIMEImage(img.read())
            chart.add_header('Content-ID', '<chart>')
            msg.attach(chart)
    
    # Send email
    with smtplib.SMTP(SMTP_CONFIG['server'], SMTP_CONFIG['port']) as server:
        server.starttls()
        server.login(SMTP_CONFIG['username'], SMTP_CONFIG['password'])
        server.send_message(msg)
    
    print(f"Alert sent for {len(high_count_df)} high-count records")

def main():
    try:
        # Get and analyze data
        df = get_data()
        print("Data retrieved successfully:")
        print(df.head())
        
        # Create visualization
        chart_path = create_bar_chart(df)
        print(f"Chart saved to {chart_path}")
        
        # Check for high counts
        high_count_df = df[df['count'] > 50]
        
        if not high_count_df.empty:
            print(f"Found {len(high_count_df)} records with count > 50")
            send_alert(high_count_df, chart_path)
        else:
            print("No records with count > 50 found")
            
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()

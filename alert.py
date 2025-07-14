def create_html_alert(alerts):
    """Generate HTML table for alerts (fixed version)"""
    if not alerts:
        return "<p>No alerts to display</p>"
    
    # Fixed HTML template (removed problematic formatting)
    html = """<html>
<head>
<style>
table {
    border-collapse: collapse;
    width: 100%;
    font-family: Arial, sans-serif;
}
th {
    background-color: #3498db;
    color: white;
    text-align: left;
    padding: 8px;
}
td {
    padding: 8px;
    border-bottom: 1px solid #ddd;
}
tr:nth-child(even) {
    background-color: #f2f2f2;
}
.alert-time {
    color: #7f8c8d;
    margin-bottom: 15px;
}
</style>
</head>
<body>
<h2>Data Processing Alerts</h2>
<div class="alert-time">Generated at: {alert_time}</div>
<table>
    <tr>
        <th>Client</th>
        <th>Region</th>
        <th>Type</th>
        <th>Date</th>
        <th>Message ID</th>
        <th>Duration</th>
        <th>Issue</th>
    </tr>""".format(alert_time=get_current_est().strftime('%Y-%m-%d %H:%M:%S %Z'))

    # Fixed alert row generation
    for alert in alerts:
        html += f"""
    <tr>
        <td>{alert.get('client', 'N/A')}</td>
        <td>{alert.get('region', 'N/A')}</td>
        <td>{alert.get('snapshot', 'N/A')}</td>
        <td>{alert.get('business_dt', 'N/A')}</td>
        <td>{alert.get('original_message_id', 'N/A')}</td>
        <td>{alert.get('duration', 'N/A')}</td>
        <td>{alert.get('issue', 'N/A')}</td>
    </tr>"""
    
    html += """
</table>
</body>
</html>"""
    return html

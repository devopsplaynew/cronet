import pandas as pd
from datetime import datetime, timedelta

# Load SLA and received CSV files
sla_file = "sla.csv"
received_file = "received.csv"

sla_df = pd.read_csv(sla_file)
received_df = pd.read_csv(received_file)

# Convert SLA and created times to datetime.time for comparison
sla_df['SLA'] = pd.to_datetime(sla_df['SLA'], format='%H:%M', errors='coerce').dt.time
received_df['created'] = pd.to_datetime(received_df['created'], format='%H:%M', errors='coerce').dt.time

# Ensure bussiness_date in received_df is datetime format
received_df['bussiness_date'] = pd.to_datetime(received_df['bussiness_date'], format='%Y-%m-%d', errors='coerce')

# Generate unique business dates, including the current date
unique_dates = received_df['bussiness_date'].dropna().unique()
current_date = datetime.now().strftime('%Y-%m-%d')
unique_dates = pd.to_datetime(list(unique_dates) + [current_date])

# Expand SLA data for all business dates
sla_expanded = sla_df.merge(
    pd.DataFrame(unique_dates, columns=['bussiness_date']),
    how='cross'
)

# Merge expanded SLA data with received data
merged_df = pd.merge(
    sla_expanded,
    received_df,
    on=['client', 'region', 'marker', 'bussiness_date'],
    how='left'
)

# Function to determine SLA status
def determine_status(row):
    sla_time = row['SLA']
    created_time = row['created']
    current_date_obj = pd.to_datetime(current_date)
    
    # No created time exists
    if pd.isnull(created_time):
        if row['bussiness_date'] == current_date_obj:  # For today's date
            current_time = datetime.now().time()
            if current_time <= sla_time:  # SLA time hasn't passed
                return "Pending"
            return "Missed"
        return "Missed"  # Historical dates without created time

    # Compare created time with SLA time
    return "Met" if created_time <= sla_time else "Missed"

# Apply SLA comparison logic
merged_df['status'] = merged_df.apply(determine_status, axis=1)

# Final output columns
output_columns = ['client', 'region', 'bussiness_date', 'marker', 'SLA', 'created', 'status']
output_df = merged_df[output_columns]

# Sort the output by business_date descending
output_df['bussiness_date'] = pd.to_datetime(output_df['bussiness_date'], format='%Y-%m-%d')
output_df = output_df.sort_values(by=['bussiness_date', 'client', 'region', 'marker'], ascending=[False, True, True, True])

# Generate HTML output with filters and status-specific column coloring
html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SLA Comparison</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f4f7fa;
        }
        h2 {
            color: #333;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-top: 20px;
        }
        th, td {
            text-align: left;
            padding: 12px 15px;
            border: 1px solid #ddd;
        }
        th {
            background-color: #4CAF50;
            color: white;
            text-align: center;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        tr:hover {
            background-color: #ddd;
        }
        .filter-input {
            width: 100%;
            padding: 5px;
            margin: 5px 0;
            box-sizing: border-box;
        }
        .status-pending {
            background-color: gold;
        }
        .status-missed {
            background-color: tomato;
            color: white;
        }
        .status-met {
            background-color: lightgreen;
        }
    </style>
    <script>
        function filterTable(columnIndex) {
            var input = document.getElementById('filter-' + columnIndex);
            var filter = input.value.toUpperCase();
            var table = document.getElementById('slaTable');
            var tr = table.getElementsByTagName('tr');
            for (var i = 1; i < tr.length; i++) {
                var td = tr[i].getElementsByTagName('td')[columnIndex];
                if (td) {
                    var txtValue = td.textContent || td.innerText;
                    tr[i].style.display = txtValue.toUpperCase().indexOf(filter) > -1 ? '' : 'none';
                }
            }
        }
    </script>
</head>
<body>
    <h2>SLA Comparison</h2>

    <table id="slaTable">
        <thead>
            <tr>
                <th>Client<br><input class="filter-input" type="text" id="filter-0" onkeyup="filterTable(0)" placeholder="Search Client"></th>
                <th>Region<br><input class="filter-input" type="text" id="filter-1" onkeyup="filterTable(1)" placeholder="Search Region"></th>
                <th>Business Date<br><input class="filter-input" type="text" id="filter-2" onkeyup="filterTable(2)" placeholder="Search Date"></th>
                <th>Marker<br><input class="filter-input" type="text" id="filter-3" onkeyup="filterTable(3)" placeholder="Search Marker"></th>
                <th>SLA<br><input class="filter-input" type="text" id="filter-4" onkeyup="filterTable(4)" placeholder="Search SLA"></th>
                <th>Created<br><input class="filter-input" type="text" id="filter-5" onkeyup="filterTable(5)" placeholder="Search Created"></th>
                <th>Status<br><input class="filter-input" type="text" id="filter-6" onkeyup="filterTable(6)" placeholder="Search Status"></th>
            </tr>
        </thead>
        <tbody>
"""

# Add rows with status-specific coloring only in the status column
for _, row in output_df.iterrows():
    status_class = f"status-{row['status'].lower()}"
    html_content += f"""
        <tr>
            <td>{row['client']}</td>
            <td>{row['region']}</td>
            <td>{row['bussiness_date'].strftime('%Y-%m-%d')}</td>
            <td>{row['marker']}</td>
            <td>{row['SLA']}</td>
            <td>{row['created'] if pd.notnull(row['created']) else ''}</td>
            <td class="{status_class}">{row['status']}</td>
        </tr>
    """

html_content += """
        </tbody>
    </table>
</body>
</html>
"""

# Save the HTML report
html_output_file = "sla_comparison.html"
with open(html_output_file, "w") as file:
    file.write(html_content)

print(f"SLA comparison report generated: {html_output_file}")

import pandas as pd
from datetime import datetime

# Load the two CSV files
sla_file = "sla.csv"
received_file = "received.csv"

sla_df = pd.read_csv(sla_file)
received_df = pd.read_csv(received_file)

# Convert SLA and created times to datetime.time for comparison
sla_df['SLA'] = pd.to_datetime(sla_df['SLA'], format='%H:%M').dt.time
received_df['created'] = pd.to_datetime(received_df['created'], format='%H:%M', errors='coerce').dt.time

# Add today's date to the SLA file
current_date = datetime.now().strftime('%Y-%m-%d')
sla_today_df = sla_df.copy()
sla_today_df['bussiness_date'] = current_date

# Merge SLA with received data for both historical and today's date
sla_df['bussiness_date'] = received_df['bussiness_date']  # Historical SLA rows
merged_df = pd.merge(
    pd.concat([sla_df, sla_today_df]),
    received_df,
    on=['client', 'region', 'marker', 'bussiness_date'],
    how='left'
)

# Function to determine SLA status
def determine_status(row):
    sla_time = datetime.strptime(str(row['SLA']), "%H:%M:%S").time()
    
    # If no `created` time exists
    if pd.isnull(row['created']):
        current_time = datetime.now().time()
        # Today's date - mark as Pending
        if row['bussiness_date'] == current_date and current_time <= sla_time:
            return "Pending"
        # Otherwise, SLA is breached
        return "Missed"
    # Compare created and SLA times
    return "Met" if row['created'] <= sla_time else "Missed"

# Apply status determination
merged_df['status'] = merged_df.apply(determine_status, axis=1)

# Final Output Columns
output_columns = ['client', 'region', 'bussiness_date', 'marker', 'SLA', 'created', 'status']
output_df = merged_df[output_columns]

# Sort by business_date descending
output_df['bussiness_date'] = pd.to_datetime(output_df['bussiness_date'], format='%Y-%m-%d')
output_df = output_df.sort_values(by='bussiness_date', ascending=False)

# Create HTML with filters, sorting, and improved styling
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

# Add table rows from the data with color formatting for the status column
for _, row in output_df.iterrows():
    status = row['status'].lower()
    row_class = 'status-pending' if status == 'pending' else 'status-missed' if status == 'missed' else 'status-met'
    html_content += f"""
            <tr>
                <td>{row['client']}</td>
                <td>{row['region']}</td>
                <td>{row['bussiness_date']}</td>
                <td>{row['marker']}</td>
                <td>{row['SLA']}</td>
                <td>{row['created']}</td>
                <td class="{row_class}">{status.capitalize()}</td>
            </tr>
    """

html_content += """
        </tbody>
    </table>
</body>
</html>
"""

# Save the final HTML file
html_output_file = "sla_comparison.html"
with open(html_output_file, "w") as file:
    file.write(html_content)

print(f"Formatted HTML file with filters has been created: {html_output_file}")

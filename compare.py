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
sla_expanded = sla_df.assign(key=1).merge(
    pd.DataFrame(unique_dates, columns=['bussiness_date']).assign(key=1),
    on='key'
).drop('key', axis=1)

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
            time_diff = datetime.combine(current_date_obj, sla_time) - datetime.now()
            if current_time <= sla_time:
                if time_diff.total_seconds() <= 300:  # 5 minutes in seconds
                    return "Warning"
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

# Generate HTML output with filters, sorting, and status-specific column coloring
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
            cursor: pointer;
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
        .status-warning {
            background-color: orange;
            color: white;
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

        function sortTable(columnIndex) {
            var table = document.getElementById("slaTable");
            var rows = Array.from(table.getElementsByTagName("tr")).slice(1); // Exclude header row
            var isAscending = table.getAttribute("data-sort-order") === "asc";
            rows.sort(function(rowA, rowB) {
                var cellA = rowA.getElementsByTagName("td")[columnIndex].innerText.toLowerCase();
                var cellB = rowB.getElementsByTagName("td")[columnIndex].innerText.toLowerCase();
                if (!isNaN(Date.parse(cellA)) && !isNaN(Date.parse(cellB))) { // For dates
                    return isAscending
                        ? new Date(cellA) - new Date(cellB)
                        : new Date(cellB) - new Date(cellA);
                }
                return isAscending
                    ? cellA.localeCompare(cellB)
                    : cellB.localeCompare(cellA);
            });
            rows.forEach(row => table.appendChild(row)); // Re-append rows in sorted order
            table.setAttribute("data-sort-order", isAscending ? "desc" : "asc");
        }

        function checkForWarnings() {
            var table = document.getElementById('slaTable');
            var rows = table.getElementsByTagName('tr');
            for (var i = 1; i < rows.length; i++) {
                var statusCell = rows[i].getElementsByTagName('td')[6];
                if (statusCell && statusCell.innerText === "Warning") {
                    alert("Warning: SLA is about to breach!");
                    break;
                }
            }
        }

        // Call the function on page load
        window.onload = checkForWarnings;
    </script>
</head>
<body>
    <h2>SLA Comparison</h2>

    <table id="slaTable">
        <thead>
            <tr>
                <th onclick="sortTable(0)">Client<br><input class="filter-input" type="text" id="filter-0" onkeyup="filterTable(0)" placeholder="Search Client"></th>
                <th onclick="sortTable(1)">Region<br><input class="filter-input" type="text" id="filter-1" onkeyup="filterTable(1)" placeholder="Search Region"></th>
                <th onclick="sortTable(2)">Business Date<br><input class="filter-input" type="text" id="filter-2" onkeyup="filterTable(2)" placeholder="Search Date"></th>
                <th onclick="sortTable(3)">Marker<br><input class="filter-input" type="text" id="filter-3" onkeyup="filterTable(3)" placeholder="Search Marker"></th>
                <th onclick="sortTable(4)">SLA<br><input class="filter-input" type="text" id="filter-4" onkeyup="filterTable(4)" placeholder="Search SLA"></th>
                <th onclick="sortTable(5)">Created<br><input class="filter-input" type="text" id="filter-5" onkeyup="filterTable(5)" placeholder="Search Created"></th>
                <th onclick="sortTable(6)">Status<br><input class="filter-input" type="text" id="filter-6" onkeyup="filterTable(6)" placeholder="Search Status"></th>
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

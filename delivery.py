import csv
# Function to generate HTML table with status highlights and filters
def generate_html(input_file, output_file, html_file):
    # Read the output file into a dictionary for quick lookup
    output_data = {}
    with open(output_file, 'r') as output_csv:
        output_reader = csv.DictReader(output_csv)
        for row in output_reader:
            key = (row['client_cd'], row['region_cd'], row['subject_Area'], row['snapshot'])
            output_data[key] = {'status': row['status'], 'max': row['max'], 'count': row['count']}
    
    # Start HTML content with improved CSS and filters
    html_content = '''
    <html>
    <head>
        <style>
            body {
                font-family: Arial, sans-serif;
                background-color: #f4f4f9;
                color: #333;
            }
            table {
                width: 80%;
                margin: 20px auto;
                border-collapse: collapse;
                box-shadow: 0 5px 10px rgba(0, 0, 0, 0.15);
                background-color: white;
            }
            th, td {
                padding: 15px;
                text-align: center;
                border-bottom: 1px solid #ddd;
            }
            th {
                background-color: #4CAF50;
                color: white;
                text-transform: uppercase;
                font-size: 14px;
            }
            tr:nth-child(even) {
                background-color: #f2f2f2;
            }
            .success {
                background-color: #28a745;
                color: white;
                font-weight: bold;
            }
            .pending {
                background-color: #ffc107;
                color: black;
                font-weight: bold;
            }
            .failed {
                background-color: #dc3545;
                color: white;
                font-weight: bold;
            }
            tr:hover {
                background-color: #f1f1f1;
            }
            .filter-input {
                width: 95%;
                padding: 5px;
                margin-bottom: 5px;
                text-align: center;
                border: 1px solid #ccc;
                border-radius: 3px;
            }
        </style>
        <script>
            function filterTable(columnIndex) {
                var input, filter, table, tr, td, i, txtValue;
                input = document.getElementsByClassName("filter-input")[columnIndex];
                filter = input.value.toUpperCase();
                table = document.getElementById("statusTable");
                tr = table.getElementsByTagName("tr");

                for (i = 1; i < tr.length; i++) {
                    td = tr[i].getElementsByTagName("td")[columnIndex];
                    if (td) {
                        txtValue = td.textContent || td.innerText;
                        if (txtValue.toUpperCase().indexOf(filter) > -1) {
                            tr[i].style.display = "";
                        } else {
                            tr[i].style.display = "none";
                        }
                    }
                }
            }
        </script>
    </head>
    <body>
        <h2 style="text-align:center;">Workflow Status Report</h2>

        <table id="statusTable">
            <tr>
                <th>Client Code<br><input type="text" class="filter-input" onkeyup="filterTable(0)" placeholder="Filter by Client Code"></th>
                <th>Region Code<br><input type="text" class="filter-input" onkeyup="filterTable(1)" placeholder="Filter by Region Code"></th>
                <th>Subject Area<br><input type="text" class="filter-input" onkeyup="filterTable(2)" placeholder="Filter by Subject Area"></th>
                <th>Snapshot<br><input type="text" class="filter-input" onkeyup="filterTable(3)" placeholder="Filter by Snapshot"></th>
                <th>Status<br><input type="text" class="filter-input" onkeyup="filterTable(4)" placeholder="Filter by Status"></th>
                <th>Max<br><input type="text" class="filter-input" onkeyup="filterTable(5)" placeholder="Filter by Max"></th>
                <th>Count<br><input type="text" class="filter-input" onkeyup="filterTable(6)" placeholder="Filter by Count"></th>
            </tr>
    '''

    # Process the input file and ensure all rows are shown, even if not in the output
    with open(input_file, 'r') as input_csv:
        input_reader = csv.DictReader(input_csv)
        for row in input_reader:
            key = (row['client_cd'], row['region_cd'], row['subject_Area'], row['snapshot'])
            output_row = output_data.get(key, {'status': 'pending', 'max': '-', 'count': '-'})  # Default to pending if not found

            # Determine the status class for coloring
            if output_row['status'] == 'success':
                status_class = 'success'
            elif output_row['status'] == 'failed':
                status_class = 'failed'
            else:
                status_class = 'pending'

            # Add row to HTML table
            html_content += f'''
            <tr>
                <td>{row['client_cd']}</td>
                <td>{row['region_cd']}</td>
                <td>{row['subject_Area']}</td>
                <td>{row['snapshot']}</td>
                <td class="{status_class}">{output_row['status']}</td>
                <td>{output_row['max']}</td>
                <td>{output_row['count']}</td>
            </tr>
            '''

    # End HTML content
    html_content += '''
        </table>
    </body>
    </html>
    '''

    # Write HTML content to file
    with open(html_file, 'w') as file:
        file.write(html_content)

# Call the function with your file paths
input_file = 'input.csv'
output_file = 'output.csv'
html_file = 'status_table.html'

generate_html(input_file, output_file, html_file)

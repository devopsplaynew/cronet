# Function to generate HTML table with status highlights
import csv
def generate_html(input_file, output_file, html_file):
    # Read the output file into a dictionary for quick lookup
    output_data = {}
    with open(output_file, 'r') as output_csv:
        output_reader = csv.DictReader(output_csv)
        for row in output_reader:
            key = (row['client_cd'], row['region_cd'], row['subject_Area'], row['snapshot'])
            output_data[key] = row['status']
    
    # Start HTML content with improved CSS
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
        </style>
    </head>
    <body>
        <h2 style="text-align:center;">Workflow Status Report</h2>
        <table>
            <tr>
                <th>Client Code</th>
                <th>Region Code</th>
                <th>Subject Area</th>
                <th>Snapshot</th>
                <th>Status</th>
            </tr>
    '''

    # Process the input file and compare with the output
    with open(input_file, 'r') as input_csv:
        input_reader = csv.DictReader(input_csv)
        for row in input_reader:
            key = (row['client_cd'], row['region_cd'], row['subject_Area'], row['snapshot'])
            status = output_data.get(key, 'pending')  # Default to pending if not found

            # Determine the status class for coloring
            if status == 'success':
                status_class = 'success'
            elif status == 'failed':
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
                <td class="{status_class}">{status}</td>
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

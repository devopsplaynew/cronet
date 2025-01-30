import smtplib
import csv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Read CSV file and generate HTML table dynamically
csv_file = "data.csv"  # Update with your actual CSV file name

html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: Arial, sans-serif;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            background: #ffffff;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
            border-radius: 8px;
            overflow: hidden;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }
        th {
            background-color: #7d87a0;
            color: white;
            font-size: 16px;
        }
        td {
            font-size: 14px;
        }
        .error {
            background-color: tomato;
            color: white;
            font-weight: bold;
        }
    </style>
</head>
<body>
    <h2>CSV Data Table</h2>
    <table>
        <tr>
"""

with open(csv_file, newline='') as file:
    reader = csv.reader(file)
    headers = next(reader)
    for header in headers:
        html_content += f"            <th>{header}</th>\n"
    html_content += "        </tr>\n"

    for row in reader:
        html_content += "        <tr>\n"
        for i, cell in enumerate(row):
            if i == len(headers) - 1:  # Highlight the last column (error_detailed_description)
                html_content += f"            <td class='error'>{cell}</td>\n"
            else:
                html_content += f"            <td>{cell}</td>\n"
        html_content += "        </tr>\n"

html_content += """    </table>
</body>
</html>
"""
print(html_content)
# Email configuration
sender_email = "praveenangel53@gmail.com"
receiver_email = "mailforkanishya@example.com"
smtp_server = "smtp.gmail.com"
smtp_port = 587
smtp_username = "praveenangel53@gmail.com"
smtp_password = "kanishya@123"  # Use the generated App Password

# Create email
msg = MIMEMultipart()
msg["From"] = sender_email
msg["To"] = receiver_email
msg["Subject"] = "Test Email from Python"
msg.attach(MIMEText("<h1>Hello, this is a test email!</h1>", "html"))

# Send email
try:
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(sender_email, receiver_email, msg.as_string())
    server.quit()
    print("Email sent successfully!")
except Exception as e:
    print(f"Failed to send email: {e}")
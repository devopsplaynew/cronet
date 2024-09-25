import openpyxl
from collections import defaultdict

# Mapping known color codes for red, green, and purple
color_mapping = {
    "FFFF0000": "Red",    # Hex color code for red
    "FF00FF00": "Green",  # Hex color code for green
    "FF800080": "Purple"   # Hex color code for purple
}

def get_color_and_row_data(file_path):
    # Load the Excel workbook
    wb = openpyxl.load_workbook(file_path)
    sheet = wb.active

    # Store the results as a dictionary where key is column name and color, and value is a set of affected row IDs and values
    color_rows = defaultdict(lambda: defaultdict(list))  # Use list to store both row IDs and values

    # Get the column headers from the first row
    headers = [cell.value for cell in sheet[1]]  # Assume headers are in the first row

    # Iterate through each cell starting from the second row (data rows)
    for row in sheet.iter_rows(min_row=2):
        mellon_id = row[0].value  # Assume the first column is "mellonid"
        for col_index, cell in enumerate(row[1:], start=1):  # Skip "mellonid" (first column)
            fill = cell.fill
            value = cell.value
            if fill and fill.fgColor and fill.fgColor.rgb:
                color_code = fill.fgColor.rgb
                color_name = color_mapping.get(color_code, None)
                
                if color_name:
                    column_name = headers[col_index]  # Get the corresponding header name
                    # Add the row ID and value (non-null value) to the color-specific list
                    color_rows[color_name][column_name].append((mellon_id, value))

    return color_rows

def generate_custom_messages(color_rows):
    messages = []

    # Iterate over each color and column, generating the custom messages
    for color, columns in color_rows.items():
        for column, mellon_data in columns.items():
            # Extract mellon_ids and corresponding values
            mellon_ids = [str(data[0]) for data in mellon_data]  # Convert mellon_id to string
            values = [data[1] for data in mellon_data]  # Store all values including nulls
            value_count = defaultdict(int)
            null_mellon_ids = set()

            for mellon_id, value in mellon_data:
                if value is None:
                    null_mellon_ids.add(str(mellon_id))  # Convert to string
                value_count[value] += 1

            # Count total nulls
            null_count = len(null_mellon_ids)

            # Prepare output for null values
            if null_count > 0:
                unique_null_mellon_ids = sorted(null_mellon_ids)
                mellon_ids_list = ','.join(unique_null_mellon_ids)
                messages.append(f"{null_count} {column} Null for mellon id {mellon_ids_list} (because {color.lower()} color)")

            # Prepare output for non-null values
            for value, count in value_count.items():
                if value is not None:  # Skip the null value case since it's already handled
                    unique_mellon_ids = sorted({mellon_ids[i] for i in range(len(values)) if values[i] == value})
                    mellon_ids_list = ','.join(unique_mellon_ids)
                    messages.append(f"{count} {column} {value} for mellon id {mellon_ids_list} (because {color.lower()} color)")

    return messages

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python count_cell_colors.py <file.xlsx>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    color_rows = get_color_and_row_data(file_path)

    # Generate custom error messages
    messages = generate_custom_messages(color_rows)

    # Print the messages
    for message in messages:
        print(message)

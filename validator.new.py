import json
import re

# Sample JSON configuration
json_data_str = '''
{
    "fields": {
        "FIELD1": {
            "size": 500,
            "type": "alpha_numeric",
            "required": true
        },
        "FIELD2": {
            "size": 100,
            "type": "alpha_numeric",
            "required": true,
            "allowed_values": ["ValidText", "XYZ789"]
        },
        "FIELD_DEC1": {
            "size": 21,
            "size_before_decimal": 10,
            "size_after_decimal": 10,
            "type": "decimal",
            "required": false,
            "range": [0, 1000]
        },
        "FIELD_NUMERIC1": {
            "size": 21,
            "type": "numeric",
            "required": false,
            "zero_check": true
        }
    },
    "duplicate_field_check": true,
    "columns": ["FIELD1", "FIELD2", "FIELD_DEC1", "FIELD_NUMERIC1"]
}
'''

# Sample table data (CSV-like format)
table_data_str = """
FIELD1,FIELD2,FIELD_DEC1,FIELD_NUMERIC1
ABC123,XYZ789,123.45,9876543210
ABC123,XYZ789,123.45,9876543210
LONG_TEXT_EXCEEDING_LIMIT,ValidText,12.34,5678
,,456.78,90
Value123,InvalidValue,1500.99,0
"""

# Parse JSON configuration
json_data = json.loads(json_data_str)
field_definitions = json_data["fields"]
columns = json_data["columns"]

# Parse table data
table_lines = table_data_str.strip().split("\n")
headers = table_lines[0].split(",")

# Ensure columns match JSON definition
if set(headers) != set(columns):
    raise ValueError("Table columns do not match JSON column definitions.")

# Convert table into list of dictionaries
table_records = [dict(zip(headers, row.split(","))) for row in table_lines[1:]]

# Validation functions
def is_alpha_numeric(value):
    return bool(re.match(r'^[a-zA-Z0-9]*$', value))

def is_numeric(value):
    return bool(re.match(r'^\d+$', value))

def is_decimal(value, before_decimal, after_decimal):
    match = re.match(r'^(\d+)(\.\d+)?$', value)
    if match:
        int_part, dec_part = match.group(1), match.group(2)
        return len(int_part) <= before_decimal and (len(dec_part[1:]) if dec_part else 0) <= after_decimal
    return False

def validate_record(record, row_idx):
    errors = []
    for field, value in record.items():
        field_spec = field_definitions.get(field)
        if not field_spec:
            errors.append(f"Row {row_idx}: Unexpected field '{field}'")
            continue
        
        value = value.strip()  # Trim spaces

        # Required field check
        if field_spec["required"] and not value:
            errors.append(f"Row {row_idx}: '{field}' is required but missing.")

        # Size constraint check
        if len(value) > field_spec["size"]:
            errors.append(f"Row {row_idx}: '{field}' exceeds max size {field_spec['size']}.")

        # Type validation
        if field_spec["type"] == "alpha_numeric" and value and not is_alpha_numeric(value):
            errors.append(f"Row {row_idx}: '{field}' should be alphanumeric.")
        elif field_spec["type"] == "numeric" and value and not is_numeric(value):
            errors.append(f"Row {row_idx}: '{field}' should be numeric.")
        elif field_spec["type"] == "decimal" and value:
            before_decimal = field_spec.get("size_before_decimal", 10)
            after_decimal = field_spec.get("size_after_decimal", 10)
            if not is_decimal(value, before_decimal, after_decimal):
                errors.append(f"Row {row_idx}: '{field}' should be decimal with {before_decimal} digits before and {after_decimal} digits after decimal.")

        # Range check for numeric and decimal fields
        if "range" in field_spec and value:
            try:
                num_value = float(value)
                min_val, max_val = field_spec["range"]
                if not (min_val <= num_value <= max_val):
                    errors.append(f"Row {row_idx}: '{field}' value {num_value} out of range {min_val}-{max_val}.")
            except ValueError:
                errors.append(f"Row {row_idx}: '{field}' should be a number for range check.")

        # Zero check
        if field_spec.get("zero_check", False) and value == "0":
            errors.append(f"Row {row_idx}: '{field}' should not be zero.")

        # Allowed values check
        if "allowed_values" in field_spec and value and value not in field_spec["allowed_values"]:
            errors.append(f"Row {row_idx}: '{field}' has an invalid value '{value}'. Allowed values: {field_spec['allowed_values']}.")

    return errors

def check_duplicates(records):
    seen = set()
    duplicates = []
    for i, record in enumerate(records, start=1):
        record_tuple = tuple(record.items())
        if record_tuple in seen:
            duplicates.append(f"Row {i} is a duplicate.")
        seen.add(record_tuple)
    return duplicates

def main():
    validation_errors = []

    # Validate each record
    for row_idx, row in enumerate(table_records, start=1):
        validation_errors.extend(validate_record(row, row_idx))

    # Check for duplicates if enabled
    if json_data.get("duplicate_field_check", False):
        validation_errors.extend(check_duplicates(table_records))

    # Print validation results
    if validation_errors:
        print("Validation Errors:")
        for error in validation_errors:
            print(error)
    else:
        print("Table data is valid according to JSON specifications.")

# Run main function
if __name__ == "__main__":
    main()

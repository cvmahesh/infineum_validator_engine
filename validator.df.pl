import json
import pandas as pd
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

# Convert JSON to dictionary
json_data = json.loads(json_data_str)
field_definitions = json_data["fields"]
columns = json_data["columns"]

# Convert table data string to Pandas DataFrame
from io import StringIO
df = pd.read_csv(StringIO(table_data_str))

# Ensure DataFrame columns match JSON column definitions
if set(df.columns) != set(columns):
    raise ValueError("Table columns do not match JSON column definitions.")

# Validation function
def validate_dataframe(df):
    errors = []

    # Required field check
    for field, spec in field_definitions.items():
        if spec["required"]:
            missing_rows = df[df[field].isna()].index.tolist()
            for row in missing_rows:
                errors.append(f"Row {row + 1}: '{field}' is required but missing.")

    # Size check
    for field, spec in field_definitions.items():
        df[field] = df[field].astype(str).str.strip()  # Ensure string format
        oversized_rows = df[df[field].str.len() > spec["size"]].index.tolist()
        for row in oversized_rows:
            errors.append(f"Row {row + 1}: '{field}' exceeds max size {spec['size']}.")

    # Type validation
    def is_alpha_numeric(value):
        return bool(re.match(r'^[a-zA-Z0-9]*$', str(value))) if value else True

    def is_numeric(value):
        return str(value).isdigit() if value else True

    def is_decimal(value, before_decimal, after_decimal):
        match = re.match(r'^(\d+)(\.\d+)?$', str(value))
        if match:
            int_part, dec_part = match.group(1), match.group(2)
            return len(int_part) <= before_decimal and (len(dec_part[1:]) if dec_part else 0) <= after_decimal
        return False

    for field, spec in field_definitions.items():
        if spec["type"] == "alpha_numeric":
            invalid_rows = df[~df[field].apply(is_alpha_numeric)].index.tolist()
            for row in invalid_rows:
                errors.append(f"Row {row + 1}: '{field}' should be alphanumeric.")
        elif spec["type"] == "numeric":
            invalid_rows = df[~df[field].apply(is_numeric)].index.tolist()
            for row in invalid_rows:
                errors.append(f"Row {row + 1}: '{field}' should be numeric.")
        elif spec["type"] == "decimal":
            before_decimal = spec.get("size_before_decimal", 10)
            after_decimal = spec.get("size_after_decimal", 10)
            invalid_rows = df[~df[field].apply(lambda x: is_decimal(x, before_decimal, after_decimal))].index.tolist()
            for row in invalid_rows:
                errors.append(f"Row {row + 1}: '{field}' should be decimal with {before_decimal} digits before and {after_decimal} digits after decimal.")

    # Range check
    for field, spec in field_definitions.items():
        if "range" in spec:
            min_val, max_val = spec["range"]
            out_of_range_rows = df[(df[field].astype(float) < min_val) | (df[field].astype(float) > max_val)].index.tolist()
            for row in out_of_range_rows:
                errors.append(f"Row {row + 1}: '{field}' value out of range {min_val}-{max_val}.")

    # Zero check
    for field, spec in field_definitions.items():
        if spec.get("zero_check", False):
            zero_rows = df[df[field] == "0"].index.tolist()
            for row in zero_rows:
                errors.append(f"Row {row + 1}: '{field}' should not be zero.")

    # Allowed values check
    for field, spec in field_definitions.items():
        if "allowed_values" in spec:
            invalid_rows = df[~df[field].isin(spec["allowed_values"])].index.tolist()
            for row in invalid_rows:
                errors.append(f"Row {row + 1}: '{field}' has an invalid value '{df.at[row, field]}'. Allowed values: {spec['allowed_values']}.")

    # Duplicate check
    if json_data.get("duplicate_field_check", False):
        duplicates = df[df.duplicated()].index.tolist()
        for row in duplicates:
            errors.append(f"Row {row + 1} is a duplicate.")

    return errors

# Main function
def main():
    validation_errors = validate_dataframe(df)

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

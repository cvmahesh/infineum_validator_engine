import json
import csv
from io import StringIO

# Sample JSON configuration
json_data_str = '''{
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
}'''

# Sample table data (CSV-like format)
# table_data_str = """FIELD1,FIELD2,FIELD_DEC1,FIELD_NUMERIC1
# ABC123,XYZ789,123.45,9876543210
# ABC123,XYZ789,123.45,9876543210
# LONG_TEXT_EXCEEDING_LIMIT,ValidText,12.34,5678
# ,,456.78,90
# Value123,InvalidValue,1500.99,0
# """

# WRONG DATA
table_data_str = """FIELD1,FIELD2,FIELD_DEC1,FIELD_NUMERIC1, EXTRA_FLD
ABC123,XYZ789,123.45,9876543210,0
ABC123,XYZ789,123.45,9876543210,0
LONG_TEXT_EXCEEDING_LIMIT,ValidText,12.34,5678,0
,,456.78,90,0
Value123,InvalidValue,1500.99,0,0
"""

def validate_columns(json_str, table_str):
    """Validates if all required columns from JSON config exist in the table data"""
    json_data = json.loads(json_str)
    required_columns = set(json_data.get("columns", []))
    
    table_reader = csv.reader(StringIO(table_str))
    table_columns = next(table_reader)  # First row is headers
    
    missing_columns = required_columns - set(table_columns)
    extra_columns = set(table_columns) - required_columns
    
    if missing_columns:
        print(f"❌ Missing Columns: {missing_columns}")
    else:
        print("✅ All required columns are present.")

    if extra_columns:
        print(f"⚠️ Extra Columns Found: {extra_columns}")
    
    return {
        "missing_columns": list(missing_columns),
        "extra_columns": list(extra_columns),
        "is_valid": not missing_columns
    }, table_columns, list(table_reader)


def check_duplicates(table_data):
    """Checks for duplicate rows in table data"""
    seen = set()
    duplicates = []
    
    for row in table_data:
        row_tuple = tuple(row)  # Convert list to tuple (hashable)
        if row_tuple in seen:
            duplicates.append(row)
        else:
            seen.add(row_tuple)

    if duplicates:
        print(f"❌ Found {len(duplicates)} duplicate rows.")
    else:
        print("✅ No duplicate rows found.")
    
    return duplicates


def check_size_constraints(json_str, table_columns, table_data):
    """Checks if column values meet the size constraints"""
    json_data = json.loads(json_str)
    fields = json_data["fields"]
    column_index_map = {col: idx for idx, col in enumerate(table_columns)}
    
    size_violations = []

    for row_num, row in enumerate(table_data, start=1):
        for column, config in fields.items():
            if column in column_index_map:
                col_index = column_index_map[column]
                value = row[col_index].strip() if col_index < len(row) else ""
                
                # Check size constraint
                max_size = config.get("size", None)
                if max_size and len(value) > max_size:
                    size_violations.append((row_num, column, value, max_size))

    if size_violations:
        print(f"❌ Found {len(size_violations)} size constraint violations.")
        for row_num, column, value, max_size in size_violations:
            print(f"   Row {row_num}: {column} exceeds {max_size} chars ({len(value)} chars)")
    else:
        print("✅ No size constraint violations found.")
    
    return size_violations


# Run validation
column_result, table_columns, table_data = validate_columns(json_data_str, table_data_str)

if column_result["is_valid"]:
    duplicates = check_duplicates(table_data)
    size_issues = check_size_constraints(json_data_str, table_columns, table_data)
    
    print("\nValidation Summary:")
    print(f"✅ Columns Valid: {column_result['is_valid']}")
    print(f"❌ Duplicate Rows: {len(duplicates)}")
    print(f"❌ Size Violations: {len(size_issues)}")
else:
    print("❌ Column validation failed. Stopping further checks.")

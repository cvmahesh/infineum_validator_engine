import json
import csv
import re
import sys
import hashlib

# Sample JSON configuration
json_data_str = '''{
    "fields": {
        "FIELD1": {"size": 500, "type": "alpha_numeric", "required": true, "unique": true},
        "FIELD2": {"size": 100, "type": "alpha_numeric", "required": true, "allowed_values": ["ValidText", "XYZ789"], "unique": true},
        "FIELD_DEC1": {"size": 21, "size_before_decimal": 10, "size_after_decimal": 10, "type": "decimal", "required": false, "range": [0, 1000]},
        "FIELD_NUMERIC1": {"size": 21, "type": "numeric", "required": false, "zero_check": true}
    },
    "duplicate_field_check": true,
    "columns": ["FIELD1", "FIELD2", "FIELD_DEC1", "FIELD_NUMERIC1"]
}'''

# Sample table data (CSV-like format) with errors
table_data_str = """FIELD1,FIELD2,FIELD_DEC1,FIELD_NUMERIC1
ABC123,XYZ789,123.45,9876543210
ABC123,XYZ789,123.45,9876543210
LONG_TEXT_EXCEEDING_LIMIT,ValidText,12.34,5678
,,456.78,90
Value123,InvalidValue,1500.99,0
"""

# Global variables
validation_report = []
validation_errors = []

# Precompile regex patterns for better performance
patterns = {
    "alpha_numeric": re.compile(r"^[a-zA-Z0-9]*$"),
    "numeric": re.compile(r"^\d+$"),
    "decimal": re.compile(r"^\d+\.\d+$"),
}

# Parse JSON once and store data
json_data = json.loads(json_data_str)
fields = json_data["fields"]
required_columns = set(json_data["columns"])

# Logging functions
def log(message):
    """Appends validation messages to the global validation_report"""
    validation_report.append(message)

def log_error(message):
    """Logs an error and adds it to the validation error list."""
    validation_errors.append(message)

# Read CSV into a dictionary format
def parse_csv(table_str):
    reader = csv.DictReader(table_str.strip().split("\n"))
    return list(reader)

# Validate columns
def validate_columns(table_data):
    table_columns = set(table_data[0].keys())
    
    missing_columns = required_columns - table_columns
    extra_columns = table_columns - required_columns
    
    if missing_columns:
        log(f"Missing Columns: {missing_columns}")
        log_error(f"Missing Columns: {missing_columns}")
    
    if extra_columns:
        log(f"Extra Columns Found: {extra_columns}")

    return not missing_columns

# Check for duplicate rows
def check_duplicates(table_data):
    seen = set()
    duplicates = []
    
    for row in table_data:
        row_tuple = tuple(row.values())  # Convert dict values to tuple
        if row_tuple in seen:
            duplicates.append(row)
        else:
            seen.add(row_tuple)

    if duplicates:
        log(f"Found {len(duplicates)} duplicate rows.")
        log_error(f"Duplicate Rows Found: {len(duplicates)}")

    return duplicates

# Check field size constraints
def check_size_constraints(table_data):
    violations = []

    for i, row in enumerate(table_data, start=1):
        for column, config in fields.items():
            if column in row:
                value = row[column]
                max_size = config.get("size")
                if max_size and len(value) > max_size:
                    violations.append((i, column, len(value), max_size))

    if violations:
        log(f"Size violations found: {len(violations)}")
        log_error(f"Size Violations: {len(violations)}")

    return violations

# Check required fields
def check_required_fields(table_data):
    missing_values = []

    for i, row in enumerate(table_data, start=1):
        for column, config in fields.items():
            if config.get("required") and column in row and not row[column].strip():
                missing_values.append((i, column))

    if missing_values:
        log(f"Missing required fields: {len(missing_values)}")
        log_error(f"Missing Required Fields: {len(missing_values)}")

    return missing_values

# Check numeric and decimal fields
def check_numeric_and_decimal(table_data):
    violations = []

    for i, row in enumerate(table_data, start=1):
        for column, config in fields.items():
            value = row.get(column, "").strip()

            if not value:
                continue

            field_type = config["type"]
            if field_type in patterns and not patterns[field_type].match(value):
                violations.append((i, column, value, f"Invalid {field_type}"))

            if field_type == "decimal":
                try:
                    float_value = float(value)
                    range_values = config.get("range")
                    if range_values and not (range_values[0] <= float_value <= range_values[1]):
                        violations.append((i, column, value, "Out of range"))
                except ValueError:
                    violations.append((i, column, value, "Invalid decimal"))

    if violations:
        log(f"Numeric/Decimal violations found: {len(violations)}")
        log_error(f"Numeric/Decimal Violations: {len(violations)}")

    return violations

# Check for unique field violations
def check_unique_fields(table_data):
    unique_columns = [col for col, config in fields.items() if config.get("unique")]
    unique_combinations = set()
    duplicate_entries = []

    for i, row in enumerate(table_data, start=1):
        unique_values = tuple(row[col].strip() for col in unique_columns if col in row)
        unique_hash = hashlib.md5(str(unique_values).encode()).hexdigest()

        if unique_hash in unique_combinations:
            duplicate_entries.append((i, unique_values))
        else:
            unique_combinations.add(unique_hash)

    if duplicate_entries:
        log(f"Unique field violations found: {len(duplicate_entries)}")
        log_error(f"Unique Field Violations: {len(duplicate_entries)}")

    return duplicate_entries

# Run validations


table_data = parse_csv(table_data_str)

if validate_columns(table_data):
    duplicates = check_duplicates(table_data)
    size_issues = check_size_constraints(table_data)
    missing_values = check_required_fields(table_data)
    numeric_issues = check_numeric_and_decimal(table_data)
    unique_field_issues = check_unique_fields(table_data)

    log("\nValidation Summary:")
    log(f"Duplicate Rows: {len(duplicates)}")
    log(f"Size Violations: {len(size_issues)}")
    log(f"Missing Required Fields: {len(missing_values)}")
    log(f"Numeric/Decimal Violations: {len(numeric_issues)}")
    log(f"Unique Field Violations: {len(unique_field_issues)}")

    if validation_errors:
        log("\nValidation failed. Exiting with status 1.")
        sys.exit(1)
    else:
        log("\nValidation successful. Exiting with status 0.")
        sys.exit(0)
else:
    log("Column validation failed. Stopping further checks.")
    sys.exit(1)

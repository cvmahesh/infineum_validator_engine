import json
import csv
import re
import sys

# Sample JSON configuration
json_data_str = '''{
    "fields": {
        "FIELD1": {
            "size": 500,
            "type": "alpha_numeric",
            "required": true,
            "unique": true
        },
        "FIELD2": {
            "size": 100,
            "type": "alpha_numeric",
            "required": true,
            "allowed_values": ["ValidText", "XYZ789"],
            "unique": true
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

# Sample table data (CSV-like format) with errors
table_data_str = """FIELD1,FIELD_DEC1,FIELD2,FIELD_NUMERIC1
ABC123,XYZ789,123.45,9876543210
ABC123,XYZ789,123.45,9876543210
LONG_TEXT_EXCEEDING_LIMIT,ValidText,12.34,5678
,,456.78,90
Value123,InvalidValue,1500.99,0
"""


# Sample table data (CSV-like format) with out erros
# table_data_str = """FIELD1,FIELD2,FIELD_DEC1,FIELD_NUMERIC1
# ABC1231,XYZ789,123.45,9876543210
# ABC123,XYZ789,123.45,9876543210
# LONGTEXT,ValidText,12.34,5678
# A,B,456.78,90
# Value123,InvalidValue,100.99,0
# """

# Global string to capture output
validation_report = ""
# Global list to track validation errors
validation_errors = []


def log(message):
    """Appends validation messages to the global validation_report"""
    global validation_report
    validation_report += message + "\n"


def log_error(message):
    """Logs an error and adds it to the validation error list."""
    global validation_errors
    validation_errors.append(message)
    #log(message)


def check_pattern(json_str, table_columns, table_data):
    """Validates if the values follow the correct pattern based on field type"""
    json_data = json.loads(json_str)
    fields = json_data["fields"]
    column_index_map = {col: idx for idx, col in enumerate(table_columns)}
    
    pattern_violations = []

    # Define regex patterns for types
    patterns = {
        "alpha_numeric": r"^[a-zA-Z0-9]*$",  
        "numeric": r"^\d+$",   
        "decimal": r"^\d+\.\d+$",  
    }

    for row_num, row in enumerate(table_data, start=1):
        for column, config in fields.items():
            if column in column_index_map:
                col_index = column_index_map[column]
                value = row[col_index].strip() if col_index < len(row) else ""

                # Check if the field has a pattern defined based on type
                if "type" in config:
                    field_type = config["type"]
                    if field_type in patterns:
                        pattern = patterns[field_type]
                        # If the value does not match the pattern, log the violation
                        if value and not re.match(pattern, value):
                            pattern_violations.append((row_num, column, value, f"Invalid {field_type} pattern"))

    if pattern_violations:
        log(f" Found {len(pattern_violations)} pattern violations.")
        for row_num, column, value, error in pattern_violations:
            log(f"   Row {row_num}: {column} - {value} ({error})")
    else:
        log(" No pattern validation errors.")
    
    return pattern_violations


import json
import csv

def log(message):
    """Simple log function (assumed to be defined elsewhere)."""
    print(message)  # Replace with actual logging if needed

def validate_columns(json_str, table_str):
    """Validates if all required columns from JSON config exist in the table data and checks column order."""
    json_data = json.loads(json_str)
    required_columns = json_data.get("columns", [])
    
    table_reader = csv.reader(table_str.strip().split("\n"))
    table_columns = next(table_reader)  # First row is headers

    # Check for missing and extra columns
    missing_columns = set(required_columns) - set(table_columns)
    extra_columns = set(table_columns) - set(required_columns)
    
    if missing_columns:
        log(f"Missing Columns: {missing_columns}")
    else:
        log("All required columns are present.")

    if extra_columns:
        log(f"Extra Columns Found: {extra_columns}")

    # Check column order
    is_order_correct = table_columns[: len(required_columns)] == required_columns
    if not is_order_correct:
        log(f"Column order mismatch. Expected: {required_columns}, Found: {table_columns}")
    else:
        log("Column order Matching")

    return {
        "missing_columns": list(missing_columns),
        "extra_columns": list(extra_columns),
        "is_valid": not missing_columns and is_order_correct,
        "is_order_correct": is_order_correct
    }, table_columns, list(table_reader)


# def validate_columns(json_str, table_str):
#     """Validates if all required columns from JSON config exist in the table data"""
#     json_data = json.loads(json_str)
#     required_columns = set(json_data.get("columns", []))
    
#     table_reader = csv.reader(table_str.strip().split("\n"))
#     table_columns = next(table_reader)  # First row is headers
    
#     missing_columns = required_columns - set(table_columns)
#     extra_columns = set(table_columns) - required_columns
    
#     if missing_columns:
#         log(f" Missing Columns: {missing_columns}")
#     else:
#         log(" All required columns are present.")

#     if extra_columns:
#         log(f"Extra Columns Found: {extra_columns}")
    
#     return {
#         "missing_columns": list(missing_columns),
#         "extra_columns": list(extra_columns),
#         "is_valid": not missing_columns
#     }, table_columns, list(table_reader)


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
        log(f" Found {len(duplicates)} duplicate rows. {duplicates}")
        for row in duplicates:
            log(f"   Row : {row} - is duplicated.")
    else:
        log(" No duplicate rows found.")
    
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
        log(f" Found {len(size_violations)} size constraint violations.")
        for row_num, column, value, max_size in size_violations:
            log(f"   Row {row_num}: {column} exceeds {max_size} chars ({len(value)} chars)")
    else:
        log(" No size constraint violations found.")
    
    return size_violations


def check_required_fields(json_str, table_columns, table_data):
    """Checks if required fields are not empty"""
    json_data = json.loads(json_str)
    fields = json_data["fields"]
    column_index_map = {col: idx for idx, col in enumerate(table_columns)}
    
    missing_values = []

    for row_num, row in enumerate(table_data, start=1):
        for column, config in fields.items():
            if config.get("required", False) and column in column_index_map:
                col_index = column_index_map[column]
                value = row[col_index].strip() if col_index < len(row) else ""
                
                if value == "":
                    missing_values.append((row_num, column))

    if missing_values:
        log(f" Found {len(missing_values)} missing required fields.")
        for row_num, column in missing_values:
            log(f"   Row {row_num}: {column} is required but missing.")
    else:
        log(" No missing required fields.")
    
    return missing_values


def check_numeric_and_decimal(json_str, table_columns, table_data):
    """Validates numeric and decimal values based on constraints"""
    json_data = json.loads(json_str)
    fields = json_data["fields"]
    column_index_map = {col: idx for idx, col in enumerate(table_columns)}
    
    numeric_violations = []

    for row_num, row in enumerate(table_data, start=1):
        for column, config in fields.items():
            if column in column_index_map:
                col_index = column_index_map[column]
                value = row[col_index].strip() if col_index < len(row) else ""

                # Check numeric values
                if config["type"] == "numeric" and value:
                    if not value.isdigit():
                        numeric_violations.append((row_num, column, value, "Not a valid numeric value"))

                # Check decimal values
                elif config["type"] == "decimal" and value:
                    try:
                        float_value = float(value)
                        range_values = config.get("range", [])
                        if len(range_values) == 2 and not (range_values[0] <= float_value <= range_values[1]):
                            numeric_violations.append((row_num, column, value, "Out of range"))
                    except ValueError:
                        numeric_violations.append((row_num, column, value, "Not a valid decimal value"))

    if numeric_violations:
        log(f" Found {len(numeric_violations)} numeric/decimal violations.")
        for row_num, column, value, error in numeric_violations:
            log(f"   Row {row_num}: {column} - {value} ({error})")
    else:
        log(" No numeric/decimal validation errors.")
    
    return numeric_violations

import hashlib

def check_unique_fields(json_str, table_columns, table_data):
    """Checks if a combination of unique fields contains duplicate values."""
    json_data = json.loads(json_str)
    fields = json_data["fields"]
    column_index_map = {col: idx for idx, col in enumerate(table_columns)}

    # Identify unique columns from JSON
    unique_columns = [col for col, config in fields.items() if config.get("unique", False)]
    
    # Track unique combinations
    unique_combinations = set()
    duplicate_entries = []

    for row_num, row in enumerate(table_data, start=1):
        # Extract unique column values for this row
        unique_values = tuple(row[column_index_map[col]].strip() if col in column_index_map and column_index_map[col] < len(row) else "" for col in unique_columns)

        # Generate a hash to track this combination
        unique_hash = hashlib.md5(str(unique_values).encode()).hexdigest()

        # Check if this combination is already seen
        if unique_hash in unique_combinations:
            duplicate_entries.append((row_num, unique_values))
        else:
            unique_combinations.add(unique_hash)

    # Logging results
    if duplicate_entries:
        log(f" Found {len(duplicate_entries)} unique field level violations.")
        for row_num, unique_values in duplicate_entries:
            log(f"   Row {row_num}: Unique field combination {unique_values} is duplicated.")
    else:
        log(" No unique field violations found.")

    return duplicate_entries


# def check_unique_fields(json_str, table_columns, table_data):
#     """Checks if unique fields contain duplicate values."""
#     json_data = json.loads(json_str)
#     fields = json_data["fields"]
#     column_index_map = {col: idx for idx, col in enumerate(table_columns)}

#     unique_columns = [col for col, config in fields.items() if config.get("unique", False)]
#     unique_values = {col: set() for col in unique_columns}
#     duplicate_entries = []

#     for row_num, row in enumerate(table_data, start=1):
#         for column in unique_columns:
#             if column in column_index_map:
#                 col_index = column_index_map[column]
#                 value = row[col_index].strip() if col_index < len(row) else ""

#                 if value:  # Only check non-empty values
#                     if value in unique_values[column]:
#                         duplicate_entries.append((row_num, column, value))
#                     else:
#                         unique_values[column].add(value)

#     if duplicate_entries:
#         log(f" Found {len(duplicate_entries)} unique field level violations.")
#         for row_num, column, value in duplicate_entries:
#             log(f"   Row {row_num}: {column} - '{value}' is duplicated.")
#     else:
#         log(" No unique field violations found.")

#     return duplicate_entries


# Run validation and capture output
column_result, table_columns, table_data = validate_columns(json_data_str, table_data_str)

if column_result["is_valid"]:
    duplicates = check_duplicates(table_data)
    size_issues = check_size_constraints(json_data_str, table_columns, table_data)
    missing_values = check_required_fields(json_data_str, table_columns, table_data)
    numeric_issues = check_numeric_and_decimal(json_data_str, table_columns, table_data)
    pattern_issues = check_pattern(json_data_str, table_columns, table_data)
    duplicate_rec_by_unique = check_unique_fields(json_data_str, table_columns, table_data)

    
    log("\nValidation Summary:")
    log(f" Columns Valid: {column_result['is_valid']}")
    log(f" Duplicate Rows:(All Columns) {len(duplicates)}")
    log(f" Size Violations: {len(size_issues)}")
    log(f" Missing Required Fields: {len(missing_values)}")
    log(f" Numeric/Decimal Violations: {len(numeric_issues)}")
    log(f" pattern_issues violations: {len(pattern_issues)}")
    log(f" Duplicate Rows(For Unique Columns) violations: {len(duplicate_rec_by_unique)}")


    if duplicates:
        log_error(f"Duplicate Rows Found: {len(duplicates)}")
    if duplicate_rec_by_unique:
        log_error(f"Unique Field Violations: {len(duplicate_rec_by_unique)}")
    if size_issues:
        log_error(f"Size Violations: {len(size_issues)}")
    if missing_values:
        log_error(f"Missing Required Fields: {len(missing_values)}")
    if numeric_issues:
        log_error(f"Numeric/Decimal Violations: {len(numeric_issues)}")
    if pattern_issues:
        log_error(f"Pattern Violations: {len(pattern_issues)}")


else:
    log(" Column validation failed. Stopping further checks.")
    log_error("Column validation failed. Stopping further checks.")


# Print the entire validation report at the end
print(validation_report)


# Exit with appropriate status
if validation_errors:
    print("\nValidation failed with below errors. Exiting with status 1.")
    for error in validation_errors: 
        print(f"  - {error}")  # Print each error in a readable format
 
    sys.exit(1)  # Exit with error
else:
    print("\nValidation successful. Exiting with status 0.")
    sys.exit(0)  # Exit successfully

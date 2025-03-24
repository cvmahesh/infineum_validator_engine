import json
import csv

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
table_data_str = """FIELD1,FIELD2,FIELD_DEC1,FIELD_NUMERIC1
ABC123,XYZ789,123.45,9876543210
ABC123,XYZ789,123.45,9876543210
LONG_TEXT_EXCEEDING_LIMIT,ValidText,12.34,5678
,,456.78,90
Value123,InvalidValue,1500.99,0
"""

# Global string to capture output
validation_report = ""

def log(message):
    """Appends validation messages to the global validation_report"""
    global validation_report
    validation_report += message + "\n"

def validate_columns(json_str, table_str):
    """Validates if all required columns from JSON config exist in the table data"""
    json_data = json.loads(json_str)
    required_columns = set(json_data.get("columns", []))
    
    table_reader = csv.reader(table_str.strip().split("\n"))
    table_columns = next(table_reader)  # First row is headers
    
    missing_columns = required_columns - set(table_columns)
    extra_columns = set(table_columns) - required_columns
    
    if missing_columns:
        log(f"❌ Missing Columns: {missing_columns}")
    else:
        log("✅ All required columns are present.")

    if extra_columns:
        log(f"⚠️ Extra Columns Found: {extra_columns}")
    
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
        log(f"❌ Found {len(duplicates)} duplicate rows.")
    else:
        log("✅ No duplicate rows found.")
    
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
        log(f"❌ Found {len(size_violations)} size constraint violations.")
        for row_num, column, value, max_size in size_violations:
            log(f"   Row {row_num}: {column} exceeds {max_size} chars ({len(value)} chars)")
    else:
        log("✅ No size constraint violations found.")
    
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
        log(f"❌ Found {len(missing_values)} missing required fields.")
        for row_num, column in missing_values:
            log(f"   Row {row_num}: {column} is required but missing.")
    else:
        log("✅ No missing required fields.")
    
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
        log(f"❌ Found {len(numeric_violations)} numeric/decimal violations.")
        for row_num, column, value, error in numeric_violations:
            log(f"   Row {row_num}: {column} - {value} ({error})")
    else:
        log("✅ No numeric/decimal validation errors.")
    
    return numeric_violations


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
        log(f"❌ Found {len(pattern_violations)} pattern violations.")
        for row_num, column, value, error in pattern_violations:
            log(f"   Row {row_num}: {column} - {value} ({error})")
    else:
        log("✅ No pattern validation errors.")
    
    return pattern_violations



# Run validation and capture output
column_result, table_columns, table_data = validate_columns(json_data_str, table_data_str)

if column_result["is_valid"]:
    duplicates = check_duplicates(table_data)
    size_issues = check_size_constraints(json_data_str, table_columns, table_data)
    missing_values = check_required_fields(json_data_str, table_columns, table_data)
    numeric_issues = check_numeric_and_decimal(json_data_str, table_columns, table_data)
    
    log("\nValidation Summary:")
    log(f"✅ Columns Valid: {column_result['is_valid']}")
    log(f"❌ Duplicate Rows: {len(duplicates)}")
    log(f"❌ Size Violations: {len(size_issues)}")
    log(f"❌ Missing Required Fields: {len(missing_values)}")
    log(f"❌ Numeric/Decimal Violations: {len(numeric_issues)}")
else:
    log("❌ Column validation failed. Stopping further checks.")

# Print the entire validation report at the end
print(validation_report)

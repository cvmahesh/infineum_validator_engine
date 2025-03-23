import json
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, length, count, when, lit
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType
import pandas as pd
from io import StringIO

# Snowflake connection details (Replace with actual credentials)
CONNECTION_PARAMETERS = {
    "account": "<your_snowflake_account>",
    "user": "<your_username>",
    "password": "<your_password>",
    "warehouse": "<your_warehouse>",
    "database": "<your_database>",
    "schema": "<your_schema>",
    "role": "<your_role>"
}

# Initialize Snowpark session
session = Session.builder.configs(CONNECTION_PARAMETERS).create()

# Sample JSON configuration
json_data_str = '''
{
    "fields": {
        "FIELD1": {"size": 500, "type": "alpha_numeric", "required": true},
        "FIELD2": {"size": 100, "type": "alpha_numeric", "required": true, "allowed_values": ["ValidText", "XYZ789"]},
        "FIELD_DEC1": {"size": 21, "size_before_decimal": 10, "size_after_decimal": 10, "type": "decimal", "required": false, "range": [0, 1000]},
        "FIELD_NUMERIC1": {"size": 21, "type": "numeric", "required": false, "zero_check": true}
    },
    "duplicate_field_check": true,
    "columns": ["FIELD1", "FIELD2", "FIELD_DEC1", "FIELD_NUMERIC1"]
}
'''

# Sample table data as CSV format
table_data_str = """
FIELD1,FIELD2,FIELD_DEC1,FIELD_NUMERIC1
ABC123,XYZ789,123.45,9876543210
ABC123,XYZ789,123.45,9876543210
LONG_TEXT_EXCEEDING_LIMIT,ValidText,12.34,5678
,,456.78,90
Value123,InvalidValue,1500.99,0
"""

# Convert JSON string to dictionary
json_data = json.loads(json_data_str)
field_definitions = json_data["fields"]
columns = json_data["columns"]

# Convert CSV data into a Pandas DataFrame
df_pd = pd.read_csv(StringIO(table_data_str))

# Convert Pandas DataFrame to Snowpark DataFrame
df_snowpark = session.create_dataframe(df_pd)


# Validation Functions
def check_required_fields(df):
    """Check for missing required fields."""
    errors = []
    for field, spec in field_definitions.items():
        if spec["required"]:
            missing_count = df.filter((col(field).is_null()) | (col(field) == lit(""))).count()
            if missing_count > 0:
                errors.append(f"'{field}' is required but missing in {missing_count} rows.")
    return errors


def check_size_constraints(df):
    """Check if any field exceeds its defined size."""
    errors = []
    for field, spec in field_definitions.items():
        size_exceed_count = df.filter(length(col(field)) > spec["size"]).count()
        if size_exceed_count > 0:
            errors.append(f"'{field}' exceeds max size {spec['size']} in {size_exceed_count} rows.")
    return errors


def check_numeric_and_decimal(df):
    """Validate numeric and decimal fields based on JSON config."""
    errors = []
    for field, spec in field_definitions.items():
        if spec["type"] == "numeric":
            invalid_numeric_count = df.filter(~col(field).rlike(r'^\d+$')).count()
            if invalid_numeric_count > 0:
                errors.append(f"'{field}' should be numeric in {invalid_numeric_count} rows.")

        elif spec["type"] == "decimal":
            before_decimal = spec.get("size_before_decimal", 10)
            after_decimal = spec.get("size_after_decimal", 10)
            regex_pattern = rf'^\d{{1,{before_decimal}}}(\.\d{{1,{after_decimal}}})?$'
            invalid_decimal_count = df.filter(~col(field).rlike(regex_pattern)).count()
            if invalid_decimal_count > 0:
                errors.append(f"'{field}' should be a decimal with {before_decimal} digits before and {after_decimal} digits after decimal in {invalid_decimal_count} rows.")
    return errors


def check_range(df):
    """Check if values fall within allowed range."""
    errors = []
    for field, spec in field_definitions.items():
        if "range" in spec:
            min_val, max_val = spec["range"]
            out_of_range_count = df.filter((col(field).cast(FloatType()) < min_val) | (col(field).cast(FloatType()) > max_val)).count()
            if out_of_range_count > 0:
                errors.append(f"'{field}' values out of range {min_val}-{max_val} in {out_of_range_count} rows.")
    return errors


def check_zero_values(df):
    """Check for fields that should not be zero."""
    errors = []
    for field, spec in field_definitions.items():
        if spec.get("zero_check", False):
            zero_count = df.filter(col(field) == lit("0")).count()
            if zero_count > 0:
                errors.append(f"'{field}' should not be zero in {zero_count} rows.")
    return errors


def check_allowed_values(df):
    """Check if fields contain only allowed values."""
    errors = []
    for field, spec in field_definitions.items():
        if "allowed_values" in spec:
            invalid_value_count = df.filter(~col(field).isin(spec["allowed_values"])).count()
            if invalid_value_count > 0:
                errors.append(f"'{field}' contains invalid values in {invalid_value_count} rows. Allowed values: {spec['allowed_values']}.")
    return errors


def check_duplicates(df):
    """Check for duplicate rows."""
    errors = []
    if json_data.get("duplicate_field_check", False):
        duplicate_count = df.group_by(columns).count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            errors.append(f"Duplicate rows found: {duplicate_count}.")
    return errors


# Main Function
def main():
    validation_errors = []
    
    # Run all validation checks
    validation_errors.extend(check_required_fields(df_snowpark))
    validation_errors.extend(check_size_constraints(df_snowpark))
    validation_errors.extend(check_numeric_and_decimal(df_snowpark))
    validation_errors.extend(check_range(df_snowpark))
    validation_errors.extend(check_zero_values(df_snowpark))
    validation_errors.extend(check_allowed_values(df_snowpark))
    validation_errors.extend(check_duplicates(df_snowpark))

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

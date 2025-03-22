import json
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Helper function to check if a value matches the specified pattern
def check_pattern(value, field_type):
    patterns = {
        'alpha_numeric': r'^[a-zA-Z0-9]+$',  # Alphanumeric pattern
        'numeric': r'^\d+$',  # Numeric pattern
        'decimal': r'^\d{1,10}(\.\d{1,10})?$'  # Decimal pattern
    }
    import re
    return bool(re.match(patterns.get(field_type, ''), value))

# Helper function to read input file into a Snowpark DataFrame
def load_input_file(session, input_filename):
    # Assuming the input file is CSV, you can adjust it to match other formats (e.g., JSON, Parquet)
    return session.read.csv(input_filename, header=True, inferSchema=True)

# Validation function for Snowpark DataFrame
def validate_data(session, data, validation_rules, snowpark_table):
    errors = []

    # Get the schema of the Snowpark table
    snowpark_table_df = session.table(snowpark_table)
    table_columns = snowpark_table_df.columns
    table_schema = snowpark_table_df.schema

    # Check for duplicates in the specified fields (if duplicate check is enabled)
    if validation_rules.get("duplicate_field_check", False):
        seen = set()
        for field in validation_rules["columns"]:
            if field in data.columns:
                data = data.withColumn(f"{field}_duplicate", col(field).isin(seen))
                seen.add(col(field))

    # Iterate through the input data rows
    for row in data.collect():
        for field, field_rule in validation_rules['fields'].items():
            if field in row.asDict():
                value = row[field]

                # Null check for required fields
                if field_rule['required'] and (value is None or value == ''):
                    errors.append(f"Field {field} is required but is null or empty.")

                # Size check (for alpha_numeric fields)
                if field_rule['type'] == 'alpha_numeric' and len(str(value)) > field_rule.get('size', 0):
                    errors.append(f"Field {field} exceeds the allowed size.")

                # Pattern check (ensure value matches the expected pattern)
                if not check_pattern(value, field_rule['type']):
                    errors.append(f"Field {field} does not match the expected pattern ({field_rule['type']}).")

                # Zero check (for numeric fields)
                if field_rule['type'] == 'numeric' and value == 0:
                    errors.append(f"Field {field} cannot have zero value.")

                # Range check (for decimal fields)
                if field_rule['type'] == 'decimal':
                    if isinstance(value, float):
                        value_str = str(value)
                        size_before_decimal = field_rule.get('size_before_decimal', 0)
                        size_after_decimal = field_rule.get('size_after_decimal', 0)

                        # Split value into before and after decimal parts
                        if value_str.find('.') != -1:
                            before_decimal, after_decimal = value_str.split('.')
                        else:
                            before_decimal, after_decimal = value_str, ""

                        if len(before_decimal) > size_before_decimal:
                            errors.append(f"Field {field} exceeds the allowed number of digits before decimal.")
                        if len(after_decimal) > size_after_decimal:
                            errors.append(f"Field {field} exceeds the allowed number of digits after decimal.")
                
    return errors

# Function to insert valid records into Snowflake table
def insert_valid_records(session, data, snowpark_table, validation_rules):
    # First, we validate the data
    validation_errors = validate_data(session, data, validation_rules, snowpark_table)

    if validation_errors:
        return validation_errors

    # If no errors, proceed with insertion into the Snowflake table
    data.write.mode("append").save_as_table(snowpark_table)
    return f"Successfully inserted {len(data.collect())} valid records into {snowpark_table}."

# Main function
def main(session, input_filename, validation_json, snowpark_table):
    # Load the JSON validation template
    with open(validation_json, 'r') as file:
        validation_rules = json.load(file)

    # Load the input file into a Snowpark DataFrame
    data = load_input_file(session, input_filename)

    # Insert valid records into Snowflake table
    result = insert_valid_records(session, data, snowpark_table, validation_rules)
    return result

# Snowflake connection parameters
connection_parameters = {
    "account": "<your_account>",
    "user": "<your_username>",
    "password": "<your_password>",
    "role": "<your_role>",
    "warehouse": "<your_warehouse>",
    "database": "<your_database>",
    "schema": "<your_schema>"
}

# Create a Snowpark session
session = Session.builder.configs(connection_parameters).create()

# Example usage:
input_filename = "path_to_input_file.csv"  # Path to the input file
validation_json = "validation_template.json"  # Path to the JSON validation template
snowpark_table = "YOUR_SNOWFLAKE_TABLE"  # Snowflake table to insert records

result = main(session, input_filename, validation_json, snowpark_table)
print(result)

# Close the session
session.close()

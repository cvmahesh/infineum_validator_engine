import json
import re
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StringType, DecimalType
from decimal import Decimal

def validate_csv_against_table(file_name: str, table_name: str, json_template: str, session: Session) -> bool:
    # Parse the JSON template into a Python dictionary
    template = json.loads(json_template)
    
    # Extract fields and columns from the JSON template
    fields = template.get("fields", {})
    expected_columns = template.get("columns", [])
    duplicate_field_check = template.get("duplicate_field_check", False)
    
    if not expected_columns:
        raise ValueError("The JSON template must contain a 'columns' key.")

    # Step 1: Read the CSV file into a Snowpark DataFrame
    csv_df = session.read.option("header", "true").csv(file_name)

    # Step 2: Validate if the CSV structure matches the template (columns)
    csv_columns = csv_df.columns
    
    # Check if all expected columns are present in the CSV
    if set(expected_columns) != set(csv_columns):
        print(f"CSV columns {csv_columns} do not match expected columns {expected_columns}.")
        return False

    # Step 3: Validate the data types, sizes, and required fields based on the template
    for column in expected_columns:
        if column not in fields:
            print(f"Column {column} not found in the template.")
            return False
        
        field = fields[column]
        column_type = field["type"]
        required = field["required"]
        column_size = field["size"]
        
        # Check if the column exists in the CSV
        if column not in csv_columns:
            print(f"Missing column: {column}")
            return False

        # Step 3a: Validate data types and sizes
        if column_type == "alpha_numeric":
            # Check that the field is alphanumeric and within size limits
            csv_df = csv_df.filter(col(column).isNotNull())
            invalid_alpha_numeric = csv_df.filter(~col(column).rlike("^[a-zA-Z0-9]*$")).count()
            if invalid_alpha_numeric > 0:
                print(f"Column {column} contains invalid alpha_numeric values.")
                return False
            invalid_size = csv_df.filter(col(column).rlength() > column_size).count()
            if invalid_size > 0:
                print(f"Column {column} exceeds the size limit of {column_size}.")
                return False
            
        elif column_type == "decimal":
            # Validate decimal fields with size before and after decimal
            size_before_decimal = field.get("size_before_decimal", 0)
            size_after_decimal = field.get("size_after_decimal", 0)
            
            # Check if the field is a valid decimal number
            invalid_decimal = csv_df.filter(~col(column).rlike(f"^[0-9]{{0,{size_before_decimal}}}(\.[0-9]{{0,{size_after_decimal}}})?$")).count()
            if invalid_decimal > 0:
                print(f"Column {column} does not match decimal format with size before {size_before_decimal} and size after {size_after_decimal}.")
                return False
            invalid_size = csv_df.filter(col(column).rlength() > column_size).count()
            if invalid_size > 0:
                print(f"Column {column} exceeds the size limit of {column_size}.")
                return False
        
        # Step 3b: Validate required fields (check if they are not null)
        if required:
            null_count = csv_df.filter(col(column).isNull()).count()
            if null_count > 0:
                print(f"Required column {column} has {null_count} null values.")
                return False

    # Step 4: Check for duplicate rows if specified in the template
    if duplicate_field_check:
        duplicate_count = csv_df.group_by(*csv_columns).count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            print("Duplicate rows found in the CSV file.")
            return False
    
    # Step 5: Read the Snowflake table to compare data types
    snowflake_df = session.table(table_name)
    snowflake_columns = snowflake_df.columns
    
    # Ensure the columns in the Snowflake table match the CSV columns
    if set(csv_columns) != set(snowflake_columns):
        print(f"Snowflake table columns {snowflake_columns} do not match CSV columns {csv_columns}.")
        return False

    # Step 6: If all checks pass, return True
    print("CSV file structure is valid against the Snowflake table.")
    return True


# Example usage:
# session = Session.builder.configs(<snowflake_connection_config>).create()
# file_name = "your_csv_file_path"
# table_name = "your_snowflake_table"
# json_template = '''{
#     "fields": {
#         "FIELD1": {
#             "size": 500,
#             "type": "alpha_numeric",
#             "required": true
#         },
#         "FIELD2": {
#             "size": 100,
#             "type": "alpha_numeric",
#             "required": true
#         },
#         "FIELD_DEC1": {
#             "size": 21,
#             "size_before_decimal": 10,
#             "size_after_decimal": 10,
#             "type": "decimal",
#             "required": false
#         },
#         "FIELD_NUMERIC1": {
#             "size": 21,
#             "size_before_decimal": 10,
#             "size_after_decimal": 10,
#             "type": "decimal",
#             "required": false
#         }
#     },
#     "duplicate_field_check": true,
#     "columns": ["FIELD1", "FIELD2", "FIELD_DEC1", "FIELD_NUMERIC1"]
# }'''

# validate_csv_against_table(file_name, table_name, json_template, session)

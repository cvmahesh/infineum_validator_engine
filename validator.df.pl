import pandas as pd
import json
import re


def log(msg):
    print(msg)


def log_error(msg):
    print(f"[ERROR] {msg}")


def read_data(data_file):
    try:
        if data_file.endswith(".csv"):
            return pd.read_csv(data_file, dtype={"MMYEAR": str})
        else:
            raise ValueError("Unsupported file format. Use .csv only")
    except Exception as e:
        log_error(f"Error reading data file: {e}")
        return None


def read_rules(json_file):
    try:
        with open(json_file) as f:
            return json.load(f)
    except Exception as e:
        log_error(f"Error reading JSON file: {e}")
        return None


def verify_fields(df, rules):
    expected_fields = list(rules.get("fields", {}).keys())  # list of expected column names
    actual_fields = list(df.columns)

    log(f"Expected Columns: {expected_fields}")
    log(f"Actual Columns:   {actual_fields}")

    missing_fields = list(set(expected_fields) - set(actual_fields))
    extra_fields = list(set(actual_fields) - set(expected_fields))
    is_valid = len(missing_fields) == 0

    if missing_fields:
        log_error(f"Missing Columns: {missing_fields}")
    else:
        log("All required columns are present.")

    if extra_fields:
        log(f"Extra Columns Found: {extra_fields}")

    # Check column order
    is_order_correct = expected_fields == actual_fields[:len(expected_fields)]
    if not is_order_correct:
        log_error(f"Column order mismatch.\nExpected Order: {expected_fields}\nActual Order:   {actual_fields}")
    else:
        log("Column order is correct.")

    return {
        "is_valid": is_valid and is_order_correct,
        "missing": missing_fields,
        "extra": extra_fields,
        "is_order_correct": is_order_correct
    }


def find_duplicates(df):
    return df[df.duplicated(keep=False)]


def validate_size(df, rules):
    issues = []
    for col, props in rules.get("fields", {}).items():
        if "size" in props and col in df.columns:
            mask = df[col].astype(str).apply(lambda x: len(x) > props["size"])
            issues.extend(df[mask].index.tolist())
    return issues


def validate_required(df, rules):
    issues = []
    for col, props in rules.get("fields", {}).items():
        if props.get("required") and col in df.columns:
            issues.extend(df[df[col].isna()].index.tolist())
    return issues


def validate_numeric(df, rules):
    issues = []
    for col, props in rules.get("fields", {}).items():
        if props.get("type") in ["numeric", "decimal"] and col in df.columns:
            coerced = pd.to_numeric(df[col], errors="coerce")
            issues.extend(df[coerced.isna()].index.tolist())
    return issues


def validate_pattern(df, rules):
    issues = []
    for col, props in rules.get("fields", {}).items():
        if col in df.columns:
            if props.get("type") == "alpha_numeric":
                pattern = r"^[a-zA-Z0-9\s]*$"
            elif props.get("type") == "numeric":
                pattern = r"^\d+$"
            elif props.get("type") == "decimal":
                pattern = r"^\d+(\.\d+)?$"
            else:
                continue
            invalid = df[~df[col].astype(str).str.match(pattern, na=False)]
            issues.extend(invalid.index.tolist())
    return issues


def validate_unique(df, rules):
    unique_cols = [col for col, props in rules.get("fields", {}).items() if props.get("unique")]
    if unique_cols:
        dupes = df[df.duplicated(subset=unique_cols, keep=False)]
        return dupes.index.tolist()
    return []


def validate_allowed_values(df, rules):
    issues = []
    for col, props in rules.get("fields", {}).items():
        allowed = props.get("allowed_values")
        if allowed and col in df.columns:
            violations = ~df[col].isin(allowed)
            issues.extend(df[violations].index.tolist())
    return issues


def validate_zero_check(df, rules):
    issues = []
    for col, props in rules.get("fields", {}).items():
        if props.get("zero_check") and col in df.columns:
            zeroes = df[df[col] == 0]
            issues.extend(zeroes.index.tolist())
    return issues


# def validate_year_check(df, rules, min_year=1900, max_year=2100):
#     issues = []
#     for col, props in rules.get("fields", {}).items():
#         if props.get("year_check") and col in df.columns:
#             # Check for 4-digit numeric strings
#             valid_format = df[col].astype(str).str.fullmatch(r"\d{4}")
#             valid_rows = df[valid_format].copy()
            
#             # Convert to integer and check range
#             valid_rows[col] = valid_rows[col].astype(int)
#             out_of_range = valid_rows[
#                 (valid_rows[col] < min_year) | (valid_rows[col] > max_year)
#             ]
            
#             # Collect all bad indexes: invalid format + out-of-range
#             issues.extend(df[~valid_format].index.tolist())
#             issues.extend(out_of_range.index.tolist())
#     return issues


def validate_year_check(df, rules, min_year=1900, max_year=2100):
    issues = []

    for col, props in rules.get("fields", {}).items():
        if not props.get("year_check") or col not in df.columns:
            continue

        #values = df[col].astype(str)
        values = df[col].fillna("").apply(str)
        print(df[col])

        if props.get("type") == "year":
            # Check format: exactly 4 digits
            valid_format = values.str.fullmatch(r"\d{4}")
            valid_years = df[valid_format].copy()
            valid_years[col] = valid_years[col].astype(int)

            out_of_range = valid_years[
                (valid_years[col] < min_year) | (valid_years[col] > max_year)
            ]

            # Add bad format and out-of-range indexes
            issues.extend(df[~valid_format].index.tolist())
            issues.extend(out_of_range.index.tolist())

        elif props.get("type") == "month_year":
            # Accepts formats: 001.2025, 001/2025, or 2025
            pattern = r"(\d{3}[./]\d{4})|(\d{4})"
            valid_format = values.str.fullmatch(pattern)
            valid_rows = df[valid_format].copy()

            # Extract month (optional) and year
            month_year_split = valid_rows[col].str.extract(r"(?:(?P<month>\d{3})[./])?(?P<year>\d{4})")


           #month_year_split = valid_rows[col].str.extract(r"(?:(?P<month>\d{3})[./])?(?P<year>\d{4})")
            month_year_split = month_year_split.astype({"year": int})
            month_year_split["month"] = month_year_split["month"].fillna("001").astype(int)

            # Validate ranges
            invalid_months = month_year_split[
                (month_year_split["month"] < 1) | (month_year_split["month"] > 12)
            ]
            invalid_years = month_year_split[
                (month_year_split["year"] < min_year) | (month_year_split["year"] > max_year)
            ]

            # Collect indexes
            issues.extend(df[~valid_format].index.tolist())
            issues.extend(invalid_months.index.tolist())
            issues.extend(invalid_years.index.tolist())

    return issues




def run_validations(data_file, json_file):
    df = read_data(data_file)
    rules = read_rules(json_file)

    if df is None or rules is None:
        log_error("Validation aborted due to file read error.")
        return

    # Field verification
    field_check = verify_fields(df, rules)
    if not field_check["is_valid"]:
        log_error(f"Missing Fields: {field_check['missing']}")
        return

    # Run validations
    duplicates = find_duplicates(df) if rules.get("duplicate_field_check") else []
    size_issues = validate_size(df, rules)
    required_issues = validate_required(df, rules)
    numeric_issues = validate_numeric(df, rules)
    pattern_issues = validate_pattern(df, rules)
    unique_issues = validate_unique(df, rules)
    allowed_values_issues = validate_allowed_values(df, rules)
    zero_value_issues = validate_zero_check(df, rules)
    year_value_issues = validate_year_check(df, rules)

    # Log only if violations
    if len(duplicates):
        log_error(f"Duplicate Rows Found: {len(duplicates)}")
    if len(unique_issues):
        log_error(f"Unique Field Violations: {len(unique_issues)}")
    if len(size_issues):
        log_error(f"Size Violations: {len(size_issues)}")
    if len(required_issues):
        log_error(f"Missing Required Fields: {len(required_issues)}")
    if len(numeric_issues):
        log_error(f"Numeric/Decimal Violations: {len(numeric_issues)}")
    if len(pattern_issues):
        log_error(f"Pattern Violations: {len(pattern_issues)}")
    if len(allowed_values_issues):
        log_error(f"Allowed values Violations: {len(allowed_values_issues)}")
    if len(zero_value_issues):
        log_error(f"Zero Value Violations: {len(zero_value_issues)}")
    if len(year_value_issues):
        log_error(f"Year Value Violations: {len(year_value_issues)}")
    # if len(month_year_value_issues):
    #     log_error(f"Month Year Value Violations: {len(month_year_value_issues)}")

    # Final Summary Log
    log("\nValidation Summary:")
    log(f" Columns Valid: {field_check['is_valid']}")
    log(f" Duplicate Rows: {len(duplicates)}")
    log(f" Size Violations: {len(size_issues)}")
    log(f" Missing Required Fields: {len(required_issues)}")
    log(f" Numeric/Decimal Violations: {len(numeric_issues)}")
    log(f" Pattern Violations: {len(pattern_issues)}")
    log(f" Duplicate Rows(For Unique Columns) violations: {len(unique_issues)}")
    log(f" Allowed Values violations: {len(allowed_values_issues)}")
    log(f" Zero Value Violations: {len(zero_value_issues)}")
    log(f" Year Value Violations: {len(year_value_issues)}")
    # log(f" Month Year Value Violations: {len(month_year_value_issues)}")


# Example usage
if __name__ == "__main__":
    run_validations("data.csv", "data.json")

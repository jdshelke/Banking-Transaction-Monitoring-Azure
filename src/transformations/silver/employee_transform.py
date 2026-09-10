from src.utils import data_quality

def transform(df):
    dedup_df = data_quality.remove_duplicates(df, ["employee_id"])

    drop_null_df = data_quality.drop_null_records(dedup_df, ["employee_id", "branch_id"])

    trim_string_df = data_quality.trim_columns(drop_null_df, ["name", "role"])

    to_upper_case_df = data_quality.change_case(trim_string_df, ["role"], "upper")

    salary_valid_df = data_quality.validate_non_negative(to_upper_case_df, ["salary"])

    date_valid_df = data_quality.validate_date(salary_valid_df, "hire_date")

    cleaned_employees_df = data_quality.fill_null_value(date_valid_df, "Unknown", ["name", "role"])

    # print("For Employee Total Records Processed: ", cleaned_employees_df.count())

    return cleaned_employees_df

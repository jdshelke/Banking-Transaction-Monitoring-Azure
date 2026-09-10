from src.utils import data_quality

def transform(df):
    dedup_df = data_quality.remove_duplicates(df, ["loan_id"])

    drop_null_df = data_quality.drop_null_records(dedup_df, ["loan_id", "customer_id", "branch_id"])

    trim_string_df = data_quality.trim_columns(drop_null_df, ["loan_type", "status"])

    to_upper_case_df = data_quality.change_case(trim_string_df, ["loan_type", "status"], "upper")

    valid_non_negative_df = data_quality.validate_non_negative(to_upper_case_df, ["loan_amount", "interest_rate", "term_months"])

    date_valid_df = data_quality.validate_date(valid_non_negative_df, "start_date")

    cleaned_loan_df = data_quality.fill_null_value(date_valid_df, "Unknown", ["loan_type", "status"])

    # print("For Loan Total Records Processed: ", cleaned_loan_df.count())
    
    return cleaned_loan_df

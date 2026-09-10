from src.utils import data_quality

def transform(df):
    dedup_df = data_quality.remove_duplicates(df, ["account_id"])

    drop_null_df = data_quality.drop_null_records(dedup_df, ["account_id", "customer_id", "branch_id"])

    trim_string_df = data_quality.trim_columns(drop_null_df, ["account_type", "status"])

    to_upper_case_df = data_quality.change_case(trim_string_df, ["account_type", "status"], "upper")

    date_valid_df = data_quality.validate_date(to_upper_case_df, "open_date")

    cleaned_accounts_df = data_quality.fill_null_value(date_valid_df, "Unknown", ["status"])

    # print("For Account Total Records Processed: ", cleaned_accounts_df.count())

    return cleaned_accounts_df

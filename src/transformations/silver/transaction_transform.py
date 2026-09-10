from src.utils import data_quality

def transform(df):
    dedup_df = data_quality.remove_duplicates(df, ["transaction_id"])

    drop_null_df = data_quality.drop_null_records(dedup_df, ["transaction_id"])

    trim_string_df = data_quality.trim_columns(drop_null_df, ["txn_type", "channel", "merchant_category"])

    to_upper_case_df = data_quality.change_case(trim_string_df, ["txn_type", "channel", "merchant_category"], "upper")

    to_timestamp_df = data_quality.to_timestamp(to_upper_case_df, ["txn_date"])

    amount_valid_df = data_quality.validate_non_negative(to_timestamp_df, ["amount"])

    cleaned_transaction_df = data_quality.fill_null_value(amount_valid_df, "Unknown", ["merchant_category"])

    # print("For Transaction Total Records Processed: ", cleaned_transaction_df.count())

    return cleaned_transaction_df

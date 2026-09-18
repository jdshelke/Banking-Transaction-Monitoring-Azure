from src.utils import data_quality

def transform(df):
    drop_clumns_df = data_quality.drop_columns(df, ["_rescued_data"])

    drop_null_df = data_quality.drop_null_records(drop_clumns_df, ["card_txn_id", "card_id", "txn_date", "amount", "is_fraud"])

    # dedup_df = data_quality.remove_duplicates(drop_null_df, "card_txn_id")

    trim_string_df = data_quality.trim_columns(drop_null_df, ["merchant_category"])

    to_upper_case_df = data_quality.change_case(trim_string_df, ["merchant_category"], "upper")

    amount_valid_df = data_quality.validate_non_negative(to_upper_case_df, ["amount"])

    date_valid_df = data_quality.validate_date(amount_valid_df, "txn_date")

    fraud_valid_df = data_quality.validate_boolean(date_valid_df, "is_fraud")

    cleaned_card_transaction_df = data_quality.fill_null_value(fraud_valid_df, "Unknown", ["merchant_category"])
    
    # print("For Cart Transaction Total Records Processed: ", cleaned_card_transaction_df.count())

    return cleaned_card_transaction_df

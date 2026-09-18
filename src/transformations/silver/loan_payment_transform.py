from src.utils import data_quality

def transform(df):
    drop_clumns_df = data_quality.drop_columns(df, ["_rescued_data"])

    drop_null_df = data_quality.drop_null_records(drop_clumns_df, ["payment_id", "loan_id", "payment_date", "amount_paid", "late_payment_flag"])

    # dedup_df = data_quality.remove_duplicates(drop_null_df, "payment_id")

    amount_valid_df = data_quality.validate_non_negative(drop_null_df, ["amount_paid", "principal_component", "interest_component"])

    date_valid_df = data_quality.validate_date(amount_valid_df, "payment_date")

    cleaned_loan_payment_df = data_quality.validate_boolean(date_valid_df, "late_payment_flag")

    # print("For Loan Payment Total Records Processed: ", cleaned_loan_payment_df.count())

    return cleaned_loan_payment_df

from pyspark.sql.functions import col
from src.utils import data_quality

def transform(df):
    dedup_df = data_quality.remove_duplicates(df, ["card_id"])

    drop_null_df = data_quality.drop_null_records(dedup_df, ["card_id", "customer_id", "account_id"])

    trim_string_df = data_quality.trim_columns(drop_null_df, ["card_type", "status"])

    to_upper_case_df = data_quality.change_case(trim_string_df, ["card_type", "status"], "upper")

    credit_limit_valid_df = data_quality.validate_non_negative(to_upper_case_df, ["credit_limit"])

    date_valid_df = data_quality.validate_date(credit_limit_valid_df, "issue_date")

    expiry_date_valid_df = date_valid_df.filter( (col("expiry_date").isNotNull()) & (col("expiry_date") >= col("issue_date")) )

    cleaned_cards_df = data_quality.fill_null_value(expiry_date_valid_df, "Unknown", ["card_type", "status"])

    # print("For Card Total Records Processed: ", cleaned_cards_df.count())

    return cleaned_cards_df

from pyspark.sql.functions import col
from src.utils import data_quality

def transform(df):
    dedup_df = data_quality.remove_duplicates(df, ["ticket_id"])

    drop_null_df = data_quality.drop_null_records(dedup_df, ["ticket_id", "customer_id"])

    trim_string_df = data_quality.trim_columns(drop_null_df, ["issue_type", "status"])

    to_upper_case_df = data_quality.change_case(trim_string_df, ["status"], "upper")

    date_valid_df = data_quality.validate_date(to_upper_case_df, "date_opened")

    date_valid_df = data_quality.validate_date(date_valid_df, "date_resolved")

    date_valid_df = data_quality.validate_date_relationship(date_valid_df, "date_opened", "date_resolved")

    score_valid_df = date_valid_df.filter( col("satisfaction_score").isNotNull() & 
                                          (col("satisfaction_score") >= 1) & 
                                          (col("satisfaction_score") <= 5)
                                          )
    
    cleaned_support_ticket_df = data_quality.fill_null_value(score_valid_df, "Unknown", ["status"])

    # print("For Support Ticket Total Records Processed: ", cleaned_support_ticket_df.count())

    return cleaned_support_ticket_df

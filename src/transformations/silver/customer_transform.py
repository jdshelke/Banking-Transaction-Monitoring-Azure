from pyspark.sql.functions import col, when, lower
from src.utils import data_quality

def transform(df):
    drop_clumns_df = data_quality.drop_columns(df, ["_rescued_data"])

    drop_null_df = data_quality.drop_null_records(drop_clumns_df, ["customer_id"])

    # dedup_df = data_quality.remove_duplicates(drop_null_df, "customer_id")

    trim_string_df = data_quality.trim_columns(drop_null_df, ["name", "gender", "city", "state", "email", "occupation"])

    credit_score_validate = trim_string_df.withColumn("credit_score", 
                                                      when( (col("credit_score") > 900) | (col("credit_score") < 300), None )
                                                      .otherwise(col("credit_score"))                      
                                                      )
    
    standardize_gender = credit_score_validate.withColumn("gender",when(lower(col("gender")) == "male", "M")
                                                                .when(lower(col("gender")) == "female", "F")
                                                                .otherwise(None)
                                                                )
    
    cleaned_customer_df = data_quality.fill_null_value(standardize_gender, "Unknown", ["name", "city", "state", "phone", 
                                                                                       "email", "occupation"])
    
    # print("For Customers Total Records Processed: ", cleaned_customer_df.count())

    return cleaned_customer_df

from pyspark.sql.functions import col, when
from src.utils import data_quality

def transform(df):
    dedup_df = data_quality.remove_duplicates(df, ["customer_id"])

    drop_null_df = data_quality.drop_null_records(dedup_df, ["customer_id"])

    trim_string_df = data_quality.trim_columns(drop_null_df, ["name", "gender", "city", "state", "email", "occupation"])

    credit_score_validate = trim_string_df.withColumn("credit_score", 
                                                      when( (col("credit_score") > 900) | (col("credit_score") < 300), None )
                                                      .otherwise(col("credit_score"))                      
                                                      )
    
    standardize_gender = credit_score_validate.withColumn("gender", when(col("gender") == "male", "M")
                                                                          .when(col("gender") == "female", "F")
                                                                          .otherwise(None)
                                                                )
    
    cleaned_customer_df = data_quality.fill_null_value(standardize_gender, "Unknown", ["name", "city", "state", "phone", 
                                                                                       "email", "occupation"])
    
    # print("For Customers Total Records Processed: ", cleaned_customer_df.count())

    return cleaned_customer_df

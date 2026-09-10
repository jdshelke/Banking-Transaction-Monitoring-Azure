from pyspark.sql.functions import col, count, count_distinct, sum, avg, min, max, when


def create_card_fraud_summary(cards_df, card_transactions_df):

    card_details_df = cards_df.select(
        "card_id",
        "customer_id",
        "card_type",
        col("status").alias("card_status"),
        "credit_limit"
    )

    fraud_summary_df = card_transactions_df \
        .groupBy(col("card_id")) \
        .agg(
            count(col("card_txn_id")).alias("total_card_transactions"),
            sum(col("amount")).alias("total_transaction_amount"),
            avg(col("amount")).alias("average_transaction_amount"),
            max(col("amount")).alias("max_transaction_amount"),
            sum(
                when(col("is_fraud") == True, 1)
                .otherwise(0)
            ).alias("fraud_transaction_count"),
            sum(
                when(col("is_fraud") == True, col("amount"))
                .otherwise(0)
            ).alias("fraud_transaction_amount"),
            avg(
                when(col("is_fraud") == True, col("amount"))
            ).alias("average_fraud_amount"),
            max(
                when(col("is_fraud") == True, col("amount"))
            ).alias("max_fraud_amount"),
            sum(
                when(col("is_fraud") == False, 1)
                .otherwise(0)
            ).alias("non_fraud_transaction_count"),
            sum(
                when(col("is_fraud") == False, col("amount"))
                .otherwise(0)
            ).alias("non_fraud_transaction_amount"),
            count_distinct(
                col("merchant_category")
            ).alias("unique_merchant_category_count"),
            min(col("txn_date")).alias("first_transaction_date"),
            max(col("txn_date")).alias("last_transaction_date"),
            min(
                when(col("is_fraud") == True, col("txn_date"))
            ).alias("first_fraud_date"),
            max(
                when(col("is_fraud") == True, col("txn_date"))
            ).alias("last_fraud_date")
        )

    fraud_metrics_df = fraud_summary_df \
        .withColumn(
            "fraud_transaction_rate",
            when(
                col("total_card_transactions") > 0,
                col("fraud_transaction_count") /
                col("total_card_transactions")
            ).otherwise(None)
        ) \
        .withColumn(
            "fraud_amount_rate",
            when(
                col("total_transaction_amount") > 0,
                col("fraud_transaction_amount") /
                col("total_transaction_amount")
            ).otherwise(None)
        ) \
        .withColumn(
            "fraud_flag",
            when(
                col("fraud_transaction_count") > 0,
                True
            ).otherwise(False)
        )

    final_df = card_details_df.alias("c") \
        .join(
            fraud_metrics_df.alias("f"),
            col("c.card_id") == col("f.card_id"),
            "left"
        ) \
        .select(
            col("c.card_id"),
            col("c.customer_id"),
            col("c.card_type"),
            col("c.card_status"),
            col("c.credit_limit"),

            col("f.total_card_transactions"),
            col("f.total_transaction_amount"),
            col("f.average_transaction_amount"),
            col("f.max_transaction_amount"),

            col("f.fraud_transaction_count"),
            col("f.fraud_transaction_amount"),
            col("f.average_fraud_amount"),
            col("f.max_fraud_amount"),
            col("f.fraud_transaction_rate"),
            col("f.fraud_amount_rate"),

            col("f.non_fraud_transaction_count"),
            col("f.non_fraud_transaction_amount"),

            col("f.unique_merchant_category_count"),

            col("f.first_transaction_date"),
            col("f.last_transaction_date"),
            col("f.first_fraud_date"),
            col("f.last_fraud_date"),

            col("f.fraud_flag")
        )

    return final_df
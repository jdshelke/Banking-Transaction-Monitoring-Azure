from pyspark.sql.functions import col, count, sum, avg, min, max, when


def create_account_transaction_summary(accounts_df, transactions_df):

    account_details_df = accounts_df.select(
        "account_id",
        "customer_id",
        "account_type",
        col("status").alias("account_status"),
        col("balance").alias("current_balance")
    )

    transaction_summary_df = transactions_df \
        .groupBy(col("account_id")) \
        .agg(
            count(col("transaction_id")).alias("total_transaction_count"),
            sum(col("amount")).alias("total_transaction_amount"),
            min(col("amount")).alias("min_transaction_amount"),
            max(col("amount")).alias("max_transaction_amount"),
            avg(col("amount")).alias("average_transaction_amount"),
            sum(
                when(col("txn_type") == "DEPOSIT", 1)\
                .when(col("txn_type") == "INTEREST CREDIT", 1)\
                .when(col("txn_type") == "TRANSFER IN", 1)\
                .otherwise(0)
            ).alias("credit_transaction_count"),
            sum(
                when(col("txn_type") == "WITHDRAWAL", 1)\
                .when(col("txn_type") == "FEE DEBIT", 1)\
                .when(col("txn_type") == "TRANSFER OUT", 1)\
                .otherwise(0)
            ).alias("debit_transaction_count"),
            sum(
                when(col("txn_type") == "DEPOSIT", col("amount"))\
                .when(col("txn_type") == "INTEREST CREDIT", col("amount"))\
                .when(col("txn_type") == "TRANSFER IN", col("amount"))\
                .otherwise(0)
            ).alias("total_credit_amount"),
            sum(
                when(col("txn_type") == "WITHDRAWAL", col("amount"))\
                .when(col("txn_type") == "FEE DEBIT", col("amount"))\
                .when(col("txn_type") == "TRANSFER OUT", col("amount"))\
                .otherwise(0)
            ).alias("total_debit_amount"),
            max(
                when(col("txn_type") == "DEPOSIT", col("amount"))\
                .when(col("txn_type") == "INTEREST CREDIT", col("amount"))\
                .when(col("txn_type") == "TRANSFER IN", col("amount"))\
                .otherwise(None)
            ).alias("largest_credit_amount"),
            max(
                when(col("txn_type") == "WITHDRAWAL", col("amount"))\
                .when(col("txn_type") == "FEE DEBIT", col("amount"))\
                .when(col("txn_type") == "TRANSFER OUT", col("amount"))\
                .otherwise(None)
            ).alias("largest_debit_amount"),
            
            min(col("txn_date")).alias("first_transaction_date"),
            max(col("txn_date")).alias("last_transaction_date")
        )

    net_transaction_amount_df = transaction_summary_df \
        .withColumn(
            "net_transaction_amount",
            col("total_credit_amount") - col("total_debit_amount")
        )

    final_df = account_details_df.alias("ad").join(
            net_transaction_amount_df.alias("nta"),
            col("ad.account_id") == col("nta.account_id"),
            "left"
        ).select(
            col("ad.account_id"),
            col("ad.customer_id"),
            col("ad.account_type"),
            col("ad.account_status"),
            col("ad.current_balance"),
            col("nta.total_transaction_count"),
            col("nta.total_transaction_amount"),
            col("nta.min_transaction_amount"),
            col("nta.max_transaction_amount"),
            col("nta.average_transaction_amount"),
            col("nta.credit_transaction_count"),
            col("nta.debit_transaction_count"),
            col("nta.total_credit_amount"),
            col("nta.total_debit_amount"),
            col("nta.largest_credit_amount"),
            col("nta.largest_debit_amount"),
            col("nta.net_transaction_amount"),
            col("nta.first_transaction_date"),
            col("nta.last_transaction_date")
        )

    return final_df

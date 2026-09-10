from pyspark.sql.functions import col, count, sum, avg, max, when, months_between, floor, concat_ws


def create_customer_risk_profile(customers_df, customer_transaction_summary_df, 
                                 account_transaction_summary_df, loan_payment_metrics_df):

    customer_details_df = customers_df.select(
        "customer_id",
        "gender",
        "city",
        "state",
        "occupation",
        "annual_income",
        "credit_score",
        "join_date",
    )

    account_details_df = customer_details_df.alias("cd") \
        .join(
            account_transaction_summary_df.alias("ats"),
            col("ats.customer_id") == col("cd.customer_id"),
            "inner"
        ).groupBy(col("cd.customer_id")) \
        .agg(
            count(col("ats.account_id")).alias("total_accounts"),
            sum(
                when(col("ats.account_status") == "ACTIVE", 1)
                .otherwise(0)
            ).alias("active_accounts"),
            sum(col("ats.current_balance")).alias("total_account_balance"),
            max(col("ats.largest_credit_amount")).alias("largest_credit_amount"),
            max(col("ats.largest_debit_amount")).alias("largest_debit_amount")
        )

    loan_summary_df = loan_payment_metrics_df \
        .groupBy(
            col("customer_id")
        ) \
        .agg(
            count(col("loan_id")).alias("total_loans"),
            sum(
                when(col("loan_status") == "ACTIVE", 1)
                .otherwise(0)
            ).alias("active_loans"),
            sum(col("loan_amount")).alias("total_loan_amount"),
            sum(col("total_amount_paid")).alias("total_loan_amount_paid"),
            sum(col("total_principal_paid")).alias("total_principal_paid"),
            sum(col("total_interest_paid")).alias("total_interest_paid"),
            sum(col("late_payment_count")).alias("late_payment_count"),
            sum(col("late_payment_amount")).alias("late_payment_amount"),
            avg(col("late_payment_rate")).alias("average_late_payment_rate"),
            max(col("late_payment_rate")).alias("max_late_payment_rate"),
            sum(col("remaining_principal_amount")).alias("total_remaining_principal_amount")
        )
    
    customer_transaction_details = account_details_df.alias("ad") \
        .join(
            customer_transaction_summary_df.alias("cts"),
            col("ad.customer_id") == col("cts.customer_id"),
            "inner"
        ).join(
            customer_details_df.alias("cd"),
            col("ad.customer_id") == col("cd.customer_id"),
            "inner"
        ).join(
            loan_summary_df.alias("lp"),
            col("ad.customer_id") == col("lp.customer_id"),
            "left"
        ).withColumn(
            "net_transaction_amount",
            col("cts.total_credit_amount") - col("cts.total_debit_amount")
        ).withColumn(
            "credit_debit_ratio",
            when(
                col("cts.total_debit_amount") > 0,
                col("cts.total_credit_amount") / col("cts.total_debit_amount")
            ).otherwise(None)
        ).withColumn(
            "transaction_to_income_ratio",
            when(
                col("cd.annual_income") > 0,
                col("cts.total_transaction_amount") / col("cd.annual_income")
            ).otherwise(None)
        ).withColumn(
            "average_monthly_transaction_amount",
            when(
                months_between(col("cts.last_transaction_date"), col("cts.first_transaction_date")) >=1,
                col("cts.total_transaction_amount") / floor(months_between(col("cts.last_transaction_date"), col("cts.first_transaction_date")))
            ).otherwise(col("cts.total_transaction_amount"))
        ).withColumn(
            "high_value_transaction_flag",
            when(col("cts.max_transaction_amount") > 50000, True)
            .otherwise(False)
        ).withColumn(
            "balance_to_income_ratio",
            when(
                col("cd.annual_income") > 0,
                col("ad.total_account_balance") / col("cd.annual_income")
            ).otherwise(None)
        ).withColumn(
            "account_activity_ratio",
            when(
                col("ad.total_accounts") > 0,
                col("ad.active_accounts") / col("ad.total_accounts")
            ).otherwise(None)
        ).withColumn(
            "loan_to_income_ratio",
            when(
                col("cd.annual_income") > 0,
                col("lp.total_loan_amount") /
                col("cd.annual_income")
            ).otherwise(None)
        )

    risk_feature_score = customer_transaction_details \
        .withColumn(
            "risk_score",
            when(col("cd.credit_score") < 600, 25).otherwise(0) +
            when(
                (col("credit_debit_ratio") < 0.5) |
                (col("credit_debit_ratio") > 3), 
                15
            ).otherwise(0) +
            when(col("transaction_to_income_ratio") > 5, 20).otherwise(0) +
            when(col("high_value_transaction_flag") == True, 15).otherwise(0) +
            when(col("balance_to_income_ratio") > 3, 10).otherwise(0) +
            when(col("account_activity_ratio") < 0.5, 5).otherwise(0) +
            when(col("lp.late_payment_count") > 0, 10).otherwise(0) +
            when(col("lp.average_late_payment_rate") > 0.20, 10).otherwise(0) +
            when(col("loan_to_income_ratio") > 3, 10).otherwise(0)
        )

    risk_feature_level = risk_feature_score \
        .withColumn(
            "risk_level",
            when(col("risk_score") >= 60, "HIGH") \
            .when(col("risk_score") >= 30, "MEDIUM") \
            .otherwise("LOW")
        ).withColumn(
            "risk_reason",
            concat_ws(
                "; ",
                when(col("cd.credit_score") < 600, "Low credit score"),
                when(
                    (col("credit_debit_ratio") < 0.5) |
                    (col("credit_debit_ratio") > 3.0),
                    "Unusual credit/debit ratio"
                ),
                when(col("transaction_to_income_ratio") > 5, "High transaction volume relative to income"),
                when(col("high_value_transaction_flag") == True, "High-value transaction activity"),
                when(col("balance_to_income_ratio") > 3, "High balance relative to income"),
                when(col("account_activity_ratio") < 0.5, "Low account activity"),
                when(col("lp.late_payment_count") > 0, "Late loan payment history"),
                when(col("lp.average_late_payment_rate") > 0.20, "High loan late payment rate"),
                when(col("loan_to_income_ratio") > 3, "High loan amount relative to income")
            )
        )

    final_df = risk_feature_level \
        .select(
            "cd.customer_id",
            "cd.gender",
            "cd.city",
            "cd.state",
            "cd.occupation",
            "cd.annual_income",
            "cd.credit_score",
            "cd.join_date",

            "ad.total_accounts",
            "ad.active_accounts",
            "ad.total_account_balance",
            "ad.largest_credit_amount",
            "ad.largest_debit_amount",

            "cts.total_transactions",
            "cts.total_transaction_amount",
            "cts.max_transaction_amount",
            "cts.total_credit_amount",
            "cts.total_debit_amount",
            "cts.first_transaction_date",
            "cts.last_transaction_date",
            "net_transaction_amount",

            "credit_debit_ratio",
            "transaction_to_income_ratio",
            "average_monthly_transaction_amount",
            "high_value_transaction_flag",
            "balance_to_income_ratio",
            "account_activity_ratio",

            col("lp.total_loans"),
            col("lp.active_loans"),
            col("lp.total_loan_amount"),
            col("lp.total_loan_amount_paid"),
            col("lp.total_principal_paid"),
            col("lp.total_interest_paid"),
            col("lp.late_payment_count"),
            col("lp.late_payment_amount"),
            col("lp.average_late_payment_rate"),
            col("lp.max_late_payment_rate"),
            col("lp.total_remaining_principal_amount"),
            col("loan_to_income_ratio"),

            "risk_score",
            "risk_level",
            "risk_reason"
        )

    return final_df

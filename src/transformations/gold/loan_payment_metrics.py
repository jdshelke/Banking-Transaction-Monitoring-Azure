from pyspark.sql.functions import col, count, sum, avg, min, max, when


def create_loan_payment_metrics(loans_df, loan_payments_df):

    loan_details_df = loans_df.select(
        "loan_id",
        "customer_id",
        "branch_id",
        "loan_type",
        col("status").alias("loan_status"),
        "loan_amount",
        "interest_rate",
        "term_months",
        "start_date"
    )

    payment_summary_df = loan_payments_df \
        .groupBy(col("loan_id")) \
        .agg(
            count(col("payment_id")).alias("total_payment_count"),
            sum(col("amount_paid")).alias("total_amount_paid"),
            sum(col("principal_component")).alias("total_principal_paid"),
            sum(col("interest_component")).alias("total_interest_paid"),
            avg(col("amount_paid")).alias("average_payment_amount"),
            min(col("amount_paid")).alias("min_payment_amount"),
            max(col("amount_paid")).alias("max_payment_amount"),
            min(col("payment_date")).alias("first_payment_date"),
            max(col("payment_date")).alias("last_payment_date"),
            sum(
                when(col("late_payment_flag") == True, 1)
                .otherwise(0)
            ).alias("late_payment_count"),
            sum(
                when(
                    col("late_payment_flag") == True,
                    col("amount_paid")
                )
                .otherwise(0)
            ).alias("late_payment_amount")
        )

    final_df = loan_details_df.alias("l") \
        .join(
            payment_summary_df.alias("p"),
            col("l.loan_id") == col("p.loan_id"),
            "left"
        ) \
        .withColumn(
            "late_payment_rate",
            when(
                col("p.total_payment_count") > 0,
                col("p.late_payment_count") /
                col("p.total_payment_count")
            ).otherwise(None)
        ) \
        .withColumn(
            "principal_paid_ratio",
            when(
                col("l.loan_amount") > 0,
                col("p.total_principal_paid") /
                col("l.loan_amount")
            ).otherwise(None)
        ) \
        .withColumn(
            "remaining_principal_amount",
            col("l.loan_amount") -
            col("p.total_principal_paid")
        ) \
        .select(
            col("l.loan_id"),
            col("l.customer_id"),
            col("l.branch_id"),
            col("l.loan_type"),
            col("l.loan_status"),
            col("l.loan_amount"),
            col("l.interest_rate"),
            col("l.term_months"),
            col("l.start_date"),

            col("p.total_payment_count"),
            col("p.total_amount_paid"),
            col("p.total_principal_paid"),
            col("p.total_interest_paid"),
            col("p.average_payment_amount"),
            col("p.min_payment_amount"),
            col("p.max_payment_amount"),
            col("p.first_payment_date"),
            col("p.last_payment_date"),
            col("p.late_payment_count"),
            col("p.late_payment_amount"),

            col("late_payment_rate"),
            col("principal_paid_ratio"),
            col("remaining_principal_amount")
        )

    return final_df
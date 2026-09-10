from pyspark.sql.functions import col, count, count_distinct, sum, avg, when


def create_branch_performance(branches_df, accounts_df, transactions_df, 
                              loans_df, employees_df, loan_payment_metrics_df
                              ):

    branch_details_df = branches_df.select(
        "branch_id",
        "branch_name",
        "city",
        "state"
    )

    account_summary_df = accounts_df \
        .groupBy(col("branch_id")) \
        .agg(
            count_distinct(col("customer_id")).alias("total_customers"),
            count(col("account_id")).alias("total_accounts"),
            sum(
                when(col("status") == "ACTIVE", 1)
                .otherwise(0)
            ).alias("active_accounts"),
            sum(col("balance")).alias("total_account_balance")
        )

    transaction_details_df = transactions_df.alias("t") \
        .join(
            accounts_df.alias("a"),
            col("t.account_id") == col("a.account_id"),
            "inner"
        )

    transaction_summary_df = transaction_details_df \
        .groupBy(col("a.branch_id")) \
        .agg(
            count(col("t.transaction_id")).alias("total_transactions"),
            sum(col("t.amount")).alias("total_transaction_amount"),
            avg(col("t.amount")).alias("average_transaction_amount"),
            sum(
                when(col("t.txn_type") == "DEPOSIT", 1)
                .when(col("t.txn_type") == "INTEREST CREDIT", 1)
                .when(col("t.txn_type") == "TRANSFER IN", 1)
                .otherwise(0)
            ).alias("credit_transaction_count"),
            sum(
                when(col("t.txn_type") == "WITHDRAWAL", 1)
                .when(col("t.txn_type") == "FEE DEBIT", 1)
                .when(col("t.txn_type") == "TRANSFER OUT", 1)
                .otherwise(0)
            ).alias("debit_transaction_count"),
            sum(
                when(col("t.txn_type") == "DEPOSIT", col("t.amount"))
                .when(col("t.txn_type") == "INTEREST CREDIT", col("t.amount"))
                .when(col("t.txn_type") == "TRANSFER IN", col("t.amount"))
                .otherwise(0)
            ).alias("total_credit_amount"),
            sum(
                when(col("t.txn_type") == "WITHDRAWAL", col("t.amount"))
                .when(col("t.txn_type") == "FEE DEBIT", col("t.amount"))
                .when(col("t.txn_type") == "TRANSFER OUT", col("t.amount"))
                .otherwise(0)
            ).alias("total_debit_amount")
        )

    transaction_summary_df = transaction_summary_df \
        .withColumn(
            "net_transaction_amount",
            col("total_credit_amount") - col("total_debit_amount")
        )

    loan_summary_df = loans_df \
        .groupBy(col("branch_id")) \
        .agg(
            count(col("loan_id")).alias("total_loans"),
            sum(col("loan_amount")).alias("total_loan_amount"),
            sum(
                when(col("status") == "ACTIVE", 1)
                .otherwise(0)
            ).alias("active_loans"),
            sum(
                when(col("status") == "CLOSED", 1)
                .otherwise(0)
            ).alias("closed_loans")
        )

    loan_payment_summary_df = loan_payment_metrics_df \
        .groupBy(col("branch_id")) \
        .agg(
            sum(col("total_payment_count")).alias("total_loan_payments"),
            sum(col("total_principal_paid")).alias("total_principal_paid"),
            sum(col("total_interest_paid")).alias("total_interest_paid"),
            sum(col("late_payment_count")).alias("late_payment_count")
        )

    employee_summary_df = employees_df \
        .groupBy(col("branch_id")) \
        .agg(
            count(col("employee_id")).alias("employee_count"),
            avg(col("salary")).alias("average_employee_salary")
        )

    final_df = branch_details_df.alias("b") \
        .join(
            account_summary_df.alias("a"),
            col("b.branch_id") == col("a.branch_id"),
            "left"
        ) \
        .join(
            transaction_summary_df.alias("t"),
            col("b.branch_id") == col("t.branch_id"),
            "left"
        ) \
        .join(
            loan_summary_df.alias("l"),
            col("b.branch_id") == col("l.branch_id"),
            "left"
        ) \
        .join(
            loan_payment_summary_df.alias("lp"),
            col("b.branch_id") == col("lp.branch_id"),
            "left"
        ) \
        .join(
            employee_summary_df.alias("e"),
            col("b.branch_id") == col("e.branch_id"),
            "left"
        ) \
        .select(
            col("b.branch_id"),
            col("b.branch_name"),
            col("b.city"),
            col("b.state"),

            col("a.total_customers"),
            col("a.total_accounts"),
            col("a.active_accounts"),
            col("a.total_account_balance"),

            col("t.total_transactions"),
            col("t.total_transaction_amount"),
            col("t.average_transaction_amount"),
            col("t.credit_transaction_count"),
            col("t.debit_transaction_count"),
            col("t.total_credit_amount"),
            col("t.total_debit_amount"),
            col("t.net_transaction_amount"),

            col("l.total_loans"),
            col("l.total_loan_amount"),
            col("l.active_loans"),
            col("l.closed_loans"),

            col("lp.total_loan_payments"),
            col("lp.total_principal_paid"),
            col("lp.total_interest_paid"),
            col("lp.late_payment_count"),

            col("e.employee_count"),
            col("e.average_employee_salary")
        )

    return final_df
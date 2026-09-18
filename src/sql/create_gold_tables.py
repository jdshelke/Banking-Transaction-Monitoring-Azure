def createGoldTables(spark, table_name):

    # Create Gold database
    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS banking.gold
    """)

    if table_name == "customer_transaction_summary":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.gold.customer_transaction_summary (
                customer_id BIGINT,
                customer_name STRING,

                total_transactions BIGINT,
                total_transaction_amount DECIMAL(28,2),
                average_transaction_amount DECIMAL(22,6),
                min_transaction_amount DECIMAL(18,2),
                max_transaction_amount DECIMAL(18,2),

                credit_transaction_count BIGINT,
                debit_transaction_count BIGINT,

                total_credit_amount DECIMAL(28,2),
                total_debit_amount DECIMAL(28,2),

                first_transaction_date TIMESTAMP,
                last_transaction_date TIMESTAMP,

                active_account_count BIGINT,
                gold_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/gold/customer_transaction_summary";
        """)

    elif table_name == "account_transaction_summary":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.gold.account_transaction_summary (
                account_id BIGINT,
                customer_id BIGINT,
                account_type STRING,
                account_status STRING,
                current_balance DECIMAL(18,2),

                total_transaction_count BIGINT,
                total_transaction_amount DECIMAL(28,2),
                min_transaction_amount DECIMAL(18,2),
                max_transaction_amount DECIMAL(18,2),
                average_transaction_amount DECIMAL(22,6),

                credit_transaction_count BIGINT,
                debit_transaction_count BIGINT,
                total_credit_amount DECIMAL(28,2),
                total_debit_amount DECIMAL(28,2),
                largest_credit_amount DECIMAL(18,2),
                largest_debit_amount DECIMAL(18,2),
                net_transaction_amount DECIMAL(29,2),

                first_transaction_date TIMESTAMP,
                last_transaction_date TIMESTAMP,

                gold_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION 'abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/gold/      account_transaction_summary'
        """)

    elif table_name == "customer_risk_profile":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.gold.customer_risk_profile (
                customer_id BIGINT,

                gender STRING,
                city STRING,
                state STRING,
                occupation STRING,
                annual_income DECIMAL(18,2),
                credit_score INT,
                join_date DATE,

                total_accounts BIGINT,
                active_accounts BIGINT,
                total_account_balance DECIMAL(28,2),
                largest_credit_amount DECIMAL(18,2),
                largest_debit_amount DECIMAL(18,2),

                total_transactions BIGINT,
                total_transaction_amount DECIMAL(28,2),
                max_transaction_amount DECIMAL(18,2),
                total_credit_amount DECIMAL(28,2),
                total_debit_amount DECIMAL(28,2),
                first_transaction_date TIMESTAMP,
                last_transaction_date TIMESTAMP,
                net_transaction_amount DECIMAL(29,2),

                credit_debit_ratio DECIMAL(38,10),
                transaction_to_income_ratio DECIMAL(38,10),
                average_monthly_transaction_amount DECIMAL(38,12),
                high_value_transaction_flag BOOLEAN,
                balance_to_income_ratio DECIMAL(38,10),
                account_activity_ratio DOUBLE,

                total_loans BIGINT,
                active_loans BIGINT,
                total_loan_amount DECIMAL(28,2),
                total_loan_amount_paid DECIMAL(38,2),
                total_principal_paid DECIMAL(38,2),
                total_interest_paid DECIMAL(38,2),
                late_payment_count BIGINT,
                late_payment_amount DECIMAL(38,2),
                average_late_payment_rate DOUBLE,
                max_late_payment_rate DOUBLE,
                total_remaining_principal_amount DECIMAL(38,2),
                loan_to_income_ratio DECIMAL(38,10),

                risk_score INT,
                risk_level STRING,
                risk_reason STRING,

                gold_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION 'abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/gold/customer_risk_profile'
        """)

    elif table_name == "loan_payment_metrics":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.gold.loan_payment_metrics (
                loan_id BIGINT,
                customer_id BIGINT,
                branch_id BIGINT,
                loan_type STRING,
                loan_status STRING,
                loan_amount DECIMAL(18,2),
                interest_rate DECIMAL(5,2),
                term_months INT,
                start_date DATE,

                total_payment_count BIGINT,
                total_amount_paid DECIMAL(28,2),
                total_principal_paid DECIMAL(28,2),
                total_interest_paid DECIMAL(28,2),
                average_payment_amount DECIMAL(22,6),
                min_payment_amount DECIMAL(18,2),
                max_payment_amount DECIMAL(18,2),
                first_payment_date DATE,
                last_payment_date DATE,

                late_payment_count BIGINT,
                late_payment_amount DECIMAL(28,2),
                late_payment_rate DOUBLE,
                principal_paid_ratio DECIMAL(38,10),
                remaining_principal_amount DECIMAL(29,2),

                gold_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION 'abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/gold/loan_payment_metrics'
        """)

    elif table_name == "daily_transaction_metrics":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.gold.daily_transaction_metrics (
                transaction_date DATE,

                total_transaction_count BIGINT,
                total_transaction_amount DECIMAL(28,2),
                average_transaction_amount DECIMAL(22,6),
                min_transaction_amount DECIMAL(18,2),
                max_transaction_amount DECIMAL(18,2),

                credit_transaction_count BIGINT,
                debit_transaction_count BIGINT,
                total_credit_amount DECIMAL(28,2),
                total_debit_amount DECIMAL(28,2),

                largest_credit_amount DECIMAL(18,2),
                largest_debit_amount DECIMAL(18,2),
                net_transaction_amount DECIMAL(29,2),

                unique_account_count BIGINT,
                unique_channel_count BIGINT,

                gold_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION 'abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/gold/daily_transaction_metrics'
        """)
    elif table_name == "branch_performance":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.gold.branch_performance (
                branch_id BIGINT,
                branch_name STRING,
                city STRING,
                state STRING,

                total_customers BIGINT,
                total_accounts BIGINT,
                active_accounts BIGINT,
                total_account_balance DECIMAL(28,2),

                total_transactions BIGINT,
                total_transaction_amount DECIMAL(28,2),
                average_transaction_amount DECIMAL(22,6),
                credit_transaction_count BIGINT,
                debit_transaction_count BIGINT,
                total_credit_amount DECIMAL(28,2),
                total_debit_amount DECIMAL(28,2),
                net_transaction_amount DECIMAL(29,2),

                total_loans BIGINT,
                total_loan_amount DECIMAL(28,2),
                active_loans BIGINT,
                closed_loans BIGINT,

                total_loan_payments BIGINT,
                total_principal_paid DECIMAL(38,2),
                total_interest_paid DECIMAL(38,2),
                late_payment_count BIGINT,

                employee_count BIGINT,
                average_employee_salary DECIMAL(22,6),

                gold_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION 'abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/gold/branch_performance'
        """)
        
    elif table_name == "card_fraud_summary":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.gold.card_fraud_summary (
                card_id BIGINT,
                customer_id BIGINT,
                card_type STRING,
                card_status STRING,
                credit_limit DECIMAL(18,2),

                total_card_transactions BIGINT,
                total_transaction_amount DECIMAL(28,2),
                average_transaction_amount DECIMAL(22,6),
                max_transaction_amount DECIMAL(18,2),

                fraud_transaction_count BIGINT,
                fraud_transaction_amount DECIMAL(28,2),
                average_fraud_amount DECIMAL(22,6),
                max_fraud_amount DECIMAL(18,2),
                fraud_transaction_rate DOUBLE,
                fraud_amount_rate DECIMAL(38,10),

                non_fraud_transaction_count BIGINT,
                non_fraud_transaction_amount DECIMAL(28,2),

                unique_merchant_category_count BIGINT,

                first_transaction_date DATE,
                last_transaction_date DATE,
                first_fraud_date DATE,
                last_fraud_date DATE,

                fraud_flag BOOLEAN,

                gold_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION 'abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/gold/card_fraud_summary'
        """)
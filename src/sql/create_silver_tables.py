def createSilverTables(spark, table_name):

    # Create Silver schema
    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS banking.silver
    """)
    

    if table_name == "customers":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.silver.customers (
                customer_id BIGINT,
                name STRING,
                gender STRING,
                date_of_birth DATE,
                city STRING,
                state STRING,
                phone STRING,
                email STRING,
                occupation STRING,
                annual_income DECIMAL(18,2),
                join_date DATE,
                credit_score INT,
                updated_at TIMESTAMP,
                silver_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/silver/customers";
        """)

    elif table_name == "loans":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.silver.loans (
                loan_id BIGINT,
                customer_id BIGINT,
                branch_id BIGINT,
                loan_type STRING,
                loan_amount DECIMAL(18,2),
                interest_rate DECIMAL(5,2),
                term_months INT,
                start_date DATE,
                status STRING,
                updated_at TIMESTAMP,
                silver_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/silver/loans";
        """)

    elif table_name == "support_tickets":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.silver.support_tickets (
                ticket_id BIGINT,
                customer_id BIGINT,
                issue_type STRING,
                date_opened DATE,
                date_resolved DATE,
                status STRING,
                satisfaction_score INT,
                updated_at TIMESTAMP,
                silver_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/silver/support_tickets";
        """)

    elif table_name == "transactions":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.silver.transactions (
                transaction_id BIGINT,
                account_id BIGINT,
                txn_date TIMESTAMP,
                txn_type STRING,
                amount DECIMAL(18,2),
                channel STRING,
                merchant_category STRING,
                updated_at TIMESTAMP,
                silver_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/silver/transactions";
        """)

    elif table_name == "employees":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.silver.employees (
                employee_id BIGINT,
                name STRING,
                branch_id BIGINT,
                role STRING,
                hire_date DATE,
                salary DECIMAL(18,2),
                updated_at TIMESTAMP,
                silver_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/silver/employees";
        """)

    elif table_name == "loan_payments":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.silver.loan_payments (
                payment_id BIGINT,
                loan_id BIGINT,
                payment_date DATE,
                amount_paid DECIMAL(18,2),
                principal_component DECIMAL(18,2),
                interest_component DECIMAL(18,2),
                late_payment_flag BOOLEAN,
                updated_at TIMESTAMP,
                silver_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/silver/loan_payments";
        """)

    elif table_name == "cards":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.silver.cards (
                card_id BIGINT,
                customer_id BIGINT,
                account_id BIGINT,
                card_type STRING,
                issue_date DATE,
                expiry_date DATE,
                credit_limit DECIMAL(18,2),
                status STRING,
                updated_at TIMESTAMP,
                silver_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/silver/cards";
        """)

    elif table_name == "card_transactions":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.silver.card_transactions (
                card_txn_id BIGINT,
                card_id BIGINT,
                txn_date DATE,
                merchant_category STRING,
                amount DECIMAL(18,2),
                is_fraud BOOLEAN,
                updated_at TIMESTAMP,
                silver_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/silver/card_transactions";
        """)

    elif table_name == "accounts":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.silver.accounts (
                account_id BIGINT,
                customer_id BIGINT,
                branch_id BIGINT,
                account_type STRING,
                balance DECIMAL(18,2),
                open_date DATE,
                status STRING,
                updated_at TIMESTAMP,
                silver_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/silver/accounts";
        """)

    elif table_name == "branches":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS banking.silver.branches (
                branch_id BIGINT,
                branch_name STRING,
                city STRING,
                state STRING,
                opened_date DATE,
                ifsc_code STRING,
                updated_at TIMESTAMP,
                silver_ingestion_time TIMESTAMP
            )
            USING DELTA
            LOCATION "abfss://banking-transaction-monitoring@project1storageacc.dfs.core.windows.net/silver/branches";
        """)
from delta.tables import DeltaTable
from src.utils import data_quality
from pyspark.sql.functions import current_timestamp
from src.sql.create_silver_tables import createSilverTables


class SilverWriter:

    def __init__(self, spark, base_path):
        self.spark = spark
        self.base_path = base_path

    def write_delta_table(self, df, table_name, layer):

        full_table_name = f"banking.silver.{table_name}"
        checkpoint_path = f"{self.base_path}/autoloader/checkpoint/{layer}/{table_name}"

        # Primary key for each source table
        primary_keys = {
            "accounts": "account_id",
            "branches": "branch_id",
            "card_transactions": "card_txn_id",
            "cards": "card_id",
            "customers": "customer_id",
            "employees": "employee_id",
            "loan_payments": "payment_id",
            "loans": "loan_id",
            "support_tickets": "ticket_id",
            "transactions": "transaction_id"
        }

        primary_key = primary_keys[table_name]

        # Add Silver audit column
        df = df.withColumn(
            "silver_ingestion_time",
            current_timestamp()
        )

        # Create Silver table
        createSilverTables(self.spark, table_name)

        # Upsert each micro-batch
        def upsert_to_silver(batch_df, batch_id):

            target_table = DeltaTable.forName(self.spark, full_table_name)
            batch_dedup_df = data_quality.remove_duplicates(batch_df, primary_key)
            
            target_table.alias("target") \
                .merge(
                    batch_dedup_df.alias("source"),
                    f"target.{primary_key} = source.{primary_key}"
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()

            print(f"Batch {batch_id} upserted successfully into {full_table_name}")
            
        query = df.writeStream \
            .foreachBatch(upsert_to_silver) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(once=True) \
            .start()

        query.awaitTermination()
        
        print(f"Upsert completed: {full_table_name}")



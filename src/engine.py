import os
import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

class GovernanceControlPlane:
    def __init__(self, producer_config_path: str, consumer_config_path: str):
        # Configure PySpark to run locally with native Delta Lake extensions
        builder = SparkSession.builder \
            .appName("mainframe-to-agentic-ai-controlplane") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        
        # Load declarative contracts
        with open(producer_config_path, 'r') as f:
            self.producer_contract = yaml.safe_load(f)
        with open(consumer_config_path, 'r') as f:
            self.consumer_contract = yaml.safe_load(f)
            
        # Set up local directory structures to emulate cloud buckets
        os.makedirs("data/bronze", exist_ok=True)
        os.makedirs("data/gold", exist_ok=True)
        os.makedirs("data/dlq/structural", exist_ok=True)
        os.makedirs("data/dlq/semantic", exist_ok=True)

    def generate_mock_mainframe_stream(self):
        """Simulates raw, fixed-width batch dumps dropped into the landing zone."""
        # Formats: TX_ID(12) + CUST_ID(10) + RAW_BALANCE(10) + TX_STATUS(8)
        raw_lines = [
            "TXN202600001" + "CUST990123" + "0055000000" + "ACTIVE  ",  # Valid HNW ($5,500,000.00)
            "TXN202600002" + "CUST990123" + "0012250050" + "ACTIVE  ",  # Valid HNW ($1,225,005.00)
            "TXN202600003" + "          " + "0000500000" + "ACTIVE  ",  # Structural Anomaly (Blank Cust ID)
            "TXN202600004" + "CUST112233" + "-005000000" + "ACTIVE  ",  # Semantic Anomaly (Negative Balance)
            "TXN202600005" + "CUST778899" + "0001500000" + "SUSPEND ",  # Bound Anomaly (Invalid status for AI)
        ]
        return self.spark.createDataFrame([(line,) for line in raw_lines], ["raw_line"])

    def execute_producer_gate(self, raw_df):
        """First Gate: Validates structural schema positional bounds based on Producer contract."""
        print("\n[GATE 1] Evaluating Producer Contract Constraints...")
        
        # Parse fixed-width lines using contract instructions
        parsed_df = raw_df.select(
            F.substring(F.col("raw_line"), 1, 12).alias("tx_id"),
            F.substring(F.col("raw_line"), 13, 10).alias("cust_id"),
            F.substring(F.col("raw_line"), 23, 10).alias("raw_balance"),
            F.trim(F.substring(F.col("raw_line"), 33, 8)).alias("tx_status")
        )
        
        # Condition: Enforce that primary keys and core identifiers are not padded blanks
        structural_condition = (F.trim(F.col("cust_id")) != "") & (F.col("tx_id").isNotNull())
        
        valid_bronze = parsed_df.filter(structural_condition)
        quarantined_dlq = parsed_df.filter(~structural_condition)\
            .withColumn("rejection_reason", F.lit("PRODUCER_VIOLATION: Structural Blank Business Key"))
            
        # Commit clean data to local simulated Bronze Delta table
        valid_bronze.write.format("delta").mode("overwrite").save("data/bronze")
        
        if quarantined_dlq.count() > 0:
            quarantined_dlq.write.format("json").mode("overwrite").save("data/dlq/structural")
            
        return self.spark.read.format("delta").load("data/bronze")

    def execute_consumer_gate(self, bronze_df):
        """Second Gate: Transforms data, checks semantic ranges, masks PII, emits to Gold."""
        print("[GATE 2] Evaluating Consumer Contract Constraints & Security Policies...")
        
        # 1. Translate legacy data formats into uniform cloud data types
        transformed_df = bronze_df.withColumn(
            "account_balance", F.col("raw_balance").cast(DoubleType()) / 100.0
        ).withColumn(
            "transaction_hash_id", F.col("tx_id")
        ).withColumn(
            "masked_customer_id", F.col("cust_id")
        ).withColumn(
            "risk_category",
            F.when(F.col("tx_status") == "ACTIVE", F.lit("LOW")).otherwise(F.lit("OUT_OF_BOUNDS"))
        )
        
        # 2. Extract acceptable business ranges directly out of the Consumer configuration contract
        allowed_bounds = self.consumer_contract['required_schema'][3]['allowed_values']
        semantic_condition = (F.col("account_balance") >= 0.0) & (F.col("risk_category").isin(allowed_bounds))
        
        valid_silver = transformed_df.filter(semantic_condition)
        quarantined_dlq = transformed_df.filter(~semantic_condition)\
            .withColumn("rejection_reason", 
                        F.when(F.col("account_balance") < 0.0, F.lit("CONSUMER_VIOLATION: Negative Account Balance"))
                         .otherwise(F.lit("CONSUMER_VIOLATION: Risk Bound Level Out of Scope")))
        
        if quarantined_dlq.count() > 0:
            quarantined_dlq.write.format("json").mode("overwrite").save("data/dlq/semantic")
            
        # 3. Apply Crypto-Masking Policies matching the security block rules
        masking_targets = self.consumer_contract['data_security']['masking_policy']['apply_sha256']
        for col_to_mask in masking_targets:
            if col_to_mask in valid_silver.columns:
                valid_silver = valid_silver.withColumn(
                    col_to_mask, F.sha2(F.trim(F.col(col_to_mask)), 256)
                )
                
        # Select exact schema requested by consumer contract
        final_gold_product = valid_silver.select(
            "transaction_hash_id", "masked_customer_id", "account_balance", "risk_category"
        )
        
        # Save to simulated enterprise Gold tier Delta Table
        final_gold_product.write.format("delta").mode("overwrite").save("data/gold")
        return self.spark.read.format("delta").load("data/gold")

    def run_experiment(self):
        # Step 1: Simulate Ingestion Inflow
        raw_stream = self.generate_mock_mainframe_stream()
        print("--- [RAW LANDING STREAM DATA] ---")
        raw_stream.show(truncate=False)
        
        # Step 2: Ingestion Protection Gate
        bronze_table = self.execute_producer_gate(raw_stream)
        
        # Step 3: Consumption Delivery Gate
        gold_product = self.execute_consumer_gate(bronze_table)
        
        print("\n👑 [GOLD DATA PRODUCT READY FOR AGENTIC RETRIEVAL]")
        gold_product.show(truncate=False)
        
        print("\n🛑 [QUARANTINE DLQ TARGET DATA]")
        if os.path.exists("data/dlq/structural"):
            self.spark.read.format("json").load("data/dlq/structural").select("tx_id", "rejection_reason").show(truncate=False)
        if os.path.exists("data/dlq/semantic"):
            self.spark.read.format("json").load("data/dlq/semantic").select("tx_id", "account_balance", "rejection_reason").show(truncate=False)
            
        self.spark.stop()

if __name__ == "__main__":
    engine = GovernanceControlPlane(
        producer_config_path="config/ccb_retail_tx_producer_v1.yaml",
        consumer_config_path="config/awm_risk_agent_consumer_v2.yaml"
    )
    engine.run_experiment()
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Initialize local Spark session using Delta's helper to pull JVM jars
builder = SparkSession.builder \
    .appName("pipeline-checker") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def check_layer(name, path):
    try:
        df = spark.read.format("delta").load(path)
        print(f"[{name}] Record Count: {df.count():,}")
        ##df.show(2, truncate=False)
    except Exception as e:
        print(f"[{name}] Could not load as Delta (likely JSON DLQ):")
        try:
            df_json = spark.read.format("json").load(path)
            print(f"[{name}] DLQ Record Count: {df_json.count():,}")
            ##df_json.show(2, truncate=False)
        except Exception as ex:
            print(f"[{name}] Path empty or inaccessible: {path}")

if __name__ == "__main__":
    check_layer("BRONZE LAYER", "data/bronze")
    check_layer("GOLD LAYER", "data/gold")
    check_layer("STRUCTURAL DLQ", "data/dlq/structural")
    check_layer("SEMANTIC DLQ", "data/dlq/semantic")
    
    spark.stop()
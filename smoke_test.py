from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# 1. Describe the Spark session we want
builder = (
    SparkSession.builder
    .appName("smoke-test")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

# 2. Add the Delta Lake library and start Spark
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 3. Make a tiny DataFrame: one column "id" with values 0 to 4
df = spark.range(5)

# 4. Save it as a Delta table
df.write.format("delta").mode("overwrite").save("/tmp/delta_smoke")

# 5. Read it back and count the rows
count = spark.read.format("delta").load("/tmp/delta_smoke").count()
print(f"Rows read back from Delta: {count}")

# 6. Shut Spark down cleanly
spark.stop()
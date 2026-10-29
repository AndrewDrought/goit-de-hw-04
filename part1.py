from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .appName("MyGoitSparkSandbox")
    .getOrCreate()
)

# Читання CSV та визначення типів стовпців.
nuek_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("./nuek-vuh3.csv")
)
nuek_repart = nuek_df.repartition(2)

nuek_processed = (
    nuek_repart
    .where("final_priority < 3")
    .select("unit_id", "final_priority")
    .groupBy("unit_id")
    .count()
)

nuek_processed = nuek_processed.where("count > 2")
nuek_processed.collect()

# Spark UI залишається доступним, доки програма очікує введення.
input("Spark UI: http://localhost:4040/jobs/ — Press Enter to finish...")
spark.stop()

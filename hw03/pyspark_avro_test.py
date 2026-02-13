from pyspark.sql import SparkSession, functions as F
from pathlib import Path
import shutil

out = Path("data/avro_test")
if out.exists():
    shutil.rmtree(out)

spark = SparkSession.builder.master("local[*]").config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.1.1").appName("avro-test").getOrCreate()

df = spark.range(0, 10).withColumn("x", (F.col("id") * 2).cast("int"))

# try write avro
df.write.mode("overwrite").format("avro").save(str(out))

# try read avro
df2 = spark.read.format("avro").load(str(out))
print("rows:", df2.count())

spark.stop()

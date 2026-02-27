# Фрагмент main.py (контекст эксперимента)
import time
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("BroadcastJoinExperiment")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

factN = 20_000_000
countries = 200_000
parts = 48
spark.conf.set("spark.sql.shuffle.partitions", str(parts))

# Генерация fact (для join используем только country_id и amount)
fact = (
    spark.range(factN)
    .select(
        (F.rand(1) * F.lit(countries)).cast("int").alias("country_id"),
        (F.rand(2) * F.lit(1000)).alias("amount"),
    )
    .repartition(parts)
)
fact_join = fact.persist(StorageLevel.DISK_ONLY)
fact_join.count()  # материализация

# Генерация dim (справочник)
dim_join = (
    spark.range(countries)
    .select(
        F.col("id").cast("int").alias("country_id"),
        F.concat(F.lit("region_"), (F.col("id") % F.lit(50))).alias("region"),
    )
    .repartition(parts)
    .cache()
)
dim_join.count()  # материализация

def query(f, d):
    return (
        f.join(d, on="country_id")
        .groupBy("region")
        .agg(F.sum("amount").alias("sum_amount"))
    )

def run(tag, df):
    print(f"\n===== {tag} =====")
    df.explain(True)
    t0 = time.time()
    df.count()
    print("Time sec:", round(time.time() - t0, 3))

# 1) Baseline: запретить авто-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
run("BASELINE (shuffle join)", query(fact_join, dim_join))

# 2) Optimized: принудительный broadcast справочника
run("OPTIMIZED (broadcast join)", query(fact_join, F.broadcast(dim_join)))

spark.stop()
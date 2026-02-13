import os, shutil, time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

BASE = Path("data_cal")
SEED = 42

def rm(p: Path):
    if p.exists():
        shutil.rmtree(p)

def dir_size_bytes(path: Path) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total

def timed(fn):
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0

def make_df(spark, n_rows: int):
    countries = ["NL","DE","FR","ES","IT","PL","SE","NO","BE","UK"]
    devices = ["ios","android","web","tablet","tv"]

    df = spark.range(0, n_rows).withColumnRenamed("id", "row_id")
    idx_country = (F.pmod(F.col("row_id"), F.lit(len(countries))) + F.lit(1)).cast("int")
    idx_device = (F.pmod(F.col("row_id"), F.lit(len(devices))) + F.lit(1)).cast("int")


    df = (df
        .withColumn("country", F.element_at(F.array([F.lit(x) for x in countries]), idx_country))
        .withColumn("device", F.element_at(F.array([F.lit(x) for x in devices]), idx_device))
        .withColumn("user_id", (F.col("row_id") % F.lit(5_000_000)).cast("long"))
        .withColumn("session_id", F.sha2(F.concat_ws("-", F.col("row_id"), F.lit(SEED)), 256))
        .withColumn("event_ts", (F.to_timestamp(F.lit("2025-01-01")) +
                                F.expr("INTERVAL 1 seconds") * (F.col("row_id") % F.lit(60*60*24*180))))
        .withColumn("amount", (F.rand(SEED) * 100).cast("double"))
        .withColumn("qty", (F.pmod(F.col("row_id"), F.lit(10)) + 1).cast("int"))
        .withColumn("score", (F.rand(SEED + 1)).cast("double"))
    )

    for i in range(1, 41):
        df = df.withColumn(f"m{i:02d}", (F.rand(SEED + i) * 1000).cast("double"))

    df = (df
        .withColumn("title", F.concat(F.lit("event-"), (F.col("row_id") % 1000).cast("string")))
        .withColumn("comment", F.concat(F.lit("note-"), F.substring(F.col("session_id"), 1, 24)))
    )
    return df

def write(df, fmt: str, out: Path):
    if fmt == "parquet":
        df.write.mode("overwrite").parquet(str(out))
    elif fmt == "json":
        df.write.mode("overwrite").json(str(out))
    elif fmt == "avro":
        df.write.mode("overwrite").format("avro").save(str(out))
    else:
        raise ValueError(fmt)

def main():
    spark = (SparkSession.builder
        .appName("calibrate")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.1.1")
        .getOrCreate())

    n_rows = 1_000_000
    df = make_df(spark, n_rows).repartition(8)

    for fmt in ["parquet", "avro", "json"]:
        out = BASE / fmt
        rm(out)
        t = timed(lambda: write(df, fmt, out))
        size = dir_size_bytes(out)
        print(f"{fmt}: write_s={t:.2f} size_mb={size/1024/1024:.1f}")

    spark.stop()

if __name__ == "__main__":
    main()

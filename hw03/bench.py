import os, shutil, time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

BASE = Path("data_bench")
SEED = 52

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
    out = fn()
    return out, (time.perf_counter() - t0)

def make_df(spark, n_rows: int):
    countries = ["NL","DE","FR","ES","IT","PL","SE","NO","BE","UK"]
    devices = ["ios","android","web","tablet","tv"]

    df = spark.range(0, n_rows).withColumnRenamed("id", "row_id")

    idx_country = (F.pmod(F.col("row_id"), F.lit(len(countries))) + F.lit(1)).cast("int")
    idx_device  = (F.pmod(F.col("row_id"), F.lit(len(devices))) + F.lit(1)).cast("int")

    df = (df
        .withColumn("country", F.element_at(F.array([F.lit(x) for x in countries]), idx_country))
        .withColumn("device",  F.element_at(F.array([F.lit(x) for x in devices]),  idx_device))
        .withColumn("user_id", (F.col("row_id") % F.lit(5_000_000)).cast("long"))
        .withColumn("session_id", F.sha2(F.concat_ws("-", F.col("row_id"), F.lit(SEED)), 256))
        .withColumn(
            "event_ts",
            (F.to_timestamp(F.lit("2025-01-01")) +
             F.expr("INTERVAL 1 seconds") * (F.col("row_id") % F.lit(60*60*24*180)))
        )
        .withColumn("amount", (F.rand(SEED) * 100).cast("double"))
        .withColumn("qty", (F.pmod(F.col("row_id"), F.lit(10)) + 1).cast("int"))
        .withColumn("score", (F.rand(SEED + 1)).cast("double"))
    )

    # extra numeric columns -> wide table (~50 cols total)
    for i in range(1, 41):
        df = df.withColumn(f"m{i:02d}", (F.rand(SEED + i) * 1000).cast("double"))

    # short strings
    df = (df
        .withColumn("title", F.concat(F.lit("event-"), (F.col("row_id") % 1000).cast("string")))
        .withColumn("comment", F.concat(F.lit("note-"), F.substring(F.col("session_id"), 1, 24)))
    )
    return df

def write_format(df, fmt: str, path: Path):
    if fmt == "parquet":
        df.write.mode("overwrite").parquet(str(path))
    elif fmt == "json":
        df.write.mode("overwrite").json(str(path))
    elif fmt == "avro":
        df.write.mode("overwrite").format("avro").save(str(path))
    else:
        raise ValueError(fmt)

def read_format(spark, fmt: str, path: Path):
    if fmt == "parquet":
        return spark.read.parquet(str(path))
    elif fmt == "json":
        return spark.read.json(str(path))
    elif fmt == "avro":
        return spark.read.format("avro").load(str(path))
    else:
        raise ValueError(fmt)

def run_read_workloads(spark, fmt: str, path: Path):
    # Full scan
    spark.catalog.clearCache()
    _, t_full = timed(lambda: read_format(spark, fmt, path).count())

    # Filter (typical selective predicate)
    spark.catalog.clearCache()
    _, t_filter = timed(lambda: read_format(spark, fmt, path)
                        .filter((F.col("country") == "NL") & (F.col("qty") >= 5))
                        .count())

    # Aggregation (typical DWH group-by)
    spark.catalog.clearCache()
    _, t_agg = timed(lambda: read_format(spark, fmt, path)
                     .groupBy("country")
                     .agg(F.sum("amount").alias("sum_amount"),
                          F.count("*").alias("cnt"))
                     .count())
    return t_full, t_filter, t_agg

def main():
    spark = (SparkSession.builder
        .appName("format-bench")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.1.1")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate())

    # Keep shuffle moderate for a laptop
    spark.conf.set("spark.sql.shuffle.partitions", "32")

    # n_rows = 4_000_000
    n_rows = 8_300_000
    df = make_df(spark, n_rows).repartition(4)

    # optional but recommended to avoid recompute across 3 writes
    df = df.persist()
    df.count()

    formats = ["parquet", "avro", "json"]
    results = []

    for fmt in formats:
        out_path = BASE / fmt
        rm(out_path)

        # WRITE (forces materialization)
        _, t_write = timed(lambda: write_format(df, fmt, out_path))
        size_b = dir_size_bytes(out_path)

        # READ workloads
        t_full, t_filter, t_agg = run_read_workloads(spark, fmt, out_path)

        results.append((fmt, size_b, t_write, t_full, t_filter, t_agg))

    print("\nRESULTS")
    print("fmt,size_gb,write_s,fullscan_s,filter_s,agg_s")
    for fmt, size_b, t_write, t_full, t_filter, t_agg in results:
        size_gb = size_b / (1024**3)
        print(f"{fmt},{size_gb:.3f},{t_write:.2f},{t_full:.2f},{t_filter:.2f},{t_agg:.2f}")

    spark.stop()

if __name__ == "__main__":
    main()

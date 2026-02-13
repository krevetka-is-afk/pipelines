import os
import shutil
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

BASE = Path("data")
SEED = 52

def rm(path: Path):
    if path.exists():
        shutil.rmtree(path)

def dir_size_bytes(path: Path) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            total += os.path.getsize(fp)
    return total

def timed(label, fn):
    t0 = time.perf_counter()
    out = fn()
    t1 = time.perf_counter()
    return out, (t1 - t0)

def make_df(spark, n_rows: int):
    # Base range gives us a stable row id
    df = spark.range(0, n_rows).withColumnRenamed("id", "row_id")

    # Low-cardinality categoricals
    countries = ["NL","DE","FR","ES","IT","PL","SE","NO","BE","UK"]
    devices = ["ios","android","web","tablet","tv"]

    df = (df
        .withColumn("country", F.element_at(F.array([F.lit(x) for x in countries]),
                                            (F.pmod(F.col("row_id"), F.lit(len(countries))) + 1)))
        .withColumn("device", F.element_at(F.array([F.lit(x) for x in devices]),
                                           (F.pmod(F.col("row_id"), F.lit(len(devices))) + 1)))
        # High-cardinality ids
        .withColumn("user_id", (F.col("row_id") % F.lit(5_000_000)).cast("long"))
        .withColumn("session_id", F.sha2(F.concat_ws("-", F.col("row_id"), F.lit(SEED)), 256))
        # Timestamp spread across days
        .withColumn("event_ts", (F.to_timestamp(F.lit("2025-01-01")) +
                                F.expr("INTERVAL 1 seconds") * (F.col("row_id") % F.lit(60*60*24*180))))
        # Numeric metrics
        .withColumn("amount", (F.rand(SEED) * 100).cast("double"))
        .withColumn("qty", (F.pmod(F.col("row_id"), F.lit(10)) + 1).cast("int"))
        .withColumn("score", (F.rand(SEED + 1)).cast("double"))
    )

    # Add extra numeric columns to reach ~50 cols total
    for i in range(1, 41):  # tweak count as needed
        df = df.withColumn(f"m{i:02d}", (F.rand(SEED + i) * 1000).cast("double"))

    # A few short text columns (affects size/compression)
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

def run_workloads(spark, fmt: str, path: Path):
    spark.catalog.clearCache()

    def full_scan():
        df = read_format(spark, fmt, path)
        return df.count()

    def filtered():
        df = read_format(spark, fmt, path)
        return df.filter((F.col("country") == "NL") & (F.col("qty") >= 5)).count()

    def aggregated():
        df = read_format(spark, fmt, path)
        out = (df.groupBy("country")
                 .agg(F.sum("amount").alias("sum_amount"),
                      F.count("*").alias("cnt")))
        return out.count()  # forces execution

    _, t_full = timed("full_scan", full_scan)
    _, t_filt = timed("filtered", filtered)
    _, t_agg  = timed("aggregated", aggregated)

    return t_full, t_filt, t_agg

def main():

    spark = (
    SparkSession.builder
    .appName("format-bench")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.1.1")
    .getOrCreate()
    )


    n_rows = 10_000_000  # starting point; adjust after you see real sizes
    df = make_df(spark, n_rows).repartition(8)

    formats = ["parquet", "json", "avro"]
    results = []

    for fmt in formats:
        out_path = BASE / fmt
        rm(out_path)

        # WRITE
        _, t_write = timed("write", lambda: write_format(df, fmt, out_path))
        size = dir_size_bytes(out_path)

        # READ workloads
        t_full, t_filt, t_agg = run_workloads(spark, fmt, out_path)

        results.append((fmt, size, t_write, t_full, t_filt, t_agg))

    print("\nRESULTS")
    print("fmt,size_bytes,write_s,fullscan_s,filter_s,agg_s")
    for r in results:
        print(",".join([str(x) for x in r]))

    spark.stop()

if __name__ == "__main__":
    main()

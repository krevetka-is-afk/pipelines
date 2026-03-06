from __future__ import annotations

import argparse
import contextlib
import csv
import json
import math
import statistics
import time
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Callable

from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window


DF_API = "DataFrame"
RDD_API = "RDD"
SQL_API = "SQL"


@dataclass(frozen=True)
class BenchmarkConfig:
    sizes: list[int]
    repeats: int
    warmup: int
    output_dir: Path
    master: str
    shuffle_partitions: int
    seed: int
    plan_sample_size: int


@dataclass(frozen=True)
class BenchmarkCase:
    key: str
    title: str
    family: str
    baseline_api: str
    apis: tuple[str, ...]
    description: str
    optimization_reason: str
    action_builder: Callable[[SparkSession, int, str], Callable[[], int]]
    plan_builder: Callable[[SparkSession, int, str], str]


def parse_sizes(value: str) -> list[int]:
    sizes = [int(item.strip()) for item in value.split(",") if item.strip()]
    if not sizes:
        raise ValueError("At least one size is required")
    return sizes


def capture_df_explain(df: DataFrame) -> str:
    buffer = StringIO()
    for mode in ("formatted", "codegen"):
        buffer.write(f"===== explain({mode}) =====\n")
        try:
            with contextlib.redirect_stdout(buffer):
                df.explain(mode=mode)
        except TypeError:
            # For compatibility with Spark versions where explain() may not
            # support keyword arguments for mode.
            with contextlib.redirect_stdout(buffer):
                df.explain(mode)
        except Exception as error:
            # Keep benchmarking resilient even if a specific explain mode fails.
            buffer.write(f"Failed to capture explain({mode}): {error}\n")
        buffer.write("\n")
    return buffer.getvalue()


def capture_rdd_explain(rdd: RDD) -> str:
    value = rdd.toDebugString()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def create_spark_session(config: BenchmarkConfig) -> SparkSession:
    spark = (
        SparkSession.builder.appName("hw06-spark-benchmarks")
        .master(config.master)
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def input_partitions(spark: SparkSession) -> int:
    return max(8, spark.sparkContext.defaultParallelism * 2)


def base_for_aggregations(spark: SparkSession, size: int) -> DataFrame:
    partitions = input_partitions(spark)
    return (
        spark.range(0, size, 1, numPartitions=partitions)
        .select(
            (F.col("id") % 4000).cast("int").alias("k"),
            (F.col("id") % 1000).cast("double").alias("v"),
            (F.col("id") % 3).cast("int").alias("g"),
        )
        .repartition(partitions, "k")
    )


def case_aggregations_action(spark: SparkSession, size: int, api: str) -> Callable[[], int]:
    if api == DF_API:

        def run_df() -> int:
            df = base_for_aggregations(spark, size)
            out = df.groupBy("k").agg(
                F.sum("v").alias("sum_v"),
                F.avg("v").alias("avg_v"),
                F.min("v").alias("min_v"),
                F.max("v").alias("max_v"),
                F.count("*").alias("cnt"),
            )
            return out.count()

        return run_df

    if api == RDD_API:

        def run_rdd() -> int:
            df = base_for_aggregations(spark, size)
            pair = df.rdd.map(lambda row: (int(row["k"]), float(row["v"])))
            sums = pair.reduceByKey(lambda a, b: a + b).collectAsMap()
            mins = pair.reduceByKey(lambda a, b: a if a <= b else b).collectAsMap()
            maxs = pair.reduceByKey(lambda a, b: a if a >= b else b).collectAsMap()
            counts = pair.mapValues(lambda _: 1).reduceByKey(lambda a, b: a + b).collectAsMap()
            avgs = {key: sums[key] / counts[key] for key in sums}
            return len(sums) + len(mins) + len(maxs) + len(avgs)

        return run_rdd

    raise ValueError(f"Unsupported API for aggregations case: {api}")


def case_aggregations_plan(spark: SparkSession, size: int, api: str) -> str:
    df = base_for_aggregations(spark, size)
    if api == DF_API:
        out = df.groupBy("k").agg(
            F.sum("v").alias("sum_v"),
            F.avg("v").alias("avg_v"),
            F.min("v").alias("min_v"),
            F.max("v").alias("max_v"),
            F.count("*").alias("cnt"),
        )
        return capture_df_explain(out)
    if api == RDD_API:
        pair = df.rdd.map(lambda row: (int(row["k"]), float(row["v"])))
        return capture_rdd_explain(pair)
    raise ValueError(f"Unsupported API for aggregations plan: {api}")


def base_for_window(spark: SparkSession, size: int) -> DataFrame:
    partitions = input_partitions(spark)
    return (
        spark.range(0, size, 1, numPartitions=partitions)
        .select(
            (F.col("id") % 3000).cast("int").alias("grp"),
            F.col("id").cast("long").alias("id"),
            (
                F.sin(F.col("id") / F.lit(10.0))
                + F.cos(F.col("id") / F.lit(17.0))
                + (F.col("id") % 100) / F.lit(10.0)
            ).alias("score"),
        )
        .repartition(partitions, "grp")
    )


def case_window_action(spark: SparkSession, size: int, api: str) -> Callable[[], int]:
    if api == DF_API:

        def run_df() -> int:
            df = base_for_window(spark, size)
            window = Window.partitionBy("grp").orderBy(F.desc("score"), F.asc("id"))
            top = df.withColumn("rn", F.row_number().over(window)).where(F.col("rn") <= 3)
            return top.count()

        return run_df

    if api == RDD_API:

        def run_rdd() -> int:
            df = base_for_window(spark, size)
            pair = df.rdd.map(lambda row: (int(row["grp"]), (float(row["score"]), int(row["id"]))))

            def top_n(items: list[tuple[float, int]]) -> list[tuple[float, int]]:
                return sorted(items, key=lambda value: (-value[0], value[1]))[:3]

            top = pair.groupByKey().flatMap(
                lambda kv: [(kv[0], item[1], item[0]) for item in top_n(list(kv[1]))]
            )
            return top.count()

        return run_rdd

    raise ValueError(f"Unsupported API for window case: {api}")


def case_window_plan(spark: SparkSession, size: int, api: str) -> str:
    df = base_for_window(spark, size)
    if api == DF_API:
        window = Window.partitionBy("grp").orderBy(F.desc("score"), F.asc("id"))
        top = df.withColumn("rn", F.row_number().over(window)).where(F.col("rn") <= 3)
        return capture_df_explain(top)
    if api == RDD_API:
        pair = df.rdd.map(lambda row: (int(row["grp"]), (float(row["score"]), int(row["id"]))))
        return capture_rdd_explain(pair)
    raise ValueError(f"Unsupported API for window plan: {api}")


def base_for_nested(spark: SparkSession, size: int) -> DataFrame:
    partitions = input_partitions(spark)
    return (
        spark.range(0, size, 1, numPartitions=partitions)
        .select(
            F.col("id").cast("long").alias("id"),
            F.struct(
                (F.col("id") % 7).cast("int").alias("a"),
                (F.col("id") % 11).cast("int").alias("b"),
                (F.col("id").cast("double") / F.lit(10.0)).alias("c"),
            ).alias("payload"),
            F.array(
                (F.col("id") % 5).cast("int"),
                ((F.col("id") + 1) % 5).cast("int"),
                ((F.col("id") + 2) % 5).cast("int"),
                ((F.col("id") + 3) % 5).cast("int"),
            ).alias("arr"),
        )
        .repartition(partitions)
    )


def case_nested_action(spark: SparkSession, size: int, api: str) -> Callable[[], int]:
    if api == DF_API:

        def run_df() -> int:
            df = base_for_nested(spark, size)
            out = df.select(
                "id",
                (F.col("payload.a") + F.col("payload.b")).alias("ab"),
                F.expr("transform(arr, x -> x * x)").alias("arr_sq"),
                F.expr("aggregate(arr, 0, (acc, x) -> acc + x)").alias("arr_sum"),
            ).where(F.col("arr_sum") >= 6)
            return out.count()

        return run_df

    if api == RDD_API:

        def run_rdd() -> int:
            df = base_for_nested(spark, size)
            out = (
                df.rdd.map(
                    lambda row: (
                        int(row["id"]),
                        int(row["payload"]["a"]) + int(row["payload"]["b"]),
                        [int(item) * int(item) for item in row["arr"]],
                        sum(int(item) for item in row["arr"]),
                    )
                )
                .filter(lambda item: item[3] >= 6)
            )
            return out.count()

        return run_rdd

    raise ValueError(f"Unsupported API for nested case: {api}")


def case_nested_plan(spark: SparkSession, size: int, api: str) -> str:
    df = base_for_nested(spark, size)
    if api == DF_API:
        out = df.select(
            "id",
            (F.col("payload.a") + F.col("payload.b")).alias("ab"),
            F.expr("transform(arr, x -> x * x)").alias("arr_sq"),
            F.expr("aggregate(arr, 0, (acc, x) -> acc + x)").alias("arr_sum"),
        ).where(F.col("arr_sum") >= 6)
        return capture_df_explain(out)
    if api == RDD_API:
        out = df.rdd.map(lambda row: (int(row["id"]), int(row["payload"]["a"]) + int(row["payload"]["b"])))
        return capture_rdd_explain(out)
    raise ValueError(f"Unsupported API for nested plan: {api}")


def base_for_projection(spark: SparkSession, size: int) -> DataFrame:
    partitions = input_partitions(spark)
    return (
        spark.range(0, size, 1, numPartitions=partitions)
        .select(
            F.col("id").cast("long").alias("id"),
            (F.col("id") % 97).cast("long").alias("a"),
            ((F.col("id") * 7) % 101).cast("long").alias("b"),
            ((F.col("id") * 13) % 89).cast("long").alias("c"),
        )
        .repartition(partitions)
    )


def recursive_sql_expr(depth: int) -> str:
    expression = "CAST(id AS BIGINT)"
    for idx in range(1, depth + 1):
        expression = f"pmod((({expression}) + {idx}) * {idx + 3}, 1000003)"
    return expression


def case_sql_projection_action(spark: SparkSession, size: int, api: str) -> Callable[[], int]:
    depth = 24
    if api == SQL_API:

        def run_sql() -> int:
            df = base_for_projection(spark, size)
            df.createOrReplaceTempView("projection_src")
            expr = recursive_sql_expr(depth)
            query = f"""
            SELECT
                pmod(a + b + c, 3) AS bucket,
                AVG({expr}) AS avg_score,
                MIN({expr}) AS min_score,
                MAX({expr}) AS max_score
            FROM projection_src
            GROUP BY pmod(a + b + c, 3)
            """
            return spark.sql(query).count()

        return run_sql

    if api == DF_API:

        def run_df() -> int:
            df = base_for_projection(spark, size)
            expression = F.col("id").cast("long")
            for idx in range(1, depth + 1):
                expression = F.pmod((expression + F.lit(idx)) * F.lit(idx + 3), F.lit(1_000_003))
                df = df.withColumn(f"step_{idx}", expression)
            out = df.groupBy(F.pmod(F.col("a") + F.col("b") + F.col("c"), F.lit(3)).alias("bucket")).agg(
                F.avg(F.col(f"step_{depth}")).alias("avg_score"),
                F.min(F.col(f"step_{depth}")).alias("min_score"),
                F.max(F.col(f"step_{depth}")).alias("max_score"),
            )
            return out.count()

        return run_df

    raise ValueError(f"Unsupported API for SQL projection case: {api}")


def case_sql_projection_plan(spark: SparkSession, size: int, api: str) -> str:
    depth = 24
    if api == SQL_API:
        df = base_for_projection(spark, size)
        df.createOrReplaceTempView("projection_src")
        expr = recursive_sql_expr(depth)
        query = f"""
        SELECT
            pmod(a + b + c, 3) AS bucket,
            AVG({expr}) AS avg_score,
            MIN({expr}) AS min_score,
            MAX({expr}) AS max_score
        FROM projection_src
        GROUP BY pmod(a + b + c, 3)
        """
        return capture_df_explain(spark.sql(query))

    if api == DF_API:
        df = base_for_projection(spark, size)
        expression = F.col("id").cast("long")
        for idx in range(1, depth + 1):
            expression = F.pmod((expression + F.lit(idx)) * F.lit(idx + 3), F.lit(1_000_003))
            df = df.withColumn(f"step_{idx}", expression)
        out = df.groupBy(F.pmod(F.col("a") + F.col("b") + F.col("c"), F.lit(3)).alias("bucket")).agg(
            F.avg(F.col(f"step_{depth}")).alias("avg_score"),
            F.min(F.col(f"step_{depth}")).alias("min_score"),
            F.max(F.col(f"step_{depth}")).alias("max_score"),
        )
        return capture_df_explain(out)

    raise ValueError(f"Unsupported API for SQL projection plan: {api}")


def base_for_conditional(spark: SparkSession, size: int) -> DataFrame:
    partitions = input_partitions(spark)
    return (
        spark.range(0, size, 1, numPartitions=partitions)
        .select(
            F.col("id").cast("long").alias("id"),
            (F.col("id") % 1000).cast("int").alias("metric"),
            (F.col("id") % 7).cast("int").alias("grp"),
        )
        .repartition(partitions)
    )


def case_sql_conditional_action(spark: SparkSession, size: int, api: str) -> Callable[[], int]:
    if api == SQL_API:

        def run_sql() -> int:
            df = base_for_conditional(spark, size)
            df.createOrReplaceTempView("conditional_src")
            query = """
            SELECT
                grp,
                CASE
                    WHEN metric < 200 THEN 'low'
                    WHEN metric < 600 THEN 'mid'
                    ELSE 'high'
                END AS segment,
                COUNT(*) AS cnt,
                AVG(metric) AS avg_metric
            FROM conditional_src
            GROUP BY
                grp,
                CASE
                    WHEN metric < 200 THEN 'low'
                    WHEN metric < 600 THEN 'mid'
                    ELSE 'high'
                END
            """
            return spark.sql(query).count()

        return run_sql

    if api == DF_API:

        def run_df() -> int:
            df = base_for_conditional(spark, size)
            low = df.filter(F.col("metric") < 200).select("grp", F.lit("low").alias("segment"), "metric")
            mid = df.filter((F.col("metric") >= 200) & (F.col("metric") < 600)).select(
                "grp", F.lit("mid").alias("segment"), "metric"
            )
            high = df.filter(F.col("metric") >= 600).select("grp", F.lit("high").alias("segment"), "metric")
            unioned = low.union(mid).union(high)
            out = unioned.groupBy("grp", "segment").agg(
                F.count("*").alias("cnt"),
                F.avg("metric").alias("avg_metric"),
            )
            return out.count()

        return run_df

    raise ValueError(f"Unsupported API for SQL conditional case: {api}")


def case_sql_conditional_plan(spark: SparkSession, size: int, api: str) -> str:
    if api == SQL_API:
        df = base_for_conditional(spark, size)
        df.createOrReplaceTempView("conditional_src")
        query = """
        SELECT
            grp,
            CASE
                WHEN metric < 200 THEN 'low'
                WHEN metric < 600 THEN 'mid'
                ELSE 'high'
            END AS segment,
            COUNT(*) AS cnt,
            AVG(metric) AS avg_metric
        FROM conditional_src
        GROUP BY
            grp,
            CASE
                WHEN metric < 200 THEN 'low'
                WHEN metric < 600 THEN 'mid'
                ELSE 'high'
            END
        """
        return capture_df_explain(spark.sql(query))

    if api == DF_API:
        df = base_for_conditional(spark, size)
        low = df.filter(F.col("metric") < 200).select("grp", F.lit("low").alias("segment"), "metric")
        mid = df.filter((F.col("metric") >= 200) & (F.col("metric") < 600)).select(
            "grp", F.lit("mid").alias("segment"), "metric"
        )
        high = df.filter(F.col("metric") >= 600).select("grp", F.lit("high").alias("segment"), "metric")
        unioned = low.union(mid).union(high)
        out = unioned.groupBy("grp", "segment").agg(F.count("*").alias("cnt"), F.avg("metric").alias("avg_metric"))
        return capture_df_explain(out)

    raise ValueError(f"Unsupported API for SQL conditional plan: {api}")


def get_cases() -> dict[str, BenchmarkCase]:
    return {
        "df_rdd_aggregations": BenchmarkCase(
            key="df_rdd_aggregations",
            title="Multiple Aggregations",
            family="df_vs_rdd",
            baseline_api=RDD_API,
            apis=(DF_API, RDD_API),
            description="groupBy+agg (sum/avg/min/max/count) vs several RDD passes",
            optimization_reason=(
                "Catalyst объединяет агрегации в один физический план, whole-stage codegen и "
                "Tungsten снижают накладные расходы; RDD делает несколько действий и больше сериализации."
            ),
            action_builder=case_aggregations_action,
            plan_builder=case_aggregations_plan,
        ),
        "df_rdd_window_topn": BenchmarkCase(
            key="df_rdd_window_topn",
            title="Window Top-N",
            family="df_vs_rdd",
            baseline_api=RDD_API,
            apis=(DF_API, RDD_API),
            description="row_number over partition/order vs groupByKey+sort on RDD",
            optimization_reason=(
                "Window-оператор выражен декларативно, Spark строит оптимизированный план сортировки и окна; "
                "RDD-вариант материализует группы в Python и сортирует вручную."
            ),
            action_builder=case_window_action,
            plan_builder=case_window_plan,
        ),
        "df_rdd_nested_types": BenchmarkCase(
            key="df_rdd_nested_types",
            title="Nested Types",
            family="df_vs_rdd",
            baseline_api=RDD_API,
            apis=(DF_API, RDD_API),
            description="struct/array built-in functions vs manual Python parsing",
            optimization_reason=(
                "Built-in функции по nested типам исполняются в JVM-плане с codegen; "
                "RDD уходит в Python-объекты и ручную обработку коллекций."
            ),
            action_builder=case_nested_action,
            plan_builder=case_nested_plan,
        ),
        "sql_df_projection_chain": BenchmarkCase(
            key="sql_df_projection_chain",
            title="SQL Projection vs withColumn Chain",
            family="sql_vs_df",
            baseline_api=DF_API,
            apis=(SQL_API, DF_API),
            description="single SQL projection vs long DataFrame withColumn chain",
            optimization_reason=(
                "Один SQL SELECT дает более компактный plan, тогда как длинная цепочка withColumn раздувает "
                "logical plan и увеличивает planning/execution overhead."
            ),
            action_builder=case_sql_projection_action,
            plan_builder=case_sql_projection_plan,
        ),
        "sql_df_case_vs_union": BenchmarkCase(
            key="sql_df_case_vs_union",
            title="SQL CASE vs DataFrame filter+union",
            family="sql_vs_df",
            baseline_api=DF_API,
            apis=(SQL_API, DF_API),
            description="single-pass CASE WHEN vs multi-branch filter+union",
            optimization_reason=(
                "SQL CASE выполняет классификацию в одном проходе; DataFrame filter+union делает несколько веток "
                "с повторными scan/shuffle и более тяжелым физическим планом."
            ),
            action_builder=case_sql_conditional_action,
            plan_builder=case_sql_conditional_plan,
        ),
    }


def benchmark_action(action: Callable[[], int], repeats: int, warmup: int) -> tuple[list[float], int]:
    last_rows = 0
    for _ in range(warmup):
        last_rows = int(action())

    durations: list[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        last_rows = int(action())
        durations.append(time.perf_counter() - started)
    return durations, last_rows


def format_seconds(value: float) -> str:
    return f"{value:.3f}"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    header_line = "| " + " | ".join(headers) + " |"
    separator_line = "| " + " | ".join(["---"] * len(headers)) + " |"
    body = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([header_line, separator_line, *body])


def build_report(config: BenchmarkConfig, summaries: list[dict[str, object]], cases: dict[str, BenchmarkCase]) -> str:
    by_case: dict[str, list[dict[str, object]]] = {}
    for row in summaries:
        by_case.setdefault(str(row["case"]), []).append(row)

    lines: list[str] = [
        "# HW06 Spark Benchmark Report",
        "",
        "## Environment",
        "",
        "- Python: 3.14 (via `python`)",
        "- Runner: `uv`",
        "- Spark dependency: `pyspark==4.1.1`",
        f"- Spark master: `{config.master}`",
        f"- shuffle partitions: `{config.shuffle_partitions}`",
        f"- Sizes: `{', '.join(str(s) for s in config.sizes)}`",
        f"- Warmup/Repeats: `{config.warmup}/{config.repeats}`",
        "",
        "## Summary",
        "",
        "- `DF vs RDD`: показатель `speedup_vs_baseline` = `RDD_median / API_median`.",
        "- `SQL vs DF`: показатель `speedup_vs_baseline` = `DF_median / API_median`.",
        "- Значение больше 1.0 означает, что API быстрее baseline.",
        "",
    ]

    for case_key, rows in by_case.items():
        rows_sorted = sorted(rows, key=lambda item: (int(item["size"]), str(item["api"])))
        case = cases[case_key]
        lines.append(f"## {case.title}")
        lines.append("")
        lines.append(f"- Case key: `{case.key}`")
        lines.append(f"- Description: {case.description}")
        lines.append(f"- Why faster: {case.optimization_reason}")
        lines.append("")

        table_rows: list[list[str]] = []
        for row in rows_sorted:
            table_rows.append(
                [
                    str(row["size"]),
                    str(row["api"]),
                    format_seconds(float(row["median_sec"])),
                    format_seconds(float(row["min_sec"])),
                    format_seconds(float(row["max_sec"])),
                    f"{float(row['speedup_vs_baseline']):.3f}",
                    str(row["result_rows"]),
                ]
            )

        lines.append(
            markdown_table(
                headers=[
                    "size",
                    "api",
                    "median_sec",
                    "min_sec",
                    "max_sec",
                    "speedup_vs_baseline",
                    "result_rows",
                ],
                rows=table_rows,
            )
        )
        lines.append("")
        lines.append(f"- Plans: `artifacts/plans/{case.key}_*.txt`")
        lines.append("")

    lines.extend(
        [
            "## API Selection Conclusions",
            "",
            "- Use `DataFrame` for structured transformations, aggregations, windows, and nested types.",
            "- Use `SQL` when complex projections/conditions are clearer as a single declarative query.",
            "- Use `RDD` only for low-level logic that cannot be expressed efficiently via SQL/DataFrame built-ins.",
            "",
        ]
    )

    return "\n".join(lines)


def run(config: BenchmarkConfig, selected_cases: list[str]) -> None:
    output_dir = config.output_dir
    plans_dir = output_dir / "plans"
    plans_dir.mkdir(parents=True, exist_ok=True)

    cases = get_cases()
    unknown = [name for name in selected_cases if name not in cases]
    if unknown:
        raise ValueError(f"Unknown cases: {', '.join(unknown)}")

    spark = create_spark_session(config)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    raw_rows: list[dict[str, object]] = []
    summaries: list[dict[str, object]] = []

    try:
        for case_key in selected_cases:
            case = cases[case_key]

            for api in case.apis:
                plan_text = case.plan_builder(spark, config.plan_sample_size, api)
                write_text(plans_dir / f"{case.key}_{api.lower()}.txt", plan_text)

            for size in config.sizes:
                medians: dict[str, float] = {}
                summary_refs: dict[str, dict[str, object]] = {}

                for api in case.apis:
                    action = case.action_builder(spark, size, api)
                    durations, result_rows = benchmark_action(action, repeats=config.repeats, warmup=config.warmup)

                    for run_idx, duration in enumerate(durations, start=1):
                        raw_rows.append(
                            {
                                "case": case.key,
                                "family": case.family,
                                "api": api,
                                "size": size,
                                "run": run_idx,
                                "duration_sec": duration,
                            }
                        )

                    median_sec = statistics.median(durations)
                    medians[api] = median_sec
                    summary = {
                        "case": case.key,
                        "title": case.title,
                        "family": case.family,
                        "api": api,
                        "size": size,
                        "median_sec": median_sec,
                        "min_sec": min(durations),
                        "max_sec": max(durations),
                        "result_rows": result_rows,
                        "speedup_vs_baseline": math.nan,
                    }
                    summaries.append(summary)
                    summary_refs[api] = summary

                baseline_value = medians[case.baseline_api]
                for api in case.apis:
                    summary_refs[api]["speedup_vs_baseline"] = baseline_value / medians[api]

    finally:
        spark.stop()

    raw_path = output_dir / "raw_results.csv"
    summary_path = output_dir / "summary_results.csv"
    report_path = output_dir / "report.md"
    metadata_path = output_dir / "metadata.json"

    with raw_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["case", "family", "api", "size", "run", "duration_sec"],
        )
        writer.writeheader()
        writer.writerows(raw_rows)

    with summary_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "case",
                "title",
                "family",
                "api",
                "size",
                "median_sec",
                "min_sec",
                "max_sec",
                "result_rows",
                "speedup_vs_baseline",
            ],
        )
        writer.writeheader()
        writer.writerows(summaries)

    report = build_report(config, summaries, cases)
    write_text(report_path, report)

    metadata = {
        "sizes": config.sizes,
        "repeats": config.repeats,
        "warmup": config.warmup,
        "master": config.master,
        "shuffle_partitions": config.shuffle_partitions,
        "plan_sample_size": config.plan_sample_size,
        "cases": selected_cases,
    }
    write_text(metadata_path, json.dumps(metadata, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HW06 Spark API benchmarks")
    parser.add_argument(
        "--cases",
        default="all",
        help="Comma-separated case keys or 'all'",
    )
    parser.add_argument(
        "--sizes",
        default="500000,2000000,5000000",
        help="Comma-separated input sizes",
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--shuffle-partitions", type=int, default=48)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--plan-sample-size", type=int, default=20000)
    parser.add_argument("--out", default="artifacts")
    parser.add_argument(
        "--list-cases",
        action="store_true",
        help="Print available case keys and exit",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    cases = get_cases()
    if args.list_cases:
        for key, case in cases.items():
            print(f"{key}: {case.description}")
        return

    selected = list(cases.keys()) if args.cases == "all" else [part.strip() for part in args.cases.split(",") if part.strip()]
    config = BenchmarkConfig(
        sizes=parse_sizes(args.sizes),
        repeats=args.repeats,
        warmup=args.warmup,
        output_dir=Path(args.out),
        master=args.master,
        shuffle_partitions=args.shuffle_partitions,
        seed=args.seed,
        plan_sample_size=args.plan_sample_size,
    )
    run(config=config, selected_cases=selected)


if __name__ == "__main__":
    main()

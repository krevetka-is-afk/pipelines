# HW06 Spark Benchmark Report

## Environment

- Python: 3.14 (via `python`)
- Runner: `uv`
- Spark dependency: `pyspark==4.1.1`
- Spark master: `local[*]`
- shuffle partitions: `48`
- Sizes: `2000000, 8000000, 20000000`
- Warmup/Repeats: `1/5`

## Summary

- `DF vs RDD`: показатель `speedup_vs_baseline` = `RDD_median / API_median`.
- `SQL vs DF`: показатель `speedup_vs_baseline` = `DF_median / API_median`.
- Значение больше 1.0 означает, что API быстрее baseline.

## Shuffle Count Parity (df_vs_rdd)

- Counts are marker-based from saved plans: `Exchange (` for DataFrame and `ShuffledRDD[` for RDD.
- Plan sample size: `20000`.

| case | dataframe_shuffle_markers | rdd_shuffle_markers | parity |
| --- | --- | --- | --- |
| df_rdd_aggregations | 1 | 1 | yes |
| df_rdd_window_topn | 1 | 1 | yes |
| df_rdd_nested_types | 1 | 0 | no |

## Multiple Aggregations

- Case key: `df_rdd_aggregations`
- Description: groupBy+agg (sum/avg/min/max/count) vs one-pass RDD aggregateByKey
- Why faster: Catalyst объединяет агрегации в один физический план, whole-stage codegen и Tungsten снижают накладные расходы; RDD все равно платит за Python-сериализацию и не использует Catalyst.

| size | api | median_sec | min_sec | max_sec | speedup_vs_baseline | result_rows |
| --- | --- | --- | --- | --- | --- | --- |
| 2000000 | DataFrame | 0.258 | 0.250 | 0.281 | 2.392 | 4000 |
| 2000000 | RDD | 0.618 | 0.585 | 0.648 | 1.000 | 4000 |
| 8000000 | DataFrame | 0.239 | 0.227 | 0.241 | 7.972 | 4000 |
| 8000000 | RDD | 1.908 | 1.814 | 1.933 | 1.000 | 4000 |
| 20000000 | DataFrame | 0.232 | 0.229 | 0.253 | 18.690 | 4000 |
| 20000000 | RDD | 4.336 | 4.315 | 4.454 | 1.000 | 4000 |

- Plans: `artifacts/plans/df_rdd_aggregations_*.txt`

## Window Top-N

- Case key: `df_rdd_window_topn`
- Description: row_number over partition/order vs RDD aggregateByKey(top-N)
- Why faster: Window-оператор выражен декларативно, Spark строит оптимизированный план сортировки и окна; RDD-вариант делает топ-N через Python-комбайнеры и не получает SQL/Catalyst-оптимизации.

| size | api | median_sec | min_sec | max_sec | speedup_vs_baseline | result_rows |
| --- | --- | --- | --- | --- | --- | --- |
| 2000000 | DataFrame | 0.311 | 0.296 | 0.321 | 2.519 | 9000 |
| 2000000 | RDD | 0.784 | 0.716 | 0.882 | 1.000 | 9000 |
| 8000000 | DataFrame | 0.416 | 0.383 | 0.439 | 6.198 | 9000 |
| 8000000 | RDD | 2.576 | 2.526 | 2.630 | 1.000 | 9000 |
| 20000000 | DataFrame | 1.033 | 1.011 | 1.123 | 5.950 | 9000 |
| 20000000 | RDD | 6.146 | 5.984 | 6.293 | 1.000 | 9000 |

- Plans: `artifacts/plans/df_rdd_window_topn_*.txt`

## Nested Types

- Case key: `df_rdd_nested_types`
- Description: struct/array built-in functions vs manual Python parsing
- Why faster: Built-in функции по nested типам исполняются в JVM-плане с codegen; RDD уходит в Python-объекты и ручную обработку коллекций.

| size | api | median_sec | min_sec | max_sec | speedup_vs_baseline | result_rows |
| --- | --- | --- | --- | --- | --- | --- |
| 2000000 | DataFrame | 0.218 | 0.212 | 0.220 | 6.994 | 2000000 |
| 2000000 | RDD | 1.522 | 1.478 | 1.582 | 1.000 | 2000000 |
| 8000000 | DataFrame | 0.380 | 0.372 | 0.495 | 14.454 | 8000000 |
| 8000000 | RDD | 5.496 | 5.425 | 5.614 | 1.000 | 8000000 |
| 20000000 | DataFrame | 0.930 | 0.860 | 1.078 | 14.487 | 20000000 |
| 20000000 | RDD | 13.470 | 13.168 | 13.697 | 1.000 | 20000000 |

- Plans: `artifacts/plans/df_rdd_nested_types_*.txt`

## SQL Projection vs withColumn Chain

- Case key: `sql_df_projection_chain`
- Description: single SQL projection vs long DataFrame withColumn chain
- Why faster: Один SQL SELECT дает более компактный plan, тогда как длинная цепочка withColumn раздувает logical plan и увеличивает planning/execution overhead.

| size | api | median_sec | min_sec | max_sec | speedup_vs_baseline | result_rows |
| --- | --- | --- | --- | --- | --- | --- |
| 2000000 | DataFrame | 0.640 | 0.613 | 0.690 | 1.000 | 3 |
| 2000000 | SQL | 0.376 | 0.363 | 0.385 | 1.704 | 3 |
| 8000000 | DataFrame | 0.959 | 0.946 | 0.980 | 1.000 | 3 |
| 8000000 | SQL | 0.712 | 0.696 | 0.767 | 1.348 | 3 |
| 20000000 | DataFrame | 1.930 | 1.768 | 2.009 | 1.000 | 3 |
| 20000000 | SQL | 1.600 | 1.559 | 1.650 | 1.207 | 3 |

- Plans: `artifacts/plans/sql_df_projection_chain_*.txt`

## SQL CASE vs DataFrame filter+union

- Case key: `sql_df_case_vs_union`
- Description: single-pass CASE WHEN vs multi-branch filter+union
- Why faster: SQL CASE выполняет классификацию в одном проходе; DataFrame filter+union делает несколько веток с повторными scan/shuffle и более тяжелым физическим планом.

| size | api | median_sec | min_sec | max_sec | speedup_vs_baseline | result_rows |
| --- | --- | --- | --- | --- | --- | --- |
| 2000000 | DataFrame | 0.478 | 0.449 | 0.544 | 1.000 | 21 |
| 2000000 | SQL | 0.329 | 0.311 | 0.357 | 1.451 | 21 |
| 8000000 | DataFrame | 0.595 | 0.591 | 0.620 | 1.000 | 21 |
| 8000000 | SQL | 0.656 | 0.632 | 0.744 | 0.907 | 21 |
| 20000000 | DataFrame | 0.863 | 0.814 | 1.064 | 1.000 | 21 |
| 20000000 | SQL | 1.380 | 1.332 | 1.429 | 0.626 | 21 |

- Plans: `artifacts/plans/sql_df_case_vs_union_*.txt`

## API Selection Conclusions

- Use `DataFrame` for structured transformations, aggregations, windows, and nested types.
- Use `SQL` when complex projections/conditions are clearer as a single declarative query.
- Use `RDD` only for low-level logic that cannot be expressed efficiently via SQL/DataFrame built-ins.

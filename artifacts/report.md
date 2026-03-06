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

## Multiple Aggregations

- Case key: `df_rdd_aggregations`
- Description: groupBy+agg (sum/avg/min/max/count) vs several RDD passes
- Why faster: Catalyst объединяет агрегации в один физический план, whole-stage codegen и Tungsten снижают накладные расходы; RDD делает несколько действий и больше сериализации.

| size | api | median_sec | min_sec | max_sec | speedup_vs_baseline | result_rows |
| --- | --- | --- | --- | --- | --- | --- |
| 2000000 | DataFrame | 0.252 | 0.214 | 0.288 | 9.559 | 4000 |
| 2000000 | RDD | 2.413 | 2.292 | 2.528 | 1.000 | 16000 |
| 8000000 | DataFrame | 0.286 | 0.279 | 0.460 | 27.080 | 4000 |
| 8000000 | RDD | 7.754 | 7.277 | 8.086 | 1.000 | 16000 |
| 20000000 | DataFrame | 0.448 | 0.422 | 0.460 | 42.098 | 4000 |
| 20000000 | RDD | 18.865 | 18.602 | 19.826 | 1.000 | 16000 |

- Plans: `artifacts/plans/df_rdd_aggregations_*.txt`

## Window Top-N

- Case key: `df_rdd_window_topn`
- Description: row_number over partition/order vs groupByKey+sort on RDD
- Why faster: Window-оператор выражен декларативно, Spark строит оптимизированный план сортировки и окна; RDD-вариант материализует группы в Python и сортирует вручную.

| size | api | median_sec | min_sec | max_sec | speedup_vs_baseline | result_rows |
| --- | --- | --- | --- | --- | --- | --- |
| 2000000 | DataFrame | 0.367 | 0.361 | 0.377 | 2.623 | 9000 |
| 2000000 | RDD | 0.964 | 0.927 | 0.993 | 1.000 | 9000 |
| 8000000 | DataFrame | 0.839 | 0.777 | 1.096 | 3.973 | 9000 |
| 8000000 | RDD | 3.334 | 3.039 | 3.455 | 1.000 | 9000 |
| 20000000 | DataFrame | 1.809 | 1.745 | 2.395 | 4.313 | 9000 |
| 20000000 | RDD | 7.804 | 7.710 | 7.917 | 1.000 | 9000 |

- Plans: `artifacts/plans/df_rdd_window_topn_*.txt`

## Nested Types

- Case key: `df_rdd_nested_types`
- Description: struct/array built-in functions vs manual Python parsing
- Why faster: Built-in функции по nested типам исполняются в JVM-плане с codegen; RDD уходит в Python-объекты и ручную обработку коллекций.

| size | api | median_sec | min_sec | max_sec | speedup_vs_baseline | result_rows |
| --- | --- | --- | --- | --- | --- | --- |
| 2000000 | DataFrame | 0.211 | 0.207 | 0.219 | 7.384 | 2000000 |
| 2000000 | RDD | 1.558 | 1.519 | 1.577 | 1.000 | 2000000 |
| 8000000 | DataFrame | 0.391 | 0.382 | 0.407 | 15.111 | 8000000 |
| 8000000 | RDD | 5.912 | 5.767 | 6.053 | 1.000 | 8000000 |
| 20000000 | DataFrame | 0.953 | 0.893 | 1.094 | 14.437 | 20000000 |
| 20000000 | RDD | 13.761 | 13.721 | 14.177 | 1.000 | 20000000 |

- Plans: `artifacts/plans/df_rdd_nested_types_*.txt`

## SQL Projection vs withColumn Chain

- Case key: `sql_df_projection_chain`
- Description: single SQL projection vs long DataFrame withColumn chain
- Why faster: Один SQL SELECT дает более компактный plan, тогда как длинная цепочка withColumn раздувает logical plan и увеличивает planning/execution overhead.

| size | api | median_sec | min_sec | max_sec | speedup_vs_baseline | result_rows |
| --- | --- | --- | --- | --- | --- | --- |
| 2000000 | DataFrame | 0.676 | 0.645 | 0.712 | 1.000 | 3 |
| 2000000 | SQL | 0.381 | 0.360 | 0.424 | 1.775 | 3 |
| 8000000 | DataFrame | 0.975 | 0.964 | 1.149 | 1.000 | 3 |
| 8000000 | SQL | 0.707 | 0.698 | 0.807 | 1.380 | 3 |
| 20000000 | DataFrame | 1.896 | 1.825 | 1.922 | 1.000 | 3 |
| 20000000 | SQL | 1.646 | 1.609 | 1.749 | 1.151 | 3 |

- Plans: `artifacts/plans/sql_df_projection_chain_*.txt`

## SQL CASE vs DataFrame filter+union

- Case key: `sql_df_case_vs_union`
- Description: single-pass CASE WHEN vs multi-branch filter+union
- Why faster: SQL CASE выполняет классификацию в одном проходе; DataFrame filter+union делает несколько веток с повторными scan/shuffle и более тяжелым физическим планом.

| size | api | median_sec | min_sec | max_sec | speedup_vs_baseline | result_rows |
| --- | --- | --- | --- | --- | --- | --- |
| 2000000 | DataFrame | 0.464 | 0.445 | 0.562 | 1.000 | 21 |
| 2000000 | SQL | 0.363 | 0.327 | 0.399 | 1.277 | 21 |
| 8000000 | DataFrame | 0.605 | 0.582 | 0.658 | 1.000 | 21 |
| 8000000 | SQL | 0.674 | 0.663 | 0.707 | 0.898 | 21 |
| 20000000 | DataFrame | 0.916 | 0.907 | 0.937 | 1.000 | 21 |
| 20000000 | SQL | 1.533 | 1.457 | 1.551 | 0.598 | 21 |

- Plans: `artifacts/plans/sql_df_case_vs_union_*.txt`

## API Selection Conclusions

- Use `DataFrame` for structured transformations, aggregations, windows, and nested types.
- Use `SQL` when complex projections/conditions are clearer as a single declarative query.
- Use `RDD` only for low-level logic that cannot be expressed efficiently via SQL/DataFrame built-ins.

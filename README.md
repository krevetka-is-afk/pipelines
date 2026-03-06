# HW06: Spark API Performance (DataFrame vs RDD, SQL vs DataFrame)

[Ссылка на репозиторий](https://github.com/krevetka-is-afk/pipelines/tree/hw06)

## Что реализовано

Сделаны все требуемые сценарии:

1. `df_rdd_aggregations`:
   `DataFrame.groupBy().agg(sum, avg, min, max, count)` против one-pass `RDD.aggregateByKey`.
2. `df_rdd_window_topn`:
   `row_number() over(partition/order)` против `RDD.aggregateByKey(top-N)`.
3. `df_rdd_nested_types`:
   `struct/array + built-in functions` против ручной обработки вложенных структур в RDD.
4. `sql_df_projection_chain`:
   SQL `SELECT` против длинной цепочки `DataFrame.withColumn`.
5. `sql_df_case_vs_union`:
   SQL `CASE WHEN` против `DataFrame filter + union`.

Для каждого кейса сохраняются планы выполнения:

- `explain("formatted")` и `explain("codegen")` для DataFrame/SQL;
- `toDebugString` для RDD.

## Запуск (uv + python 3.14)

```bash
uv sync
uv run python main.py --list-cases
uv run python main.py --cases all --sizes 100000,300000 --repeats 2 --warmup 1 --shuffle-partitions 16 --out artifacts
```

Рекомендуемый «полный» прогон для защиты:

```bash
uv run python main.py --cases all --sizes 2000000,8000000,20000000 --repeats 5 --warmup 1 --shuffle-partitions 48 --out artifacts
```

## Где лежит отчет

- Основной отчет с таблицами: `artifacts/report.md`
- Сводные данные: `artifacts/summary_results.csv`
- Сырые прогоны: `artifacts/raw_results.csv`
- Планы: `artifacts/plans/*.txt`

## Результаты последнего полного прогона

Параметры: `sizes=2000000,8000000,20000000`, `repeats=5`, `warmup=1`, `shuffle_partitions=48`.

| Кейс | Что сравнивали | Наблюдение по median |
| --- | --- | --- |
| `df_rdd_aggregations` | DataFrame vs RDD | DataFrame быстрее в ~2.4x-18.7x |
| `df_rdd_window_topn` | DataFrame vs RDD | DataFrame быстрее в ~2.5x-6.2x |
| `df_rdd_nested_types` | DataFrame vs RDD | DataFrame быстрее в ~7.0x-14.5x |
| `sql_df_projection_chain` | SQL vs DataFrame API | SQL быстрее в ~1.21x-1.70x |
| `sql_df_case_vs_union` | SQL vs DataFrame API | SQL быстрее на 2M (~1.45x), но медленнее на 8M и 20M |

## Shuffle parity (df_vs_rdd)

Подсчет по сохраненным планам: `Exchange (` для DataFrame и `ShuffledRDD[` для RDD.

| Кейс | DataFrame shuffle markers | RDD shuffle markers | parity |
| --- | --- | --- | --- |
| `df_rdd_aggregations` | 1 | 1 | yes |
| `df_rdd_window_topn` | 1 | 1 | yes |
| `df_rdd_nested_types` | 1 | 0 | no |

## Объяснение выигрыша (Catalyst/Tungsten)

1. **Множественные агрегации (DF > RDD)**
   Catalyst объединяет агрегаты в единый физический план, а whole-stage codegen/Tungsten снижают накладные расходы исполнения. Даже при one-pass `aggregateByKey` RDD-ветка платит за Python-сериализацию и не использует Catalyst.

2. **Оконные функции (DF > RDD)**
   `row_number over(...)` выражается декларативно и выполняется в оптимизированном плане Spark. RDD-вариант делает top-N через Python-комбайнеры и все равно не получает SQL/Catalyst-оптимизации.

3. **Вложенные типы (DF > RDD)**
   Built-in функции (`transform`, `aggregate`, доступ к `struct`) выполняются внутри JVM-плана с codegen. RDD-ветка уходит в Python-объекты и ручную обработку.

4. **SQL projection vs withColumn chain (SQL > DF API)**
   Один SQL `SELECT` дает более компактный план, чем длинная цепочка `withColumn`, где растет стоимость анализа/планирования и размер дерева выражений.

5. **SQL CASE vs filter+union (SQL > DF API)**
   SQL `CASE` выполняет классификацию в одном проходе. DataFrame-реализация через `filter + union` запускает несколько веток и усложняет физический план.

## Вывод: когда выбирать API

- Выбирайте **DataFrame** для структурированных задач (агрегации, окна, nested types).
- Выбирайте **SQL**, когда выражение проще и компактнее записать одной декларативной конструкцией (`CASE`, широкие проекции, CTE).
- Используйте **RDD** только если задача действительно низкоуровневая и плохо выражается built-in операторами Spark.

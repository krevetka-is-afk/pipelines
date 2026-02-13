# RESULTS

## 1️⃣ Introduction

### Goal of the work

* Compare performance of Parquet, Avro, JSON on ~10GB dataset
* Measure:

  * Storage size
  * Write time
  * Read time (full scan, filter, aggregation)

### Research question

* How storage format affects IO and analytical workload performance

## 2️⃣ Experimental Setup

### Environment

* macOS
* Spark 4.1.1 (local mode)
* Java 17
* 8GB RAM
* PySpark

### Dataset Design

* 8,300,000 rows
* ~50 columns
* Column types:

  * Low-cardinality categorical (`country`, `device`)
  * High-cardinality identifiers (`user_id`, `session_id`)
  * Numeric metrics (`amount`, `qty`, `score`)
  * Timestamp
  * Additional 40 numeric columns
* Total JSON size ≈ 10.72 GB

Explain:

> Dataset was intentionally wide to demonstrate column pruning effects in columnar storage.

## 3️⃣ Methodology

### Measured metrics

* File size on disk
* Write time
* Read workloads:

  * Full scan (`count`)
  * Filter (`country='NL' AND qty>=5`)
  * Aggregation (`groupBy(country).agg(sum(amount), count(*))`)

### Fairness controls

* Same dataset for all formats
* Same Spark configuration
* Data cached before benchmarking (`persist + count`)
* Same partitioning

## 4️⃣ Results

| Format  | Size (GB) | Write (s) | Full Scan (s) | Filter (s) | Aggregation (s) |
| ------- | --------- | --------- | ------------- | ---------- | --------------- |
| Parquet | 3.490     | 8.65      | 0.32          | 0.28       | 0.45            |
| Avro    | 3.470     | 18.17     | 1.86          | 1.25       | 0.72            |
| JSON    | 9.985     | 34.89     | 12.42         | 10.64      | 11.40           |

## 5️⃣ Analysis

### Storage

* JSON 2.87× larger than Parquet/Avro
* Text overhead dominates

### Write Performance

* Parquet fastest
* JSON slowest due to serialization + size

### Read Performance

* Parquet dramatically faster in scan/filter/aggregation
* Avro intermediate
* JSON worst

Explain:

* Columnar layout enables column pruning
* Row-based formats require full-row reads
* Larger file size increases IO

## 6️⃣ Conclusion

Example strong conclusion:

> Parquet is the most efficient format for analytical workloads due to its columnar storage, compression, and column pruning capabilities.
> Avro provides better performance than JSON but remains less efficient for analytical queries because it is row-oriented.
> JSON is unsuitable for large-scale analytical pipelines due to excessive storage overhead and poor read performance.

## Raw results

4_000_000 rows

```csv
fmt,size_gb,write_s,fullscan_s,filter_s,agg_s
parquet,1.681,5.13,0.30,0.24,0.39
avro,1.671,23.09,1.74,0.56,0.64
json,4.811,47.73,9.04,2.52,3.49
```

8_300_000 rows

```csv
fmt,size_gb,write_s,fullscan_s,filter_s,agg_s
parquet,3.490,8.65,0.32,0.28,0.45
avro,3.470,18.17,1.86,1.25,0.72
json,9.985,34.89,12.42,10.64,11.40
```

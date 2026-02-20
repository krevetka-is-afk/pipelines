import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

object Main {

  // -------------------------
  // Config / params
  // -------------------------
  val K: Int = 100
  val ParquetPath: String = "data/events_parquet"

  // Генерация (можно включать/выключать)
  val GenerateData: Boolean = false
  val N: Long = 2_000_000L
  val NumSegments: Int = 50_000
  val NumProducts: Int = 200_000

  // -------------------------
  // Benchmark helpers
  // -------------------------
  case class BenchRow(label: String, ms: Double)
  private val bench = ArrayBuffer.empty[BenchRow]

  def time[A](label: String)(block: => A): A = {
    val t0 = System.nanoTime()
    val res = block
    val t1 = System.nanoTime()
    val ms = (t1 - t0) / 1e6
    bench += BenchRow(label, ms)              // <-- ВАЖНО: теперь сохраняем
    println(f"[$label] took $ms%.2f ms")
    res
  }

  def printBench(): Unit = {
    val maxLabel = math.max(10, bench.map(_.label.length).maxOption.getOrElse(10))
    val line = "-" * (maxLabel + 3 + 12)

    val headerFmt = "%-" + maxLabel + "s | %10s"
    val rowFmt    = "%-" + maxLabel + "s | %10.2f"

    println()
    println(line)
    println(headerFmt.format("Step", "ms"))
    println(line)
    bench.foreach(r => println(rowFmt.format(r.label, r.ms)))
    println(line)
    println()
  }

  // -------------------------
  // Domain types for RDD
  // -------------------------
  case class Item(score: Double, ts: Long, productId: String)

  // "хуже" = "больше" для PriorityQueue(max-heap) => сверху будет худший, удобно выкидывать
  implicit val worseFirst: Ordering[Item] = new Ordering[Item] {
    override def compare(a: Item, b: Item): Int = {
      val c1 = java.lang.Double.compare(b.score, a.score)
      if (c1 != 0) c1
      else {
        val c2 = java.lang.Long.compare(b.ts, a.ts)
        if (c2 != 0) c2
        else a.productId.compareTo(b.productId)
      }
    }
  }

  // -------------------------
  // Spark setup
  // -------------------------
  def mkSpark(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("pipelines_hw_03")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
//    spark.conf.set("spark.ui.showConsoleProgress", "false")
    // Для reproducible бенчей — фиксируем шифл-партиции
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    spark
  }

  // -------------------------
  // Data generation
  // -------------------------
  def generateAndWrite(spark: SparkSession, outPath: String): Unit = {
    val base = spark.range(N)
    val r = rand(42)

    val segmentId =
      when(r < 0.30, lit("seg_000000"))
        .when(r < 0.40, lit("seg_000001"))
        .otherwise(concat(lit("seg_"), lpad((floor(rand(7) * NumSegments)).cast("int"), 6, "0")))

    val productId =
      concat(lit("sku_"), lpad((floor(rand(99) * NumProducts)).cast("int"), 6, "0"))

    val score = (rand(123) * 10.0 + randn(5) * 2.0)
    val nowSec = unix_timestamp().cast("long")
    val ts = (nowSec - floor(rand(555) * 7 * 24 * 3600)).cast("long")

    val df = base.select(
      segmentId.as("segmentId"),
      productId.as("productId"),
      score.as("score"),
      ts.as("ts")
    )

    df.write.mode("overwrite").parquet(outPath)
  }

  // -------------------------
  // Implementations
  // -------------------------
  def dfWindowTopK(df: DataFrame, k: Int): DataFrame = {
    val w = Window.partitionBy("segmentId")
      .orderBy(col("score").desc, col("ts").desc, col("productId").asc)

    df.withColumn("rn", row_number().over(w))
      .where(col("rn") <= lit(k))
      .drop("rn")
  }

  def dfCollectListTopK(df: DataFrame, k: Int): DataFrame = {
    df.groupBy("segmentId")
      .agg(
        slice(
          sort_array(
            collect_list(struct(col("score"), col("ts"), col("productId"))),
            asc = false
          ),
          1, k
        ).as("topk")
      )
  }

  def rddTopKByKey(df: org.apache.spark.sql.DataFrame, k: Int): RDD[(String, Seq[Item])] = {
    // ВАЖНО: локальная копия, чтобы closure не тащила Main$
    val kk = k

    val ord: Ordering[Item] with Serializable = new Ordering[Item] with Serializable {
      override def compare(a: Item, b: Item): Int = {
        val c1 = java.lang.Double.compare(b.score, a.score)
        if (c1 != 0) c1
        else {
          val c2 = java.lang.Long.compare(b.ts, a.ts)
          if (c2 != 0) c2
          else a.productId.compareTo(b.productId)
        }
      }
    }

    val pairs: RDD[(String, Item)] =
      df.select("segmentId", "productId", "score", "ts").rdd.map {
        case Row(seg: String, pid: String, sc: Double, ts: Long) =>
          (seg, Item(sc, ts, pid))
      }

    def makeQ(): mutable.PriorityQueue[Item] =
      mutable.PriorityQueue.empty[Item](ord) // используем ЛОКАЛЬНЫЙ ord

    def addTrim(q: mutable.PriorityQueue[Item], x: Item): mutable.PriorityQueue[Item] = {
      q.enqueue(x)
      if (q.size > kk) q.dequeue()
      q
    }

    pairs
      .combineByKey[mutable.PriorityQueue[Item]](
        (x: Item) => addTrim(makeQ(), x),
        (q: mutable.PriorityQueue[Item], x: Item) => addTrim(q, x),
        (q1: mutable.PriorityQueue[Item], q2: mutable.PriorityQueue[Item]) => { q2.foreach(x => addTrim(q1, x)); q1 }
      )
      .mapValues(q => q.dequeueAll.reverse)
  }

  def rddTopKByKeySalted(df: DataFrame, k: Int, saltFactor: Int): RDD[(String, Seq[Item])] = {
    val kk = k
    val S = saltFactor

    val ord: Ordering[Item] with Serializable = new Ordering[Item] with Serializable {
      override def compare(a: Item, b: Item): Int = {
        val c1 = java.lang.Double.compare(b.score, a.score)
        if (c1 != 0) c1
        else {
          val c2 = java.lang.Long.compare(b.ts, a.ts)
          if (c2 != 0) c2
          else a.productId.compareTo(b.productId)
        }
      }
    }

    def makeQ(): mutable.PriorityQueue[Item] = mutable.PriorityQueue.empty[Item](ord)
    def addTrim(q: mutable.PriorityQueue[Item], x: Item): mutable.PriorityQueue[Item] = {
      q.enqueue(x); if (q.size > kk) q.dequeue(); q
    }

    // какие сегменты солим (у тебя это 2 heavy)
    def isHeavy(seg: String): Boolean = seg == "seg_000000" || seg == "seg_000001"

    val pairsSalted: RDD[((String, Int), Item)] =
      df.select("segmentId", "productId", "score", "ts").rdd.map {
        case Row(seg: String, pid: String, sc: Double, ts: Long) =>
          val salt = if (isHeavy(seg)) (MurmurHash3.stringHash(pid) & Int.MaxValue) % S else 0
          ((seg, salt), Item(sc, ts, pid))
      }

    val topPerSalt: RDD[((String, Int), Seq[Item])] =
      pairsSalted
        .combineByKey[mutable.PriorityQueue[Item]](
          (x: Item) => addTrim(makeQ(), x),
          (q: mutable.PriorityQueue[Item], x: Item) => addTrim(q, x),
          (q1: mutable.PriorityQueue[Item], q2: mutable.PriorityQueue[Item]) => { q2.foreach(x => addTrim(q1, x)); q1 }
        )
        .mapValues(q => q.dequeueAll.reverse)

    // 2-я стадия: обратно в segmentId и снова bounded topK
    val flattened: RDD[(String, Item)] =
      topPerSalt.flatMap { case ((seg, _), items) => items.iterator.map(it => (seg, it)) }

    flattened
      .combineByKey[mutable.PriorityQueue[Item]](
        (x: Item) => addTrim(makeQ(), x),
        (q: mutable.PriorityQueue[Item], x: Item) => addTrim(q, x),
        (q1: mutable.PriorityQueue[Item], q2: mutable.PriorityQueue[Item]) => { q2.foreach(x => addTrim(q1, x)); q1 }
      )
      .mapValues(q => q.dequeueAll.reverse)
  }

  // -------------------------
  // Main
  // -------------------------
  def main(args: Array[String]): Unit = {
    val spark = mkSpark()

    if (GenerateData) {
      time("generate+write parquet") { generateAndWrite(spark, ParquetPath) }
    }

    val df = spark.read.parquet(ParquetPath)

    // прогрев + cache
    df.cache()
    time("warmup df.count") { df.count() }

    // DF: window
    val win = dfWindowTopK(df, K)
    time("DF window topK count") { win.count() }

    // DF: collect_list (на 2 heavy сегмента — как у тебя)
    val heavy = df.where(col("segmentId").isin("seg_000000", "seg_000001"))
    val colTop = dfCollectListTopK(heavy, K)
    time("DF collect_list topK count (2 heavy segs)") { colTop.count() }

    // RDD: combineByKey
    val rdd = rddTopKByKey(df, K)
    time("RDD combineByKey topK count") { rdd.count() }

    val rddSalted = rddTopKByKeySalted(df, K, saltFactor = 64)
    time("RDD salted combineByKey topK count (S=16)") { rddSalted.count() }

    // Планы выполнения (по желанию)
    // win.explain("formatted")
    // colTop.explain("formatted")

    printBench()
    spark.stop()
  }
}
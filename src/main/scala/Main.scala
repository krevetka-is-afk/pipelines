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
  // Config
  // -------------------------
  val K: Int = 100
  val ParquetPath: String = "data/events_parquet"

  // сколько раз меряем каждую операцию
  val WarmupsPerTest: Int = 1
  val RunsPerTest: Int = 5

  // какие saltFactor прогоняем (0 = без salting, далее salted)
  val SaltFactors: Seq[Int] = Seq(4, 16, 64)

  // -------------------------
  // Benchmark stats
  // -------------------------
  case class StatRow(label: String, runs: Int, meanMs: Double, medianMs: Double, minMs: Double, maxMs: Double, stdevMs: Double)
  private val stats = ArrayBuffer.empty[StatRow]

  private def median(xs: Seq[Double]): Double = {
    val s = xs.sorted
    val n = s.size
    if (n == 0) Double.NaN
    else if (n % 2 == 1) s(n / 2)
    else (s(n / 2 - 1) + s(n / 2)) / 2.0
  }

  private def stdev(xs: Seq[Double], mean: Double): Double = {
    val n = xs.size
    if (n <= 1) 0.0
    else math.sqrt(xs.map(x => (x - mean) * (x - mean)).sum / n.toDouble)
  }

  def benchN(label: String, warmups: Int = WarmupsPerTest, runs: Int = RunsPerTest)(block: => Any): Unit = {
    // warm-up (не учитываем)
    var i = 0
    while (i < warmups) { block; i += 1 }

    // measured runs
    val times = new Array[Double](runs)
    var j = 0
    while (j < runs) {
      val t0 = System.nanoTime()
      block
      val t1 = System.nanoTime()
      times(j) = (t1 - t0) / 1e6
      j += 1
    }

    val ts = times.toSeq
    val mean = ts.sum / ts.size
    val med = median(ts)
    val mn = ts.min
    val mx = ts.max
    val sd = stdev(ts, mean)

    stats += StatRow(label, runs, mean, med, mn, mx, sd)
    println(f"[$label] mean=${mean}%.2f ms, median=${med}%.2f ms (min=${mn}%.2f, max=${mx}%.2f, sd=${sd}%.2f)")
  }

  def printStats(): Unit = {
    val maxLabel = math.max(10, stats.map(_.label.length).maxOption.getOrElse(10))
    val line = "-" * (maxLabel + 3 + 6 + 3 + 10 * 5)

    def fmtRow(s: StatRow): String = {
      val labelFmt = "%-" + maxLabel + "s"
      // runs | mean | median | min | max | sd
      f"${labelFmt.format(s.label)} | ${s.runs}%4d | ${s.meanMs}%10.2f | ${s.medianMs}%10.2f | ${s.minMs}%10.2f | ${s.maxMs}%10.2f | ${s.stdevMs}%10.2f"
    }

    println()
    println(line)
    println(("%-" + maxLabel + "s | %4s | %10s | %10s | %10s | %10s | %10s").format(
      "Step", "n", "mean(ms)", "median", "min", "max", "sd"
    ))
    println(line)
    stats.foreach(r => println(fmtRow(r)))
    println(line)
    println()
  }

  // -------------------------
  // Domain types for RDD
  // -------------------------
  case class Item(score: Double, ts: Long, productId: String)

  // -------------------------
  // Spark setup
  // -------------------------
  def mkSpark(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("pipelines_hw_03")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "100") // для локального теста, чтобы не было 200 партиций
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
//    spark.conf.set("spark.ui.showConsoleProgress", "false")
//    spark.conf.set("spark.sql.shuffle.partitions", "100")
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    spark
  }

  // -------------------------
  // Implementations (DF)
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

  // -------------------------
  // Implementations (RDD)
  // -------------------------
  def rddTopKByKey(df: DataFrame, k: Int): RDD[(String, Seq[Item])] = {
    val kk = k

    // ЛОКАЛЬНЫЙ serializable ordering: чтобы closure не тащила Main$
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

    val pairs: RDD[(String, Item)] =
      df.select("segmentId", "productId", "score", "ts").rdd.map {
        case Row(seg: String, pid: String, sc: Double, ts: Long) =>
          (seg, Item(sc, ts, pid))
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

    try {
      val df = spark.read.parquet(ParquetPath)

      // Базовый df кэшируем, чтобы не мерить I/O каждый раз
      df.cache()
      benchN("warmup df.count", warmups = 1, runs = 3) { df.count() } // отдельный warmup/база

      val win = dfWindowTopK(df, K)
      val heavy = df.where(col("segmentId").isin("seg_000000", "seg_000001"))
      val colTop = dfCollectListTopK(heavy, K)

      benchN("DF window topK count") { win.count() }
      benchN("DF collect_list topK count (2 heavy segs)") { colTop.count() }

      // RDD unsalted
      val rddUnsalted = rddTopKByKey(df, K)
      benchN("RDD combineByKey topK count") { rddUnsalted.count() }

      // RDD salted for each S
      SaltFactors.foreach { s =>
        val rddSalted = rddTopKByKeySalted(df, K, saltFactor = s)
        benchN(s"RDD salted combineByKey topK count (S=$s)") { rddSalted.count() }
      }

      printStats()
    } finally {
      spark.stop()
    }
  }
}
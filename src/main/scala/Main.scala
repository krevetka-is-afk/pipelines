object Main {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder()
      .appName("gen-dataset")
      .master("local[*]")
      .getOrCreate()

    // (не обязательно, но удобно) меньше/больше партиций под твой ноут
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    val N = 2_000_000L          // <-- масштаб (потом 5-20 млн, если железо позволяет)
    val numSegments = 50_000    // сколько разных segmentId
    val numProducts = 200_000   // сколько разных productId

    // Генерация идёт через spark.range(N): это DataFrame с одной колонкой id = 0..N-1
    val base = spark.range(N)

    // rand(seed) — детерминированный псевдо-рандом (важно для повторяемости)
    val r = rand(42)

    // Делаем "жирные" сегменты: 30% строк -> seg_000000, 10% -> seg_000001, остальное -> равномерно
    val segmentId =
      when(r < 0.30, lit("seg_000000"))
        .when(r < 0.40, lit("seg_000001"))
        .otherwise(
          concat(lit("seg_"), lpad((floor(rand(7) * numSegments)).cast("int"), 6, "0"))
        )

    // productId: тоже строка с фиксированной шириной
    val productId =
      concat(lit("sku_"), lpad((floor(rand(99) * numProducts)).cast("int"), 6, "0"))

    // score: например, имитация взвешенных событий (распределение можно менять)
    val score = (rand(123) * 10.0 + randn(5) * 2.0)  // randn ~ нормальное распределение

    // ts: "псевдо-время" в секундах (например последние 7 дней)
    val nowSec = (unix_timestamp()).cast("long")
    val ts = (nowSec - floor(rand(555) * 7 * 24 * 3600)).cast("long")

    val df = base.select(
      segmentId.as("segmentId"),
      productId.as("productId"),
      score.as("score"),
      ts.as("ts")
    )

    // Сохраним в Parquet — так сравнение DF/RDD будет честнее (чтение колоночное)
    val outPath = "data/events_parquet"
    df.write.mode("overwrite").parquet(outPath)

    // быстрый sanity-check
    val df2 = spark.read.parquet(outPath)
    df2.show(5, truncate=false)
    println(df2.count())
  }
}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("demo")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      ("a", 1),
      ("b", 2),
      ("a", 3)
    ).toDF("k", "v")
    df.groupBy("k").sum("v").show()

    spark.stop()
  }
}
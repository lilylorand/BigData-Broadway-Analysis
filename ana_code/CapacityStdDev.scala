import org.apache.spark.sql.SparkSession

object CapacityStdDev {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Capacity Standard Deviation")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "false")
      .csv(args(0))
      .toDF("year", "season", "attendance", "capacity", "gross", "soldOut")

    val result = df.selectExpr(
      "stddev(cast(capacity as double)) as capacity_stddev"
    )

    result.show()

    spark.stop()
  }
}
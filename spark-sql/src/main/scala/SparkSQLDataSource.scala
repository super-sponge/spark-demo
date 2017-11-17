import org.apache.spark.sql.SparkSession

object SparkSQLDataSource {
  def main(args: Array[String]) {
    val ss = SparkSession.builder().appName("SparkSQLDataSource").getOrCreate()
    val df = ss.read.parquet("data/users.parquet")
    df.show()
    df.write.save("data/out/users.parquet")

    ss.stop()
  }

}

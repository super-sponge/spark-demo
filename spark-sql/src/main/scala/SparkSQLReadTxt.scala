import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object SparkSQLReadTxt {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("SparkSQLReadTxt").getOrCreate()
    import ss.implicits._

    //load textfile to Dataframe
//    val peopleDF = ss.sparkContext.textFile("data/people.txt")
//      .map(_.split(","))
//      .map(x => Person(x(0), x(1).trim.toInt)).toDF()
//    peopleDF.createOrReplaceTempView("people")
//    ss.sql("select * from people").show()
//

    val peopleRDD = ss.sparkContext.textFile("data/people.txt")
    val schemastring = "name age"
    val fields = schemastring.split(" ")
      .map(StructField(_, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(a =>Row(a(0), a(1).trim))

    val peopleDF1 = ss.createDataFrame(rowRDD, schema)
    peopleDF1.createOrReplaceTempView("people1")
    ss.sql("select * from people1").show()

    ss.stop()
  }

}

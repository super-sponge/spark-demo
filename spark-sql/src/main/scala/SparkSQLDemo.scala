import org.apache.spark.sql.SparkSession

object SparkSQLDemo {

  case class Person(name: String, age: Long)
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
        .builder()
      .appName("SparkSQL demo")
      .getOrCreate()

    import ss.implicits._
//    import ss.sqlContext.implicits._

//    val df = ss.read.json("data/people.json")
//    df.show()
//    df.printSchema()
//    df.select("name").show()
//    df.select($"name", $"age" + 1).show()
//    df.filter($"age" > 21).show()
//    df.groupBy("age").count().show()
//
//    //Register the dataframe as a SQL temporary view
//    df.createOrReplaceTempView("people")
//    val sqlDF = ss.sql("select * from people")
//    sqlDF.show()
      //Register the dataframe as global view
//    df.createGlobalTempView("people")
//    df.createOrReplaceTempView("people")
//    ss.sql("select * from global_temp.people").show()
//    ss.newSession().sql("select * from global_temp.people").show()

    val peopleDS = ss.read.json("data/people.json").as[Person]
    peopleDS.show()


    ss.stop()

    }


}

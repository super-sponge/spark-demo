import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator


case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)

object SparkSQLAggrTypeSafe {
  
  object TypeSafeAverage extends Aggregator[Employee, Average, Double] {
    override def zero: Average = Average(0L, 0L)

    override def reduce(average: Average, employee: Employee): Average = {
      average.sum += employee.salary
      average.count += 1
      average
    }

    override def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    override def bufferEncoder: Encoder[Average] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("SparkSQLAggrTypeSafe").getOrCreate()
    import ss.implicits._
    val ds = ss.read.json("data/employees.json").as[Employee]
    ds.show()
    val averageSalary = TypeSafeAverage.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()

    ss.stop()
  }
}

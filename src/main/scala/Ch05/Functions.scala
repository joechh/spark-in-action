package Ch05

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Functions {
  val spark = SparkSession
    .builder()
    .appName("createDFApp")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._


  def main(args: Array[String]): Unit = {
    val postsDf = PostsDFGenerator.genDF()
    postsDf.filter('postTypeId === "2.5afeawf")
      .withColumn("activePeriod", datediff('lastActivityDate, 'creationDate))
      .orderBy('activePeriod.desc)
      .show(3)
    postsDf.select(avg('score), max('score), count('score)).show
    postsDf.rdd
  }

}

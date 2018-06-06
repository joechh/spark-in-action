package Ch05

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowOpartion {
  val spark = SparkSession
    .builder()
    .appName("createDFApp")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._


  def main(args: Array[String]): Unit = {
    val postsDf = PostsDFGenerator.genDF()
    postsDf
      .filter('postTypeId === 1)
      .select('ownerUserId, 'acceptedAnswerId, 'score, max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser")
      .withColumn("toMax", 'maxPerUser - 'score)
      .show(10)

    postsDf
      .filter('postTypeId === 1)
      .select('ownerUserId, 'id, 'creationDate,
        lag('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "prev",
        lead('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "next"
      )
      .orderBy('ownerUserId, 'id)
      .show(10)
  }
}

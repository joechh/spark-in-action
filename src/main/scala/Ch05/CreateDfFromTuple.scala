package Ch05

import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateDfFromTuple {
  val spark = SparkSession
    .builder()
    .appName("createDFApp")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def genDF(): DataFrame = {
    val itPostsRows = spark.sparkContext.textFile("src/main/resources/ch05/italianPosts.csv")
    val itPostsSplit = itPostsRows.map(_.split("~"))
    val itPostsRdd = itPostsSplit.map(arr => (arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), arr(9), arr(10), arr(11), arr(12)))
    val itPostsDf = itPostsRdd.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
    itPostsDf
  }

  def main(args: Array[String]): Unit = {
    val itPostsDf = genDF()
    itPostsDf.show(5)
    itPostsDf.printSchema()
  }

}

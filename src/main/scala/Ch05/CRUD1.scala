package Ch05

import org.apache.spark.sql.{DataFrame, SparkSession}

object CRUD1 {
  val spark = SparkSession
    .builder()
    .appName("createDFApp")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val postsDf = genDF()
    val postsDfIdBody = postsDf.select("id", "body")
    val postsDfIdBodyByColumn = postsDf.select(postsDf.col("id"), postsDf.col("body"))
    val postsDfIdBodyBySymbol1 = postsDf.select('id, 'body)
    val postsDfIdBodyBySymbol2 = postsDf.select(Symbol("id"), Symbol("body"))
    val postsDfIdBodyBySymbol3 = postsDf.select($"id", $"body")
    println(postsDfIdBody.filter('body.contains("Italiano")).count)
    val noAnswer = postsDf.filter(('postTypeId === 1) and ('acceptedAnswerId.isNull))


    postsDf
      .filter('postTypeId === 1)
      .withColumn("ratio", 'viewCount / 'score)
      .where('ratio < 35)
      .sort('ratio.desc)
      .show

    postsDf.cache()
    postsDf.unpersist()
    spark.sqlContext.uncacheTable()
    spark.sqlContext.clearCache()
    spark.sqlContext.cacheTable()

  }

  def genDF(): DataFrame = {
    val itPostsRows = spark.sparkContext.textFile("src/main/resources/ch05/italianPosts.csv")
    val itPostsSplit = itPostsRows.map(_.split("~"))
    val itPostsRdd = itPostsSplit.map(arr => (arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), arr(9), arr(10), arr(11), arr(12)))
    val itPostsDf = itPostsRdd.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
    itPostsDf
  }
}


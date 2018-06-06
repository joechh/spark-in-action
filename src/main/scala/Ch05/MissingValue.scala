package Ch05

import org.apache.spark.sql.SparkSession

object MissingValue {
  val spark = SparkSession
    .builder()
    .appName("createDFApp")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val postsDF = PostsDFGenerator.genDF()

    println(postsDF.count())
    val cleanPostsDF = postsDF.na.drop()
    println(cleanPostsDF.count())
    val answeredPostsDF = postsDF.na.drop(Array("acceptedAnswerId"))
    println(answeredPostsDF.count())
    postsDF.na.fill(Map("viewCount" -> 0))
    postsDF.na.replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3300))

  }

}

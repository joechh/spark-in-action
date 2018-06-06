package Ch05

import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}

object PostsDFGenerator {
  val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  def genDF(): DataFrame = {
    val itPostsRows = spark.sparkContext.textFile("src/main/resources/ch05/italianPosts.csv")
    itPostsRows.map(stringToPost).toDF
  }


  import StringImplicits._

  def stringToPost(row: String): Post = {
    val r = row.split("~")
    Post(r(0).toIntSafe,
      r(1).toTimestampSafe,
      r(2).toLongSafe,
      r(3),
      r(4).toIntSafe,
      r(5).toTimestampSafe,
      r(6).toIntSafe,
      r(7),
      r(8),
      r(9).toIntSafe,
      r(10).toLongSafe,
      r(11).toLongSafe,
      r(12).toLong)
  }


  case class Post(commentCount: Option[Int], lastActivityDate: Option[java.sql.Timestamp], ownerUserId: Option[Long],
                  body: String, score: Option[Int], creationDate: Option[java.sql.Timestamp], viewCount: Option[Int],
                  title: String, tags: String, answerCount: Option[Int], acceptedAnswerId: Option[Long],
                  postTypeId: Option[Long], id: Long)

  object StringImplicits {

    implicit class StringImprovements(val s: String) {

      import scala.util.control.Exception.catching

      def toIntSafe = catching(classOf[NumberFormatException]).opt(s.toInt)

      def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong

      def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
    }

  }

}

package Ch05

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object CreateDfFromStructType {
  val postSchema = StructType(Seq(
    StructField("commentCount", IntegerType, true),
    StructField("lastActivityDate", TimestampType, true),
    StructField("ownerUserId", LongType, true),
    StructField("body", StringType, true),
    StructField("score", IntegerType, true),
    StructField("creationDate", TimestampType, true),
    StructField("viewCount", IntegerType, true),
    StructField("title", StringType, true),
    StructField("tags", StringType, true),
    StructField("answerCount", IntegerType, true),
    StructField("acceptedAnswerId", LongType, true),
    StructField("postTypeId", LongType, true),
    StructField("id", LongType, false))
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("createDFApp")
      .master("local[*]")
      .getOrCreate()

    val itPostsRows = spark.sparkContext.textFile("src/main/resources/ch05/italianPosts.csv")
    val itPostRow = itPostsRows.map(stringToRow)
    val itPostsDf = spark.createDataFrame(itPostRow, postSchema)


    itPostsDf.show(5)
    itPostsDf.printSchema()

  }

  import StringImplicits._

  def stringToRow(row: String): Row = {
    val r = row.split("~")
    Row(r(0).toIntSafe.getOrElse(null),
      r(1).toTimestampSafe.getOrElse(null),
      r(2).toLongSafe.getOrElse(null),
      r(3),
      r(4).toIntSafe.getOrElse(null),
      r(5).toTimestampSafe.getOrElse(null),
      r(6).toIntSafe.getOrElse(null),
      r(7),
      r(8),
      r(9).toIntSafe.getOrElse(null),
      r(10).toLongSafe.getOrElse(null),
      r(11).toLongSafe.getOrElse(null),
      r(12).toLong)
  }

  object StringImplicits {

    implicit class StringImprovements(val s: String) {

      import scala.util.control.Exception.catching

      def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt

      def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong

      def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
    }

  }

}

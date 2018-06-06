package Ch05


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object Grouping {
  val spark = SparkSession
    .builder()
    .appName("createDFApp")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._


  def main(args: Array[String]): Unit = {
    val postsDF = PostsDFGenerator.genDF()
    val postmapped = postsDF.rdd.map { row =>
      Row.fromSeq {
        row.toSeq
          .updated(3, row.getString(3).replace("&lt;", "<").replace("&gt;", ">"))
          .updated(8, row.getString(8).replace("&lt;", "<").replace("&gt;", ">"))
      }
    }
    val postsDfNew = spark.createDataFrame(postmapped, postsDF.schema)
    postsDfNew
      .groupBy('ownerUserId, 'tags, 'postTypeId)
      .count()
      .orderBy('ownerUserId desc)
      .show(10)

    postsDfNew
      .groupBy('ownerUserId)
      .agg(max('lastActivityDate), max('score))
      .show(10)

    postsDfNew
      .groupBy('ownerUserId)
      .agg(Map("lastActivityDate" -> "max", "score" -> "max"))
      .show(10)

    postsDfNew
      .groupBy('ownerUserId)
      .agg(max('lastActivityDate), max('score).gt(5))
      .show(10)

    val smplDF = postsDfNew.where('ownerUserId >= 13 and 'ownerUserId <= 15)
    smplDF.groupBy('ownerUserId, 'tags, 'postTypeId).count.show
    smplDF.rollup('ownerUserId, 'tags, 'postTypeId).count.show
    smplDF.cube('ownerUserId, 'tags, 'postTypeId).count.show
  }

}

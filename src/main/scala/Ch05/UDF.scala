package Ch05

import org.apache.spark.sql.SparkSession

object UDF {
  val spark = SparkSession
    .builder()
    .appName("createDFApp")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val postsDF = PostsDFGenerator.genDF()
    val countTags = spark.udf.register("countTags",
      (tags: String) => {
        "&lt;".r.findAllMatchIn(tags).length
      }
    )
    postsDF.filter('postTypeId === 1)
      .select('tags, countTags('tags) as "tagCnt")
      .show(10, false)
  }
}

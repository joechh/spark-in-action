package Ch05

import org.apache.spark.sql.{Row, SparkSession}

object ConvertToRdd {
  val spark = SparkSession
    .builder()
    .appName("createDFApp")
    .master("local[*]")
    .getOrCreate()

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
    postsDfNew.show(5, false)
  }
}

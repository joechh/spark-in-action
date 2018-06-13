package Ch05

import org.apache.spark.sql.{SaveMode, SparkSession}

object TempTable {
  val spark = SparkSession
    .builder()
    .appName("createDFApp")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val postsDf = PostsDFGenerator.genDF()
    postsDf.createOrReplaceTempView("posts_temp")
    postsDf.write.mode(SaveMode.Overwrite).saveAsTable("posts")
    val resultDF = spark.sql("select * from posts")
  }


}

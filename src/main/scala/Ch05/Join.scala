package Ch05

import org.apache.spark.sql.{Row, SparkSession}
import java.sql.Timestamp

import org.apache.spark.sql.types._


object Join {
  val spark = SparkSession
    .builder()
    .appName("createDFApp")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val itVotesRaw = spark
      .sparkContext
      .textFile("src/main/resources/ch05/italianVotes.csv")
      .map(x => x.split("~"))

    val postsDf = PostsDFGenerator.genDF()
    val itVotesRows = itVotesRaw.map(row => Row(row(0).toLong, row(1).toLong, row(2).toInt, Timestamp.valueOf(row(3))))
    val votesSchema = StructType(Seq(
      StructField("id", LongType, false),
      StructField("postId", LongType, false),
      StructField("voteTypeId", IntegerType, false),
      StructField("creationDate", TimestampType, false)
    ))
    val votesDf = spark.createDataFrame(itVotesRows, votesSchema)
    votesDf.show


    val postVotes = postsDf.join(votesDf, postsDf("id") === 'id)
    postVotes.show()
    postsDf.printSchema()
    postVotes.printSchema()


  }

}

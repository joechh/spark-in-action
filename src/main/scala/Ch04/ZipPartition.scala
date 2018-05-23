package Ch04

import org.apache.spark.sql.SparkSession

object ZipPartition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("aggApp")
      .getOrCreate()

    val sc = spark.sparkContext
    val rdd1 = sc.parallelize(1 to 10, 10)
    val rdd2 = sc.parallelize((1 to 8).map(x => "n" + x), 10)
    rdd1.zipPartitions(rdd2, true)((it1, it2) => {
      it1.zipAll(it2, -1, "empty").map {
        case (x1, x2) => x1 + "-" + x2
      }
    }).foreach(println)

    rdd1.map {
      case i if i > 1 => i + 1
      case i if i > 2 => i + 1
    }


  }

}

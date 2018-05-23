package Ch04

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Aggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ComplimentaryApp")
      .getOrCreate()

    val tranFile = spark.sparkContext.textFile("src/main/resources/ch04/ch04_data_transactions.txt")
    val transData: RDD[Array[String]] = tranFile.map(_.split("#"))
    val transByProdId = transData.map(tran => (tran(3).toInt, tran))
    val totalByProdId = transByProdId.mapValues(tran => tran(5).toDouble)
      .reduceByKey {
        case (acc, newValue) => acc + newValue
      }
    val productById = spark.sparkContext.textFile("src/main/resources/ch04/ch04_data_products.txt")
      .map(_.split("#"))
      .map(arr => (arr(0).toInt, arr))
    //Q1
    val totalsAndProds = totalByProdId.join(productById)
    val totalsWithMissingProds = totalByProdId.rightOuterJoin(productById)
    //Q2-1
    val missingProd = totalsWithMissingProds.filter(_._2._1.isEmpty).map(_._2._2)
    //Q2-2
    val missingProd2 = productById.subtractByKey(totalByProdId).values
    val prodTotCogroup = totalByProdId.cogroup(productById)

    //_._2._1: totalByProdId value
    //_._2._2: productById value
    prodTotCogroup.filter(_._2._1.isEmpty)
      .foreach(x => println(x._2._2.head.mkString(", ")))

    val totalsAndProds2 = prodTotCogroup.filter(!_._2._1.isEmpty)
      .map(x => (x._2._2.head(0).toInt, x._2._1.head, x._2._2.head))
    totalsAndProds2.foreach(println)

    println()

    val sortedProds = totalsAndProds.sortBy(_._2._1, false)
    sortedProds.collect.foreach(println)


    val transByCust: RDD[(Int, Array[String])] = transData.map(trans => (trans(2).toInt, trans))


    def createComb: (Array[String]) => (Double, Double, Int, Double) = {
      case t => {
        val total = t(5).toDouble
        val q = t(4).toInt
        //min, max, count, and total
        (Double.MaxValue, Double.MinValue, q, total)
      }
    }

    def seqOp: ((Double, Double, Int, Double), Array[String]) => (Double, Double, Int, Double) = {
      case ((min, max, accNum, accTotal), t) => {
        val total = t(5).toDouble
        val q = t(4).toInt
        (math.min(min, total / q), math.max(max, total / q), accNum + q, accTotal + total)
      }
    }

    def combinator: ((Double, Double, Int, Double), (Double, Double, Int, Double)) => (Double, Double, Int, Double) = {
      case ((min1, max1, num1, total1), (min2, max2, num2, total2)) => {
        (math.min(min1, min2), math.max(max1, max2), num1 + num2, total1 + total2)
      }
    }

    val avgByCust = transByCust.combineByKey(
      createComb,
      seqOp,
      combinator,
      partitioner = new HashPartitioner(transByCust.partitions.size)
    )
    avgByCust.foreach(println)


  }

}

case class Employee(lastName: String) extends Ordered[Employee] {
  override def compare(that: Employee): Int = {
    this.lastName.compareTo(that.lastName)
  }
}




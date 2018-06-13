package Ch04

import Ch03.App.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object Complimentary {

  def doTask1(transByCust: RDD[(Int, Array[String])]) = {
    println(s"number transaction: ${transByCust.countByKey().values.sum}")
    val (cid, purch) = transByCust.countByKey().toList.sortBy(_._2).last
    println((cid, purch))
    Array(Array("2015-03-30", "11:59PM", "53", "4", "1", "0.00"))
  }

  def doTask2(transByCust: RDD[(Int, Array[String])]) = {
    transByCust.mapValues(tran => {
      if (tran(3).toInt == 25 && tran(4).toDouble > 1)
        tran(5) = (tran(5).toDouble * 0.95).toString
      tran
    }
    )
  }

  def doTask3(transByCust: RDD[(Int, Array[String])]): RDD[(Int, Array[String])] = {
    transByCust.flatMapValues(tran => {
      if (tran(3).toInt == 81 && tran(4).toDouble >= 5) {
        val cloned = tran.clone()
        cloned(5) = "0.00"
        cloned(3) = "70"
        cloned(4) = "1"
        List(tran, cloned)
      }
      else {
        List(tran)
      }
    }
    )
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ComplimentaryApp")
      .getOrCreate()

    val tranFile = spark.sparkContext.textFile("src/main/resources/ch04/ch04_data_transactions.txt")
    val transData: RDD[Array[String]] = tranFile.map(_.split("#"))
    var transByCust: RDD[(Int, Array[String])] = transData.map(trans => (trans(2).toInt, trans))

    println(s"number of customers: ${transByCust.keys.distinct().count}")
    var compTrans: Array[Array[String]] = doTask1(transByCust)
    transByCust.lookup(53).foreach(array => println(array.mkString(",")))
    transByCust = doTask2(transByCust)
    println(transByCust.count())
    transByCust = doTask3(transByCust)

    val amounts = transByCust.mapValues(t => t(5).toDouble)
    val (id, sum) = amounts.foldByKey(0)((acc, newValue) => acc + newValue)
      .collect
      .toList
      .sortBy(_._2)
      .last

    println((id, sum))
    compTrans = compTrans :+ Array("2015-03-30", "11:59 PM", "76", "63", "1", "0.00")
    println(compTrans.size)

    val compTransRdd = spark.sparkContext.parallelize(compTrans)
    val compTransRddByCust: RDD[(Int, Array[String])] = {
      compTransRdd.map(tran => (tran(2).toInt, tran))
    }
    val resultRdd = transByCust.union(compTransRddByCust)

    //have no opportunity to overwrite
    //resultRdd.map(_._2.mkString("#")).saveAsTextFile("ch04output-transByCust")

    import spark.implicits._
    resultRdd.map(_._2.mkString("#"))
      .toDF()
      .write
      .mode(SaveMode.Overwrite)
      .csv("ch04output-transByCustOverwrite")

    def SeqOp(acc: List[String], newValue: Array[String]) = acc :+ newValue(3)

    def Combinator(accPart: List[String], newPart: List[String]) = accPart ::: newPart

    val prods = transByCust.aggregateByKey(List[String]())(SeqOp, Combinator)

    prods.collect().foreach(println)
    println(prods.getNumPartitions)
    println(prods.partitioner)
    println(prods.map(_ => "").partitioner)

    println(prods.mapPartitions(it => {
      val res = for (e <- it) yield ""
      res
    }, true).partitioner)

    println(prods.mapPartitions(it => {
      it.map(_ => "")
    }, true).partitioner)


    val rdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5), 100)
    println(rdd.getNumPartitions)
    println(rdd.coalesce(50).getNumPartitions)
    println(rdd.coalesce(1).getNumPartitions)
    println(rdd.coalesce(1, true).getNumPartitions)
  }
}

package Ch06

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Minutes, StreamingContext}

object Stateful {

  def stateSpec: (Seq[Double], Option[Double]) => Option[Double] = {
    (values, total) => {
      total match {
        case Some(total) => Some(values.sum + total)
        case None => Some(values.sum)
      }

    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(conf, Milliseconds(5000))
    val fileStream = ssc.textFileStream("hdfs://localhost:9000/user/joechh/sparkDir")
    val orders: DStream[Orders] = parseFileToOrder(fileStream)
    val numPerType = orders.map(x => (x.buy, 1)).reduceByKey(_ + _)
    val amountPerClient = orders.map(x => (x.clientId, x.amount * x.price))
    val amountState = amountPerClient.updateStateByKey(stateSpec)
    val top5Client = amountState.transform(rdd => rdd.sortBy(_._2, false).zipWithIndex().filter(_._2 < 5).map(_._1))
    val buySellList = numPerType.map { x =>
      x._1 match {
        case true => ("BUYS", List(x._2.toString))
        case false => ("SELLS", List(x._2.toString))
      }

    }

    val top5ClientList = top5Client
      .repartition(1)
      .map(_._1.toString)
      .glom()
      .map(arr => ("TOP5CLIENTS", arr.toList))

    val stocksPerWindow = orders.map(x => (x.symbol, x.amount))
      .reduceByKeyAndWindow((acc, numValue) => acc + numValue, (acc, numValue) => acc - numValue, Minutes(60))


    val top5Stocks = stocksPerWindow.transform(_.sortBy(_._2, false).map(_._1).zipWithIndex().filter(_._2 < 5))
      .repartition(1)
      .map(_._1.toString)
      .glom()
      .map(arr => ("TOP5STOCKS", arr.toList))

    val finalStream = buySellList.union(top5ClientList).union(top5Stocks)
    finalStream.print()


    ssc.checkpoint("hdfs://localhost:9000/user/joechh/sparkCheckDir")
    ssc.start()
    ssc.awaitTermination()

  }


  def parseFileToOrder(fileStream: DStream[String]): DStream[Orders] = {
    fileStream.flatMap(line => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val words = line.split(",")
      try {
        assert(words(6) == "B" || words(6) == "S")
        List(Orders(new Timestamp(dateFormat.parse(words(0)).getTime),
          words(1).toLong,
          words(2).toLong,
          words(3),
          words(4).toInt,
          words(5).toDouble,
          words(6) == "B"
        ))
      }
      catch {
        case e: Throwable => println("wrong line format(" + e + "):" + line)
          List()
      }
    })
  }

}

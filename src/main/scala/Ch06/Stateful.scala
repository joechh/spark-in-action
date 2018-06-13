package Ch06

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object BasicStreaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(conf, Milliseconds(5000))
    val fileStream = ssc.textFileStream("hdfs://localhost:9000/user/joechh/sparkDir")
    val orders: DStream[Order] = parseFileToOrder(fileStream)
    val numPerType = orders.map(x => (x.buy, 1)).reduceByKey(_ + _)
    numPerType.repartition(1).saveAsTextFiles("/home/joechh/ch06output/output", "txt")
    numPerType.print(10)
    ssc.start()
    ssc.awaitTermination()

  }


  def parseFileToOrder(fileStream: DStream[String]): DStream[Order] = {
    fileStream.flatMap(line => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val words = line.split(",")
      try {
        assert(words(6) == "B" || words(6) == "S")
        List(Order(new Timestamp(dateFormat.parse(words(0)).getTime),
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

case class Order(time: java.sql.Timestamp, orderId: Long, clientId: Long,
                 symbol: String, amount: Int, price: Double, buy: Boolean)

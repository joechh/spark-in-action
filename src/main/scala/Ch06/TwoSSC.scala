package Ch06

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object TwoSSC {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc1 = new StreamingContext(conf, Milliseconds(5000))
    val ssc2 = new StreamingContext(ssc1.sparkContext, Milliseconds(1000))
    val stream1 = ssc1.socketTextStream("localhost", 9999)
    val stream2 = ssc2.socketTextStream("localhost", 9998)
    stream1.print()
    stream2.print()


    ssc1.start()
    ssc2.start() //Exception occurs
    ssc1.awaitTermination()
    ssc2.awaitTermination()
  }
}
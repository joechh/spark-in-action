package Ch3

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object AppWithBc {


  val spark = SparkSession.builder()
    .appName("Github Push counter")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def filter(ghLog: DataFrame) = {
    //type1
    ghLog.filter("type= 'PushEvent'")
    //type2
    ghLog.filter(ghLog.col("type") === "PushEvent")
    //type3
    ghLog.filter('type === "PushEvent")
    //type4
    ghLog.filter($"type" === "PushEvent")
  }

  def loadFromFile(filePath: String): Set[String] = {
    val employees = Set() ++ {
      for {
        line <- Source.fromFile(filePath).getLines()
      } yield line.trim
    }
    employees
    //Alternative
    //Source.fromFile(filePath).getLines().map(_.trim).toSet
  }

  def getEmpPredicate(employees: Broadcast[Set[String]]) = {
    val isEmp: (String => Boolean) = (arg: String) => employees.value.contains(arg)
    val isEmp2: (String => Boolean) = arg => employees.value.contains(arg)
    val isEmp3: (String => Boolean) = employees.value.contains(_)
    val isEmp4: (String => Boolean) = employees.value.contains
    isEmp
  }

  def main(args: Array[String]): Unit = {

    val homeDir = System.getenv("HOME")
    val inputPath = homeDir + "/sia/github-archive/2015-03-01-0.json"
    val ghLog = spark.read.json(inputPath)

    val pushes = filter(ghLog)
    println(s"all events: ${ghLog.count}")
    println(s"push events: ${pushes.count}")
    pushes.show(5)

    val ordered = pushes
      .groupBy(pushes("actor.login"))
      .count()
      .orderBy('count.desc)
    ordered.show(5)

    val employees = loadFromFile(homeDir + "/first-edition/ch03/ghEmployees.txt")
    val bcEmployees = spark.sparkContext.broadcast(employees)
    val isEmp = getEmpPredicate(bcEmployees)
    val isEmployee = spark.udf.register("isEmpUdf", isEmp)
    val filtered = ordered.filter(isEmployee($"login"))
    filtered.show(5)
    //sol2, call isEmpUdf udf
    ordered.select("*")
      .where("isEmpUdf(login)=True")
      .show(5)
  }
}

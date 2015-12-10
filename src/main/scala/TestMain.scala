

import org.apache.spark.{SparkContext, SparkConf}
object TestMain {
  def main (args: Array[String]) {
    //Create Spark Context
    val conf = new SparkConf().setMaster("local").setAppName("MySpark_Test")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(1 to 1000000).collect().filter(_<100)
    data.foreach(println)
  }

}

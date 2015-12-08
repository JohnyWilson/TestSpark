/**   * Created by urlama1 on 12/7/2015.    */

import org.apache.spark.{SparkConf, SparkContext}
object WordCount {
  def main (args: Array[String]) {
    //Create Spark Context
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val WordsCounted = sc.textFile("Data/romeo.txt")
                          .flatMap(_.split(" "))
                          .map((_,1))
                          .reduceByKey(_+_)
                          .map{ case(a,b) => (b,a)}
                          .sortByKey(ascending = false)
    WordsCounted.foreach(println)
  }
}

/**   * Created by urlama1 on 12/7/2015.    */

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main (args: Array[String]) {
    //Create Spark Context
    val conf = new SparkConf().setMaster("local").setAppName("MySpark_Test")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(1 to 1000000).collect().filter(_<100)
    data.foreach(println)
  }

}

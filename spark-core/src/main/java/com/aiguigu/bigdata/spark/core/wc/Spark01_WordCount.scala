package com.aiguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]):Unit={
    //Application
    //Spark framework
    //TODO connect with spark
    //JDBC: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO business logic
    //1。 Read file by line
    // "hello world" => hello, world, hello, word
    val lines:RDD[String] = sc.textFile("data")

    //2. Parse word by word from line
    val words: RDD[String]=lines.flatMap(_.split(" "))
    //3. Group words
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    //(hello, hello,hello),(world, world) -> (hello,3), (world, 2)
    //4. Calculate the frequency of each word
    val wordToCout = wordGroup.map{
      case(word, list)=>{
        (word,list.size)
      }
    }
    //5. Print result
    val array: Array[(String, Int)] = wordToCout.collect();
    array.foreach(println);
    //TODO close connection
    sc.stop()
  }

}

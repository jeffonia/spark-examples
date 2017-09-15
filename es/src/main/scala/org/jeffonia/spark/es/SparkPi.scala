/*
 * Feel free to share it
 */
package org.jeffonia.spark.es

import scala.math.random

import org.jeffonia.spark.common.Logging

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gjf11847 on 2017/7/24.
 */
object SparkPi extends Logging {
  val jsonString: String =
    "{\"query\": {\"bool\": {\"filter\": [{\"range\": {\"date\": {\"gte\": \"1499244825036\"," +
      "\"lte\": " +
      "\"1500627225000\"}}}]}}}"

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("jeff")
  val spark: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    for (i <- 1 to 3) {
      sparkPiWithEs()
      sparkPiWithoutEs()
    }
  }

  def sparkPiWithEs(): Unit = {
    val start = System.currentTimeMillis()
    val count = spark.parallelize(1 until 1000, 2).repartition(10)
      //      .mapPartitions(partition => {
      //        //        client = transportClient("10.100.157.99", "test-cluster", 9700, client)
      //        partition
      //      })
      .map { i =>
      EsClient.readFromEs(EsClient.esTransport, jsonString, "premier", "loginfo")
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    logInfo(s"With es time: ${System.currentTimeMillis() - start} for $count")
  }

  def sparkPiWithoutEs(): Unit = {
    val start = System.currentTimeMillis()
    spark.textFile("", 3)
    val count = spark.parallelize(1 until 1000, 2).repartition(10).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    logInfo(s"Without es time: ${System.currentTimeMillis() - start} for $count")
  }
}

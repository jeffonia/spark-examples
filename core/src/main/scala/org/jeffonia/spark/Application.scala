package org.jeffonia.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by gjf11847 on 2017/8/31.
  */
class Application {


}

//case class LogInfo(date: String, time: String, threadName: String, logLevel: String, className: String, lineNumber: String)

case class TimeStamp(year: String, month: String, day: String, hour: String, minute: String, second: String)

case class ThreadName(name: String)

object Application {
  lazy val sparkSession: SparkSession = SparkSession
    .builder
    .appName("Spark Pi")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val fileStr = "D:\\gjf11847\\桌面\\ys_log_Info.log"
    val logInfoPattern = LogInfoPattern
      .builder()
      .add(raw"(\d{4}.\d{2}.\d{2})", "date") // date
      .add(raw"\s[\w]{2}")
      .add(raw"\s(\d{2}\:\d{2}\:\d{2})", "time") // time
      .add(raw"\s[\w\+\:]{1,}")
      .add(raw"\s{1,}([\w]{1,})", "logLevel") // logLevel
      .add(raw"\s{1,}\[([\w\-]{1,})\]", "threadName") // threadName
      .add(raw"\s{1,}([\w\.]{1,})", "className") // className
      .add(raw"\s{1,}(\d{1,})", "lineNumber") // lineNumber
      .add(raw"\s([\w]{1,})", "methodName") //methodName
      .add(raw"\s\-")
      .add(raw"\s(.{1,})", "content")
      .build()

    val regexVal = logInfoPattern.getRegex
    val fields: Array[String] = logInfoPattern.getFields
    val sourceData = sparkSession.sparkContext.textFile(fileStr)
    sourceData.map {
      case regexVal(all@_*) => all.toArray
//        .map(fieldValue => (fields(all.indexOf(fieldValue)), fieldValue))
      case _ => Array[String]()
    }
      .map(elem => LogInfo(elem:_*))
      .filter(!_.equals(false))
      .foreach(println)
  }
}

package org.jeffonia.spark.hbase

/**
 * ClassName: HbaseClient
 * Description:
 * Date: 2016/8/17 17:21
 *
 * @author jeff
 * @version ${project_version}
 * @since JDK 1.7.04
 */
object HbaseClient {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1 * 30))

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.foreachRDD { x => hbaseSave(x) }
    //    val tuple3 = TableUtil.getTables("test")
    //    println("++++++++++++++++++++" + tuple3._1 + ", " + tuple3._2 + ", " + tuple3._3)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def hbaseSave(result: RDD[(String, Int)]) = {
    result.repartition(100).foreachPartition {
      partitionOfRecords => {
        val table = HBaseFactory.getTable("test")
        partitionOfRecords.foreach(record => {
          val put = new Put(Bytes.toBytes(record._1))
          table.put(put)
        })
      }
    }

  }
}

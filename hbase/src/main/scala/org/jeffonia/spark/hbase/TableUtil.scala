package org.jeffonia.spark.hbase


/**
 * ClassName: TableUtil
 * Description:
 * Date: 2016/8/17 17:33
 *
 * @author jeff
 * @version ${project_version}
 * @since JDK 1.7.04
 */
object TableUtil {
  private val HBASE_FAMILY = Bytes.toBytes("f")

  /**
   * return  (dropTable,currentTable,targetTable)
   * dropTable:  table to be dropped
   * currnetTable:  current table used online
   * targetTable:  target table to insert data
   */
  def getTables(dictRowkey: String) = {
    //    val tableName = "dictionary"
    val tableName = "test"
    val table = HBaseFactory.getTable(tableName)

    val get = new Get(Bytes.toBytes("1"))
    var currentTable = ""
    val rs = table.get(get)
    if (rs != null) {
      currentTable = new String(rs.getValue(HBASE_FAMILY, Bytes.toBytes("test")))
    }
    table.close()
    println(currentTable)
    //    val num = currentTable.substring(currentTable.length() - 1, currentTable.length()).toInt
    //    val name = currentTable.substring(0, currentTable.length() - 1)
    //
    //    if (num == 1) (name + "3", currentTable, name + "2")
    //    else if (num == 2) (name + "1", currentTable, name + "3")
    //    else (name + "2", currentTable, name + "1")
  }

  /**
   * hbase batch save
   */
  def hbaseBatchSave(result: List[(String, List[(String, String)])], targetTable: String, useMD5:
  Boolean) {
    try {
      val num = 5000
      val table = HBaseFactory.getTable(targetTable)
      table.setWriteBufferSize(5 * 1024 * 1024)
      table.setAutoFlush(false, true)
      //put to hbase
      val puts = new util.ArrayList[Put]()
      result.foreach { x =>
        if (x._2.nonEmpty) {
          var put = new Put(Bytes.toBytes(x._1))
          //put.setDurability(Durability.SKIP_WAL)
          if (useMD5) {
            put = new Put(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(x._1))))
          }
          for (p <- x._2) {
            put.add(HBASE_FAMILY, Bytes.toBytes(p._1), Bytes.toBytes(p._2))
          }
          puts.add(put)
          if (puts.size() >= num) {
            table.put(puts)
            table.flushCommits()
            puts.clear()
          }
        }
      }
      if (puts.size() > 0) {
        table.put(puts)
        table.flushCommits()
      }
      table.close()
      println("===[HBase][" + targetTable + "]Batch " + result.length + " items will be put into " +
        "hbase.")
    } catch {
      case t: Throwable =>
        val msg = "[error]===HBaseOp hbaseSave error. " + t.getMessage
        println(msg)
        throw new Exception(msg)
    }
  }

  def hbaseSave(result: RDD[(String, Int)]) = {
    val table = HBaseFactory.getTable("test")
    val puts = new util.ArrayList[Put]()
    result.repartition(100).foreach {
      x => {
        var put = new Put(Bytes.toBytes(x._1))
        puts.add(put)
        if (puts.size() >= 1) {
          table.put(puts)
          table.flushCommits()
          puts.clear()
        }
      }
    }
    if (puts.size() > 0) {
      table.put(puts)
      table.flushCommits()
    }
    //    Thread.sleep(1 * 10 * 1000)
    table.close()
  }

}

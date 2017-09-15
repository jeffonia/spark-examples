package org.jeffonia.spark.hbase

/**
 * ClassName: HBaseFactory
 * Description:
 * Date: 2016/8/17 17:22
 *
 * @author jeff
 * @version ${project_version}
 * @since JDK 1.7.04
 */
object HBaseFactory {
  val hbaseConfig = HBaseConfiguration.create()
  hbaseConfig.set("hbase.zookeeper.quorum", "spmaster,spslave1,spslave2,spslave3,spslave4")
  hbaseConfig.set("hbase.zookeeper.property.clientPort", "2182")
  hbaseConfig.setInt("timeout", 120000)
  val conn = new HTablePool(hbaseConfig, 1)

  def getWriterConf(tableName: String): JobConf = {
    val jobConf = new JobConf(hbaseConfig, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf
  }

  def getScannerConf(tableName: String, columnFamily: String, column: String): Configuration = {
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, tableName)
    val scan = new Scan
    scan.setFilter(new SingleColumnValueFilter(columnFamily.getBytes(),
      column.getBytes(), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(10)))
    hbaseConfig.set(TableInputFormat.SCAN, scanToString(scan))
    hbaseConfig
  }

  private def scanToString(scan: Scan) = {
    val protoBuf = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(protoBuf.toByteArray)
  }

  def getTable(tableName: String): HTableInterface = {
    conn.getTable(tableName)
  }
}

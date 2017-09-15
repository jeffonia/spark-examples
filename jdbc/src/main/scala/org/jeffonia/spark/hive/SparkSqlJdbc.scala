/*
 * Copyright (c) 2017.
 * Some examples about spark, feel free for share it.
 */

package org.jeffonia.spark.hive

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException, Statement}

import org.jeffonia.spark.common.{Logging, Utils}

/**
 * Created by gjf11847 on 2017/9/15.
 */
object SparkSqlJdbc extends Logging {
  private val URL = "jdbc:hive2://172.18.188.3:10061/default"
  private val USER = "hotel"
  private val PASSWORD = ""
  private var connection: Connection = _

  def main(args: Array[String]): Unit = {
    var statement: Statement = null
    var preparedStatement: PreparedStatement = null
    try
      Utils.classForName("org.apache.hive.jdbc.HiveDriver")
    catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
    }
    val query = "SELECT * FROM xptest1 limit 100000"
    try {
      connection = DriverManager.getConnection(URL, USER, PASSWORD)
      statement = connection.createStatement
      val resultSet = statement.executeQuery(query)
      preparedStatement = connection.prepareStatement(query)
      val thread = new Thread(new Cancel(preparedStatement))
      thread.start()
      logInfo(s"Query: $query is working...")
      val pResultSet = preparedStatement.executeQuery
      var count = 0
      while ( {
        pResultSet.next
      }) count = count + 1
      logInfo(s"Resultset has data in rows: $count")
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    } finally try {
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }

  private class Cancel private[dc](val preparedStatement: PreparedStatement) extends Runnable {

    override def run(): Unit = {
      try
        Thread.sleep(3 * 1000)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
      logInfo("Trying to cancel query.")
      if (null == preparedStatement) {
        logInfo("Statement has been closed")
        return
      }
      try
        preparedStatement.cancel()
      catch {
        case e: SQLException =>
          e.printStackTrace()
      }
    }
  }

}

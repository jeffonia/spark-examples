/*
 * Feel free to share it
 */
package org.jeffonia.spark.core

import scala.collection.mutable
import scala.util.matching.UnanchoredRegex

/**
 * Created by gjf11847 on 2017/9/1.
 */
class LogInfoPattern(patterns: mutable.ListBuffer[String], fields: mutable.ListBuffer[String])
  extends Serializable {

  def getRegex: UnanchoredRegex = {
    patterns.mkString.r.unanchored
  }

  def getLogInfo(fields: Array[String]): LogInfo = {
    LogInfo(fields: _*)
  }

  def getFields: Array[String] = fields.toArray
}

// case class LogInfo(fields: mutable.ListBuffer[String])
case class LogInfo(fields: String*)

object LogInfoPattern {

  def builder(): PatterBuilder = new PatterBuilder

  class PatterBuilder {
    lazy val patterns: mutable.ListBuffer[String] = mutable.ListBuffer[String]()
    lazy val fields: mutable.ListBuffer[String] = mutable.ListBuffer[String]()

    def add(pattern: String, field: String): PatterBuilder = {
      fields.+=(field)
      add(pattern)
    }

    def add(pattern: String): PatterBuilder = {
      patterns.+=(pattern)
      this
    }

    def remove(pattern: String, field: String): PatterBuilder = {
      fields.-=(pattern)
      remove(pattern)
    }

    def remove(pattern: String): PatterBuilder = {
      patterns.-=(pattern)
      this
    }

    def build(): LogInfoPattern = {
      new LogInfoPattern(patterns, fields)
    }
  }

}

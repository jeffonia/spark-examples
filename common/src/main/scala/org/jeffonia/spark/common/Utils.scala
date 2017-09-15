/*
 * Feel free to share it
 */
package org.jeffonia.spark.common

import scala.util.Try

object Utils {

  /**
   * Preferred alternative to Class forName(className).
   */
  def classForName(className: String): Class[_] = {
    // scalastyle:off classforname
    Class.forName(className, true, getCurrentClassLoader)
    // scalastyle:on classforname
  }

  /**
   * Get the current thread ClassLoader.
   */
  def getCurrentClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getRootClassLoader)

  /**
   * Get the root ClassLoader.
   */
  def getRootClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * Determines whether the provided class is loadable in the current thread.
   */
  def classIsLoadable(clazz: String): Boolean = {
    // scalastyle:off classforname
    Try {
      Class.forName(clazz, false, getCurrentClassLoader)
    }.isSuccess
    // scalastyle:on classforname
  }

}

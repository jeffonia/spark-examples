
/*
 * Copyright (c) 2017.
 * Some examples about spark, feel free for share it.
 */

package org.jeffonia.spark.common

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.google.gson._
import com.google.gson.stream.JsonReader

/**
 * Created by gjf11847 on 2017/8/24.
 */
object JsonUtils {

  val gSon: Gson = new Gson()

  val jsonParser = new JsonParser()

  def load(file: String): JsonElement = {
    val result = jsonParser.parse(new JsonReader(Files.newReader(new File(file), Charsets.UTF_8)))
    result match {
      case _: JsonObject => result.getAsJsonObject
      case _: JsonArray => result.getAsJsonArray
      case _: JsonPrimitive => result.getAsJsonPrimitive
      case _: JsonNull | _ => result.getAsJsonNull
    }

  }

}

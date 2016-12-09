/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package co.aa8y.spark.sql.datasource.dynamojson

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.commons.lang3.time.FastDateFormat

/**
 * Options for the JSON data source.
 *
 * Most of these map directly to Jackson's internal options, specified in [[JsonParser.Feature]].
 */
class JSONOptions(
    @transient private val parameters: Map[String, String])
  extends Serializable  {

  val samplingRatio =
    parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
  val primitivesAsString =
    parameters.get("primitivesAsString").map(_.toBoolean).getOrElse(false)
  val prefersDecimal =
    parameters.get("prefersDecimal").map(_.toBoolean).getOrElse(false)
  val allowComments =
    parameters.get("allowComments").map(_.toBoolean).getOrElse(false)
  val allowUnquotedFieldNames =
    parameters.get("allowUnquotedFieldNames").map(_.toBoolean).getOrElse(false)
  val allowSingleQuotes =
    parameters.get("allowSingleQuotes").map(_.toBoolean).getOrElse(true)
  val allowNumericLeadingZeros =
    parameters.get("allowNumericLeadingZeros").map(_.toBoolean).getOrElse(false)
  val allowNonNumericNumbers =
    parameters.get("allowNonNumericNumbers").map(_.toBoolean).getOrElse(true)
  val allowBackslashEscapingAnyCharacter =
    parameters.get("allowBackslashEscapingAnyCharacter").map(_.toBoolean).getOrElse(false)
  val compressionCodec = parameters.get("compression").map(CompressionCodecs.getCodecClassName)
  private val parseMode = parameters.getOrElse("mode", "PERMISSIVE")
  val columnNameOfCorruptRecord = parameters.get("columnNameOfCorruptRecord")

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"))

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(
      parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"))

  // Parse mode flags
  // if (!ParseModes.isValidMode(parseMode)) {
  //   logWarning(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  // }

  val failFast = ParseModes.isFailFastMode(parseMode)
  val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  val permissive = ParseModes.isPermissiveMode(parseMode)

  /** Sets config options on a Jackson [[JsonFactory]]. */
  def setJacksonOptions(factory: JsonFactory): Unit = {
    factory.configure(JsonParser.Feature.ALLOW_COMMENTS, allowComments)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, allowUnquotedFieldNames)
    factory.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, allowSingleQuotes)
    factory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, allowNumericLeadingZeros)
    factory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, allowNonNumericNumbers)
    factory.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
      allowBackslashEscapingAnyCharacter)
  }
}

private object ParseModes {
  val PERMISSIVE_MODE = "PERMISSIVE"
  val DROP_MALFORMED_MODE = "DROPMALFORMED"
  val FAIL_FAST_MODE = "FAILFAST"

  val DEFAULT = PERMISSIVE_MODE

  def isValidMode(mode: String): Boolean = {
    mode.toUpperCase match {
      case PERMISSIVE_MODE | DROP_MALFORMED_MODE | FAIL_FAST_MODE => true
      case _ => false
    }
  }

  def isDropMalformedMode(mode: String): Boolean = mode.toUpperCase == DROP_MALFORMED_MODE
  def isFailFastMode(mode: String): Boolean = mode.toUpperCase == FAIL_FAST_MODE
  def isPermissiveMode(mode: String): Boolean = if (isValidMode(mode))  {
    mode.toUpperCase == PERMISSIVE_MODE
  } else {
    true // We default to permissive is the mode string is not valid
  }
}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.{BZip2Codec, DeflateCodec, GzipCodec, Lz4Codec, SnappyCodec}

import org.apache.spark.util.Utils

private object CompressionCodecs {
  private val shortCompressionCodecNames = Map(
    "none" -> null,
    "uncompressed" -> null,
    "bzip2" -> classOf[BZip2Codec].getName,
    "deflate" -> classOf[DeflateCodec].getName,
    "gzip" -> classOf[GzipCodec].getName,
    "lz4" -> classOf[Lz4Codec].getName,
    "snappy" -> classOf[SnappyCodec].getName)

  /**
   * Return the full version of the given codec class.
   * If it is already a class name, just return it.
   */
  def getCodecClassName(name: String): String = {
    val codecName = shortCompressionCodecNames.getOrElse(name.toLowerCase, name)
    try {
      // Validate the codec name
      if (codecName != null) {
        Class.forName(codecName, true, 
					Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader))
      }
      codecName
    } catch {
      case e: ClassNotFoundException =>
        throw new IllegalArgumentException(s"Codec [$codecName] " +
          s"is not available. Known codecs are ${shortCompressionCodecNames.keys.mkString(", ")}.")
    }
  }

  /**
   * Set compression configurations to Hadoop `Configuration`.
   * `codec` should be a full class path
   */
  def setCodecConfiguration(conf: Configuration, codec: String): Unit = {
    if (codec != null) {
      conf.set("mapreduce.output.fileoutputformat.compress", "true")
      conf.set("mapreduce.output.fileoutputformat.compress.type", CompressionType.BLOCK.toString)
      conf.set("mapreduce.output.fileoutputformat.compress.codec", codec)
      conf.set("mapreduce.map.output.compress", "true")
      conf.set("mapreduce.map.output.compress.codec", codec)
    } else {
      // This infers the option `compression` is set to `uncompressed` or `none`.
      conf.set("mapreduce.output.fileoutputformat.compress", "false")
      conf.set("mapreduce.map.output.compress", "false")
    }
  }
}

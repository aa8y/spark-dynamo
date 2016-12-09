package co.aa8y.spark.sql

import org.apache.spark.sql.{ DataFrame, DataFrameReader }

object RichDataFrameReader {

  implicit class DynamoDataFrameReader(reader: DataFrameReader) {
    def dynamojson(path: String): DataFrame = dynamojson(Seq(path): _*)

    @scala.annotation.varargs
    def dynamojson(paths: String*): DataFrame = reader.format("dynamojson").load(paths : _*)
  }
}

package co.aa8y.spark.sql.datasource.dynamojson

import org.apache.spark.sql.types.{ ArrayType, BooleanType, ByteType, DecimalType, LongType }
import org.apache.spark.sql.types.{ DoubleType, MapType, NullType, StringType }
import org.apache.spark.sql.types.{ DataType, StructField, StructType }
import com.fasterxml.jackson.core._
import org.scalatest.FunSpec

class InferSchemaSpec extends FunSpec {
  describe("InferSchema") {
    describe("inferField()") {
      it("should parse dynamo json") {
        val parser = new JsonFactory().createParser("""{"id":{"n":"1"},"name":{"s":"Doe, John"}}""")
        parser.nextToken
        val computedType = InferSchema.inferField(parser, new JSONOptions(Map()))
        val expectedType = StructType(Array(
          StructField("id", DoubleType, true), StructField("name", StringType, true))
        )
        assert(computedType === expectedType)
      }
    }
  }
}

package co.aa8y.spark.sql.datasource.dynamojson

import org.apache.spark.sql.types.{ ArrayType, BooleanType, ByteType, DecimalType, LongType }
import org.apache.spark.sql.types.{ DoubleType, MapType, NullType, StringType }
import org.apache.spark.sql.types.{ DataType, StructField, StructType }
import com.fasterxml.jackson.core.JsonFactory
import org.scalatest.FunSpec

class InferSchemaSpec extends FunSpec {
  describe("InferSchema") {
    describe("inferField()") {
      def inferField(dynamoJson: String): DataType = {
        val parser = new JsonFactory().createParser(dynamoJson)
        parser.nextToken // First token is always null.
        InferSchema.inferField(parser, new JSONOptions(Map()))
      }
      it("should parse Dynamo JSON with a single field.") {
        val computedType = inferField("""{"id": {"n": "1"}}""")
        val expectedType = StructType(Array(
          StructField("id", DoubleType, true)
        ))
        assert(computedType === expectedType)
      }
      it("should parse Dynamo JSON with multiple fields, all primitive types.") {
        val computedType = inferField("""
          |{
          |  "id": {"n": "1"}, 
          |  "name": {"s": "Doe, John"},
          |  "is_admin": {"bOOL": "true"}, 
          |  "gender": {"nULL": "true"}
          |}
        """.stripMargin)
        val expectedType = StructType(Array(
          StructField("gender", NullType, true),
          StructField("id", DoubleType, true),
          StructField("is_admin", BooleanType, true),
          StructField("name", StringType, true)
        ))
        assert(computedType === expectedType)
      }
      it("should parse Dynamo JSON with Dynamo maps.") {
        val computedType = inferField("""
          |{
          |  "id": {"n": "1"},
          |  "name": {
          |    "m": {
          |      "first_name": {"s": "John"}, 
          |      "last_name": {"s": "Doe"}
          |    }
          |  },
          |  "height": {
          |    "m": {
          |      "unit": {"s": "centimeters"}, 
          |      "value": {"n": "158"}
          |    }
          |  }
          |}
        """.stripMargin)
        val expectedType = StructType(Array(
          StructField("height", StructType(Array(
            StructField("unit", StringType, true),
            StructField("value", DoubleType, true)
          )), true),
          StructField("id", DoubleType, true),
          StructField("name", StructType(Array(
            StructField("first_name", StringType, true),
            StructField("last_name", StringType, true)
          )), true)
        ))
        assert(computedType === expectedType)
      }
      it("should parse Dynamo JSON with Dynamo sets.") {
        val computedType = inferField("""
          |{
          |  "id": {"n": "1"},
          |  "skills": {
          |    "m": {
          |      "programming": {"sS": ["c", "c++", "rust"]}
          |    }
          |  }
          |}
        """.stripMargin)
        val expectedType = StructType(Array(
          StructField("id", DoubleType, true),
          StructField("skills", StructType(Array(
            StructField("programming", ArrayType(StringType, true), true)
          )), true)
        ))
        assert(computedType === expectedType)
      }
      it("should parse Dynamo JSON with Dynamo maps and sets.") {
        val computedType = inferField("""
          |{
          |  "id": {"n": "1"},
          |  "name": {
          |    "m": {
          |      "first_name": {"s": "John"}, 
          |      "last_name": {"s": "Doe"}
          |    }
          |  },
          |  "height": {
          |    "m": {
          |      "unit": {"s": "centimeters"}, 
          |      "value": {"n": "158"}
          |    }
          |  },
          |  "skills": {
          |    "m": {
          |      "programming": {"sS": ["c", "c++", "rust"]}
          |    }
          |  }
          |}
        """.stripMargin)
        val expectedType = StructType(Array(
          StructField("height", StructType(Array(
            StructField("unit", StringType, true),
            StructField("value", DoubleType, true)
          )), true),
          StructField("id", DoubleType, true),
          StructField("name", StructType(Array(
            StructField("first_name", StringType, true),
            StructField("last_name", StringType, true)
          )), true),
          StructField("skills", StructType(Array(
            StructField("programming", ArrayType(StringType, true), true)
          )), true)
        ))
        assert(computedType === expectedType)
      }
    }
  }
}

package co.aa8y.spark.sql.datasource.dynamojson

import java.util.Comparator

import com.fasterxml.jackson.core.{ JsonParser, JsonToken }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ ArrayType, BooleanType, ByteType, DecimalType, LongType }
import org.apache.spark.sql.types.{ DoubleType, MapType, NullType, StringType }
import org.apache.spark.sql.types.{ DataType, StructField, StructType }

private[sql] object InferSchema {
  def inferField(parser: JsonParser, configOpts: JSONOptions): DataType = {
    import JsonToken._

    parser.getCurrentToken match {
      case FIELD_NAME =>
        parser.nextToken()
        inferField(parser, configOpts)

      case null | VALUE_NULL => NullType

      case VALUE_STRING if parser.getTextLength < 1 => NullType

      case VALUE_STRING => StringType

      case (VALUE_TRUE | VALUE_FALSE) if configOpts.primitivesAsString => StringType

      case VALUE_TRUE | VALUE_FALSE => BooleanType

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT) if configOpts.primitivesAsString => StringType

      case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
        import JsonParser.NumberType._

        def numberType = {
          val decimalValue = parser.getDecimalValue
          val precision = math.max(decimalValue.precision, decimalValue.scale)

          if (precision > DecimalType.MAX_PRECISION) DoubleType
          else DecimalType(precision, decimalValue.scale)
        }
        parser.getNumberType match {
          // For Integer values, use LongType by default.
          case INT | LONG => LongType
          // Since we do not have a data type backed by BigInteger,
          // when we see a Java BigInteger, we use DecimalType.
          case BIG_INTEGER | BIG_DECIMAL => numberType
          case FLOAT | DOUBLE if configOpts.prefersDecimal => numberType
          case FLOAT | DOUBLE => DoubleType
        }

      case START_OBJECT =>
        val builder = Array.newBuilder[StructField]
        while (nextUntil(parser, END_OBJECT)) {
          val fieldName = parser.getCurrentName
          val fieldType = inferField(parser, configOpts)
          builder += StructField(fieldName, fieldType, nullable = true)
        }
        val fields = builder.result
        inferDynamoType(fields)

      /*
      case START_ARRAY =>
        // If this JSON array is empty, we use NullType as a placeholder.
        // If this array is not empty in other JSON objects, we can resolve
        // the type as we pass through all JSON objects.
        var elementType: DataType = NullType
        while (nextUntil(parser, END_ARRAY)) {
          elementType = compatibleType(elementType, inferField(parser, configOpts))
        }
        ArrayType(elementType)
        */
    }
  }

  private def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = parser.nextToken match {
    case null => false
    case x => x != stopOn
  }

  // http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
  private def inferDynamoType(fields: Array[StructField]): DataType = fields match {
    case Array(StructField(fieldName, fieldType, _, _)) => fieldName match {
      case "b" => ByteType
      case "bOOL" => BooleanType
      case "n" => DoubleType
      case "nULL" => NullType
      case "s" => StringType
      case "bS" => ArrayType(elementType = ByteType, containsNull = true)
      case "nS" => ArrayType(elementType = DoubleType, containsNull = true)
      case "sS" => ArrayType(elementType = StringType, containsNull = true)
      case "m" => fieldType match {
        case schema: StructType =>
          val fields = schema.fields.map { case StructField(keyName, valueType, nullable, _) =>
            val actualValueType = valueType match {
              case v: StructType => inferDynamoType(v.fields)
              case v => v
            }
            StructField(keyName, actualValueType, nullable)
          }
					StructType(fields)
      }
    }
    case _ => StructType(fields)
  }
}

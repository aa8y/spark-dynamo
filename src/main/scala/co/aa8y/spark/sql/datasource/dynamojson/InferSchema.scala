package co.aa8y.spark.sql.datasource.dynamojson

import java.util.Comparator

import scala.annotation.tailrec

import com.fasterxml.jackson.core.{ JsonParser, JsonToken }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
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
        // Keys in JSON objects are not ordered. Sort the key/field names lexicographically
        // to make sure each JSON line gives us the field names in the same order.
        val fields = builder.result.sortWith((l, r) => l.name < r.name)
        inferDynamoType(fields)

      case START_ARRAY =>
        // If this JSON array is empty, we use NullType as a placeholder.
        // If this array is not empty in other JSON objects, we can resolve
        // the type as we pass through all JSON objects.
        var elementType: DataType = NullType
        while (nextUntil(parser, END_ARRAY)) {
          elementType = compatibleType(elementType, inferField(parser, configOpts))
        }
        ArrayType(elementType)
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
      case _ => StructType(fields)
    }
    case _ => StructType(fields)
  }

  def compatibleType(t1: DataType, t2: DataType): DataType = {
    TypeCoercion.findTightestCommonTypeOfTwo(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
        // in most case, also have better precision.
        case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) => DoubleType

        case (t1: DecimalType, t2: DecimalType) =>
          val scale = math.max(t1.scale, t2.scale)
          val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
          // DecimalType can't support precision > 38
          if (range + scale > 38) DoubleType
          else DecimalType(range + scale, scale)

        case (StructType(fields1), StructType(fields2)) =>
          // Both fields1 and fields2 should be sorted by name, since inferField performs sorting.
          // Therefore, we can take advantage of the fact that we're merging sorted lists and skip
          // building a hash map or performing additional sorting.
          assert(isSorted(fields1), s"StructType's fields were not sorted: ${fields1.toSeq}")
          assert(isSorted(fields2), s"StructType's fields were not sorted: ${fields2.toSeq}")

          val mergedFields = mergeSorted(fields1, fields2)
            .sortWith((l, r) => l.name < r.name)
            .foldLeft(Vector[StructField]()) { (merged, next) =>
              if (merged.isEmpty) merged :+ next
              else if (merged.last.name == next.name) {
                val dataType = compatibleType(merged.last.dataType, next.dataType)
                merged.dropRight(1) :+ StructField(next.name, dataType, nullable = true)
              } else merged :+ next
            }
          StructType(mergedFields)

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)

        // strings and every string is a Json object.
        case (_, _) => StringType
      }
    }
  }

  @tailrec
  private def mergeSorted(l: Array[StructField], r: Array[StructField])(
    implicit acc: List[StructField] = Nil
  ): List[StructField] = {
    if (l.isEmpty) acc ++ r
    else if (r.isEmpty) acc ++ l
    else if (l.head.name < r.head.name) mergeSorted(l.tail, r)(acc :+ l.head)
    else if (l.head.name > r.head.name) mergeSorted(l, r.tail)(acc :+ r.head)
    else mergeSorted(l.tail, r.tail)(acc :+ l.head :+ r.head)
  }

  @tailrec
  private def isSorted(fields: Array[StructField]): Boolean = fields match {
    case f if f.size <= 1 => true
    case Array(f1, f2, _*) if (f1.name < f2.name) => isSorted(fields.drop(2))
    case _ => false
  }
}

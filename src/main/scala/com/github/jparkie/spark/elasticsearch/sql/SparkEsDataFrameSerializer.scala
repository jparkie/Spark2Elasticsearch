package com.github.jparkie.spark.elasticsearch.sql

import java.sql.{ Date, Timestamp }

import com.github.jparkie.spark.elasticsearch.SparkEsSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.elasticsearch.common.xcontent.{ XContentBuilder, XContentFactory }

import scala.collection.JavaConverters._

/**
 * Serializes a Row from a DataFrame into an Array[Byte].
 *
 * @param schema The StructType of a DataFrame.
 */
class SparkEsDataFrameSerializer(schema: StructType) extends SparkEsSerializer[Row] {
  /**
   * Serializes a Row from a DataFrame into an Array[Byte].
   *
   * @param value A Row.
   * @return The source JSON as Array[Byte].
   */
  override def write(value: Row): Array[Byte] = {
    val currentJsonBuilder = XContentFactory.jsonBuilder()

    write(schema, value, currentJsonBuilder)

    currentJsonBuilder
      .bytes()
      .toBytes
  }

  private[sql] def write(dataType: DataType, value: Any, builder: XContentBuilder): XContentBuilder = {
    dataType match {
      case structType @ StructType(_)  => writeStruct(structType, value, builder)
      case arrayType @ ArrayType(_, _) => writeArray(arrayType, value, builder)
      case mapType @ MapType(_, _, _)  => writeMap(mapType, value, builder)
      case _                           => writePrimitive(dataType, value, builder)
    }
  }

  private[sql] def writeStruct(structType: StructType, value: Any, builder: XContentBuilder): XContentBuilder = {
    value match {
      case currentRow: Row =>
        builder.startObject()

        structType.fields.view.zipWithIndex foreach {
          case (field, index) =>
            builder.field(field.name)
            if (currentRow.isNullAt(index)) {
              builder.nullValue()
            } else {
              write(field.dataType, currentRow(index), builder)
            }
        }

        builder.endObject()
    }

    builder
  }

  private[sql] def writeArray(arrayType: ArrayType, value: Any, builder: XContentBuilder): XContentBuilder = {
    value match {
      case array: Array[_] =>
        serializeArray(arrayType.elementType, array, builder)
      case seq: Seq[_] =>
        serializeArray(arrayType.elementType, seq, builder)
      case _ =>
        throw new IllegalArgumentException(s"Unknown ArrayType: $value.")
    }
  }

  private[sql] def serializeArray(dataType: DataType, value: Seq[_], builder: XContentBuilder): XContentBuilder = {
    // TODO: Consider utilizing builder.value(Iterable[_]).
    builder.startArray()

    if (value != null) {
      value foreach { element =>
        write(dataType, element, builder)
      }
    }

    builder.endArray()
    builder
  }

  private[sql] def writeMap(mapType: MapType, value: Any, builder: XContentBuilder): XContentBuilder = {
    value match {
      case scalaMap: scala.collection.Map[_, _] =>
        serializeMap(mapType, scalaMap, builder)
      case javaMap: java.util.Map[_, _] =>
        serializeMap(mapType, javaMap.asScala, builder)
      case _ =>
        throw new IllegalArgumentException(s"Unknown MapType: $value.")
    }
  }

  private[sql] def serializeMap(mapType: MapType, value: scala.collection.Map[_, _], builder: XContentBuilder): XContentBuilder = {
    // TODO: Consider utilizing builder.value(Map[_, AnyRef]).
    builder.startObject()

    for ((currentKey, currentValue) <- value) {
      builder.field(currentKey.toString)
      write(mapType.valueType, currentValue, builder)
    }

    builder.endObject()
    builder
  }

  private[sql] def writePrimitive(dataType: DataType, value: Any, builder: XContentBuilder): XContentBuilder = {
    dataType match {
      case BinaryType    => builder.value(value.asInstanceOf[Array[Byte]])
      case BooleanType   => builder.value(value.asInstanceOf[Boolean])
      case ByteType      => builder.value(value.asInstanceOf[Byte])
      case ShortType     => builder.value(value.asInstanceOf[Short])
      case IntegerType   => builder.value(value.asInstanceOf[Int])
      case LongType      => builder.value(value.asInstanceOf[Long])
      case DoubleType    => builder.value(value.asInstanceOf[Double])
      case FloatType     => builder.value(value.asInstanceOf[Float])
      case TimestampType => builder.value(value.asInstanceOf[Timestamp])
      case DateType      => builder.value(value.asInstanceOf[Date])
      case StringType    => builder.value(value.toString)
      case _ =>
        throw new IllegalArgumentException(s"Unknown DataType: $value.")
    }
  }
}

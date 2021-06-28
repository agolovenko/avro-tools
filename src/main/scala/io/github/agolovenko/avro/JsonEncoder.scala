package io.github.agolovenko.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import play.api.libs.json._

import java.lang.{Boolean => JBool, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.nio.ByteBuffer
import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

class JsonEncoder(stringEncoders: Map[String, Any => String] = Map.empty) {
  import Schema.Type._
  def apply(data: GenericData.Record): JsObject =
    if (GenericData.get().validate(data.getSchema, data)) jsObj(data, data.getSchema)
    else throw new IllegalArgumentException("Invalid record")

  private def jsObj(data: GenericData.Record, schema: Schema): JsObject = {
    val fields = schema.getFields.asScala.map { field => field.name() -> jsVal(data.get(field.pos()), field.schema()) }

    JsObject(fields)
  }

  private def jsVal(data: Any, schema: Schema): JsValue =
    stringEncoders
      .get(typeName(schema))
      .fold {
        (schema.getType, data) match {
          case (NULL, null)                         => JsNull
          case (RECORD, record: GenericData.Record) => jsObj(record, schema)
          case (ENUM, enum: GenericData.EnumSymbol) => JsString(enum.toString)
          case (ARRAY, array: GenericData.Array[_]) => jsArray(array, schema)
          case (MAP, map: JMap[_, _])               => jsMap(map, schema)
          case (UNION, _)                           => union(data, schema)
          case (BYTES, _: ByteBuffer)               => throw new IllegalArgumentException(s"$BYTES type is supported through string encoders")
          case (FIXED, _: GenericData.Fixed)        => throw new IllegalArgumentException(s"$FIXED type is supported through string encoders")

          case (STRING, str: String)     => JsString(str)
          case (INT, int: JInt)          => JsNumber(BigDecimal(int.intValue()))
          case (LONG, long: JLong)       => JsNumber(BigDecimal(long.longValue()))
          case (FLOAT, float: JFloat)    => JsNumber(BigDecimal(float.doubleValue()))
          case (DOUBLE, double: JDouble) => JsNumber(BigDecimal(double.doubleValue()))
          case (BOOLEAN, bool: JBool)    => JsBoolean(bool.booleanValue())

          case (schemaType, value) => throw new IllegalArgumentException(s"Unexpected value '$value' of type $schemaType")
        }
      } { encoder =>
        try {
          JsString(encoder(data))
        } catch {
          case NonFatal(e) => throw new IllegalArgumentException(s"Failed to encode value '$data' of type ${typeName(schema)}: ${e.getMessage}")
        }
      }

  private def jsArray(data: GenericData.Array[_], schema: Schema): JsArray = {
    val elements = data.asScala.map { jsVal(_, schema.getElementType) }

    JsArray(elements)
  }

  private def jsMap(map: JMap[_, _], schema: Schema): JsObject = {
    val fields = map.asScala.map { case (key, value) => key.asInstanceOf[String] -> jsVal(value, schema.getValueType) }

    JsObject(fields)
  }

  private def union(data: Any, schema: Schema): JsValue = {
    val it = schema.getTypes.asScala.iterator.flatMap { subSchema => Try(jsVal(data, subSchema)).toOption }

    if (it.hasNext) it.next()
    else throw new IllegalArgumentException(s"Unexpected value '$data' of type ${typeName(schema)}")
  }
}

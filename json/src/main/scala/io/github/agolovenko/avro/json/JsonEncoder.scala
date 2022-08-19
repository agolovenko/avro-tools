package io.github.agolovenko.avro.json

import io.github.agolovenko.avro.{ParserException, Path, typeName}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import play.api.libs.json._

import java.lang.{Boolean => JBool, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.nio.ByteBuffer
import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

class JsonEncoder(stringEncoders: PartialFunction[(Any, Schema, Path), String] = PartialFunction.empty) {
  import Schema.Type._

  private val liftedEncoders = stringEncoders.lift

  def apply(data: GenericData.Record): JsObject = {
    implicit val path: Path = new Path
    if (GenericData.get().validate(data.getSchema, data)) jsObj(data, data.getSchema)
    else throw new ParserException("Invalid record")
  }

  private def jsObj(data: GenericData.Record, schema: Schema)(implicit path: Path): JsObject = {
    val fields = schema.getFields.asScala.map { field =>
      path.push(field.name())
      try {
        field.name() -> jsVal(data.get(field.pos()), field.schema())
      } finally {
        path.pop()
        ()
      }
    }

    JsObject(fields)
  }

  private def jsVal(data: Any, schema: Schema)(implicit path: Path): JsValue = {
    val encoded =
      try {
        liftedEncoders((data, schema, path))
      } catch {
        case NonFatal(e) => throw new ParserException(s"Failed to encode value '$data' of type ${typeName(schema)}: ${e.getMessage}")
      }

    encoded
      .fold {
        (schema.getType, data) match {
          case (NULL, null)                         => JsNull
          case (RECORD, record: GenericData.Record) => jsObj(record, schema)
          case (ENUM, enum: GenericData.EnumSymbol) => JsString(enum.toString)
          case (ARRAY, array: GenericData.Array[_]) => jsArray(array, schema)
          case (MAP, map: JMap[_, _])               => jsMap(map.asInstanceOf[JMap[String, _]], schema)
          case (UNION, _)                           => union(data, schema)
          case (BYTES, _: ByteBuffer)               => throw new ParserException(s"$BYTES type is supported through string encoders")
          case (FIXED, _: GenericData.Fixed)        => throw new ParserException(s"$FIXED type is supported through string encoders")

          case (STRING, str: String)     => JsString(str)
          case (INT, int: JInt)          => JsNumber(BigDecimal(int.intValue()))
          case (LONG, long: JLong)       => JsNumber(BigDecimal(long.longValue()))
          case (FLOAT, float: JFloat)    => JsNumber(BigDecimal(float.doubleValue()))
          case (DOUBLE, double: JDouble) => JsNumber(BigDecimal(double.doubleValue()))
          case (BOOLEAN, bool: JBool)    => JsBoolean(bool.booleanValue())

          case (schemaType, value) => throw new ParserException(s"Unexpected value '$value' of type $schemaType")
        }
      }(JsString.apply)
  }

  private def jsArray(data: GenericData.Array[_], schema: Schema)(implicit path: Path): JsArray = {
    val elements = data.asScala.zipWithIndex.map {
      case (element, idx) =>
        path.push(s"[$idx]")
        try {
          jsVal(element, schema.getElementType)
        } finally {
          path.pop()
          ()
        }
    }

    JsArray(elements)
  }

  private def jsMap(map: JMap[String, _], schema: Schema)(implicit path: Path): JsObject = {
    val fields = map.asScala.map {
      case (key, value) =>
        path.push(key)
        try {
          key -> jsVal(value, schema.getValueType)
        } finally {
          path.pop()
          ()
        }
    }

    JsObject(fields)
  }

  private def union(data: Any, schema: Schema)(implicit path: Path): JsValue = {
    val it = schema.getTypes.asScala.iterator.flatMap { subSchema => Try(jsVal(data, subSchema)).toOption }

    if (it.hasNext) it.next()
    else throw new ParserException(s"Unexpected value '$data' of type ${typeName(schema)}")
  }
}

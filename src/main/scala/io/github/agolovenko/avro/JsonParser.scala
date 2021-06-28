package io.github.agolovenko.avro

import io.github.agolovenko.avro.StackType.Stack
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData
import org.apache.avro.{JsonProperties, Schema}
import play.api.libs.json._

import java.lang.{Boolean => JBool, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.nio.ByteBuffer
import java.util.{HashMap => JHashMap, List => JList, Map => JMap}
import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

class JsonParser(stringParsers: Map[String, String => Any] = Map.empty) {
  def apply(data: JsValue, schema: Schema): GenericData.Record = {
    implicit val path = new Stack[String]()
    if (schema.getType == RECORD)
      readRecord(JsDefined(data), schema, defaultValue = None)
    else
      throw new JsonParserException(s"Unsupported root schema of type ${schema.getType}")
  }

  private def readAny(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Stack[String]): Any = schema.getType match {
    case RECORD => readRecord(data, schema, defaultValue)
    case ENUM   => readEnum(data, schema, defaultValue)
    case ARRAY  => readArray(data, schema, defaultValue)
    case MAP    => readMap(data, schema, defaultValue)
    case UNION  => readUnion(data, schema, defaultValue)
    case BYTES  => readBytes(data, schema, defaultValue)
    case FIXED  => readFixed(data, schema, defaultValue)

    case STRING  => read[String](data, schema, defaultValue)
    case INT     => read[Int](data, schema, defaultValue)
    case LONG    => read[Long](data, schema, defaultValue)
    case FLOAT   => read[Float](data, schema, defaultValue)
    case DOUBLE  => read[Double](data, schema, defaultValue)
    case BOOLEAN => read[Boolean](data, schema, defaultValue)

    case NULL => readNull(data, schema, defaultValue)
  }

  private def readRecord(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Stack[String]): GenericData.Record =
    data match {
      case JsDefined(obj: JsObject) =>
        val result = new GenericData.Record(schema)
        schema.getFields.asScala.foreach { field =>
          path.push(field.name())
          val value = readAny(obj \ field.name(), field.schema(), Option(field.defaultVal()))
          result.put(field.name(), value)
          path.pop()
        }
        result
      case JsDefined(otherNode) => throw new WrongTypeException(schema, otherNode)
      case _                    => fallbackToDefault(defaultValue, schema).asInstanceOf[GenericData.Record]
    }

  private def readEnum(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Stack[String]): GenericData.EnumSymbol = {
    val symbol = read[String](data, schema, defaultValue)

    if (schema.getEnumSymbols.contains(symbol)) new GenericData.EnumSymbol(schema, symbol)
    else throw new WrongTypeException(schema, data.get)
  }

  private def readArray(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Stack[String]): GenericData.Array[Any] =
    data match {
      case JsDefined(arr: JsArray) =>
        val result = new GenericData.Array[Any](arr.value.size, schema)
        arr.value.zipWithIndex.foreach {
          case (jsValue, idx) =>
            path.push(s"[$idx]")
            val value = readAny(JsDefined(jsValue), schema.getElementType, None)
            result.add(idx, value)
            path.pop()
        }
        result
      case JsDefined(otherNode) => throw new WrongTypeException(schema, otherNode)
      case _                    => fallbackToDefault(defaultValue, schema).asInstanceOf[GenericData.Array[Any]]
    }

  private def readMap(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Stack[String]): JMap[String, Any] =
    data match {
      case JsDefined(obj: JsObject) =>
        val result = new JHashMap[String, Any]()
        obj.value.foreach {
          case (key, jsValue) =>
            path.push(key)
            val value = readAny(JsDefined(jsValue), schema.getValueType, None)
            result.put(key, value)
            path.pop()
        }
        result
      case JsDefined(otherNode) => throw new WrongTypeException(schema, otherNode)
      case _                    => fallbackToDefault(defaultValue, schema).asInstanceOf[JMap[String, Any]]
    }

  private def readUnion(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Stack[String]): Any = {
    val it = schema.getTypes.asScala.iterator.zipWithIndex
      .flatMap {
        case (subSchema, idx) =>
          Try(readAny(data, subSchema, defaultValue.filter(_ => idx == 0))).toOption
      }

    if (it.hasNext) it.next()
    else
      data match {
        case JsDefined(otherNode) => throw new WrongTypeException(schema, otherNode)
        case _                    => throw new MissingValueException(schema)
      }
  }

  private def readBytes(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Stack[String]): Any =
    read[String](data, schema, defaultValue)

  private def readFixed(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Stack[String]): GenericData.Fixed = {
    val bytes = readBytes(data, schema, defaultValue).asInstanceOf[Array[Byte]]

    if (bytes.length == schema.getFixedSize) new GenericData.Fixed(schema, bytes)
    else throw new WrongTypeException(schema, data.get, Some(s"incorrect size: ${bytes.length} instead of ${schema.getFixedSize}"))
  }

  private def read[T: Reads](data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Stack[String]): Any = data match {
    case JsDefined(JsString(str)) => parseString(str, schema)
    case JsDefined(value) =>
      value
        .validate[T]
        .fold(
          invalid = _ => throw new WrongTypeException(schema, value),
          valid = identity
        )
    case _ => fallbackToDefault(defaultValue, schema)
  }

  private def parseString(str: String, schema: Schema)(implicit path: Stack[String]): Any = {
    if (schema.getType == STRING || schema.getType == ENUM) str
    else
      stringParsers.get(typeName(schema)).fold(throw new WrongTypeException(schema, JsString(str), Some("no string parser supplied"))) { parser =>
        try {
          parser(str)
        } catch {
          case NonFatal(e) => throw new WrongTypeException(schema, JsString(str), Option(e.getMessage))
        }
      }
  }

  private def readNull(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Stack[String]): Null = data match {
    case JsDefined(JsNull)    => null
    case JsDefined(otherNode) => throw new WrongTypeException(schema, otherNode)
    case _                    => fallbackToDefault(defaultValue, schema).asInstanceOf[Null]
  }

  private def fallbackToDefault(defaultValue: Option[Any], schema: Schema)(implicit path: Stack[String]): Any =
    defaultValue.fold(throw new MissingValueException(schema)) { extractDefaultValue(_, schema) }

  private def extractDefaultValue(defaultValue: Any, schema: Schema)(implicit path: Stack[String]): Any = (schema.getType, defaultValue) match {
    case (NULL, JsonProperties.NULL_VALUE) => null
    case (STRING, value: String)           => value
    case (ENUM, value: String)             => value
    case (INT, value: JInt)                => value.intValue()
    case (LONG, value: JLong)              => value.longValue()
    case (FLOAT, value: JFloat)            => value.floatValue()
    case (DOUBLE, value: JDouble)          => value.doubleValue()
    case (BOOLEAN, value: JBool)           => value.booleanValue()
    case (BYTES, value: Array[Byte])       => ByteBuffer.wrap(value)
    case (FIXED, value: Array[Byte])       => value

    case (ARRAY, list: JList[_]) =>
      val extracted = list.asScala.map { extractDefaultValue(_, schema.getElementType) }
      new GenericData.Array(schema, extracted.asJava)

    case (MAP, map: JMap[_, _]) =>
      map.asScala.view.mapValues { extractDefaultValue(_, schema.getValueType) }.toMap.asJava

    case (RECORD, map: JMap[_, _]) =>
      val result = new GenericData.Record(schema)
      map.asScala.foreach {
        case (k, value) =>
          val key       = k.asInstanceOf[String]
          val extracted = extractDefaultValue(value, schema.getField(key).schema())
          result.put(key, extracted)
      }
      result

    case _ => throw new JsonParserException(s"Unsupported default value $defaultValue for type ${schema.getType}")
  }
}

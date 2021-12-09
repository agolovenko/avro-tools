package io.github.agolovenko.avro.json

import io.github.agolovenko.avro._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData
import play.api.libs.json._

import java.util.{HashMap => JHashMap, Map => JMap}
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

class JsonParser(schema: Schema, stringParsers: Map[String, String => Any] = Map.empty, fieldRenamings: FieldRenamings = new FieldRenamings()) {
  def apply(data: JsValue): GenericData.Record = {
    implicit val path: Path = new Path
    if (schema.getType == RECORD)
      readRecord(JsDefined(data), schema, defaultValue = None)
    else
      throw new ParserException(s"Unsupported root schema of type ${schema.getType}")
  }

  private def readAny(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any = schema.getType match {
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

  private def readRecord(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Path): GenericData.Record =
    data match {
      case JsDefined(obj: JsObject) =>
        val result = new GenericData.Record(schema)
        schema.getFields.asScala.foreach { field =>
          val fieldName = fieldRenamings(field.name())
          path.push(fieldName)
          try {
            val value = readAny(obj \ fieldName, field.schema(), Option(field.defaultVal()))
            result.put(field.name(), value)
          } finally {
            path.pop()
            ()
          }
        }
        result
      case JsDefined(otherNode) => throw new WrongTypeException(schema, otherNode.toString())
      case _                    => fallbackToDefault(defaultValue, schema).asInstanceOf[GenericData.Record]
    }

  private def readEnum(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Path): GenericData.EnumSymbol = {
    val symbol = read[String](data, schema, defaultValue)

    if (schema.getEnumSymbols.contains(symbol)) new GenericData.EnumSymbol(schema, symbol)
    else throw new WrongTypeException(schema, data.get.toString())
  }

  private def readArray(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Path): GenericData.Array[Any] =
    data match {
      case JsDefined(arr: JsArray) =>
        val result = new GenericData.Array[Any](arr.value.size, schema)
        arr.value.zipWithIndex.foreach {
          case (jsValue, idx) =>
            path.push(s"[$idx]")
            try {
              val value = readAny(JsDefined(jsValue), schema.getElementType, None)
              result.add(idx, value)
            } finally {
              path.pop()
              ()
            }
        }
        result
      case JsDefined(otherNode) => throw new WrongTypeException(schema, otherNode.toString())
      case _                    => fallbackToDefault(defaultValue, schema).asInstanceOf[GenericData.Array[Any]]
    }

  private def readMap(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Path): JMap[String, Any] =
    data match {
      case JsDefined(obj: JsObject) =>
        val result = new JHashMap[String, Any]()
        obj.value.foreach {
          case (key, jsValue) =>
            path.push(key)
            try {
              val value = readAny(JsDefined(jsValue), schema.getValueType, None)
              result.put(key, value)
            } finally {
              path.pop()
              ()
            }
        }
        result
      case JsDefined(otherNode) => throw new WrongTypeException(schema, otherNode.toString())
      case _                    => fallbackToDefault(defaultValue, schema).asInstanceOf[JMap[String, Any]]
    }

  private def readUnion(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any = {
    def unionIt = schema.getTypes.asScala.iterator.zipWithIndex.map {
      case (subSchema, idx) => Try(readAny(data, subSchema, defaultValue.filter(_ => idx == 0)))
    }

    val it = unionIt.flatMap(_.toOption)

    if (it.hasNext) it.next()
    else
      data match {
        case JsDefined(otherNode) =>
          val explanations = unionIt.flatMap(_.failed.map(_.getMessage).toOption).toSeq
          throw new WrongTypeException(schema, otherNode.toString(), explanations)
        case _ => throw new MissingValueException(schema)
      }
  }

  private def readBytes(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any =
    read[String](data, schema, defaultValue)

  private def readFixed(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Path): GenericData.Fixed = {
    val bytes = readBytes(data, schema, defaultValue).asInstanceOf[Array[Byte]]

    if (bytes.length == schema.getFixedSize) new GenericData.Fixed(schema, bytes)
    else throw new WrongTypeException(schema, data.get.toString(), Seq(s"incorrect size: ${bytes.length} instead of ${schema.getFixedSize}"))
  }

  private def read[T: Reads](data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any = data match {
    case JsDefined(JsString(str)) => parseString(str, schema)
    case JsDefined(value) =>
      value
        .validate[T]
        .fold(
          invalid = _ => throw new WrongTypeException(schema, value.toString()),
          valid = identity
        )
    case _ => fallbackToDefault(defaultValue, schema)
  }

  private def parseString(str: String, schema: Schema)(implicit path: Path): Any = {
    if (schema.getType == STRING || schema.getType == ENUM) str
    else
      stringParsers.get(typeName(schema)).fold(throw new WrongTypeException(schema, str, Seq("no string parser supplied"))) { parser =>
        try {
          parser(str)
        } catch {
          case NonFatal(e) => throw new WrongTypeException(schema, str, Seq(e.getMessage))
        }
      }
  }

  private def readNull(data: JsLookupResult, schema: Schema, defaultValue: Option[Any])(implicit path: Path): Null = data match {
    case JsDefined(JsNull)    => null
    case JsDefined(otherNode) => throw new WrongTypeException(schema, otherNode.toString())
    case _                    => fallbackToDefault(defaultValue, schema).asInstanceOf[Null]
  }
}

package io.github.agolovenko.avro

import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData
import org.apache.avro.{JsonProperties, Schema}

import java.lang.{Boolean => JBool, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.nio.ByteBuffer
import java.util.{List => JList, Map => JMap}
import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

abstract class AbstractParser[Input](stringParsers: PartialFunction[ParserContext, Any], validations: PartialFunction[ValidationContext, Unit]) {
  private val liftedParsers     = stringParsers.lift
  private val liftedValidations = validations.lift

  def apply(data: Input): GenericData.Record

  protected def parseString(str: String, schema: Schema)(implicit path: Path): Any =
    if (schema.getType == STRING || schema.getType == ENUM) str
    else {
      val parsed =
        try {
          liftedParsers(ParserContext(str, schema, path))
        } catch {
          case NonFatal(e) => throw new WrongTypeException(schema, str, Seq(e.getMessage))
        }

      parsed.getOrElse(throw new WrongTypeException(schema, str, Seq("no string parser supplied")))
    }

  protected def validate(value: Any, schema: Schema)(implicit path: Path): Unit = {
    if (value != null)
      try {
        liftedValidations(ValidationContext(value, schema, path))
      } catch {
        case NonFatal(e) => throw new InvalidValueException(value, e.getMessage)
      }

    ()
  }

  protected def fallbackToDefault(defaultValue: Option[Any], schema: Schema)(implicit path: Path): Any =
    defaultValue.fold(throw new MissingValueException(schema)) {
      extractDefaultValue(_, schema)
    }

  private def extractDefaultValue(defaultValue: Any, schema: Schema)(implicit path: Path): Any = (schema.getType, defaultValue) match {
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
      val extracted = list.asScala.map {
        extractDefaultValue(_, schema.getElementType)
      }
      new GenericData.Array(schema, extracted.asJava)

    case (MAP, map: JMap[_, _]) =>
      map.asScala.view
        .mapValues {
          extractDefaultValue(_, schema.getValueType)
        }
        .toMap
        .asJava

    case (RECORD, map: JMap[_, _]) =>
      val result = new GenericData.Record(schema)
      map.asScala.foreach {
        case (k, value) =>
          val key       = k.asInstanceOf[String]
          val extracted = extractDefaultValue(value, schema.getField(key).schema())
          result.put(key, extracted)
      }
      result

    case _ => throw new ParserException(s"Unsupported default value $defaultValue for type ${schema.getType}")
  }
}

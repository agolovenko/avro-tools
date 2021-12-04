package io.github.agolovenko.avro.xml

import io.github.agolovenko.avro.{FieldRenamings, Path, typeName}
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData
import org.apache.avro.{JsonProperties, Schema}

import java.lang.{Boolean => JBool, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.nio.ByteBuffer
import java.util.{List => JList, Map => JMap}
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal
import scala.xml._

class XmlParser(schema: Schema, stringParsers: Map[String, String => Any] = Map.empty, fieldRenamings: FieldRenamings = new FieldRenamings(Map.empty)) {
  def apply(data: Elem): GenericData.Record = {
    implicit val path = new Path
    if (schema.getType == RECORD)
      if (schema.getName == data.label) readRecord(data, schema, defaultValue = None)
      else throw new XmlParserException(s"Expected '${schema.getName}' root node, got instead: '${data.label}'")
    else throw new XmlParserException(s"Unsupported root schema of type ${schema.getType}")
  }

  private def readAny(data: NodeSeq, attributes: Option[Seq[Node]], schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any =
    schema.getType match {
      case RECORD  => readRecord(data, schema, defaultValue)
      case ENUM    => readEnum(data, attributes, schema, defaultValue)
      case MAP     => throw new XmlParserException("'MAP' type is not supported for XML format")
      case ARRAY   => readArray(data, schema, defaultValue)
      case UNION   => readUnion(data, attributes, schema, defaultValue)
      case BYTES   => readBytes(data, attributes, schema, defaultValue)
      case FIXED   => readFixed(data, attributes, schema, defaultValue)
      case STRING  => read(data, attributes, schema, defaultValue)
      case INT     => read(data, attributes, schema, defaultValue)
      case LONG    => read(data, attributes, schema, defaultValue)
      case FLOAT   => read(data, attributes, schema, defaultValue)
      case DOUBLE  => read(data, attributes, schema, defaultValue)
      case BOOLEAN => read(data, attributes, schema, defaultValue)

      case NULL => readNull(data, attributes, schema, defaultValue)
    }

  private def readRecord(data: NodeSeq, schema: Schema, defaultValue: Option[Any])(implicit path: Path): GenericData.Record =
    data match {
      case Single(elem: Elem) =>
        val result = new GenericData.Record(schema)
        schema.getFields.asScala.foreach { field =>
          val fieldName = fieldRenamings(field.name())
          path.push(fieldName)
          val value = readAny(elem \ fieldName, elem.attributes.get(field.name()).map(_.toSeq), field.schema(), Option(field.defaultVal()))
          result.put(field.name(), value)
          path.pop()
        }
        result
      case nodes if nodes.isEmpty => fallbackToDefault(defaultValue, schema).asInstanceOf[GenericData.Record]
      case _                      => throw new WrongTypeException(schema, data)
    }

  private def readEnum(data: NodeSeq, attributes: Option[Seq[Node]], schema: Schema, defaultValue: Option[Any])(
      implicit path: Path
  ): GenericData.EnumSymbol = {
    val symbol = read(data, attributes, schema, defaultValue)

    if (schema.getEnumSymbols.contains(symbol)) new GenericData.EnumSymbol(schema, symbol)
    else throw new WrongTypeException(schema, data)
  }

  private def readArray(data: NodeSeq, schema: Schema, defaultValue: Option[Any])(implicit path: Path): GenericData.Array[Any] = {
    val elemLabel =
      if (schema.getElementType.getType == RECORD)
        schema.getElementType.getName
      else throw new XmlParserException(s"Only 'RECORD' is supported as 'ARRAY' member, got instead ${schema.getElementType.getName}")

    data match {
      case Single(elem: Elem) if elem.label != elemLabel => parseArray(elem.child, schema)
      case nodes if nodes.isEmpty                        => fallbackToDefault(defaultValue, schema).asInstanceOf[GenericData.Array[Any]]
      case nodes if nodes.forall(_.label == elemLabel)   => parseArray(nodes, schema)
      case _                                             => throw new WrongTypeException(schema, data)
    }
  }

  private def parseArray(nodes: Seq[Node], schema: Schema)(implicit path: Path): GenericData.Array[Any] = {
    val elems = nodes.collect { case elem: Elem => elem }

    val result = new GenericData.Array[Any](elems.size, schema)
    elems.zipWithIndex.foreach {
      case (child, idx) =>
        path.push(s"[$idx]")
        val value = readAny(child, attributes = None, schema.getElementType, None)
        result.add(idx, value)
        path.pop()
    }
    result
  }

  private def readUnion(data: NodeSeq, attributes: Option[Seq[Node]], schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any = {
    def unionIt = schema.getTypes.asScala.iterator.zipWithIndex.map {
      case (subSchema, idx) => Try(readAny(data, attributes, subSchema, defaultValue.filter(_ => idx == 0)))
    }

    val it = unionIt.flatMap(_.toOption)

    if (it.hasNext) it.next()
    else if (data.isEmpty) throw new MissingValueException(schema)
    else {
      val explanation = unionIt.flatMap(_.failed.map(_.getMessage).toOption).mkString("; ")
      throw new WrongTypeException(schema, data, Some(explanation))
    }
  }

  private def readBytes(data: NodeSeq, attributes: Option[Seq[Node]], schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any =
    read(data, attributes, schema, defaultValue)

  private def readFixed(data: NodeSeq, attributes: Option[Seq[Node]], schema: Schema, defaultValue: Option[Any])(
      implicit path: Path
  ): GenericData.Fixed = {
    val bytes = readBytes(data, attributes, schema, defaultValue).asInstanceOf[Array[Byte]]

    if (bytes.length == schema.getFixedSize) new GenericData.Fixed(schema, bytes)
    else throw new WrongTypeException(schema, data, Some(s"incorrect size: ${bytes.length} instead of ${schema.getFixedSize}"))
  }

  private def read(data: NodeSeq, attributes: Option[Seq[Node]], schema: Schema, defaultValue: Option[Any])(implicit path: Path): Any =
    TextNode
      .toText(attributes)
      .map(parseString(_, schema, data))
      .getOrElse {
        data match {
          case TextNode(text)         => parseString(text, schema, data)
          case nodes if nodes.isEmpty => fallbackToDefault(defaultValue, schema)
          case _                      => throw new WrongTypeException(schema, data)
        }
      }

  private def parseString(str: String, schema: Schema, data: NodeSeq)(implicit path: Path): Any =
    if (schema.getType == STRING || schema.getType == ENUM) str
    else
      stringParsers.get(typeName(schema)).fold(throw new WrongTypeException(schema, data, Some("no string parser supplied"))) { parser =>
        try {
          parser(str)
        } catch {
          case NonFatal(e) => throw new WrongTypeException(schema, data, Option(e.getMessage))
        }
      }

  private def readNull(data: NodeSeq, attributes: Option[Seq[Node]], schema: Schema, defaultValue: Option[Any])(implicit path: Path): Null =
    if (data.isEmpty && attributes.isEmpty) fallbackToDefault(defaultValue, schema).asInstanceOf[Null]
    else throw new WrongTypeException(schema, data)

  private def fallbackToDefault(defaultValue: Option[Any], schema: Schema)(implicit path: Path): Any =
    defaultValue.fold(throw new MissingValueException(schema)) { extractDefaultValue(_, schema) }

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
      val extracted = list.asScala.map { extractDefaultValue(_, schema.getElementType) }
      new GenericData.Array(schema, extracted.asJava)

    case (RECORD, map: JMap[_, _]) =>
      val result = new GenericData.Record(schema)
      map.asScala.foreach {
        case (k, value) =>
          val key       = k.asInstanceOf[String]
          val extracted = extractDefaultValue(value, schema.getField(key).schema())
          result.put(key, extracted)
      }
      result

    case _ => throw new XmlParserException(s"Unsupported default value $defaultValue for type ${schema.getType}")
  }
}

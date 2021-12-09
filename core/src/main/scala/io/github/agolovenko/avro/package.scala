package io.github.agolovenko

import org.apache.avro.Schema.Type._
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.{JsonProperties, Schema}

import java.io.ByteArrayOutputStream
import java.lang.{Boolean => JBool, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Base64, List => JList, Map => JMap}
import scala.collection.compat._
import scala.jdk.CollectionConverters._

package object avro {
  def toBytes(record: GenericData.Record): Array[Byte] = {
    val writer = new GenericDatumWriter[GenericData.Record]()
    writer.setSchema(record.getSchema)

    val out     = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()

    out.toByteArray
  }

  def toRecord(bytes: Array[Byte], schema: Schema): GenericData.Record = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader  = new GenericDatumReader[GenericData.Record](schema)

    reader.read(null, decoder)
  }

  def typeName(schema: Schema): String =
    if (schema.getLogicalType != null) schema.getLogicalType.getName
    else
      schema.getType match {
        case UNION => schema.getTypes.asScala.map(typeName).mkString("[", "|", "]")
        case ENUM  => schema.getEnumSymbols.asScala.mkString("[", "|", "]")
        case _     => schema.getType.name()
      }

  def toBase64(bytes: Array[Byte]): String = new String(Base64.getEncoder.encode(bytes), StandardCharsets.UTF_8)

  private[avro] def fallbackToDefault(defaultValue: Option[Any], schema: Schema)(implicit path: Path): Any =
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

    case _ => throw new ParserException(s"Unsupported default value $defaultValue for type ${schema.getType}")
  }
}

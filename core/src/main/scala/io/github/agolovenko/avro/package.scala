package io.github.agolovenko

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.Base64
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
}
